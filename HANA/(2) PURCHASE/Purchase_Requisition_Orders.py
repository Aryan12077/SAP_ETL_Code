"""
SAP Z_2013_PR_STATUS — Purchase Requisition Status Extractor  v1.0
===================================================================
Replicates the Z_2013_PR_STATUS ABAP report by extracting
requisition-level data from EBAN as the primary anchor, joining
through EBKN → AUFK / SKAT (account-assignment dimension), MAKT
(material descriptions), EKPO → EKKO → LFA1 (PO/vendor dimension),
and EKBE (GR/IR history dimension).

Report columns reproduced
-------------------------
BANFN   PUR.REQ.NO          SAKTO  G/L ACCOUNT
BNFPO   ITEM                TXT50  G/L ACCOUNT DESC.
MATNR   MAT_CODE            MAKTX  MATERIAL DESC. (from MAKT)
TXZ01   DESCRIPTION         EBELN  PO/RFQ NO.
BADAT   REQ.DATE            BEDAT  PO DATE
LFDAT   DEL.DATE            LIFNR  VENDOR (CODE)
EKGRP   PUR.GROUP           NAME1  VENDOR NAME
MENGE   QUANTITY            WAERS  CURRENCY
MEINS   UOM                 NETWR  TOTAL PO VAL
FRGZU   REL.STATUS          LOEKZ  PO STATUS (EKPO)
FRGKZ   REL.IND             BUDAT  FST GRN.DT / FST INV.DT
EBAKZ   CLOSED              MENGE  TOTAL GRN QT / TOTAL INV QT
AFNAM   REQUISITIONER       DMBTR  TTL INV_VAL_BOOKED
BEDNR   TRACKING NO.        BWART  MOVEMENT TYPE
ERNAM   CREATED BY          VGABE  TRANSACTION TYPE
WERKS   PLANT
AUFNR   INT.ORDER
KTEXT   INT.ORDER DESC. (from AUFK)

Extraction Order (v1.0)
-----------------------
  Phase 0 — EBAN  : anchor (BADAT + WERKS filter) → _BANFN_SET, _MATNR_SET
  Phase 1 — EBKN  : account assignments (BANFN IN) → _AUFNR_SET, _SAKTO_SET
  Phase 2 — AUFK  : internal order master (AUFNR IN)
  Phase 3 — SKAT  : G/L account descriptions (SAKNR IN)
  Phase 4 — MAKT  : material descriptions (MATNR IN)
  Phase 5 — MCH1  : batch master, optional (MATNR IN)
  Phase 6 — EKPO  : PO items linked to PR (BANFN IN) → _EBELN_SET
  Phase 7 — EKKO  : PO headers (EBELN IN) → _LIFNR_SET
  Phase 8 — LFA1  : vendor master (LIFNR IN)
  Phase 9 — EKBE  : GR + IR history (EBELN IN)

Root Cause Notes vs. a naive EBAN-only query
---------------------------------------------
• EBAN gives one row per PR item but has NO PO reference, vendor,
  or GR/IR data — those require the EKPO → EKKO / EKBE join chain.
• EKPO links back to EBAN via BANFN + BNFPO.  One PR item can be
  split across multiple PO items; all are captured.
• EKBE contains one row per material-document line; GR totals and
  IR totals must be aggregated in the downstream SQL.
• EBKN is needed for the account-assignment columns (AUFNR, SAKTO).
  SKAT requires a chart-of-accounts key (KTOPL config variable).

Author  : [Your Name]
Version : 1.0.0
"""

from __future__ import annotations

import os
import sys

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_THIS_DIR, os.path.dirname(_THIS_DIR), os.path.dirname(os.path.dirname(_THIS_DIR))):
    if _p not in sys.path:
        sys.path.append(_p)

# connections.py must be imported BEFORE pyrfc / psycopg2 (.env + DLL paths)
from connections import (  # noqa: E402
    get_postgres_connection,
    get_postgres_schema,
    get_sap_connection,
)

import logging  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from datetime import datetime  # noqa: E402
from typing import Any, Dict, Generator, List, Optional, Set, Tuple  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2 import extras  # noqa: E402
from pyrfc import (  # noqa: E402
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)

SAP_SYSTEM: str = "HANA"


# ==================================================================
# CONFIGURATION — edit this section only
# ==================================================================

# Requisition date range applied to EBAN.BADAT (primary anchor filter).
# Format: YYYYMMDD
DATE_FROM: str = os.getenv("DATE_FROM", "20250101")
DATE_TO:   str = os.getenv("DATE_TO",   "20251231")

# Plants to include — leave [] for all plants.
# These are applied as EBAN.WERKS IN (...) on the anchor query.
# Example: ["HP01", "HP02"]
PLANTS: List[str] = os.getenv("PLANTS", "1101").split(",") if os.getenv("PLANTS") else []

# Set True to include PRs that are marked as Closed (EBAN.EBAKZ = 'X').
# Set False (default) to replicate the Z_2013_PR_STATUS default view
# which excludes closed PRs.
INCLUDE_CLOSED: bool = (
    os.getenv("INCLUDE_CLOSED", "false").lower() == "true"
)

# Set True to include PR items flagged for deletion (EBAN.LOEKZ = 'X').
INCLUDE_DELETED: bool = (
    os.getenv("INCLUDE_DELETED", "false").lower() == "true"
)

# Chart of accounts key — required for the SKAT G/L description lookup.
# Find yours in SAP via T-code OB13 or field T001-KTOPL for your company code.
# Example values: "INT", "CAIN", "CA10"
CHART_OF_ACCOUNTS: str = os.getenv("CHART_OF_ACCOUNTS", "INT")

# Language for MAKT / SKAT text lookups.
LANGUAGE: str = os.getenv("SAP_LANG", "E")

# Include MCH1 (batch master) pass?
# Set True only if batches are relevant to your PR reporting.
INCLUDE_BATCH_MASTER: bool = (
    os.getenv("INCLUDE_BATCH_MASTER", "false").lower() == "true"
)

# Staging table prefix → STG_SAP_PRSTATUS_<TABLE>
STAGING_PREFIX: str = os.getenv("PR_STAGING_PREFIX", "PRSTATUS")


# ==================================================================
# Module-level key sets — populated at runtime during extraction
# ==================================================================
_BANFN_SET: Set[str] = set()   # PR numbers       (EBAN anchor → EBKN, EKPO)
_MATNR_SET: Set[str] = set()   # Material numbers  (EBAN → MAKT, MCH1)
_AUFNR_SET: Set[str] = set()   # Internal orders   (EBKN → AUFK)
_SAKTO_SET: Set[str] = set()   # G/L accounts      (EBKN → SKAT)
_EBELN_SET: Set[str] = set()   # PO numbers        (EKPO → EKKO, EKBE)
_LIFNR_SET: Set[str] = set()   # Vendor numbers    (EKKO → LFA1)


# ==================================================================
# WHERE clause builders  (shared utilities)
# ==================================================================

def _format_options(where: str) -> List[Dict[str, str]]:
    """
    Convert a WHERE string into RFC OPTIONS rows (max 72 chars each).
    Words are never split across rows.
    """
    if not where:
        return []
    lines: List[Dict[str, str]] = []
    current = ""
    for word in where.split():
        if len(current) + len(word) + 1 > 72:
            lines.append({"TEXT": current.rstrip()})
            current = word + " "
        else:
            current += word + " "
    if current.strip():
        lines.append({"TEXT": current.rstrip()})
    return lines


def _build_in_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """
    Build RFC OPTIONS rows for a large IN filter.
    Values packed greedily per 72-char row; subsequent rows use 'OR'.
    'extra' is appended as 'AND <extra>' after the IN clause.
    Returns a '__NO_MATCH__' guard when values is empty.
    """
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.append({"TEXT": f"AND {extra}"})
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    first_clause = True
    i = 0

    while i < len(sorted_vals):
        prefix      = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
        max_content = 72 - len(prefix) - 1
        row_vals: List[str] = []
        row_len = 0

        while i < len(sorted_vals):
            quoted = f"'{sorted_vals[i]}'"
            need   = len(quoted) + (1 if row_vals else 0)
            if row_vals and row_len + need > max_content:
                break
            row_vals.append(quoted)
            row_len += need
            i += 1

        options.append({"TEXT": prefix + ",".join(row_vals) + ")"})
        first_clause = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})

    return options


# ------------------------------------------------------------------
# Per-table WHERE builders
# ------------------------------------------------------------------

def _build_eban_options() -> List[Dict[str, str]]:
    """
    EBAN anchor filter:
      • BADAT range  — primary date anchor (PR requisition date)
      • WERKS IN     — plant(s), if configured
      • LOEKZ        — exclude deleted items unless INCLUDE_DELETED
      • EBAKZ        — exclude closed PRs unless INCLUDE_CLOSED
    """
    where = f"BADAT >= '{DATE_FROM}' AND BADAT <= '{DATE_TO}'"

    if PLANTS:
        plant_list = ",".join(f"'{p}'" for p in PLANTS)
        where += f" AND WERKS IN ({plant_list})"

    if not INCLUDE_DELETED:
        # EBAN.LOEKZ = 'X' means item is flagged for deletion.
        # We filter for LOEKZ = '' (not deleted).
        where += " AND LOEKZ = ''"

    if not INCLUDE_CLOSED:
        # EBAN.EBAKZ = 'X' means PR item is closed (fully processed).
        where += " AND EBAKZ = ''"

    return _format_options(where)


def _build_ebkn_options() -> List[Dict[str, str]]:
    """
    EBKN filter: BANFN IN (_BANFN_SET).
    Account assignment details linked to PR items.
    """
    return _build_in_options("BANFN", _BANFN_SET)


def _build_aufk_options() -> List[Dict[str, str]]:
    """
    AUFK filter: AUFNR IN (_AUFNR_SET).
    Internal order master data for display in report.
    Returns '__NO_MATCH__' guard if no internal orders were found.
    """
    return _build_in_options("AUFNR", _AUFNR_SET)


def _build_skat_options() -> List[Dict[str, str]]:
    """
    SKAT filter: SAKNR IN (_SAKTO_SET) AND KTOPL = '...' AND SPRAS = '...'.
    SKAT primary key: KTOPL + SAKNR + SPRAS — all three must be provided.
    """
    extra = f"KTOPL = '{CHART_OF_ACCOUNTS}' AND SPRAS = '{LANGUAGE}'"
    return _build_in_options("SAKNR", _SAKTO_SET, extra=extra)


# def _build_makt_options() -> List[Dict[str, str]]:
#     """
#     MAKT filter: MATNR IN (_MATNR_SET) AND SPRAS = '...'.
#     """
#     return _build_in_options("MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'")


def _build_mch1_options() -> List[Dict[str, str]]:
    """
    MCH1 filter: MATNR IN (_MATNR_SET).
    MCH1 = multi-plant batch master. Used when batch-level data
    is needed in the PR status report.
    """
    return _build_in_options("MATNR", _MATNR_SET)


def _build_ekpo_pr_options() -> List[Dict[str, str]]:
    """
    EKPO filter: BANFN IN (_BANFN_SET).
    Retrieves all PO items that were created referencing any
    PR in scope.  The BANFN column on EKPO stores the originating
    PR number; this is the correct join key (not EBELN).
    Note: items with BANFN = '' were created without a PR reference
    and will not appear — which is the expected behaviour.
    """
    return _build_in_options("BANFN", _BANFN_SET)


def _build_ekko_options() -> List[Dict[str, str]]:
    """
    EKKO filter: EBELN IN (_EBELN_SET).
    _EBELN_SET is populated from EKPO.EBELN after the EKPO pass.
    """
    return _build_in_options("EBELN", _EBELN_SET)


def _build_lfa1_options() -> List[Dict[str, str]]:
    """
    LFA1 filter: LIFNR IN (_LIFNR_SET).
    _LIFNR_SET is populated from EKKO.LIFNR after the EKKO pass.
    """
    return _build_in_options("LIFNR", _LIFNR_SET)


def _build_ekbe_options() -> List[Dict[str, str]]:
    """
    EKBE filter: EBELN IN (_EBELN_SET).
    Retrieves all GR (VGABE=1 / BEWTP='E') and IR (VGABE=2 / BEWTP='Q')
    history lines for all POs in scope.
    The downstream SQL aggregates these to derive:
      • FST GRN DT  — MIN(BUDAT) WHERE VGABE = '1'
      • TOTAL GRN QT — SUM(MENGE) WHERE VGABE = '1' AND SHKZG = 'S'
      • FST INV DT  — MIN(BUDAT) WHERE VGABE = '2'
      • TOTAL INV QT — SUM(MENGE) WHERE VGABE = '2' AND SHKZG = 'S'
      • TTL INV_VAL_BOOKED — SUM(DMBTR) WHERE VGABE = '2' AND SHKZG = 'S'
    """
    return _build_in_options("EBELN", _EBELN_SET)


# ==================================================================
# STAGING TABLE DEFINITIONS  (extraction order matters)
# ==================================================================
#
# Execution sequence in main():
#   EBAN → EBKN → AUFK → SKAT → MAKT → [MCH1] → EKPO → EKKO → LFA1 → EKBE
#
# Each table's options_fn() is called AFTER all upstream key sets
# are populated, so the IN lists are always complete at call time.
# ==================================================================

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ==============================================================
    # PHASE 0 — Purchase Requisition Items (EBAN) — primary anchor
    #
    # EBAN is the root table for the Z_2013_PR_STATUS report.
    # Filtered by BADAT (PR requisition date) + WERKS (plant).
    # Populates _BANFN_SET (for EBKN, EKPO) and _MATNR_SET (for MAKT).
    #
    # Key field notes:
    #   BADAT  — PR date (requisition date); used as anchor date filter
    #   LFDAT  — Requested delivery date
    #   FRGZU  — Release status bitstring (e.g. "1111" = fully released)
    #   FRGKZ  — Release indicator flag ('X' = blocked, ' ' = released)
    #   EBAKZ  — Closed indicator ('X' = item fully processed / closed)
    #   LOEKZ  — Deletion flag ('X' = item deleted)
    #   BEDNR  — Requirement tracking number (shown as TRACKING NO.)
    #   AFNAM  — Name of requisitioner
    # ==============================================================
    {
        "table":       "EBAN",
        "description": "PR Items — primary anchor (BADAT + WERKS filter)",
        "columns": [
            "BANFN",   # Purchase Requisition Number    → _BANFN_SET
            "BNFPO",   # PR Item Number
            "BSART",   # PR Document Type
            "BSTYP",   # Purchasing Document Category
            "LOEKZ",   # Item Deletion Indicator
            "STATU",   # Processing Status
            "ESTKZ",   # Creation Indicator
            "FRGKZ",   # Release Indicator
            "FRGZU",   # Release Status
            "FRGST",   # Release Strategy
            "EKGRP",   # Purchasing Group
            "ERNAM",   # Created By
            "ERDAT",   # date of last change/Creation Date
            "AFNAM",   # Requisitioner / Requester Name
            "MATNR",   # Material Number                → _MATNR_SET
            "TXZ01",   # Short Description
            "MENGE",   # Requisition Quantity
            "MEINS",   # Unit of Measure
            "BADAT",   # Date of Purchase Requisition (requisition date)
            "LFDAT",   # Item Delivery Date
            "WERKS",   # Plant
            "LGORT",   # Storage Location
            "MATKL",   # Material Group
            "PSTYP",   # Item Category
            "KNTTP",   # Account Assignment Category
            "PREIS",   # Valuation Price
            "PEINH",   # Price Unit
            "WAERS",   # Currency
            "EBAKZ",   # PR Item Closed Indicator
            "BEDNR",   # Requirement Tracking Number
            "FIXKZ",   # Item Fixed Indicator / purchase requisition fixed
        ],
        "options_fn":  _build_eban_options,
        "primary_keys": ["BANFN", "BNFPO"],
        "key_out": {
            "BANFN": "_BANFN_SET",
            "MATNR": "_MATNR_SET",
        },
        "is_anchor": True,
    },

    # ==============================================================
    # PHASE 1 — PR Account Assignments (EBKN)
    #
    # EBKN is the account assignment table for purchase requisitions
    # (equivalent to EKKN for purchase orders).
    # Filtered by BANFN IN (_BANFN_SET).
    # Populates _AUFNR_SET (for AUFK) and _SAKTO_SET (for SKAT).
    #
    # ZEKKN = '01' is the primary/first account assignment.
    # ==============================================================
    {
        "table":       "EBKN",
        "description": "PR Account Assignments (BANFN IN filter)",
        "columns": [
            "BANFN",      # PR Number (FK)
            "BNFPO",      # PR Item Number (FK)
            "ZEBKN",      # Sequential Account Assignment Number
            "LOEKZ",      # Deletion Indicator
            "MENGE",      # Quantity for this assignment
            "VPROZ",      # Distribution Percentage
            "NETWR",      # Net Value for this assignment
            "SAKTO",      # G/L Account Number              → _SAKTO_SET
            "GSBER",      # Business Area
            "KOSTL",      # Cost Center
            "AUFNR",      # Internal Order Number            → _AUFNR_SET
            "PRCTR",      # Profit Center
            "PS_PSP_PNR", # WBS Element
            "NPLNR",      # Network Number
            "KOKRS",      # Controlling Area
            "WEMPF",      # Goods Recipient
            "ABLAD",      # Unloading Point
        ],
        "options_fn":  _build_ebkn_options,
        "primary_keys": ["BANFN", "BNFPO", "ZEKKN"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
            "SAKTO": "_SAKTO_SET",
        },
    },

    # ==============================================================
    # PHASE 2 — Internal Order Master (AUFK)
    #
    # Provides KTEXT (internal order description) for the INT.ORDER DESC.
    # column in the report.
    # Only extracted when _AUFNR_SET is non-empty (PRs with order
    # account assignment). The __NO_MATCH__ guard handles the empty case.
    # ==============================================================
    {
        "table":       "AUFK",
        "description": "Internal Order Master (AUFNR IN filter)",
        "columns": [
            "AUFNR",   # Internal Order Number
            "AUART",   # Order Type
            "KTEXT",   # Internal Order Short Description
            "KOSTL",   # Responsible Cost Center
            "GSBER",   # Business Area
            "BUKRS",   # Company Code
            "WERKS",   # Plant
            "OBJNR",   # Object Number (used for CO object linking)
            "ERDAT",   # Created On
            "ERNAM",   # Created By
        ],
        "options_fn":  _build_aufk_options,
        "primary_keys": ["AUFNR"],
    },

    # ==============================================================
    # PHASE 3 — G/L Account Descriptions (SKAT)
    #
    # Provides TXT50 (G/L account long description) for the
    # G/L ACCOUNT DESC. column in the report.
    #
    # SKAT primary key: KTOPL + SAKNR + SPRAS.
    # KTOPL must match your company's chart of accounts (config var).
    # Only extracted when _SAKTO_SET is non-empty.
    # ==============================================================
    {
        "table":       "SKAT",
        "description": "G/L Account Descriptions (SAKNR IN filter)",
        "columns": [
            "KTOPL",   # Chart of Accounts
            "SAKNR",   # G/L Account Number
            "SPRAS",   # Language Key
            "TXT20",   # G/L Account Short Text
            "TXT50",   # G/L Account Long Text (used in report)
        ],
        "options_fn":  _build_skat_options,
        "primary_keys": ["KTOPL", "SAKNR", "SPRAS"],
    },

    # ==============================================================
    # PHASE 4 — Material Descriptions (MAKT)
    #
    # Provides MAKTX (material short description) — the DESCRIPTION
    # column when TXZ01 is blank (non-material PRs have TXZ01 filled
    # directly; material-coded PRs carry the description in MAKT).
    # ==============================================================
    # {
    #     "table":       "MAKT",
    #     "description": "Material Descriptions (MATNR IN filter)",
    #     "columns": [
    #         "MATNR",   # Material Number
    #         "SPRAS",   # Language Key
    #         "MAKTX",   # Material Short Description
    #     ],
    #     "options_fn": _build_makt_options,
    #     "primary_keys": ["MATNR", "SPRAS"],
    # },

    # ==============================================================
    # PHASE 5 — Batch Master (MCH1)  [optional — see INCLUDE_BATCH_MASTER]
    #
    # MCH1 = multi-plant batch master (cross-plant batches).
    # MCHA = plant/storage-location batch master.
    # Only included when INCLUDE_BATCH_MASTER = True.
    # The CHARG field is the batch number used to link to PR item
    # batch references.
    # ==============================================================
    {
        "table":       "MCH1",
        "description": "Batch Master Cross-Plant (MATNR IN filter) [optional]",
        "columns": [
            "MATNR",   # Material Number
            "CHARG",   # Batch Number
            "ERSDA",   # Date Batch was Created
            "VFDAT",   # Shelf Life Expiration Date
            "MHDRZ",   # Minimum Remaining Shelf Life (days)
        ],
        "options_fn": _build_mch1_options,
        "primary_keys": ["MATNR", "CHARG"],
        "optional": True,   # Skipped when INCLUDE_BATCH_MASTER = False
    },

    # ==============================================================
    # PHASE 6 — Purchase Order Items linked to PRs (EKPO)
    #
    # EKPO.BANFN = EBAN.BANFN is the join key (PR number reference
    # stored on the PO item after conversion from PR → PO).
    # This is the ONLY correct join — do NOT join via EBELN+EBELP
    # without also checking BANFN+BNFPO.
    #
    # Populates _EBELN_SET for EKKO and EKBE downstream.
    #
    # LOEKZ on EKPO represents the PO item deletion flag (shown as
    # PO STATUS in the report: 'L' = blocked, ' ' = active).
    # ==============================================================
    {
        "table":       "EKPO",
        "description": "PO Items linked to PRs (BANFN IN filter)",
        "columns": [
            "EBELN",   # Purchase Order Number           → _EBELN_SET
            "EBELP",   # PO Item Number
            "LOEKZ",   # PO Item Deletion Indicator
            "STATU",   # PO Item Status
            "BANFN",   # Originating PR Number (FK → EBAN)
            "BNFPO",   # Originating PR Item (FK → EBAN)
            "MATNR",   # Material Number
            "TXZ01",   # PO Item Short Text
            "WERKS",   # Plant
            "LGORT",   # Storage Location
            "MATKL",   # Material Group
            "PSTYP",   # Item Category
            "KNTTP",   # Account Assignment Category
            "MENGE",   # Total PO Quantity
            "MEINS",   # PO Unit of Measure
            "LMEIN",   # Base (Stockkeeping) Unit
            "NETPR",   # Net Price
            "PEINH",   # Price Unit
            "NETWR",   # Net Order Value in PO Currency
            "BRTWR",   # Gross Order Value
            "ELIKZ",   # Delivery Completed Indicator
            "EREKZ",   # Final Invoice Indicator
            "WEPOS",   # Goods Receipt Indicator
            "REPOS",   # Invoice Receipt Indicator
            "WEBRE",   # GR-Based Invoice Verification
        ],
        "options_fn":  _build_ekpo_pr_options,
        "primary_keys": ["EBELN", "EBELP"],
        "key_out": {
            "EBELN": "_EBELN_SET",
        },
    },

    # ==============================================================
    # PHASE 7 — Purchase Order Headers (EKKO)
    #
    # Provides PO DATE (BEDAT), VENDOR (LIFNR), CURRENCY (WAERS).
    # Filtered by EBELN IN (_EBELN_SET) populated from EKPO.
    # Populates _LIFNR_SET for LFA1 vendor name lookup.
    # ==============================================================
    {
        "table":       "EKKO",
        "description": "PO Headers (EBELN IN filter)",
        "columns": [
            "EBELN",   # Purchase Order Number
            "BUKRS",   # Company Code
            "BSART",   # PO Document Type
            "BSTYP",   # PO Document Category
            "LOEKZ",   # Header Deletion Indicator
            "STATU",   # Header Status
            "AEDAT",   # Last Changed On
            "ERNAM",   # Created By
            "LIFNR",   # Vendor Account Number           → _LIFNR_SET
            "EKGRP",   # Purchasing Group
            "WAERS",   # Currency Key
            "BEDAT",   # PO Date (shown as PO DATE in report)
            "ZTERM",   # Payment Terms
            "INCO1",   # Incoterms Part 1
            "INCO2",   # Incoterms Part 2
        ],
        "options_fn":  _build_ekko_options,
        "primary_keys": ["EBELN"],
        "key_out": {
            "LIFNR": "_LIFNR_SET",
        },
    },

    # ==============================================================
    # PHASE 8 — Vendor Master (LFA1)
    #
    # Provides VENDOR NAME (NAME1) for each vendor linked to the POs.
    # Filtered by LIFNR IN (_LIFNR_SET) populated from EKKO.
    # ==============================================================
    {
        "table":       "LFA1",
        "description": "Vendor Master (LIFNR IN filter)",
        "columns": [
            "LIFNR",   # Vendor Account Number
            "NAME1",   # Vendor Name 1
            "NAME2",   # Vendor Name 2
            "ORT01",   # City
            "LAND1",   # Country Key
            "REGIO",   # Region
            "KTOKK",   # Vendor Account Group
            "SPERM",   # Purchasing Block
            "LOEVM",   # Central Deletion Flag
        ],
        "options_fn": _build_lfa1_options,
        "primary_keys": ["LIFNR"],
    },

    # ==============================================================
    # PHASE 9 — PO History / GR + IR (EKBE)
    #
    # EKBE is the source for all GR and invoice history against POs.
    # One EKBE row = one material-document line.
    # Filtered by EBELN IN (_EBELN_SET).
    #
    # Downstream SQL aggregations to reproduce Z_2013_PR_STATUS cols:
    #   FST GRN DT       → MIN(BUDAT) WHERE VGABE = '1' AND SHKZG = 'S'
    #   TOTAL GRN QT     → SUM(MENGE) WHERE VGABE = '1' AND SHKZG = 'S'
    #                       minus SUM(MENGE) WHERE VGABE = '1' AND SHKZG = 'H'
    #   FST INV DT       → MIN(BUDAT) WHERE VGABE = '2' AND SHKZG = 'S'
    #   TOTAL INV QT     → SUM(MENGE) WHERE VGABE = '2' AND SHKZG = 'S'
    #                       minus SUM(MENGE) WHERE VGABE = '2' AND SHKZG = 'H'
    #   TTL INV_VAL_BOK  → SUM(DMBTR) WHERE VGABE = '2' AND SHKZG = 'S'
    #                       minus SUM(DMBTR) WHERE VGABE = '2' AND SHKZG = 'H'
    #
    # VGABE values: '1'=GR, '2'=Invoice, '3'=Subsequent debit/credit
    # SHKZG values: 'S'=debit (positive posting), 'H'=credit (reversal)
    # BWART: 101=GR, 102=GR reversal, 103=GR to blocked stock, etc.
    # ==============================================================
    {
        "table":       "EKBE",
        "description": "PO History / GR + IR (EBELN IN filter)",
        "columns": [
            "EBELN",   # PO Number (FK)
            "EBELP",   # PO Item Number (FK)
            "ZEKKN",   # Account Assignment Number
            "VGABE",   # Transaction/Event Type ('1'=GR, '2'=IR)
            "GJAHR",   # Fiscal Year of Material Document
            "BELNR",   # Material / Accounting Document Number
            "BUZEI",   # Document Item
            "BEWTP",   # PO History Category
            "BWART",   # Movement Type (101=GR, 102=GR reversal)
            "BUDAT",   # Posting Date in the Company Code
            "BLDAT",   # Document Date
            "MENGE",   # Quantity Posted
            "BPMNG",   # Quantity in PO Price Unit
            "DMBTR",   # Amount in Local Currency
            "WRBTR",   # Amount in Document Currency
            "WAERS",   # Currency Key
            "SHKZG",   # Debit/Credit Indicator ('S'=debit, 'H'=credit)
            "ELIKZ",   # Delivery-Completed Indicator set by this posting
            "XBLNR",   # Reference Document Number
        ],
        "options_fn":  _build_ekbe_options,
        "primary_keys": ["EBELN", "EBELP", "ZEKKN", "VGABE", "GJAHR", "BELNR", "BUZEI"],
    },
]


# ==================================================================
# Extraction tuning  (override via .env)
# ==================================================================
@dataclass
class ExtractionConfig:
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "5000")))
    max_buffer_width: int   = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries:      int   = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s:    float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every:     int   = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema:   str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")


# ==================================================================
# Logging
# ==================================================================
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_pr_status_staging_{run_id}.log")

    log = logging.getLogger(f"sap_pr_status_staging.{run_id}")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    ))

    log.addHandler(fh)
    log.addHandler(ch)
    log.info("Log file: %s", log_file)
    return log


# ==================================================================
# SAP connection wrapper
# ==================================================================
class SAPConnection:
    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system = system
        self._log    = log
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP %s (attempt %d/3)…", self._system, attempt)
                self.conn = get_sap_connection(self._system)
                self._log.info("SAP connection established.")
                return
            except LogonError:
                self._log.exception("SAP logon failed — check credentials.")
                raise
            except CommunicationError as exc:
                self._log.warning("Attempt %d failed: %s", attempt, exc)
                if attempt == 3:
                    raise
                time.sleep(5)

    def read_table(
        self,
        table: str,
        fields: List[str],
        options: Optional[List[Dict]] = None,
        rowcount: int = 5000,
        rowskips: int = 0,
    ) -> Dict[str, Any]:
        try:
            return self.conn.call(
                "BBP_RFC_READ_TABLE",
                QUERY_TABLE=table,
                DELIMITER="|",
                FIELDS=[{"FIELDNAME": f} for f in fields],
                OPTIONS=options or [],
                ROWCOUNT=rowcount,
                ROWSKIPS=rowskips,
            )
        except (ABAPApplicationError, ABAPRuntimeError):
            self._log.exception("ABAP error reading %s.", table)
            raise
        except CommunicationError:
            self._log.warning("Communication error -- attempting reconnect...")
            self._connect()
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self._log.info("SAP connection closed.")
            self.conn = None


# ==================================================================
# Column metadata (field lengths from DD03L)
# ==================================================================
class ColumnMetadata:
    _FALLBACK_WIDTH = 100

    def __init__(self, sap: SAPConnection, log: logging.Logger) -> None:
        self._sap = sap
        self._log = log

    def get_widths(self, table: str, columns: List[str]) -> Dict[str, int]:
        self._log.info("[%s] Fetching column widths from DD03L...", table)
        try:
            result = self._sap.read_table(
                "DD03L",
                ["FIELDNAME", "LENG"],
                [{"TEXT": f"TABNAME = '{table.upper()}' AND AS4LOCAL = 'A'"}],
                rowcount=2000,
            )
        except Exception:
            self._log.warning(
                "[%s] DD03L lookup failed -- using fallback width %d.",
                table, self._FALLBACK_WIDTH,
            )
            return {c: self._FALLBACK_WIDTH for c in columns}

        col_set = set(columns)
        widths: Dict[str, int] = {}
        for row in result.get("DATA", []):
            parts = row["WA"].split("|")
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            if name not in col_set:
                continue
            try:
                widths[name] = max(1, int(parts[1].strip()))
            except ValueError:
                widths[name] = self._FALLBACK_WIDTH

        for col in columns:
            widths.setdefault(col, self._FALLBACK_WIDTH)

        self._log.debug("[%s] Widths: %s", table, widths)
        return widths


# ==================================================================
# Column chunker (respects RFC buffer width limit)
# ==================================================================
class ColumnChunker:
    def __init__(
        self,
        columns: List[str],
        widths: Dict[str, int],
        max_buffer: int,
        log: logging.Logger,
    ) -> None:
        self._columns    = columns
        self._widths     = widths
        self._max_buffer = max_buffer
        self._log        = log

    def chunks(self) -> List[List[str]]:
        result: List[List[str]] = []
        current: List[str] = []
        used = 0

        for col in self._columns:
            w = self._widths.get(col, 100) + 1   # +1 for the '|' delimiter
            if current and used + w > self._max_buffer:
                result.append(current)
                current = [col]
                used = w
            else:
                current.append(col)
                used += w

        if current:
            result.append(current)

        self._log.info(
            "Column chunking: %d columns -> %d chunk(s) (buffer limit %d).",
            len(self._columns), len(result), self._max_buffer,
        )
        for i, chunk in enumerate(result, 1):
            self._log.debug("  Chunk %d: %s", i, chunk)
        return result


# ==================================================================
# Data sanitiser
# ==================================================================
def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


# ==================================================================
# Key collector
# ==================================================================
def _collect_keys(
    rows: List[List[Any]],
    columns: List[str],
    key_out: Dict[str, str],
    log: logging.Logger,
) -> None:
    current_module = sys.modules[__name__]
    col_idx = {col: i for i, col in enumerate(columns)}

    for col, set_name in key_out.items():
        if col not in col_idx:
            log.warning("key_out column '%s' not in columns list -- skipped.", col)
            continue
        idx = col_idx[col]
        target_set: Set[str] = getattr(current_module, set_name)
        before = len(target_set)
        for row in rows:
            v = row[idx]
            if v:
                target_set.add(v.strip())
        added = len(target_set) - before
        if added:
            log.debug(
                "Collected %d new '%s' keys (total %d) -> %s",
                added, col, len(target_set), set_name,
            )


# ==================================================================
# SAP batch extractor
# ==================================================================
class BatchExtractor:
    def __init__(
        self,
        sap: SAPConnection,
        table: str,
        columns: List[str],
        chunks: List[List[str]],
        options: List[Dict[str, str]],
        cfg: ExtractionConfig,
        log: logging.Logger,
    ) -> None:
        self._sap     = sap
        self._table   = table
        self._columns = columns
        self._chunks  = chunks
        self._options = options
        self._cfg     = cfg
        self._log     = log

    def batches(self) -> Generator[List[List[Any]], None, None]:
        offset = 0
        total  = 0

        while True:
            batch = self._fetch_offset(offset)
            if not batch:
                self._log.info(
                    "[%s] No more rows. Total extracted: %d", self._table, total
                )
                break

            total  += len(batch)
            offset += self._cfg.batch_size
            self._log.info(
                "[%s] Fetched %d rows (running total: %d).",
                self._table, len(batch), total,
            )
            yield batch

    def _fetch_offset(self, offset: int) -> List[List[Any]]:
        merged: Dict[int, Dict[str, str]] = {}

        for chunk_idx, chunk in enumerate(self._chunks):
            for attempt in range(1, self._cfg.max_retries + 2):
                try:
                    result = self._sap.read_table(
                        self._table,
                        chunk,
                        options=self._options,
                        rowcount=self._cfg.batch_size,
                        rowskips=offset,
                    )
                    rows = result.get("DATA", [])

                    if chunk_idx == 0 and not rows:
                        return []

                    for idx, row in enumerate(rows):
                        values = row["WA"].split("|")
                        if idx not in merged:
                            merged[idx] = {}
                        for col, val in zip(chunk, values):
                            merged[idx][col] = _clean(val)
                    break

                except Exception as exc:
                    if attempt > self._cfg.max_retries:
                        self._log.error(
                            "[%s] Chunk %d exhausted retries at offset %d.",
                            self._table, chunk_idx + 1, offset,
                        )
                        raise
                    self._log.warning(
                        "[%s] Chunk %d attempt %d/%d failed: %s -- retrying in %.0fs...",
                        self._table, chunk_idx + 1, attempt,
                        self._cfg.max_retries, exc, self._cfg.retry_delay_s,
                    )
                    time.sleep(self._cfg.retry_delay_s)

        return [
            [merged[i].get(col, "") for col in self._columns]
            for i in sorted(merged)
        ]


# ==================================================================
# PostgreSQL loader
# ==================================================================
class PostgresLoader:
    def __init__(
        self,
        schema: str,
        prefix: str,
        log: logging.Logger,
        cfg: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn    = None
        self.cur     = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL…")
        self.conn = get_postgres_connection()
        self.cur  = self.conn.cursor()
        self._log.info("PostgreSQL connected.")

    def _table_exists(self, schema: str, table: str) -> bool:
        self.cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return self.cur.fetchone() is not None

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        table_name = f"stg_sap_{self._prefix.lower()}_{sap_table.lower()}"
        stg        = f"{self._schema}.{table_name}"
        safe_cols  = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        if self._table_exists(self._schema, table_name):
            self.cur.execute(f'TRUNCATE TABLE "{self._schema}"."{table_name}";')
            self.conn.commit()
            self._log.info("Staging table %s.%s truncated.", self._schema, table_name)
            return stg

        col_defs  = ",\n    ".join(f'"{c}" TEXT' for c in safe_cols)
        self.cur.execute(f"""
        CREATE TABLE {stg} (
            {col_defs},
            load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """)
        self.conn.commit()
        self._log.info("Staging table %s created.", stg)
        return stg

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_list  = ", ".join(f'"{c}"' for c in safe_cols)
        query     = f"INSERT INTO {stg_table} ({col_list}) VALUES %s"
        extras.execute_values(self.cur, query, rows)
        self.conn.commit()

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def close(self) -> None:
        if self.cur:  self.cur.close()
        if self.conn: self.conn.close()
        self._log.info("PostgreSQL connection closed.")


# ==================================================================
# Per-table extraction pipeline
# ==================================================================
def extract_table(
    table_def: Dict[str, Any],
    sap: SAPConnection,
    loader: PostgresLoader,
    cfg: ExtractionConfig,
    log: logging.Logger,
) -> Tuple[bool, int]:
    table   = table_def["table"].upper()
    columns = list(dict.fromkeys(table_def["columns"]))
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

    options_fn = table_def.get("options_fn")
    options    = options_fn() if callable(options_fn) else []

    log.info("=" * 60)
    log.info("Table : %s  (%s)", table, desc)
    log.info("Fields: %s", columns)
    if options:
        preview = " | ".join(r["TEXT"] for r in options[:4])
        suffix  = " | ..." if len(options) > 4 else ""
        log.info("OPTIONS (%d rows): %s%s", len(options), preview, suffix)

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table         = loader.create_staging_table(table, columns)
    extractor         = BatchExtractor(sap, table, columns, chunks, options, cfg, log)
    rows_loaded       = 0
    rows_since_commit = 0

    for batch in extractor.batches():
        if key_out:
            _collect_keys(batch, columns, key_out, log)

        loader.load_batch(stg_table, columns, batch)
        rows_loaded       += len(batch)
        rows_since_commit += len(batch)

        if rows_since_commit >= cfg.commit_every:
            loader.commit()
            log.info("[%s] Committed %d rows so far.", table, rows_loaded)
            rows_since_commit = 0

    loader.commit()

    elapsed = time.time() - t0
    rate    = rows_loaded / elapsed if elapsed > 0 else 0
    log.info(
        "[%s] Done -- %d rows in %.1fs (%.0f rows/s).",
        table, rows_loaded, elapsed, rate,
    )
    return True, rows_loaded


# ==================================================================
# Anchor guard
# ==================================================================
def _check_anchor_result(count: int, log: logging.Logger) -> None:
    """Warn when EBAN returns 0 rows — all downstream tables will be empty."""
    if count == 0:
        log.warning(
            "EBAN returned 0 rows. Verify: BADAT range %s–%s, "
            "plant(s) %s, INCLUDE_DELETED=%s, INCLUDE_CLOSED=%s in SAP.",
            DATE_FROM, DATE_TO,
            PLANTS if PLANTS else "ALL",
            INCLUDE_DELETED,
            INCLUDE_CLOSED,
        )


# ==================================================================
# Extraction diagnostics
# ==================================================================
def _log_extraction_diagnostics(
    loader:  PostgresLoader,
    results: Dict[str, Tuple[bool, int]],
    log:     logging.Logger,
) -> None:
    """
    Log detailed metrics to aid diagnosis of PR Status row count mismatches
    and to verify join chain completeness.
    """
    log.info("=" * 60)
    log.info("EXTRACTION DIAGNOSTICS")
    log.info("=" * 60)

    schema = loader._schema
    prefix = loader._prefix.lower()

    eban_count = results.get("EBAN", (False, 0))[1]
    ebkn_count = results.get("EBKN", (False, 0))[1]
    ekpo_count = results.get("EKPO", (False, 0))[1]
    ekbe_count = results.get("EKBE", (False, 0))[1]

    # -- Key set sizes -------------------------------------------------
    log.info("Key sets populated:")
    log.info("    _BANFN_SET  : %6d unique PR numbers",    len(_BANFN_SET))
    log.info("    _MATNR_SET  : %6d unique materials",     len(_MATNR_SET))
    log.info("    _AUFNR_SET  : %6d unique internal orders",len(_AUFNR_SET))
    log.info("    _SAKTO_SET  : %6d unique G/L accounts",  len(_SAKTO_SET))
    log.info("    _EBELN_SET  : %6d unique PO numbers",    len(_EBELN_SET))
    log.info("    _LIFNR_SET  : %6d unique vendors",       len(_LIFNR_SET))

    # -- Raw row counts ------------------------------------------------
    log.info("Staging row counts:")
    for tbl_name, (ok, count) in results.items():
        status = "OK     " if ok else "FAILED "
        log.info("    %-12s : %8d rows  [%s]", tbl_name, count, status)

    # -- PR → PO conversion rate ---------------------------------------
    if eban_count > 0:
        po_pct = 100.0 * len(_EBELN_SET) / len(_BANFN_SET) if _BANFN_SET else 0
        log.info(
            "PR → PO conversion: %d PR numbers → %d PO numbers "
            "(%.1f%% of PRs have at least one PO)",
            len(_BANFN_SET), len(_EBELN_SET), po_pct,
        )

    # -- Live queries against staging ----------------------------------
    try:
        # Unique BANFN+BNFPO in EBAN
        loader.cur.execute(
            f"SELECT COUNT(DISTINCT banfn || '|' || bnfpo) "
            f"FROM {schema}.stg_sap_{prefix}_eban"
        )
        unique_pr_lines: int = loader.cur.fetchone()[0]
        log.info("Unique BANFN+BNFPO in EBAN staging   : %d", unique_pr_lines)

        if ebkn_count > 0:
            loader.cur.execute(
                f"SELECT COUNT(DISTINCT banfn || '|' || bnfpo) "
                f"FROM {schema}.stg_sap_{prefix}_ebkn"
            )
            unique_ebkn_lines: int = loader.cur.fetchone()[0]
            pct = 100.0 * unique_ebkn_lines / unique_pr_lines if unique_pr_lines else 0
            log.info(
                "Unique BANFN+BNFPO in EBKN staging   : %d  "
                "(%.1f%% of PR items have account assignments)",
                unique_ebkn_lines, pct,
            )

        if ekpo_count > 0:
            loader.cur.execute(
                f"SELECT COUNT(DISTINCT banfn || '|' || bnfpo) "
                f"FROM {schema}.stg_sap_{prefix}_ekpo"
            )
            unique_ekpo_pr: int = loader.cur.fetchone()[0]
            pct = 100.0 * unique_ekpo_pr / unique_pr_lines if unique_pr_lines else 0
            log.info(
                "Unique BANFN+BNFPO in EKPO staging   : %d  "
                "(%.1f%% of PR items have been converted to PO items)",
                unique_ekpo_pr, pct,
            )

        if ekbe_count > 0:
            loader.cur.execute(
                f"SELECT COUNT(DISTINCT ebeln || '|' || ebelp) "
                f"FROM {schema}.stg_sap_{prefix}_ekbe"
            )
            unique_ekbe_lines: int = loader.cur.fetchone()[0]
            log.info(
                "Unique EBELN+EBELP in EKBE staging   : %d  "
                "(PO items with GR or IR history)",
                unique_ekbe_lines,
            )

            # GR vs IR split
            loader.cur.execute(
                f"SELECT vgabe, COUNT(*), SUM(CASE WHEN shkzg='S' THEN 1 ELSE 0 END) "
                f"FROM {schema}.stg_sap_{prefix}_ekbe "
                f"GROUP BY vgabe ORDER BY vgabe"
            )
            for row in loader.cur.fetchall():
                vgabe, total, debits = row
                label = {
                    "1": "GR (Goods Receipts)",
                    "2": "IR (Invoice Receipts)",
                    "3": "Subseq. Debit/Credit",
                }.get(str(vgabe), f"VGABE={vgabe}")
                log.info(
                    "    EKBE %-24s : %d rows (%d debit postings)",
                    label, total, debits,
                )

    except Exception as exc:
        log.warning("Diagnostic SQL query failed: %s", exc)

    log.info("=" * 60)


# ==================================================================
# Main orchestrator
# ==================================================================
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info(
        "SAP PR Status Staging Pipeline  v1.0  [run: %s]", run_id
    )
    log.info("Report: Z_2013_PR_STATUS (Purchase Requisition Status)")
    log.info("=" * 60)
    log.info("EBAN BADAT range     : %s → %s", DATE_FROM, DATE_TO)
    log.info("Plants               : %s", PLANTS if PLANTS else "ALL")
    log.info("Include closed PRs   : %s", INCLUDE_CLOSED)
    log.info("Include deleted items: %s", INCLUDE_DELETED)
    log.info("Chart of accounts    : %s", CHART_OF_ACCOUNTS)
    log.info("Language             : %s", LANGUAGE)
    log.info("Batch master (MCH1)  : %s", INCLUDE_BATCH_MASTER)
    log.info("Staging prefix       : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables defined       : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
    except Exception as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    sap    = None
    loader = None
    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, STAGING_PREFIX, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections -- aborting.")
        sys.exit(1)

    results:      Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()
    anchor_done   = False

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()

            # -- Skip optional tables when disabled --------------------
            if tbl_def.get("optional") and tbl_name == "MCH1":
                if not INCLUDE_BATCH_MASTER:
                    log.info("Skipping optional table %s (INCLUDE_BATCH_MASTER=False).", tbl_name)
                    results[tbl_name] = (True, 0)
                    continue

            try:
                ok, count = extract_table(tbl_def, sap, loader, cfg, log)
                results[tbl_name] = (ok, count)

                # -- Anchor guard after EBAN --------------------------
                if tbl_def.get("is_anchor") and not anchor_done:
                    _check_anchor_result(count, log)
                    anchor_done = True

            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)

    finally:
        # -- Diagnostics before closing connections -------------------
        try:
            if loader:
                _log_extraction_diagnostics(loader, results, log)
        except Exception:
            log.exception("Diagnostics logging failed.")

        if sap:
            sap.close()
        if loader:
            loader.close()

    # -- Final summary ------------------------------------------------
    total_elapsed = time.time() - overall_start
    succeeded     = [(t, n) for t, (ok, n) in results.items() if ok]
    failed        = [t for t, (ok, _) in results.items() if not ok]

    log.info("=" * 60)
    log.info("Run complete in %.1fs", total_elapsed)
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-12s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)
    log.info(
        "Downstream SQL hint:\n"
        "  EBAN\n"
        "  LEFT JOIN EBKN  ON banfn + bnfpo  (account assignment; zekkn='01' for primary)\n"
        "  LEFT JOIN AUFK  ON aufnr           (internal order description)\n"
        "  LEFT JOIN SKAT  ON sakto=saknr + ktopl + spras  (G/L account description)\n"
        "  LEFT JOIN MAKT  ON matnr + spras   (material description)\n"
        "  LEFT JOIN EKPO  ON banfn + bnfpo   (PO items converted from this PR)\n"
        "  LEFT JOIN EKKO  ON ebeln           (PO header: date, vendor, currency)\n"
        "  LEFT JOIN LFA1  ON lifnr           (vendor name)\n"
        "  LEFT JOIN EKBE  ON ebeln + ebelp   (aggregate GR qty / IR qty / INV value)\n"
        "  LEFT JOIN MCH1  ON matnr           (batch master, if enabled)\n"
        "\n"
        "  Key EKBE aggregations for report columns:\n"
        "    TOTAL GRN QT  : SUM(menge) FILTER (WHERE vgabe='1' AND shkzg='S')\n"
        "                  - SUM(menge) FILTER (WHERE vgabe='1' AND shkzg='H')\n"
        "    TOTAL INV QT  : SUM(menge) FILTER (WHERE vgabe='2' AND shkzg='S')\n"
        "                  - SUM(menge) FILTER (WHERE vgabe='2' AND shkzg='H')\n"
        "    TTL INV VALUE : SUM(dmbtr) FILTER (WHERE vgabe='2' AND shkzg='S')\n"
        "                  - SUM(dmbtr) FILTER (WHERE vgabe='2' AND shkzg='H')\n"
        "    FST GRN DT    : MIN(budat) FILTER (WHERE vgabe='1' AND shkzg='S')\n"
        "    FST INV DT    : MIN(budat) FILTER (WHERE vgabe='2' AND shkzg='S')"
    )
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)