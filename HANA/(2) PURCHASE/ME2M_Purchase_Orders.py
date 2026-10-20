"""
SAP ME2M — Purchase Orders by Material Staging Extractor  v2.0
===============================================================
Replicates SAP ECC ME2M (Purchase Orders by Material) behavior by
extracting schedule-line-level data from EKET in addition to the
standard PO header/item tables.

v2.0 Changes vs v1.0
---------------------
1. EKET added as new extraction pass (schedule lines — the root cause
   of the row count mismatch: 657 vs 3000+).
2. EKES added for vendor confirmations.
3. EKET pre-scan phase added: discovers old POs with active schedule
   lines whose EKKO.BEDAT falls outside the configured date range but
   whose EKET.EINDT falls inside — these were completely invisible
   to the v1.0 extractor.
4. EKKO supplement pass validates and appends pre-scan EBELNs.
5. Extraction order changed: EKKO → [prescan+supplement] → EKPO →
   EKET → EKBE → EKES → EKKN → MAKT → LFA1.
6. Detailed diagnostic logging added after extraction.
7. New config variables: INCLUDE_OPEN_SCHEDULES, EINDT_FROM, EINDT_TO.

Root Cause Analysis
-------------------
v1.0 anchor:  EKKO.BEDAT (header document date)
SAP ME2M:     item-driven + schedule-line-driven

Problems with EKKO.BEDAT-only anchor:
  (a) One EKPO item can have N EKET schedule lines.
      v1.0 SQL produced 1 row per item; ME2M produces 1 row per schedule line.
      A PO with 5 items each having 3 schedule lines gives 15 ME2M rows but
      only 5 rows from v1.0.
  (b) Old POs (created before DATE_FROM) can have EKET.EINDT within the
      reporting window.  These were entirely excluded by EKKO.BEDAT filter.
  (c) Scheduling agreements (BSTYP='L') are schedule-line-centric by design.

Extraction Order (v2.0)
-----------------------
  Phase 0 — EKKO  : header anchor (BEDAT + BUKRS filter) → _EBELN_SET, _LIFNR_SET
  Phase 1 — PRESCAN: EKET pre-scan by EINDT → adds old-PO EBELNs to _EBELN_SET
  Phase 2 — EKKO supplement: validates prescan EBELNs via BUKRS, appends staging
  Pass  3 — EKPO  : items (EBELN IN) → _MATNR_SET
  Pass  4 — EKET  : schedule lines (EBELN IN)
  Pass  5 — EKBE  : PO history / GR / IR (EBELN IN)
  Pass  6 — EKES  : vendor confirmations (EBELN IN)
  Pass  7 — EKKN  : account assignments (EBELN IN)
  Pass  8 — MAKT  : material descriptions (MATNR IN)
  Pass  9 — LFA1  : vendor master (LIFNR IN)

Author  : [Your Name]
Version : 2.0.0
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

# Document date range applied to EKKO.BEDAT (initial anchor filter)
DATE_FROM: str = os.getenv("DATE_FROM", "20260801")
DATE_TO:   str = os.getenv("DATE_TO",   "20260831")

# Delivery date range applied to EKET.EINDT in the pre-scan.
# Defaults to same as DATE_FROM/DATE_TO. Widen if needed to catch
# old POs with schedule lines that fall in a different window.
EINDT_FROM: str = os.getenv("EINDT_FROM", DATE_FROM)
EINDT_TO:   str = os.getenv("EINDT_TO",   DATE_TO)

# Set True to run the EKET pre-scan phase that discovers old POs
# with active schedule lines not caught by EKKO.BEDAT filter.
# Set False in environments where EKET is very large to skip the
# pre-scan and rely solely on EKKO.BEDAT for PO selection.
INCLUDE_OPEN_SCHEDULES: bool = (
    os.getenv("INCLUDE_OPEN_SCHEDULES", "true").lower() == "true"
)

# Company codes — applied to EKKO.BUKRS
COMPANY_CODES: List[str] = [
    c.strip() for c in os.getenv("COMPANY_CODES", "1100").split(",") if c.strip()
]

# Purchasing document types — leave [] for all types
DOCUMENT_TYPES: List[str] = []

# Purchasing document categories — leave [] for all
# "F" = standard PO, "K" = contract, "L" = scheduling agreement
DOC_CATEGORIES: List[str] = []

# Plants to include — leave [] for all plants
PLANTS: List[str] = [
    p.strip() for p in os.getenv("PLANTS", "").split(",") if p.strip()
]

# Language for MAKT text lookupc
LANGUAGE: str = "E"

# Staging table prefix → STG_SAP_ME2M_<TABLE>
STAGING_PREFIX: str = "ME2M"


# ==================================================================
# Module-level key sets — populated at runtime during extraction
# ==================================================================
_EBELN_SET: Set[str] = set()   # PO numbers      (EKKO + prescan → all downstream)
_MATNR_SET: Set[str] = set()   # Material numbers (EKPO → MAKT)
_LIFNR_SET: Set[str] = set()   # Vendor numbers   (EKKO → LFA1)


# ==================================================================
# WHERE clause builders
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
    Extra appended as 'AND ...' row.
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

def _build_ekko_options() -> List[Dict[str, str]]:
    """
    EKKO anchor filter: BEDAT range + BUKRS + optional BSART/BSTYP.
    Excludes header-deleted POs (LOEKZ = '').
    """
    where = f"BEDAT >= '{DATE_FROM}' AND BEDAT <= '{DATE_TO}'"
    if COMPANY_CODES:
        codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
        where += f" AND BUKRS IN ({codes})"
    where += " AND LOEKZ = ''"
    if DOCUMENT_TYPES:
        types = ",".join(f"'{t}'" for t in DOCUMENT_TYPES)
        where += f" AND BSART IN ({types})"
    if DOC_CATEGORIES:
        cats = ",".join(f"'{c}'" for c in DOC_CATEGORIES)
        where += f" AND BSTYP IN ({cats})"
    return _format_options(where)


def _build_ekpo_options() -> List[Dict[str, str]]:
    """
    EKPO filter: EBELN IN (_EBELN_SET) + optional WERKS.
    _EBELN_SET is fully populated (EKKO anchor + prescan supplement)
    before this builder is called.
    """
    extra = ""
    if PLANTS:
        plant_list = ",".join(f"'{p}'" for p in PLANTS)
        extra = f"WERKS IN ({plant_list})"
    return _build_in_options("EBELN", _EBELN_SET, extra=extra)


def _build_eket_options() -> List[Dict[str, str]]:
    """
    EKET full extraction: EBELN IN (_EBELN_SET).
    Retrieves ALL schedule lines for every PO in scope, including
    future delivery dates and partially-delivered lines.
    """
    return _build_in_options("EBELN", _EBELN_SET)


def _build_ekbe_options() -> List[Dict[str, str]]:
    """
    EKBE filter: EBELN IN (_EBELN_SET).
    All history categories (GR, IR etc.) in one pass — the SQL query
    differentiates by BEWTP/VGABE/BWART.
    """
    return _build_in_options("EBELN", _EBELN_SET)


def _build_ekes_options() -> List[Dict[str, str]]:
    """
    EKES filter: EBELN IN (_EBELN_SET).
    Vendor confirmation records tied to PO items.
    """
    return _build_in_options("EBELN", _EBELN_SET)


def _build_ekkn_options() -> List[Dict[str, str]]:
    """
    EKKN filter: EBELN IN (_EBELN_SET).
    All account assignment lines extracted; SQL joins on ZEKKN='01'
    for the primary assignment.
    """
    return _build_in_options("EBELN", _EBELN_SET)


# ==================================================================
# STAGING TABLE DEFINITIONS  (extraction order matters)
# ==================================================================
#
# Execution sequence in main():
#   EKKO  → [EKET prescan + EKKO supplement] → EKPO → EKET → EKBE
#         → EKES → EKKN → MAKT → LFA1
#
# The prescan is orchestrated by main() between EKKO and EKPO;
# it is NOT a regular STAGING_TABLE_DEFS entry.
# ==================================================================

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ==============================================================
    # PASS 0 — Purchase Order Header (EKKO) — initial anchor
    # Filtered by BEDAT + BUKRS.  Collects _EBELN_SET + _LIFNR_SET.
    # After this pass, main() triggers the EKET pre-scan which may
    # add more EBELNs (old POs) before EKPO is processed.
    # ==============================================================
    {
        "table":       "EKKO",
        "description": "PO Header — initial anchor (BEDAT + BUKRS filter)",
        "columns": [
            "EBELN",   # PO Number                    → _EBELN_SET
            "BUKRS",   # Company Code
            "BSART",   # Purchasing Document Type
            "BSTYP",   # Purchasing Document Category
            "LOEKZ",   # Header Deletion Indicator
            "STATU",   # Status
            "AEDAT",   # Date of Last Change
            "ERNAM",   # Created By
            "LIFNR",   # Vendor Account Number        → _LIFNR_SET
            "EKGRP",   # Purchasing Group
            "WAERS",   # Currency Key
            "BEDAT",   # Purchasing Document Date
            "FRGGR",   # Release Group
            "FRGSX",   # Release Strategy
            "FRGKE",   # Release Indicator
            "FRGZU",   # Release Status
            "FRGRL",   # Release Not Yet Effected
            "ZTERM",   # Payment Terms
            "INCO1",   # Incoterms Part 1
            "INCO2",   # Incoterms Part 2
            "KONNR",   # Principal Purchase Agreement Number
        ],
        "options_fn":  _build_ekko_options,
        "primary_keys": ["EBELN"],
        "key_out": {
            "EBELN": "_EBELN_SET",
            "LIFNR": "_LIFNR_SET",
        },
        "is_anchor": True,
    },

    # ==============================================================
    # PASS 1 — Purchase Order Items (EKPO)
    # Filtered by EBELN IN (_EBELN_SET) — now includes EBELNs from
    # the EKET pre-scan for old POs with active schedule lines.
    # Collects _MATNR_SET.
    # ==============================================================
    {
        "table":       "EKPO",
        "description": "PO Items (EBELN IN — full set after prescan)",
        "columns": [
            "EBELN",   # PO Number (FK)
            "EBELP",   # Item Number
            "LOEKZ",   # Item Deletion Indicator
            "STATU",   # Item Status
            "MATNR",   # Material Number              → _MATNR_SET
            "TXZ01",   # Item Short Text
            "WERKS",   # Plant
            "LGORT",   # Storage Location
            "MATKL",   # Material Group
            "PSTYP",   # Item Category
            "KNTTP",   # Account Assignment Category
            "MENGE",   # Order Quantity
            "MEINS",   # Order Unit of Measure
            "LMEIN",   # Base (Stockkeeping) Unit
            "UMREZ",   # Numerator: Order UoM → Base UoM
            "UMREN",   # Denominator: Order UoM → Base UoM
            "NETPR",   # Net Price
            "PEINH",   # Price Unit
            "NETWR",   # Net Order Value in PO Currency
            "BRTWR",   # Gross Order Value
            "KTMNG",   # Target Quantity (contracts/SAs)
            "ELIKZ",   # Delivery Completed Indicator
            "EREKZ",   # Final Invoice Indicator
            "WEPOS",   # Goods Receipt Indicator
            "REPOS",   # Invoice Receipt Indicator
            "WEBRE",   # GR-Based Invoice Verification
            "BANFN",   # Purchase Requisition Number
            "BNFPO",   # PR Item Number
            "BWTAR",   # Valuation Type
            "INFNR",   # Info Record Number
            "KONNR",   # Reference Contract Number
            "KTPNR",   # Reference Contract Item
        ],
        "options_fn":  _build_ekpo_options,
        "primary_keys": ["EBELN", "EBELP"],
        "key_out": {
            "MATNR": "_MATNR_SET",
        },
    },

    # ==============================================================
    # PASS 2 — Delivery Schedule Lines (EKET)  ← NEW in v2.0
    #
    # This is the table that was missing and caused the 657 vs 3000+
    # row count discrepancy.
    #
    # SAP ME2M generates one output row per EKET schedule line, NOT
    # per EKPO item.  A PO item with 5 schedule lines produces 5 rows
    # in ME2M but only 1 row in the v1.0 SQL.
    #
    # Key EKET fields for ME2M quantity derivations:
    #   MENGE  — scheduled quantity for this delivery line
    #   WEMNG  — GR quantity posted against this schedule line
    #   GLMNG  — confirmed (settled) GR quantity
    #   DABMG  — open/outstanding quantity to be delivered
    #   AMENG  — quantity already released (scheduling agreement)
    #   EINDT  — requested delivery date (drives the schedule view)
    #
    # INVALID fields (DO NOT add):
    #   ELIKZ  — not a standard EKET field (it is on EKPO)
    #   DATES  — not a field name
    # ==============================================================
    {
        "table":       "EKET",
        "description": "PO Delivery Schedule Lines (EBELN IN filter)",
        "columns": [
            "EBELN",   # PO Number (FK)
            "EBELP",   # Item Number (FK)
            "ETENR",   # Schedule Line Number         → ME2M "Schedule Line"
            "EINDT",   # Delivery Date (requested)    → ME2M row driver
            "SLFDT",   # Statistics-relevant delivery date
            "MENGE",   # Schedule Line Quantity
            "WEMNG",   # GR Quantity (this schedule line)
            "GLMNG",   # Confirmed/settled GR quantity
            "DABMG",   # Open quantity to be delivered (MENGE − WEMNG)
            "AMENG",   # Released quantity (SA release orders)
            "FIXKZ",   # Schedule Line Fixed Indicator
            "ESTKZ",   # GR Created Indicator
            "BEDAT",   # PO Header Date (denormalized on schedule line)
            "BANFN",   # Purchase Requisition Number
            "BNFPO",   # PR Item Number
            "ABART",   # Release Type (scheduling agreements)
            "ALTDT",   # Alternative Delivery Date (from vendor)
            "MBDAT",   # Material Availability Date
            "LDDAT",   # Loading Date
            "TDDAT",   # Transportation Planning Date
            "WADAT",   # Goods Issue Date (STO)
            "ELDAT",   # Latest Possible GR Date
        ],
        "options_fn":  _build_eket_options,
        "primary_keys": ["EBELN", "EBELP", "ETENR"],
    },

    # ==============================================================
    # PASS 3 — PO History / Account History (EKBE)
    #
    # Used to derive item-level GR/IR totals for ME2M columns:
    #   Still to be delivered  — EKPO.MENGE − Σ GR qty
    #   Still to be invoiced   — EKPO.MENGE − Σ IR qty
    #   Qty Released to Date   — Σ GR qty
    #
    # BEWTP values: 'E'=GR, 'Q'=Invoice, 'R'=Subsequent debit/credit
    # SHKZG values: 'S'=debit (positive), 'H'=credit (reversal/negative)
    # ==============================================================
    {
        "table":       "EKBE",
        "description": "PO History / GR+IR (EBELN IN filter)",
        "columns": [
            "EBELN",   # PO Number (FK)
            "EBELP",   # Item Number (FK)
            "ZEKKN",   # Account Assignment Number
            "VGABE",   # Transaction/Event Type ('E'=GR, 'Q'=IR)
            "GJAHR",   # Fiscal Year of Material Document
            "BELNR",   # Material/Accounting Document Number
            "BUZEI",   # Document Item
            "BEWTP",   # PO History Category
            "BWART",   # Movement Type (101=GR, 102=GR reversal)
            "BUDAT",   # Posting Date
            "MENGE",   # Quantity posted
            "BPMNG",   # Quantity in PO Price Unit
            "DMBTR",   # Amount in Local Currency
            "WRBTR",   # Amount in Document Currency
            "WAERS",   # Currency Key
            "SHKZG",   # Debit/Credit Indicator
            "ELIKZ",   # Delivery-Completed set by this posting
            "XBLNR",   # Reference Document Number
            "BLDAT",   # Document Date
        ],
        "options_fn":  _build_ekbe_options,
        "primary_keys": ["EBELN", "EBELP", "ZEKKN", "VGABE", "GJAHR", "BELNR", "BUZEI"],
    },

    # ==============================================================
    # PASS 4 — Vendor Confirmations (EKES)  ← NEW in v2.0
    #
    # Captures vendor order acknowledgements and delivery confirmations.
    # ME2M uses these to show confirmed delivery dates and quantities.
    #
    # EBTYP (Confirmation Category) common values:
    #   'AB' = Order Acknowledgement
    #   'LA' = Loading Confirmation
    #   'WE' = GR Confirmation (some systems)
    # ==============================================================
    {
        "table":       "EKES",
        "description": "Vendor Confirmations per PO Item (EBELN IN filter)",
        "columns": [
            "EBELN",   # PO Number (FK)
            "EBELP",   # Item Number (FK)
            "ETENS",   # Confirmation Counter
            "EINDT",   # Confirmed Delivery Date
            "MENGE",   # Confirmed Quantity
            #"WEMNG",   # GR Quantity against this confirmation
            "EBTYP",   # Confirmation Category ('AB'=acknowledgement, 'LA'=loading)
            "KZDIS",   # MRP Relevance Indicator
            "SGT_SCAT", #Stock Segment
        ],
        "options_fn":  _build_ekes_options,
        "primary_keys": ["EBELN", "EBELP", "ETENS"],
    },

    # ==============================================================
    # PASS 5 — Account Assignments (EKKN)
    # ZEKKN='01' is the primary assignment.
    # ==============================================================
    {
        "table":       "EKKN",
        "description": "PO Account Assignments (EBELN IN filter)",
        "columns": [
            "EBELN",      # PO Number (FK)
            "EBELP",      # Item Number (FK)
            "ZEKKN",      # Sequential Account Assignment Number
            "LOEKZ",      # Deletion Indicator
            "MENGE",      # Quantity for this assignment
            "VPROZ",      # Distribution Percentage
            "NETWR",      # Net Value for this assignment
            "SAKTO",      # G/L Account Number
            "GSBER",      # Business Area
            "KOSTL",      # Cost Center
            "AUFNR",      # Internal Order Number
            "PRCTR",      # Profit Center
            "PS_PSP_PNR", # WBS Element
            "NPLNR",      # Network Number
            "KOKRS",      # Controlling Area
            "WEMPF",      # Goods Recipient
            "ABLAD",      # Unloading Point
        ],
        "options_fn":  _build_ekkn_options,
        "primary_keys": ["EBELN", "EBELP", "ZEKKN"],
    },

    # ==============================================================
    # PASS 6 — Material Descriptions (MAKT, English only)
    # ==============================================================
    {
        "table":       "MAKT",
        "description": "Material Descriptions (MATNR IN filter, English)",
        "columns": [
            "MATNR",   # Material Number
            "MAKTX",   # Material Short Description
        ],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR"],
    },

    # ==============================================================
    # PASS 7 — Vendor Master (LFA1)
    # _LIFNR_SET populated from EKKO anchor + supplement.
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
        "options_fn": lambda: _build_in_options("LIFNR", _LIFNR_SET),
        "primary_keys": ["LIFNR"],
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
    log_file = os.path.join(log_dir, f"sap_me2m_staging_{run_id}.log")

    log = logging.getLogger(f"sap_me2m_staging.{run_id}")
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
            w = self._widths.get(col, 100) + 1
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
                self._log.info("[%s] No more rows. Total extracted: %d", self._table, total)
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

        col_defs = ",\n    ".join(f'"{c}" TEXT' for c in safe_cols)
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
# EKET pre-scan — discovers old POs with active schedule lines
# ==================================================================
def _prescan_eket_for_open_schedules(
    sap:    SAPConnection,
    loader: PostgresLoader,
    cfg:    ExtractionConfig,
    log:    logging.Logger,
) -> int:
    """
    Pre-scan EKET by EINDT to discover POs created before DATE_FROM
    that still have active schedule lines falling within [EINDT_FROM,
    EINDT_TO].  These are completely invisible to the EKKO.BEDAT
    anchor and were the primary cause of the 657 vs 3000+ row gap.

    Workflow
    --------
    1. Extract EBELN keys from EKET WHERE EINDT BETWEEN EINDT_FROM AND EINDT_TO.
    2. Identify EBELNs not already in _EBELN_SET (new, old-PO candidates).
    3. Validate candidates via a targeted EKKO read (BUKRS + LOEKZ filter)
       to avoid pulling POs from other company codes into scope.
    4. Append validated EKKO rows to the EKKO staging table (created in
       the main anchor pass — this function only appends, never recreates).
    5. Update _EBELN_SET and _LIFNR_SET with confirmed keys.

    Returns the number of newly confirmed EBELNs added to _EBELN_SET.
    """
    global _EBELN_SET, _LIFNR_SET

    if not INCLUDE_OPEN_SCHEDULES:
        log.info("[PRESCAN] INCLUDE_OPEN_SCHEDULES=False — skipping EKET pre-scan.")
        return 0

    log.info("=" * 60)
    log.info("[PRESCAN] EKET pre-scan: finding old POs with active schedule lines.")
    log.info("[PRESCAN] EINDT window: %s → %s", EINDT_FROM, EINDT_TO)
    log.info("[PRESCAN] _EBELN_SET before prescan: %d POs (from EKKO anchor).", len(_EBELN_SET))

    # ------------------------------------------------------------------
    # Step 1: Scan EKET by EINDT — collect candidate EBELN keys only
    # ------------------------------------------------------------------
    prescan_cols    = ["EBELN", "EBELP", "ETENR", "EINDT", "MENGE", "WEMNG"]
    prescan_options = _format_options(
        f"EINDT >= '{EINDT_FROM}' AND EINDT <= '{EINDT_TO}'"
    )

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths("EKET", prescan_cols)
    chunker = ColumnChunker(prescan_cols, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    extractor = BatchExtractor(
        sap, "EKET", prescan_cols, chunks, prescan_options, cfg, log
    )

    candidate_ebelns: Set[str] = set()
    prescan_rows = 0
    for batch in extractor.batches():
        prescan_rows += len(batch)
        ebeln_idx = prescan_cols.index("EBELN")
        for row in batch:
            v = row[ebeln_idx]
            if v:
                candidate_ebelns.add(v)

    new_ebelns = candidate_ebelns - _EBELN_SET

    log.info(
        "[PRESCAN] EKET rows scanned  : %d",  prescan_rows,
    )
    log.info(
        "[PRESCAN] Candidate EBELNs   : %d total | %d already known | %d new",
        len(candidate_ebelns),
        len(candidate_ebelns) - len(new_ebelns),
        len(new_ebelns),
    )

    if not new_ebelns:
        log.info("[PRESCAN] No new EBELNs — all schedule-line POs already captured by EKKO anchor.")
        return 0

    # ------------------------------------------------------------------
    # Step 2: Validate new EBELNs against EKKO with BUKRS filter
    # ------------------------------------------------------------------
    log.info(
        "[PRESCAN] Validating %d candidate EBELNs via EKKO (BUKRS=%s)...",
        len(new_ebelns), COMPANY_CODES,
    )

    ekko_def  = next(t for t in STAGING_TABLE_DEFS if t["table"] == "EKKO")
    ekko_cols = list(dict.fromkeys(ekko_def["columns"]))
    col_idx   = {col: i for i, col in enumerate(ekko_cols)}

    supp_extra = "LOEKZ = ''"
    if COMPANY_CODES:
        codes      = ",".join(f"'{c}'" for c in COMPANY_CODES)
        supp_extra = f"BUKRS IN ({codes}) AND LOEKZ = ''"
    supp_options = _build_in_options(
        "EBELN", new_ebelns,
        extra=supp_extra,
    )

    ekko_widths  = meta.get_widths("EKKO", ekko_cols)
    ekko_chunker = ColumnChunker(ekko_cols, ekko_widths, cfg.max_buffer_width, log)
    ekko_chunks  = ekko_chunker.chunks()

    ekko_extractor = BatchExtractor(
        sap, "EKKO", ekko_cols, ekko_chunks, supp_options, cfg, log
    )

    # EKKO staging table already exists (created by main anchor pass)
    ekko_stg  = (
        f"{loader._schema}.stg_sap_{loader._prefix.lower()}_ekko"
    )
    confirmed = 0

    for batch in ekko_extractor.batches():
        for row in batch:
            ebeln = row[col_idx["EBELN"]]
            lifnr = row[col_idx["LIFNR"]]
            if ebeln:
                _EBELN_SET.add(ebeln)
                confirmed += 1
            if lifnr:
                _LIFNR_SET.add(lifnr)
        # Append supplement rows to the existing EKKO staging table
        loader.load_batch(ekko_stg, ekko_cols, batch)

    loader.commit()

    log.info(
        "[PRESCAN] Supplement complete: %d new EBELNs confirmed and appended to EKKO staging.",
        confirmed,
    )
    log.info(
        "[PRESCAN] _EBELN_SET after prescan: %d POs | _LIFNR_SET: %d vendors.",
        len(_EBELN_SET), len(_LIFNR_SET),
    )
    return confirmed


# ==================================================================
# Anchor guard
# ==================================================================
def _check_anchor_result(count: int, log: logging.Logger) -> None:
    """Warn when EKKO returns 0 rows — all downstream tables will be empty."""
    if count == 0:
        log.warning(
            "EKKO returned 0 rows. Verify: BEDAT range %s–%s, "
            "company code(s) %s, document type(s) %s in SAP.",
            DATE_FROM, DATE_TO,
            COMPANY_CODES,
            DOCUMENT_TYPES if DOCUMENT_TYPES else "ALL",
        )


# ==================================================================
# Extraction diagnostics  (Step 10)
# ==================================================================
def _log_extraction_diagnostics(
    loader:  PostgresLoader,
    results: Dict[str, Tuple[bool, int]],
    log:     logging.Logger,
) -> None:
    """
    Log detailed metrics to aid diagnosis of ME2M row count mismatches.
    Queries the PostgreSQL staging tables for exact counts and ratios.
    """
    log.info("=" * 60)
    log.info("EXTRACTION DIAGNOSTICS")
    log.info("=" * 60)

    schema = loader._schema
    prefix = loader._prefix.lower()

    ekpo_count = results.get("EKPO", (False, 0))[1]
    eket_count = results.get("EKET", (False, 0))[1]
    ekes_count = results.get("EKES", (False, 0))[1]
    ekbe_count = results.get("EKBE", (False, 0))[1]

    # -- Key set sizes -------------------------------------------------
    log.info("Key sets populated:")
    log.info("    _EBELN_SET  : %6d unique PO numbers", len(_EBELN_SET))
    log.info("    _MATNR_SET  : %6d unique materials",  len(_MATNR_SET))
    log.info("    _LIFNR_SET  : %6d unique vendors",    len(_LIFNR_SET))

    # -- Raw row counts ------------------------------------------------
    log.info("Staging row counts:")
    for tbl_name, (ok, count) in results.items():
        status = "OK     " if ok else "FAILED "
        log.info("    %-12s : %8d rows  [%s]", tbl_name, count, status)

    # -- Schedule-line expansion metrics ------------------------------
    if ekpo_count > 0 and eket_count > 0:
        ratio = eket_count / ekpo_count
        log.info("Schedule-line expansion ratio (EKET rows / EKPO rows): %.2fx", ratio)
        log.info(
            "Estimated ME2M output rows (EKET-driven): %d  "
            "(vs EKPO-only v1.0 estimate: %d)",
            eket_count, ekpo_count,
        )
    elif ekpo_count > 0 and eket_count == 0:
        log.warning(
            "EKET returned 0 rows.  All items lack schedule lines — "
            "ME2M output will be one row per EKPO item (%d rows).",
            ekpo_count,
        )

    # -- Live queries against staging ----------------------------------
    try:
        # Unique EBELN+EBELP in EKPO
        loader.cur.execute(
            f"SELECT COUNT(DISTINCT ebeln || '|' || ebelp) "
            f"FROM {schema}.stg_sap_{prefix}_ekpo"
        )
        unique_po_lines: int = loader.cur.fetchone()[0]
        log.info("Unique EBELN+EBELP in EKPO staging : %d", unique_po_lines)

        if eket_count > 0:
            # Unique EBELN+EBELP in EKET
            loader.cur.execute(
                f"SELECT COUNT(DISTINCT ebeln || '|' || ebelp) "
                f"FROM {schema}.stg_sap_{prefix}_eket"
            )
            unique_eket_lines: int = loader.cur.fetchone()[0]
            pct = 100.0 * unique_eket_lines / unique_po_lines if unique_po_lines else 0
            log.info(
                "Unique EBELN+EBELP in EKET staging : %d  (%.1f%% of EKPO items "
                "have at least one schedule line)",
                unique_eket_lines, pct,
            )

            # Average schedule lines per item
            loader.cur.execute(
                f"SELECT ROUND(COUNT(*)::NUMERIC / "
                f"NULLIF(COUNT(DISTINCT ebeln || '|' || ebelp), 0), 2) "
                f"FROM {schema}.stg_sap_{prefix}_eket"
            )
            avg_lines = loader.cur.fetchone()[0]
            log.info("Avg schedule lines per PO item     : %s", avg_lines)

        if ekes_count > 0:
            loader.cur.execute(
                f"SELECT COUNT(DISTINCT ebeln || '|' || ebelp) "
                f"FROM {schema}.stg_sap_{prefix}_ekes"
            )
            unique_ekes_lines: int = loader.cur.fetchone()[0]
            log.info(
                "Unique EBELN+EBELP in EKES staging : %d  (items with vendor confirmations)",
                unique_ekes_lines,
            )

        log.info(
            "Estimated final ME2M SQL rows      : %d  "
            "(EKET rows + EKPO items without any schedule line)",
            eket_count + max(0, unique_po_lines - (unique_eket_lines if eket_count > 0 else 0)),
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
    log.info("SAP ME2M Purchase Orders Staging Pipeline  v2.0  [run: %s]", run_id)
    log.info("=" * 60)
    log.info("Company codes        : %s", COMPANY_CODES)
    log.info("EKKO BEDAT range     : %s → %s", DATE_FROM, DATE_TO)
    log.info("EKET EINDT range     : %s → %s", EINDT_FROM, EINDT_TO)
    log.info("Open schedules scan  : %s", INCLUDE_OPEN_SCHEDULES)
    log.info("Doc. types           : %s", DOCUMENT_TYPES  or "ALL")
    log.info("Doc. categories      : %s", DOC_CATEGORIES  or "ALL")
    log.info("Plants               : %s", PLANTS          or "ALL")
    log.info("Language             : %s", LANGUAGE)
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
    prescan_done  = False   # guard — run prescan exactly once, after EKKO

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                ok, count = extract_table(tbl_def, sap, loader, cfg, log)
                results[tbl_name] = (ok, count)

                # ── After EKKO anchor, run EKET pre-scan ──────────────
                if tbl_def.get("is_anchor") and not prescan_done:
                    _check_anchor_result(count, log)
                    prescan_done = True
                    try:
                        _prescan_eket_for_open_schedules(sap, loader, cfg, log)
                    except Exception:
                        log.exception(
                            "[PRESCAN] Pre-scan failed — continuing without "
                            "open-schedule expansion.  Row count may be lower than SAP."
                        )

            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)

    finally:
        # ── Diagnostics before closing connections ─────────────────────
        try:
            if loader:
                _log_extraction_diagnostics(loader, results, log)
        except Exception:
            log.exception("Diagnostics logging failed.")

        if sap:
            sap.close()
        if loader:
            loader.close()

    # ── Final summary ──────────────────────────────────────────────────
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
        "  EKPO INNER JOIN EKKO ON ebeln\n"
        "  LEFT JOIN EKET   ON ebeln+ebelp   (1 row per schedule line)\n"
        "  LEFT JOIN EKBE   ON ebeln+ebelp   (aggregate GR/IR qty)\n"
        "  LEFT JOIN EKES   ON ebeln+ebelp   (aggregate confirmed qty)\n"
        "  LEFT JOIN EKKN   ON ebeln+ebelp+zekkn='01'\n"
        "  LEFT JOIN MAKT   ON matnr\n"
        "  LEFT JOIN LFA1   ON lifnr"
    )
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)("\nExtraction cancelled by user.")
        sys.exit(1)