"""
SAP IW29 Display PM Notifications — Staging Extractor
======================================================
Extracts columns matching the IW29 (Display Notifications) transaction report
from SAP ECC Plant Maintenance tables and loads them into PostgreSQL staging
tables.

IW29 is the display-only notification list report. It draws from the QMEL
notification header table and its associated status objects, and is typically
used for reporting / read-only access across all PM notification types (M1,
M2, M3, etc.). IW28 is the edit counterpart.

IW29 Report Column → SAP Field Mapping
-----------------------------------------
PlPl               (Planning Plant)           → QMIH.IWERK
Notifctn           (Notification Number)      → QMEL.QMNUM
Created by         (Created By)               → QMEL.ERNAM
CreatTme           (Creation Time)            → QMEL.ERZEIT
Notif.date         (Notification Date)        → QMEL.QMDAT
Description        (Short Text / Issue)       → QMEL.QMTXT
Equipment          (Equipment Number)         → QMIH.EQUNR  ← NO zero-stripping
PG                 (Planner Group)            → QMIH.INGRP
Order              (Associated Work Order)    → QMEL.AUFNR
SysStatus          (Active status string)     → JEST + TJ02T  (via QMEL.OBJNR)
B                  (Breakdown Indicator)      → QMIH.MSAUS   ('X' = True)
Malf.start         (Malfunction Start Date)   → QMIH.AUSVN
MalfStrt           (Malfunction Start Time)   → QMIH.AUZTV
MalfEnd            (Malfunction End Date)     → QMIH.AUSBS
Malf.end           (Malfunction End Time)     → QMIH.AUZTB
FunctLoc           (Functional Location)      → ILOA.TPLNR  (via QMIH.ILOAN)
PlantSection       (Plant Section)            → ILOA.BEBER  (via QMIH.ILOAN)

Source tables (extraction order):
  Pass 1  — QMEL  : Notification header, filtered by QMDAT only (IWERK is
                    absent from QMEL; plant scoping is done via QMIH join).
                    Collects OBJNR → _OBJNR_SET, QMNUM → _QMNUM_SET.
  Pass 1B — QMIH  : PM-specific notification data (breakdown, equipment, plant).
                    Filtered by QMNUM IN (_QMNUM_SET). Joins 1:1 with QMEL.
                    Collects ILOAN → _ILOAN_SET, EQUNR → _EQUNR_SET.
  Pass 1C — ILOA  : Location account assignment (functional location, plant section).
                    Filtered by ILOAN IN (_ILOAN_SET).
  Pass 2  — JEST  : Individual object status (active statuses only).
                    Filtered by OBJNR IN (_OBJNR_SET) + INACT=''.
                    Collects STAT → _STAT_SET.
  Pass 3  — TJ02T : System status texts (4-char codes, e.g. NOPR, OSNO, ORAS).
                    Filtered by ISTAT IN (_STAT_SET) + SPRAS.

  ---- Optional additional passes (enabled via flags below) ----
  Pass 4  — EQKT  : Equipment short texts (language-dependent).
                    Filtered by EQUNR IN (_EQUNR_SET) + SPRAS.
                    Enable by setting INCLUDE_EQKT = True.

Why QMIH and ILOA instead of QMEL directly:
  The customer QMEL metadata export does not include the PM-specific fields
  IWERK, INGRP, EQUNR, MSAUS, AUSVN/AUZTV, AUSBS/AUZTB, TPLNR, BEBER. These
  fields live in the QMIH extension table (one PM notification row per QMEL row)
  and in ILOA (the location assignment record pointed to by QMIH.ILOAN). This
  three-pass design (QMEL → QMIH → ILOA) mirrors the internal table split that
  the VIQMEL view performs in standard SAP, without requiring view access.

CRITICAL — Equipment number handling:
  QMIH.EQUNR values are stored with their raw SAP format (leading zeros intact,
  e.g. '000000010939098'). The _clean() function strips only whitespace and
  control characters — it does NOT remove leading zeros. Do NOT add zero-
  stripping logic; downstream joins against EQUI or EQKT require exact values.

IW29 vs IW28 notes:
  - IW29 is display-only and covers all PM notification types by default.
  - IW28 is the change counterpart; both share the same QMEL-based schema.
  - NOTIFICATION_TYPES = [] includes all types (M1/M2/M3/ZM1 etc.).
  - Set NOTIFICATION_TYPES = ['M3'] to restrict to malfunction reports only.
  - Staging table names use the prefix STG_SAP_IW29_ to avoid collision.

SysStatus assembly note:
  JEST stores one row per active status per OBJNR. In IW29, the SysStatus
  column concatenates all active 4-char codes (e.g. 'NOPR ORAS'). This script
  stages the raw JEST + TJ02T rows; the downstream SQL/dbt layer should
  STRING_AGG them joined on OBJNR to reproduce the IW29 display.

Author  : [Your Name]
Version : 1.2.0
"""

from __future__ import annotations

import os
import platform

# -------------------------------------------------------------
# PostgreSQL DLL handling (required for Python 3.8+ on Windows)
# Replace the path below with your actual PostgreSQL bin path.
# -------------------------------------------------------------
pg_bin_path = r"C:\Program Files\PostgreSQL\18\bin"

if platform.system() == "Windows":
    if os.path.exists(pg_bin_path):
        os.add_dll_directory(pg_bin_path)
    os.add_dll_directory(os.getcwd())

import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
from pyrfc import (
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)

# -------------------------------------------------------------
# RFC SDK registration  (must run before any pyrfc import use)
# -------------------------------------------------------------
load_dotenv()

RFC_SDK_PATH = os.getenv("RFC_SDK_PATH", r"D:\For ESS\nwrfc750P_18-70002755\nwrfcsdk\lib")
if os.path.isdir(RFC_SDK_PATH):
    if sys.platform == "win32":
        try:
            os.add_dll_directory(RFC_SDK_PATH)
        except AttributeError:
            os.environ["PATH"] = RFC_SDK_PATH + ";" + os.environ.get("PATH", "")
    else:
        os.environ["PATH"] = RFC_SDK_PATH + ":" + os.environ.get("PATH", "")


# =============================================================
# CONFIGURATION — edit this section only
# =============================================================

# Notification date range applied to QMEL.QMDAT  (YYYYMMDD format)
DATE_FROM: str = os.getenv("DATE_FROM", "20260101")
DATE_TO:   str = os.getenv("DATE_TO",   "20260131")

# Maintenance planning plant(s) — applied to QMEL.IWERK
PLANTS: List[str] = ["1102"]

# Notification types to include — leave empty [] for all types.
# Common values:
#   'M1' = General maintenance request
#   'M2' = Activity report
#   'M3' = Malfunction report
#   'ZM1', 'ZM2', etc. — customer-defined types
NOTIFICATION_TYPES: List[str] = []

# Language key for text tables (TJ02T, EQKT)
LANGUAGE: str = "E"

# Set True to also extract equipment descriptions from EQKT (Pass 4).
# Requires QMEL.EQUNR values to be populated for the selected notifications.
INCLUDE_EQKT: bool = True

# Staging table name prefix.
# Change to "IW28" if you want shared staging tables with the change pipeline.
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "IW29")


# =============================================================
# Module-level key sets — populated at runtime during extraction
# =============================================================
_OBJNR_SET: Set[str] = set()   # Object numbers        (QMEL  → JEST / TJ02T)
_QMNUM_SET: Set[str] = set()   # Notification numbers  (QMEL  → QMIH)
_ILOAN_SET: Set[str] = set()   # Location assignments  (QMIH  → ILOA)
_EQUNR_SET: Set[str] = set()   # Equipment numbers     (QMIH  → EQKT, if INCLUDE_EQKT)
_STAT_SET:  Set[str] = set()   # Status codes          (JEST  → TJ02T)
_AUFNR_SET: Set[str] = set()


# =============================================================
# WHERE clause builders
# =============================================================

def _format_options(where: str) -> List[Dict[str, str]]:
    """
    Converts a simple WHERE string into RFC OPTIONS rows (<=72 chars each).
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
    Builds RFC OPTIONS rows for a large IN filter.
    Each row TEXT <=72 characters. Values packed greedily per row.
    Subsequent rows preceded by 'OR'. Extra appended as 'AND ...' row.

    If values is empty a guaranteed-no-match clause is emitted so that
    the RFC call returns zero rows rather than an unfiltered full table.
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


def _build_qmel_options() -> List[Dict[str, str]]:
    """
    Anchor table OPTIONS: filter QMEL by QMDAT date range only.

    WHY NO IWERK FILTER HERE:
      IWERK (Planning Plant) is NOT a field on the QMEL table in this system
      (confirmed by sap_qmel_metadata). It lives on QMIH. Filtering QMEL by
      IWERK causes SAPSQL_PARSE_ERROR. Plant scoping is handled downstream —
      QMIH (Pass 1B) carries IWERK and the join QMIH.QMNUM = QMEL.QMNUM
      naturally restricts final results to the target plant.

    IW29 behaviour:
      - NOTIFICATION_TYPES = [] → no QMART filter (all notification types).
      - Set NOTIFICATION_TYPES = ['M3'] to restrict to malfunction reports.
    """
    where = f"QMDAT >= '{DATE_FROM}' AND QMDAT <= '{DATE_TO}'"
    if NOTIFICATION_TYPES:
        types = ",".join(f"'{t}'" for t in NOTIFICATION_TYPES)
        where += f" AND QMART IN ({types})"
    return _format_options(where)


def _build_jest_options() -> List[Dict[str, str]]:
    """JEST: active statuses only (INACT = '' means not inactive)."""
    return _build_in_options(
        "OBJNR",
        _OBJNR_SET,
        extra="INACT = ''",
    )


def _build_qmih_options() -> List[Dict[str, str]]:
    """QMIH: filter by QMNUM IN (_QMNUM_SET). Populated after Pass 1 (QMEL) runs."""
    return _build_in_options("QMNUM", _QMNUM_SET)


def _build_iloa_options() -> List[Dict[str, str]]:
    """ILOA: filter by ILOAN IN (_ILOAN_SET). Populated after Pass 1B (QMIH) runs."""
    return _build_in_options("ILOAN", _ILOAN_SET)


def _build_pmco_options() -> List[Dict[str, str]]:
    """
    PMCO uses the Order Object Number (starts with 'OR').
    We take the AUFNRs collected from QMEL and prepend 'OR'.
    """
    # Create the 'OR...' object numbers, ignoring blanks
    order_objnrs = {f"OR{aufnr}" for aufnr in _AUFNR_SET if aufnr.strip()}
    
    # Filter PMCO to only pull Actual Costs (WRTTP = '04') for our specific orders
    return _build_in_options("OBJNR", order_objnrs, extra="WRTTP = '04'")


# =============================================================
# TABLE DEFINITIONS
# Execution order is critical — later passes depend on keys
# collected by earlier passes.
#
# Column metadata status legend:
#   [META-OK]  — confirmed present in customer metadata export.
#   [META-STD] — standard SAP field not in QMEL metadata export;
#                sourced from QMIH or ILOA where it correctly lives.
# =============================================================

def _build_staging_table_defs() -> List[Dict[str, Any]]:
    """
    Builds the ordered list of table definitions.
    Execution order: QMEL → QMIH → ILOA → JEST → TJ02T → EQKT (optional).
    EQKT is included only when INCLUDE_EQKT is True.
    """

    defs: List[Dict[str, Any]] = [

        # =====================================================
        # PASS 1 — Notification Header (anchor table)
        # Filtered by QMDAT date range only (+ optional QMART).
        # IWERK does NOT exist in QMEL (confirmed by sap_qmel_metadata);
        # plant scoping is achieved via the QMIH join in Pass 1B.
        # Produces:
        #   OBJNR → _OBJNR_SET  (for JEST / TJ02T)
        #   QMNUM → _QMNUM_SET  (for QMIH)
        # All columns confirmed against sap_qmel_metadata.
        # =====================================================
        {
            "table": "QMEL",
            "description": "Notification Header — IW29 scope (all PM notification types)",
            "columns": [

                # --- IW29 Core Display Fields ---
                "QMNUM",    # [META-OK]  Notification Number            (IW29: Notifctn)  → primary key + _QMNUM_SET
                "QMART",    # [META-OK]  Notification Type              (M1/M2/M3/etc.)
                "QMTXT",    # [META-OK]  Short Text / Issue             (IW29: Description)
                "ERNAM",    # [META-OK]  Created By                     (IW29: Created by)
                "ERZEIT",   # [META-OK]  Creation Time                  (IW29: CreatTme)
                "ERDAT",    # [META-OK]  Creation Date
                "QMDAT",    # [META-OK]  Notification Date              (IW29: Notif.date)
                "AUFNR",    # [META-OK]  Associated Work Order          (IW29: Order)
                "OBJNR",    # [META-OK]  Object Number                  → _OBJNR_SET (JEST/TJ02T)

                # --- Priority / Classification ---
                "PRIOK",    # [META-OK]  Priority
                "ARTPR",    # [META-OK]  Priority Type

                # --- Notification Reference / Origin ---
                "QMNAM",    # [META-OK]  Notification Reference Number
                "HERKZ",    # [META-OK]  Origin of Notification
                "BUNAME",   # [META-OK]  Author of Q/PM Notification

                # --- Catalog / Coding ---
                "RBNR",     # [META-OK]  Catalog Profile
                "QMKAT",    # [META-OK]  Catalog Type - Coding
                "QMGRP",    # [META-OK]  Code Group - Coding
                "QMCOD",    # [META-OK]  Coding

                # --- Required Start / End (Date + Time pairs) ---
                "STRMN",    # [META-OK]  Required Start Date
                "STRUR",    # [META-OK]  Required Start Time
                "LTRMN",    # [META-OK]  Required End Date
                "LTRUR",    # [META-OK]  Requested End Time

                # --- Notification Timing ---
                "MZEIT",    # [META-OK]  Time of Notification

                # --- Completion ---
                "QMDAB",    # [META-OK]  Date for Notification Completion
                "QMZAB",    # [META-OK]  Completion Time of Notification

                # --- Change / Audit Trail ---
                "AENAM",    # [META-OK]  Last Changed By
                "AEDAT",    # [META-OK]  Last Changed Date
                "AEZEIT",   # [META-OK]  Time of Change

                # --- Lifecycle / Flags ---
                "MAKNZ",    # [META-OK]  Task Records Exist             ('X' if tasks attached)
                "KZKRI",    # [META-OK]  Indicator: Critical Part
                "KZLOESCH", # [META-OK]  Deletion Indicator
                "PHASE",    # [META-OK]  Notification Processing Phase
                "WAERS",           # Currency Key (Crucial for cost interpretation)
                "ESTIMATED_COSTS", # [META-OK]  Estimated Costs for a Claim
                "CLAIMED_COSTS",   # [META-OK]  Costs Claimed
                "RESULT_COSTS",    # [META-OK]  Costs Accepted
            ],
            "options_fn": _build_qmel_options,
            "primary_keys": ["QMNUM"],
            "key_out": {
                "OBJNR": "_OBJNR_SET",
                "QMNUM": "_QMNUM_SET",
                "AUFNR": "_AUFNR_SET",
            },
        },

        # =====================================================
        # PASS 1B — PM Notification Data (QMIH)
        # 1:1 extension of QMEL for Plant Maintenance notifications.
        # Contains all fields absent from the QMEL metadata export:
        # planning plant, planner group, equipment, breakdown flag,
        # malfunction times, and the ILOAN pointer for location data.
        # Filtered by QMNUM IN (_QMNUM_SET).
        # Produces:
        #   ILOAN → _ILOAN_SET  (for ILOA)
        #   EQUNR → _EQUNR_SET  (for EQKT, if INCLUDE_EQKT)
        #
        # CRITICAL — EQUNR zero-stripping: QMIH.EQUNR is stored with
        # leading zeros. _clean() preserves them. Never strip zeros —
        # downstream joins to EQUI/EQKT require the exact raw value.
        # =====================================================
        {
            "table": "QMIH",
            "description": "PM Notification Data — breakdown, equipment, plant, location pointer",
            "columns": [
                # --- Core join / plant fields (all confirmed sap_QMIH_metadata) ---
                "QMNUM",    # [META-OK] Notification Number            (join key → QMEL.QMNUM)
                "IWERK",    # [META-OK] Maintenance Planning Plant      (IW29: PlPl)
                "INGRP",    # [META-OK] Planner Group                  (IW29: PG)
                "EQUNR",    # [META-OK] Equipment Number               (IW29: Equipment) ← no zero-strip → _EQUNR_SET
                "ILOAN",    # [META-OK] Location Account Assignment    → _ILOAN_SET (ILOA)
                # --- Breakdown / malfunction fields ---
                "MSAUS",    # [META-OK] Breakdown Indicator            (IW29: B, 'X'=True)
                "AUSVN",    # [META-OK] Start of Malfunction (Date)   (IW29: Malf.start)
                "AUZTV",    # [META-OK] Start of Malfunction (Time)   (IW29: MalfStrt)
                "AUSBS",    # [META-OK] End of Malfunction (Date)     (IW29: MalfEnd)
                "AUZTB",    # [META-OK] End of Malfunction (Time)     (IW29: Malf.end)
                "AUSZT",    # [META-OK] Breakdown Duration
                "MAUEH",    # [META-OK] Unit for Breakdown Duration
                # --- Location / affected object fields ---
                "BTPLN",    # [META-OK] Functional Location Affected
                "BEQUI",    # [META-OK] Equipment Affected
                # --- PM planning fields ---
                "ILART",    # [META-OK] Maintenance Activity Type
                "WARPL",    # [META-OK] Maintenance Plan
                "REVNR",    # [META-OK] Revision for Plant Maintenance
            ],
            "options_fn": _build_qmih_options,
            "primary_keys": ["QMNUM"],
            "key_out": {
                "ILOAN": "_ILOAN_SET",
                "EQUNR": "_EQUNR_SET",
            },
        },

        # =====================================================
        # PASS 1C — Location Account Assignment (ILOA)
        # Provides Functional Location (TPLNR) and Plant Section
        # (BEBER) — the two remaining [META-STD] fields that cannot
        # be read directly from QMEL.
        # Filtered by ILOAN IN (_ILOAN_SET).
        # Join key in downstream SQL: ILOA.ILOAN = QMIH.ILOAN.
        # =====================================================
        {
            "table": "ILOA",
            "description": "Location Account Assignment — functional location and plant section",
            "columns": [
                # --- Core join / location fields (all confirmed sap_ILOA_metadata) ---
                "ILOAN",    # [META-OK] Location Account Assignment    (primary key / join key → QMIH.ILOAN)
                "TPLNR",    # [META-OK] Functional Location            (IW29: FunctLoc)
                "BEBER",    # [META-OK] Plant Section                  (IW29: PlantSection)
                "SWERK",    # [META-OK] Maintenance Plant
                "STORT",    # [META-OK] Location of Maintenance Object
                # --- Account assignment fields ---
                "KOSTL",    # [META-OK] Cost Center
                "BUKRS",    # [META-OK] Company Code
                "GSBER",    # [META-OK] Business Area
                "KOKRS",    # [META-OK] Controlling Area
                "AUFNR",    # [META-OK] Settlement Order
                "DAUFN",    # [META-OK] Standing Order Number
            ],
            "options_fn": _build_iloa_options,
            "primary_keys": ["ILOAN"],
        },

        # =====================================================
        # PASS 2 — Individual Status per Object Number
        # Filtered by OBJNR IN (_OBJNR_SET) AND INACT = '' (active only).
        # Produces: STAT keys (_STAT_SET) for TJ02T text lookup.
        # One row per status per OBJNR — downstream SQL aggregates with
        # STRING_AGG to reproduce IW29 SysStatus column
        # (e.g. 'NOPR ORAS' when both statuses are active simultaneously).
        #
        # All columns confirmed against sap_jest_metadata.
        # _DATAAGING omitted — data-aging partition field, not needed here.
        # =====================================================
        {
            "table": "JEST",
            "description": "Individual Status per Object (active statuses only)",
            "columns": [
                "OBJNR",    # [META-OK]  Object Number              (join key → QMEL.OBJNR)
                "STAT",     # [META-OK]  Status Code                → _STAT_SET
                "INACT",    # [META-OK]  Inactive Indicator         ('' = active)
                "CHGNR",    # [META-OK]  Change Number              (status revision counter)
            ],
            "options_fn": _build_jest_options,
            "primary_keys": ["OBJNR", "STAT"],
            "key_out": {
                "STAT": "_STAT_SET",
            },
        },

        # =====================================================
        # PASS 3 — System Status Texts
        # Filtered by ISTAT IN (_STAT_SET) + SPRAS.
        # Maps internal codes (e.g. 'I0068') → 4-char text (e.g. 'NOPR').
        # Common IW29 status codes:
        #   NOPR = Notification in Process
        #   OSNO = Outstanding Notification
        #   ORAS = Order Assigned
        #   NOCO = Notification Completed
        #   ISOS = Isolated Status (customer-defined)
        #
        # All columns confirmed against sap_tj02t_metadata.
        # =====================================================
        {
            "table": "TJ02T",
            "description": "System Status Texts (NOPR, OSNO, ORAS, NOCO, ...)",
            "columns": [
                "ISTAT",    # [META-OK]  Internal Status Code       (join key → JEST.STAT)
                "SPRAS",    # [META-OK]  Language Key
                "TXT04",    # [META-OK]  4-character Status Text    (IW29: SysStatus component)
                "TXT30",    # [META-OK]  30-character Status Text   (full description)
            ],
            "options_fn": lambda: _build_in_options(
                "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
            ),
            "primary_keys": ["ISTAT", "SPRAS"],
        },
        {
            "table": "QMFE",
            "description": "Notification Items — Root Cause / Damage Codes",
            "columns": [
                "QMNUM",    # Notification No (join key → QMEL.QMNUM)
                "FENUM",    # Item Number
                "FEGRP",    # Code Group - Problem
                "FECOD",    # Problem or Damage Code (Root Cause)
                "FETXT",    # Notification Item Short Text
                "FEQKLAS",  # Defect Class
            ],
            "options_fn": lambda: _build_in_options("QMNUM", _QMNUM_SET),
            "primary_keys": ["QMNUM", "FENUM"],
        },
        {
            "table": "PMCO",
            "description": "PM Cost Summary — Total Costs per Order",
            "columns": [
                "OBJNR",    
                "GJAHR",    
                "WRTTP",    
                "COCUR",    
                "WRT00", "WRT01", "WRT02", "WRT03", "WRT04", 
                "WRT05", "WRT06", "WRT07", "WRT08", 
                "WRT09", "WRT10", "WRT11", "WRT12", 
                "WRT13", "WRT14", "WRT15", "WRT16", 
            ],
            "options_fn": _build_pmco_options,  # <--- UPDATED
            "primary_keys": ["OBJNR", "GJAHR", "WRTTP"],
        },
    ]

    # =========================================================
    # PASS 4 (optional) — Equipment Short Texts
    # Only included when INCLUDE_EQKT = True.
    # Filtered by EQUNR IN (_EQUNR_SET).
    # _EQUNR_SET is populated by Pass 1B (QMIH), not QMEL.
    # Join key in downstream SQL: EQKT.EQUNR = QMIH.EQUNR.
    #
    # All columns confirmed against sap_eqkt_metadata.
    # TXASP omitted — primary-language indicator, not needed in staging.
    # =========================================================
    if INCLUDE_EQKT:
        defs.append({
            "table": "EQKT",
            "description": "Equipment Descriptions (language-dependent) — optional enrichment",
            "columns": [
                "EQUNR",    # [META-OK]  Equipment Number           (join key → QMIH.EQUNR)
                "SPRAS",    # [META-OK]  Language Key
                "EQKTX",    # [META-OK]  Equipment Short Text       (mixed case)
                "EQKTU",    # [META-OK]  Equipment Description (uppercase) — useful for case-insensitive search
                "KZLTX",    # [META-OK]  Long Text Indicator        ('X' if long text exists)
            ],
            "options_fn": lambda: _build_in_options(
                "EQUNR", _EQUNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
            ),
            "primary_keys": ["EQUNR", "SPRAS"],
        })

    return defs


# Evaluated once at module load; INCLUDE_EQKT is resolved at this point.
STAGING_TABLE_DEFS: List[Dict[str, Any]] = _build_staging_table_defs()


# =============================================================
# Extraction tuning  (override via .env)
# =============================================================
@dataclass
class ExtractionConfig:
    batch_size: int       = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "10000")))
    max_buffer_width: int = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries: int      = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s: float  = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every: int     = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA_HANA",   "sap_stg_hana"))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")


# =============================================================
# Logging
# =============================================================
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_iw29_staging_{run_id}.log")

    log = logging.getLogger(f"sap_iw29_staging.{run_id}")
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


# =============================================================
# Environment / connection config
# =============================================================
def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is missing or empty.")
    return val


def build_sap_config() -> Dict[str, str]:
    return {
        "saprouter": os.getenv("HANA_ROUTER"),
        "ashost":    _require_env("HANA_ASHOST"),
        "sysnr":     os.getenv("HANA_SYSNR", "00"),
        "client":    os.getenv("HANA_CLIENT", "500"),
        "user":      _require_env("HANA_USER"),
        "passwd":    _require_env("HANA_PASSWD"),
        "lang":      os.getenv("HANA_LANG", "EN"),
    }


# =============================================================
# SAP connection wrapper
# =============================================================
class SAPConnection:
    def __init__(self, config: Dict[str, str], log: logging.Logger) -> None:
        self._cfg = config
        self._log = log
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP (attempt %d/3)...", attempt)
                self.conn = Connection(**self._cfg)
                self._log.info("SAP connection established.")
                return
            except LogonError:
                self._log.exception("SAP logon failed -- check credentials.")
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


# =============================================================
# Column metadata (field lengths from DD03L)
# =============================================================
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


# =============================================================
# Column chunker (respects RFC buffer width limit)
# =============================================================
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
            w = self._widths.get(col, 100) + 1   # +1 for "|" delimiter
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


# =============================================================
# Data sanitiser
# NOTE: This function intentionally does NOT remove leading zeros
# from any field value. QMEL.EQUNR values must be preserved exactly
# as stored in SAP to ensure correct joins against EQUI and EQKT.
# =============================================================
def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


# =============================================================
# Key collector
# =============================================================
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
            # Preserve value as-is (no leading-zero removal) for EQUNR joins.
            if v:
                target_set.add(v.strip())
        added = len(target_set) - before
        if added:
            log.debug(
                "Collected %d new '%s' keys (total %d) -> %s",
                added, col, len(target_set), set_name,
            )


# =============================================================
# SAP batch generator
# =============================================================
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


# =============================================================
# PostgreSQL Loader
# =============================================================
class PostgresLoader:
    def __init__(
        self,
        cfg:    Dict[str, Any],
        schema: str,
        prefix: str,
        log:    logging.Logger,
    ) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]    = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL...")
        try:
            self.conn    = psycopg2.connect(**self._cfg)
            self._cursor = self.conn.cursor()
            self._log.info("PostgreSQL connection established.")
        except Exception:
            self._log.exception("PostgreSQL connection failed.")
            raise

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg_name  = f"stg_sap_{self._prefix.lower()}_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

        self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        col_defs = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
        ddl = f"""
        -- DROP TABLE IF EXISTS {self._schema}.{stg_name};
        CREATE TABLE IF NOT EXISTS {self._schema}.{stg_name} (
            {col_defs},
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """
        self._log.info("Creating staging table %s.%s...", self._schema, stg_name)
        self._cursor.execute(ddl)
        self._cursor.execute(f"TRUNCATE TABLE {self._schema}.{stg_name};")
        self.conn.commit()
        self._log.info("Staging table %s.%s created and truncated.", self._schema, stg_name)
        return f'"{self._schema}"."{stg_name}"'

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [
            f'"{c.replace("-", "_").replace("/", "_").lower()}"'
            for c in columns
        ]
        query = f"INSERT INTO {stg_table} ({', '.join(safe_cols)}) VALUES %s"
        extras.execute_values(self._cursor, query, rows)

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self.conn:
            self.conn.close()
        self._log.info("PostgreSQL connection closed.")


# =============================================================
# Per-table pipeline
# =============================================================
def extract_table(
    table_def: Dict[str, Any],
    sap:       SAPConnection,
    loader:    PostgresLoader,
    cfg:       ExtractionConfig,
    log:       logging.Logger,
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

    stg_table = loader.create_staging_table(table, columns)

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


# =============================================================
# Main orchestrator
# =============================================================
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP IW29 Display Notifications Staging Pipeline  [run: %s]", run_id)
    log.info("Plants              : %s", PLANTS)
    log.info("Date range (QMDAT) : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Notification types  : %s", NOTIFICATION_TYPES if NOTIFICATION_TYPES else "ALL")
    log.info("Language            : %s", LANGUAGE)
    log.info("Include EQKT        : %s", INCLUDE_EQKT)
    log.info("Staging prefix      : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables              : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg        = ExtractionConfig()
        sap_config = build_sap_config()
        pg_config  = {
            "host":     _require_env("PG_HOST"),
            "port":     os.getenv("PG_PORT", "5432"),
            "database": _require_env("PG_DATABASE"),
            "user":     _require_env("PG_USER"),
            "password": _require_env("PG_PASSWORD"),
        }
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(sap_config, log)
        loader = PostgresLoader(pg_config, cfg.staging_schema, STAGING_PREFIX, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections -- aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                ok, count = extract_table(tbl_def, sap, loader, cfg, log)
                results[tbl_name] = (ok, count)
            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)
    finally:
        sap.close()
        loader.close()

    total_elapsed = time.time() - overall_start
    succeeded     = [(t, n) for t, (ok, n) in results.items() if ok]
    failed        = [t for t, (ok, _) in results.items() if not ok]

    log.info("=" * 60)
    log.info("Run complete in %.1fs", total_elapsed)
    log.info("Key sets collected:")
    log.info("    QMNUM : %d notification numbers",  len(_QMNUM_SET))
    log.info("    OBJNR : %d object numbers",        len(_OBJNR_SET))
    log.info("    ILOAN : %d location assignments",  len(_ILOAN_SET))
    log.info("    EQUNR : %d equipment numbers",     len(_EQUNR_SET))
    log.info("    STAT  : %d active status codes",   len(_STAT_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)

    if failed:
        sys.exit(1)


# =============================================================
# Downstream SQL reference — full IW29 view assembly
# =============================================================
# After loading, reconstruct the complete IW29 display in your
# dbt model or view with:
#
#   SELECT
#       -- Notification header (QMEL)
#       q.qmnum,
#       q.qmart,
#       q.qmtxt,
#       q.ernam,
#       q.erzeit,
#       q.erdat,
#       q.qmdat,
#       q.aufnr,
#       q.priok,
#       q.artpr,
#       q.qmnam,
#       q.herkz,
#       q.buname,
#       q.rbnr,
#       q.qmkat,
#       q.qmgrp,
#       q.qmcod,
#       q.strmn,
#       q.strur,
#       q.ltrmn,
#       q.ltrur,
#       q.mzeit,
#       q.qmdab,
#       q.qmzab,
#       q.aenam,
#       q.aedat,
#       q.aezeit,
#       q.maknz,
#       q.kzkri,
#       q.kzloesch,
#       q.phase,
#       -- PM-specific fields (QMIH)
#       h.iwerk,
#       h.ingrp,
#       h.equnr,
#       h.msaus,
#       h.ausvn,
#       h.auztv,
#       h.ausbs,
#       h.auztb,
#       h.auszt,
#       h.maueh,
#       h.btpln,
#       h.bequi,
#       h.ilart,
#       h.warpl,
#       h.revnr,
#       -- Location fields (ILOA via QMIH.ILOAN)
#       l.tplnr,
#       l.beber,
#       l.swerk,
#       l.stort,
#       l.kostl,
#       l.bukrs,
#       l.gsber,
#       l.kokrs,
#       l.aufnr,
#       l.daufn,
#       -- System status (JEST + TJ02T aggregated)
#       STRING_AGG(t.txt04, ' ' ORDER BY t.txt04) AS sys_status
#   FROM      sap_stg_ecc.stg_sap_iw29_qmel   q
#   LEFT JOIN sap_stg_ecc.stg_sap_iw29_qmih   h  ON h.qmnum = q.qmnum
#   LEFT JOIN sap_stg_ecc.stg_sap_iw29_iloa   l  ON l.iloan = h.iloan
#   LEFT JOIN sap_stg_ecc.stg_sap_iw29_jest   j  ON j.objnr = q.objnr
#                                               AND j.inact  = ''
#   LEFT JOIN sap_stg_ecc.stg_sap_iw29_tj02t  t  ON t.istat = j.stat
#                                               AND t.spras  = 'E'
#   GROUP BY
#       q.qmnum, q.qmart, q.qmtxt, q.ernam, q.erzeit, q.erdat,
#       q.qmdat, q.aufnr, q.priok, q.artpr, q.qmnam, q.herkz,
#       q.buname, q.rbnr, q.qmkat, q.qmgrp, q.qmcod,
#       q.strmn, q.strur, q.ltrmn, q.ltrur, q.mzeit,
#       q.qmdab, q.qmzab, q.aenam, q.aedat, q.aezeit,
#       q.maknz, q.kzkri, q.kzloesch, q.phase,
#       h.iwerk, h.ingrp, h.equnr, h.msaus,
#       h.ausvn, h.auztv, h.ausbs, h.auztb, h.swerk, h.gewrk,
#       l.tplnr, l.beber, l.kostl, l.bukrs;
# =============================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)