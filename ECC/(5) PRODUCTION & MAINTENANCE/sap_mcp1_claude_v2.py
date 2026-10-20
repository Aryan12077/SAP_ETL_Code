"""
SAP MCP1 — Production Order Operations / Lead Time Staging Extractor
=====================================================================
Extracts columns matching the MCP1 (Production Order Information System —
Operations view) transaction report from SAP ECC PP tables and loads them
into SQL Server staging tables.

MCP1 Report Column → SAP Field Mapping
----------------------------------------
Plant                → AUFK.WERKS  (order plant)
Order                → AUFK.AUFNR  (production order number)
Created on           → AUFK.ERDAT  (order creation date)
Oper./Act.           → AFVC.VORNR  (operation/activity number)
Operation Text       → AFVC.LTXA1  (operation short text)
Work Center          → CRHD.ARBPL  (work center name, via AFVC.ARBID)
Work Center Plant    → CRHD.WERKS  (plant of work center)
Tgt Lead Time (day)  → AFVV.BEARZ + AFVV.BEAZE  (processing time, normalised to DAY)
Setup Time           → AFVV.RUEST  (planned setup time)
Wait Time            → AFVV.LIEGZ  (planned wait / queue time)
Move Time            → AFVV.TRANZ  (planned move / transit time)
Act Lead Time (day)  → AFRU.ISMNW + AFRU.ISMNU  (actual confirmed work, normalised to DAY)
Actual Duration      → AFRU.IDAUR  (actual duration from confirmation)
Posting Date         → AFRU.BUDAT  (actual confirmation posting date)

Source tables (extraction order):
  Pass 1 — AUFK : Production order master (anchor). Collects AUFNR & AUFPL via AFKO.
  Pass 2 — AFKO : Order header scheduling data. Collects AUFPL (routing key).
  Pass 3 — AFVC : Operation / activity master data. Collects ARBID & VORNR.
  Pass 4 — AFVV : Standard values / planned lead times per operation.
  Pass 5 — AFRU : Actual confirmations (actual lead times) per operation.
  Pass 6 — CRHD : Work centre master (name, plant). Driven by ARBID set.

Relationship chain:
  AUFK.AUFNR ──► AFKO.AUFNR  (1:1 – order header)
  AFKO.AUFPL ──► AFVC.AUFPL  (1:N – routing → operations)
  AFVC.AUFPL + AFVC.VORNR ──► AFVV.AUFPL + AFVV.VORNR  (1:1 – standard values)
  AFVC.AUFPL + AFVC.VORNR ──► AFRU.AUFPL + AFRU.VORNR  (1:N – confirmations)
  AFVC.ARBID ──► CRHD.OBJID  (N:1 – work centre)

Fallback behaviour (mirrors COOIS extractor pattern)
----------------------------------------------------
  WHERE syntax  : First attempts IN (...) operator; on zero anchor rows retries
                  with OR-chained = clauses.
  RFC function  : Tries BBP_RFC_READ_TABLE first, then RFC_READ_TABLE.
  Probe order   : Four combinations tried in sequence:
                    1. BBP_RFC_READ_TABLE + IN  syntax
                    2. BBP_RFC_READ_TABLE + OR  syntax
                    3. RFC_READ_TABLE     + IN  syntax
                    4. RFC_READ_TABLE     + OR  syntax
                  Winning combination used for all subsequent tables.

Date filter
-----------
  AUFK.ERDAT restricts extraction to the chosen period (order creation date).
  AFRU.BUDAT can optionally also be filtered (confirmation posting date).
  Set DATE_FROM / DATE_TO as YYYYMMDD strings (or env vars).
  Leave either empty ("") to skip that boundary.

Time normalisation
------------------
  SAP stores planned times (AFVV.BEARZ etc.) in mixed units (MIN, H, S, DAY).
  This extractor reads the raw numeric value AND the unit field, then converts
  to fractional days in Python before writing to staging.
  Actual confirmed work (AFRU.ISMNW) follows the same approach.
  Both normalised columns and the raw originals are written to staging so the
  consumer can verify the conversion.
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
    # Also point to the current script directory where DLLs may be copied.
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

# ---------------------------------------------------------------------------
# RFC SDK registration
# ---------------------------------------------------------------------------
load_dotenv()

RFC_SDK_PATH = os.getenv("RFC_SDK_PATH", r"C:\nwrfcsdk\lib")
if os.path.isdir(RFC_SDK_PATH):
    if sys.platform == "win32":
        try:
            os.add_dll_directory(RFC_SDK_PATH)
        except AttributeError:
            os.environ["PATH"] = RFC_SDK_PATH + ";" + os.environ.get("PATH", "")
    else:
        os.environ["PATH"] = RFC_SDK_PATH + ":" + os.environ.get("PATH", "")

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Plant filter — AUFK.WERKS. Add multiple plants as needed.
PLANTS: List[str] = ["1202"]

# Order type filter — AUFK.AUART. Leave [] for all PP order types.
ORDER_TYPES: List[str] = []

# Date range on AUFK.ERDAT (order creation date) — format YYYYMMDD.
# Matches the "01.01.2026 TO 28.02.2026" range shown in the PDF report header.
DATE_FROM: str = os.getenv("DATE_FROM", "20260101")
DATE_TO:   str = os.getenv("DATE_TO",   "20260228")

# Optional secondary date filter on AFRU.BUDAT (confirmation posting date).
# Leave both empty to skip — all confirmations for matching orders will be read.
CONF_DATE_FROM: str = os.getenv("CONF_DATE_FROM", "")
CONF_DATE_TO:   str = os.getenv("CONF_DATE_TO",   "")

# Language key for text lookups.
LANGUAGE: str = "E"

# Staging table prefix: STG_SAP_MCP1_<TABLE>
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "MCP1")

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Time-unit conversion to fractional DAY
# ---------------------------------------------------------------------------
# SAP unit codes → fraction of one working day.
# Adjust the denominator for your plant calendar (default: 480 min/day = 8h shift).
_MINUTES_PER_DAY: float = float(os.getenv("MINUTES_PER_DAY", "480"))

_TO_DAY: Dict[str, float] = {
    "MIN": 1.0 / _MINUTES_PER_DAY,
    "H":   60.0 / _MINUTES_PER_DAY,
    "S":   1.0 / (_MINUTES_PER_DAY * 60.0),
    "DAY": 1.0,
    "":    1.0 / _MINUTES_PER_DAY,  # unknown unit → assume minutes
}


def _to_days(value_str: str, unit_str: str) -> str:
    """Convert a raw SAP time value + unit to a fractional-day string."""
    try:
        val = float(value_str.strip()) if value_str.strip() else 0.0
    except ValueError:
        return ""
    factor = _TO_DAY.get(unit_str.strip().upper(), _TO_DAY[""])
    return f"{val * factor:.6f}"


# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_AUFNR_SET: Set[str] = set()   # production order numbers  (AUFK  → AFKO)
_AUFPL_SET: Set[str] = set()   # routing numbers           (AFKO  → AFVC / AFVV / AFRU)
_ARBID_SET: Set[str] = set()   # work-centre object IDs    (AFVC  → CRHD)

# Active WHERE-syntax mode.
_USE_IN_OPERATOR: bool = True

# ---------------------------------------------------------------------------
# WHERE clause helpers (identical pattern to the COOIS extractor)
# ---------------------------------------------------------------------------

def _format_options(where: str) -> List[Dict[str, str]]:
    """Split a long WHERE string into 72-character RFC OPTIONS rows."""
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


def _build_or_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """Build RFC OPTIONS rows using OR-chained = clauses, chunked to 72 chars."""
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.append({"TEXT": f"AND {extra}"})
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    i = 0
    first_group = True

    while i < len(sorted_vals):
        prefix = "( " if first_group else "OR ( "
        row_parts: List[str] = []
        row_len = len(prefix)

        while i < len(sorted_vals):
            clause = f"{field_name} = '{sorted_vals[i]}'"
            sep_len = len(" OR ") if row_parts else 0
            if row_parts and row_len + sep_len + len(clause) + 2 > 72:
                break
            row_parts.append(clause)
            row_len += sep_len + len(clause)
            i += 1

        options.append({"TEXT": prefix + " OR ".join(row_parts) + " )"})
        first_group = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})

    return options


def _build_in_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """Build RFC OPTIONS rows using IN (...) clauses, chunked to 72 chars."""
    if not _USE_IN_OPERATOR:
        return _build_or_options(field_name, values, extra)

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
        prefix   = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
        max_fill = 72 - len(prefix) - 1
        row_vals: List[str] = []
        row_len = 0

        while i < len(sorted_vals):
            quoted = f"'{sorted_vals[i]}'"
            need   = len(quoted) + (1 if row_vals else 0)
            if row_vals and row_len + need > max_fill:
                break
            row_vals.append(quoted)
            row_len += need
            i += 1

        options.append({"TEXT": prefix + ",".join(row_vals) + ")"})
        first_clause = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})

    return options


# ---------------------------------------------------------------------------
# Table-specific WHERE builders
# ---------------------------------------------------------------------------

def _build_aufk_options() -> List[Dict[str, str]]:
    """WHERE for anchor table AUFK: plant, order type, date range, not deleted."""
    if _USE_IN_OPERATOR:
        plants_str = ",".join(f"'{p}'" for p in PLANTS)
        where = f"WERKS IN ({plants_str})"

        if ORDER_TYPES:
            types_str = ",".join(f"'{t}'" for t in ORDER_TYPES)
            where += f" AND AUART IN ({types_str})"

        if DATE_FROM:
            where += f" AND ERDAT GE '{DATE_FROM}'"
        if DATE_TO:
            where += f" AND ERDAT LE '{DATE_TO}'"

        where += " AND LOEKZ = ''"
        return _format_options(where)

    # OR-chained fallback
    options: List[Dict[str, str]] = []
    plant_clauses = " OR ".join(f"WERKS = '{p}'" for p in PLANTS)
    options.append({"TEXT": f"( {plant_clauses} )"})

    if ORDER_TYPES:
        type_clauses = " OR ".join(f"AUART = '{t}'" for t in ORDER_TYPES)
        options.append({"TEXT": f"AND ( {type_clauses} )"})

    if DATE_FROM:
        options.append({"TEXT": f"AND ERDAT GE '{DATE_FROM}'"})
    if DATE_TO:
        options.append({"TEXT": f"AND ERDAT LE '{DATE_TO}'"})

    options.append({"TEXT": "AND LOEKZ = ''"})
    return options


def _build_afru_options() -> List[Dict[str, str]]:
    """
    WHERE for AFRU (confirmations), driven by _AUFPL_SET.
    Optionally restricts by confirmation posting date (BUDAT).
    STOKZ = '' excludes reversed confirmations.
    """
    extra_parts: List[str] = ["STOKZ = ''"]
    if CONF_DATE_FROM:
        extra_parts.append(f"BUDAT GE '{CONF_DATE_FROM}'")
    if CONF_DATE_TO:
        extra_parts.append(f"BUDAT LE '{CONF_DATE_TO}'")

    extra = " AND ".join(extra_parts)
    return _build_in_options("AUFPL", _AUFPL_SET, extra=extra)


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
#
# Each entry drives one complete extract-and-load pipeline pass.
#
# Keys:
#   table        → SAP transparent table name
#   description  → human-readable label (for logs)
#   columns      → fields to read (subset of the table's real columns)
#   options_fn   → callable returning RFC OPTIONS rows (WHERE clause)
#   primary_keys → documentation / future dedup logic
#   key_out      → {SAP_COLUMN: "_GLOBAL_SET_NAME"} — populates runtime key sets
#   is_anchor    → True on the first table; triggers the RFC/syntax probe
#   post_process → optional callable(rows, columns) → rows for derived columns
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ── PASS 1 ─ AUFK : Production Order Master (anchor) ──────────────────
    # Provides order-level attributes. AUFNR feeds all child tables.
    {
        "table": "AUFK",
        "description": "Production Order Master",
        "columns": [
            "AUFNR",   # Production order number          → order key
            "AUART",   # Order type
            "WERKS",   # Plant                             → "Plant" in MCP1
            "ERDAT",   # Created on                        → "Created on" in MCP1
            "KTEXT",   # Order short text / description
            "OBJNR",   # Object number (status link)
            "LOEKZ",   # Deletion flag (filter = '')
            "PRCTR",   # Profit centre
            "KOSTL",   # Cost centre
        ],
        "options_fn": _build_aufk_options,
        "primary_keys": ["AUFNR"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
        },
        "is_anchor": True,
    },

    # ── PASS 2 ─ AFKO : Order Header Scheduling Data ───────────────────────
    # One row per order; provides the AUFPL (routing key) that ties to AFVC.
    {
        "table": "AFKO",
        "description": "Order Header Scheduling Data",
        "columns": [
            "AUFNR",   # Order number (FK to AUFK)
            "AUFPL",   # Routing number of operations      → AFVC / AFVV / AFRU key
            "GSTRP",   # Basic start date
            "GLTRP",   # Basic finish date
            "GSTRS",   # Scheduled start
            "GLTRS",   # Scheduled finish
            "GSTRI",   # Actual start date
            "GLTRI",   # Actual finish date
            "GAMNG",   # Total order quantity
            "GMEIN",   # Base unit of measure (header level)
            "FEVOR",   # Production supervisor
            "PLGRP",   # Planner group
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR"],
        "key_out": {
            "AUFPL": "_AUFPL_SET",
        },
    },

    # ── PASS 3 ─ AFVC : Production Order Operations (master data) ──────────
    # One row per operation (VORNR) within a routing (AUFPL).
    # LTXA1 is the operation text shown in MCP1 "Oper./Act." column.
    # ARBID is the internal work-centre ID resolved to a name via CRHD.
    #
    # NOTE — fields NOT present in AFVC per metadata:
    #   LMNGA  (Total Yield Confirmed)  → lives in AFVV, extracted in Pass 4
    #   MEINH  (Operation UoM)          → lives in AFVV.MEINH, extracted in Pass 4
    {
        "table": "AFVC",
        "description": "Production Order Operations Master",
        "columns": [
            "AUFPL",   # Routing number (FK to AFKO.AUFPL)
            "APLZL",   # Internal counter for operations (unique within AUFPL)
            "VORNR",   # Operation / activity number        → "Oper./Act." in MCP1
            "LTXA1",   # Operation short text               → operation description
            "ARBID",   # Object ID of work centre           → resolves to CRHD.ARBPL
            "WERKS",   # Plant for this operation
            "STEUS",   # Control key (PP01 = internal op., PP03 = external, etc.)
            "LOEKZ",   # Operation deletion indicator
            "LARNT",   # Activity type (costing link)
        ],
        "options_fn": lambda: _build_in_options("AUFPL", _AUFPL_SET),
        "primary_keys": ["AUFPL", "APLZL"],
        "key_out": {
            "ARBID": "_ARBID_SET",
        },
    },

    # ── PASS 4 ─ AFVV : Standard Values / Planned Lead Times ───────────────
    # One row per operation; contains planned processing, setup, wait, move times.
    # These are the source for the "TgtLeadTm." column in MCP1.
    # Raw value + unit are both extracted; the post_process hook converts to DAY.
    #
    # NOTE — fields NOT present in AFVV per metadata:
    #   VORNR       → not stored in AFVV; join exclusively via APLZL
    #   RUESE       → correct field name is RSTZE  (unit of setup time)
    #   LIEGEZEIT_E → correct field name is LIGZE  (unit of wait time)
    #   TRANZZEIT_E → correct field name is TRAZE  (unit of move time)
    #   ABRUE_E     → correct field name is ARUZE  (unit of teardown time)
    #
    # Also available in AFVV (not in AFVC):
    #   MEINH  → operation unit of measure
    #   LMNGA  → total yield confirmed
    #   MGVRG  → operation quantity (planned)
    {
        "table": "AFVV",
        "description": "Operation Standard Values (Planned Lead Times)",
        "columns": [
            "AUFPL",   # Routing number (FK)
            "APLZL",   # Internal counter (join key to AFVC — VORNR is NOT in AFVV)
            "MEINH",   # Operation unit of measure          → op UoM (was missing from AFVC)
            "MGVRG",   # Operation quantity (planned)
            "LMNGA",   # Total yield confirmed
            "BEARZ",   # Processing time (raw numeric)     → TgtLeadTm. base
            "BEAZE",   # Unit of processing time (MIN/H/DAY)
            "RUEST",   # Setup time (raw numeric)
            "RSTZE",   # Unit of setup time                 (was incorrectly RUESE)
            "ABRUE",   # Teardown time (raw numeric)
            "ARUZE",   # Unit of teardown time              (was incorrectly ABRUE_E)
            "LIEGZ",   # Wait time used for scheduling (raw numeric)
            "LIGZE",   # Unit of wait time                  (was incorrectly LIEGEZEIT_E)
            "TRANZ",   # Move time used for scheduling (raw numeric)
            "TRAZE",   # Unit of move time                  (was incorrectly TRANZZEIT_E)
        ],
        "options_fn": lambda: _build_in_options("AUFPL", _AUFPL_SET),
        "primary_keys": ["AUFPL", "APLZL"],
    },

    # ── PASS 5 ─ AFRU : Actual Confirmations (Actual Lead Times) ───────────
    # One or more confirmation rows per operation.
    # ISMNW / ISMNU is the actual confirmed work → "ActLeadTm." in MCP1.
    # BUDAT is the posting date (matches the PDF date range filter on confirmation).
    # Reversed confirmations (STOKZ ≠ '') are excluded in the WHERE clause.
    {
        "table": "AFRU",
        "description": "Production Order Confirmations (Actual Lead Times)",
        "columns": [
            "AUFPL",   # Routing number (FK)
            "APLZL",   # Internal counter (join key to AFVC)
            "VORNR",   # Operation number
            "RUECK",   # Confirmation number (primary key of this confirmation)
            "RMZHL",   # Confirmation counter (sub-key within RUECK)
            "BUDAT",   # Posting date of confirmation       → date range filter
            "ISMNW",   # Actual confirmed work (raw numeric) → ActLeadTm. base
            "ISMNU",   # Unit of actual work (MIN/H/DAY)
            "IDAUR",   # Actual duration explicitly entered during confirmation
            "IDAUE",   # Unit of actual duration
            "GMNGA",   # Confirmed yield quantity
            "LMNGA",   # Confirmed scrap quantity
            "STOKZ",   # Reversal indicator (filter = '' → active confirmations)
            "ERSDA",   # Date record was created
        ],
        "options_fn": _build_afru_options,
        "primary_keys": ["AUFPL", "APLZL", "RUECK", "RMZHL"],
    },

    # ── PASS 6 ─ CRHD : Work Centre Master ─────────────────────────────────
    # Resolves internal ARBID to the human-readable ARBPL (work centre name).
    # Driven by _ARBID_SET collected from AFVC.
    # BEGDA / ENDDA are time-dependency bounds; we read all valid records and
    # let the consumer pick the relevant date slice if needed.
    {
        "table": "CRHD",
        "description": "Work Centre Master Data",
        "columns": [
            "OBJID",   # Internal object ID (= AFVC.ARBID)  → join key
            "OBJTY",   # Object type (normally 'A' for work centre)
            "ARBPL",   # Work centre name                   → "Work Center" in MCP1
            "WERKS",   # Plant of work centre
            "BEGDA",   # Valid-from date
            "ENDDA",   # Valid-to date
            "VERWE",   # Work centre category
            "LVORM",   # Deletion flag
            "STOBJ",   # Object number (for further status lookups if needed)
        ],
        "options_fn": lambda: _build_in_options("OBJID", _ARBID_SET),
        "primary_keys": ["OBJID", "BEGDA"],
    },
]


# ---------------------------------------------------------------------------
# Post-processing hooks
# ---------------------------------------------------------------------------

# Registry of post-process hooks (keyed by the string name stored in table defs)
_POST_PROCESS_REGISTRY: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# Extraction tuning
# ---------------------------------------------------------------------------
@dataclass
class ExtractionConfig:
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "5000")))
    max_buffer_width: int   = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries:      int   = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s:    float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every:     int   = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema:   str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA_ECC",   "sap_stg_ecc"))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_mcp1_staging_{run_id}.log")

    log = logging.getLogger(f"sap_mcp1_staging.{run_id}")
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


# ---------------------------------------------------------------------------
# Environment / connection helpers
# ---------------------------------------------------------------------------
def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is missing or empty.")
    return val


def build_sap_config() -> Dict[str, str]:
    return {
        "user":   _require_env("SAP_USER"),
        "passwd": _require_env("SAP_PASSWORD"),
        "ashost": _require_env("SAP_ASHOST"),
        "sysnr":  _require_env("SAP_SYSNR"),
        "client": _require_env("SAP_CLIENT"),
        "lang":   os.getenv("SAP_LANG", "EN"),
    }



# ---------------------------------------------------------------------------
# SAP connection wrapper — with RFC function fallback
# ---------------------------------------------------------------------------
class SAPConnection:
    """Wraps pyrfc.Connection and manages the active RFC read-table function."""

    def __init__(self, config: Dict[str, str], log: logging.Logger) -> None:
        self._cfg      = config
        self._log      = log
        self._fn_index = 0
        self.conn: Optional[Connection] = None
        self._connect()

    @property
    def rfc_function(self) -> str:
        return RFC_FUNCTION_PRIORITY[self._fn_index]

    def switch_rfc_function(self) -> bool:
        if self._fn_index < len(RFC_FUNCTION_PRIORITY) - 1:
            self._fn_index += 1
            self._log.warning("Switching RFC read function → %s", self.rfc_function)
            return True
        self._log.error(
            "All RFC read functions exhausted (%s). No further fallback.",
            RFC_FUNCTION_PRIORITY,
        )
        return False

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP (attempt %d/3)...", attempt)
                self.conn = Connection(**self._cfg)
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
        table:    str,
        fields:   List[str],
        options:  Optional[List[Dict]] = None,
        rowcount: int = 5000,
        rowskips: int = 0,
    ) -> Dict[str, Any]:
        self._log.debug(
            "read_table via %s: table=%s rowskips=%d rowcount=%d",
            self.rfc_function, table, rowskips, rowcount,
        )
        try:
            return self.conn.call(
                self.rfc_function,
                QUERY_TABLE = table,
                DELIMITER   = "|",
                FIELDS      = [{"FIELDNAME": f} for f in fields],
                OPTIONS     = options or [],
                ROWCOUNT    = rowcount,
                ROWSKIPS    = rowskips,
            )
        except (ABAPApplicationError, ABAPRuntimeError):
            self._log.exception("ABAP error reading %s via %s.", table, self.rfc_function)
            raise
        except CommunicationError:
            self._log.warning("Communication error — attempting reconnect...")
            self._connect()
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self._log.info("SAP connection closed.")
            self.conn = None


# ---------------------------------------------------------------------------
# Column metadata
# ---------------------------------------------------------------------------
class ColumnMetadata:
    _FALLBACK_WIDTH = 100

    def __init__(self, sap: SAPConnection, log: logging.Logger) -> None:
        self._sap = sap
        self._log = log

    def get_widths(self, table: str, columns: List[str]) -> Dict[str, int]:
        """Fetch column widths from DD03L; fall back to a default on error."""
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
                "[%s] DD03L lookup failed — using fallback width %d.",
                table, self._FALLBACK_WIDTH,
            )
            return {c: self._FALLBACK_WIDTH for c in columns}

        col_set = set(columns)
        widths:  Dict[str, int] = {}
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


# ---------------------------------------------------------------------------
# Column chunker (avoids exceeding RFC buffer width)
# ---------------------------------------------------------------------------
class ColumnChunker:
    def __init__(
        self,
        columns:    List[str],
        widths:     Dict[str, int],
        max_buffer: int,
        log:        logging.Logger,
    ) -> None:
        self._columns    = columns
        self._widths     = widths
        self._max_buffer = max_buffer
        self._log        = log

    def chunks(self) -> List[List[str]]:
        result:  List[List[str]] = []
        current: List[str]       = []
        used = 0

        for col in self._columns:
            w = self._widths.get(col, 100) + 1  # +1 for "|" delimiter
            if current and used + w > self._max_buffer:
                result.append(current)
                current = [col]
                used    = w
            else:
                current.append(col)
                used += w

        if current:
            result.append(current)

        self._log.info(
            "Column chunking: %d columns → %d chunk(s) (buffer limit %d).",
            len(self._columns), len(result), self._max_buffer,
        )
        return result


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


def _collect_keys(
    rows:    List[List[Any]],
    columns: List[str],
    key_out: Dict[str, str],
    log:     logging.Logger,
) -> None:
    current_module = sys.modules[__name__]
    col_idx = {col: i for i, col in enumerate(columns)}

    for col, set_name in key_out.items():
        if col not in col_idx:
            log.warning("key_out column '%s' not in columns list — skipped.", col)
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
                "Collected %d new '%s' keys (total %d) → %s",
                added, col, len(target_set), set_name,
            )


# ---------------------------------------------------------------------------
# SAP Batch Extractor
# ---------------------------------------------------------------------------
class BatchExtractor:
    def __init__(
        self,
        sap:     SAPConnection,
        table:   str,
        columns: List[str],
        chunks:  List[List[str]],
        options: List[Dict[str, str]],
        cfg:     ExtractionConfig,
        log:     logging.Logger,
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
                        options  = self._options,
                        rowcount = self._cfg.batch_size,
                        rowskips = offset,
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
                        "[%s] Chunk %d attempt %d/%d failed: %s — retrying in %.0fs...",
                        self._table, chunk_idx + 1, attempt,
                        self._cfg.max_retries, exc, self._cfg.retry_delay_s,
                    )
                    time.sleep(self._cfg.retry_delay_s)

        return [
            [merged[i].get(col, "") for col in self._columns]
            for i in sorted(merged)
        ]


# ---------------------------------------------------------------------------
# SQL table manager
# ---------------------------------------------------------------------------
class PostgresLoader:
    def __init__(
        self,
        cfg:      Dict[str, Any],
        schema:   str,
        prefix:   str,
        log:      logging.Logger,
    ) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]  = None

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
        DROP TABLE IF EXISTS {self._schema}.{stg_name};
        CREATE TABLE {self._schema}.{stg_name} (
            {col_defs},
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """
        self._log.info("Creating staging table %s.%s...", self._schema, stg_name)
        self._cursor.execute(ddl)
        self.conn.commit()
        self._log.info("Staging table %s.%s created.", self._schema, stg_name)
        return f'"{self._schema}"."{stg_name}"'

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [f'"{c.replace("-", "_").replace("/", "_").lower()}"' for c in columns]
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


# ---------------------------------------------------------------------------
# Per-table pipeline
# ---------------------------------------------------------------------------
def extract_table(
    table_def: Dict[str, Any],
    sap:       SAPConnection,
    loader:    PostgresLoader,
    cfg:       ExtractionConfig,
    log:       logging.Logger,
) -> Tuple[bool, int]:
    """Extract one SAP table into its staging counterpart. Returns (ok, row_count)."""
    table      = table_def["table"].upper()
    columns    = list(dict.fromkeys(table_def["columns"]))
    desc       = table_def.get("description", table)
    key_out    = table_def.get("key_out", {})
    pp_name    = table_def.get("post_process")
    post_proc  = _POST_PROCESS_REGISTRY.get(pp_name) if pp_name else None

    options_fn = table_def.get("options_fn")
    options    = options_fn() if callable(options_fn) else []

    log.info("=" * 60)
    log.info("Table  : %s  (%s)", table, desc)
    log.info("RFC fn : %s", sap.rfc_function)
    log.info("Syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Fields : %s", columns)
    if options:
        preview = " | ".join(r["TEXT"] for r in options[:4])
        suffix  = " | ..." if len(options) > 4 else ""
        log.info("OPTIONS (%d rows): %s%s", len(options), preview, suffix)
    if post_proc:
        log.info("Post-process : %s (time normalisation → DAY)", pp_name)

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    # Staging table is created with the base columns first;
    # derived columns are added after the first post_process call if present.
    stg_table = loader.create_staging_table(table, columns)

    extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log)

    rows_loaded       = 0
    rows_since_commit = 0
    effective_cols    = columns  # updated after first post_process call

    for batch in extractor.batches():
        if key_out:
            _collect_keys(batch, columns, key_out, log)

        if post_proc:
            batch, effective_cols = post_proc(batch, columns)

        loader.load_batch(stg_table, effective_cols, batch)
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
        "[%s] Done — %d rows in %.1fs (%.0f rows/s).",
        table, rows_loaded, elapsed, rate,
    )
    return True, rows_loaded


# ---------------------------------------------------------------------------
# Anchor-table probe: try all RFC fn × WHERE syntax combinations
# ---------------------------------------------------------------------------
def _probe_anchor_table(
    anchor_def: Dict[str, Any],
    sap:        SAPConnection,
    loader:     PostgresLoader,
    cfg:        ExtractionConfig,
    log:        logging.Logger,
) -> Tuple[bool, int]:
    """
    Try up to four combinations to extract AUFK (the anchor table):

        Probe 1  BBP_RFC_READ_TABLE + IN  syntax
        Probe 2  BBP_RFC_READ_TABLE + OR  syntax
        Probe 3  RFC_READ_TABLE     + IN  syntax
        Probe 4  RFC_READ_TABLE     + OR  syntax

    Stops on the first combination that returns at least one row.
    """
    global _USE_IN_OPERATOR

    table = anchor_def["table"].upper()

    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))
        attempts.append((False, fn_idx))

    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap._fn_index    = fn_idx

        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info(
            "─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        _AUFNR_SET.clear()

        try:
            ok, count = extract_table(anchor_def, sap, loader, cfg, log)
        except Exception:
            log.warning("Probe %d raised an exception — trying next combination.", probe_num)
            continue

        if count > 0:
            log.info(
                "✓ Anchor probe succeeded on attempt %d — "
                "RFC=%s | syntax=%s | rows=%d",
                probe_num, sap.rfc_function, syntax_label, count,
            )
            return ok, count

        log.warning(
            "Probe %d returned 0 rows (RFC=%s | syntax=%s).",
            probe_num, sap.rfc_function, syntax_label,
        )

    log.error(
        "All %d anchor probes returned 0 rows for %s. "
        "Verify plant(s) %s, order types %s, and date range %s–%s in SAP.",
        len(attempts), table, PLANTS, ORDER_TYPES or "ALL",
        DATE_FROM or "unbounded", DATE_TO or "unbounded",
    )
    return True, 0


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP MCP1 Production Order Operations Staging Pipeline  [run: %s]", run_id)
    log.info("Plants             : %s", PLANTS)
    log.info("Order types        : %s", ORDER_TYPES if ORDER_TYPES else "ALL")
    log.info("Order date from    : %s", DATE_FROM        if DATE_FROM        else "unbounded")
    log.info("Order date to      : %s", DATE_TO          if DATE_TO          else "unbounded")
    log.info("Conf. date from    : %s", CONF_DATE_FROM   if CONF_DATE_FROM   else "unbounded")
    log.info("Conf. date to      : %s", CONF_DATE_TO     if CONF_DATE_TO     else "unbounded")
    log.info("Language           : %s", LANGUAGE)
    log.info("Minutes/day        : %.0f  (time normalisation factor)", _MINUTES_PER_DAY)
    log.info("Staging prefix     : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables             : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority       : %s", RFC_FUNCTION_PRIORITY)
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
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                if tbl_def.get("is_anchor"):
                    ok, count = _probe_anchor_table(tbl_def, sap, loader, cfg, log)
                    if count == 0:
                        log.warning(
                            "Anchor table (AUFK) returned 0 rows after all probes. "
                            "Child tables will have empty WHERE sets — they will be skipped."
                        )
                else:
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
    log.info("Active RFC function : %s", RFC_FUNCTION_PRIORITY[sap._fn_index] if sap else "N/A")
    log.info("Active WHERE syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Key sets collected:")
    log.info("    AUFNR : %d production order numbers", len(_AUFNR_SET))
    log.info("    AUFPL : %d routing numbers",          len(_AUFPL_SET))
    log.info("    ARBID : %d work-centre object IDs",   len(_ARBID_SET))
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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)