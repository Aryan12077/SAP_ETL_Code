"""
SAP COOIS — Production Order Information System Staging Extractor
=================================================================
Extracts columns matching the COOIS (Production Order Information System)
transaction report from SAP ECC PP (Production Planning) tables and loads
them into SQL Server staging tables.

COOIS Report Column → SAP Field Mapping
----------------------------------------
Plant                → AUFK.WERKS  (order plant)
Created on           → AUFK.ERDAT  (order creation date)
Order                → AUFK.AUFNR  (production order number)
Material             → AFPO.MATNR  (material number produced)
Material description → MAKT.MAKTX  (material short text)
Target qty           → AFPO.PSMNG  (planned order quantity)
Del. qty             → AFPO.WEMNG  (quantity of goods received / delivered)
Unit                 → AFPO.MEINS  (base unit of measure)
Bsc start            → AFKO.GSTRP  (basic start date)
Basic fin.           → AFKO.GLTRP  (basic finish date)
System Status        → JEST.STAT + TJ02T.TXT04 (via AUFK.OBJNR)

Source tables (extraction order):
  Pass 1 — AUFK : Production order master (anchor). Collects AUFNR & OBJNR.
  Pass 2 — AFKO : Order header scheduling data. Collects dates.
  Pass 3 — AFPO : Order item (material, quantities, UoM).
  Pass 4 — MAKT : Material descriptions (short texts).
  Pass 5 — JEST : Individual object statuses (active only).
  Pass 6 — TJ02T: System status texts (short + long).

Fallback behaviour (mirrors IH08 extractor pattern)
----------------------------------------------------
  WHERE syntax  : First attempts IN (...) operator. On zero rows from the
                  anchor table (AUFK), retries with OR-chained = clauses.
  RFC function  : Tries BBP_RFC_READ_TABLE first, then RFC_READ_TABLE.
  Probe order   : Four combinations are tried in sequence:
                    1. BBP_RFC_READ_TABLE  + IN  syntax
                    2. BBP_RFC_READ_TABLE  + OR  syntax
                    3. RFC_READ_TABLE      + IN  syntax
                    4. RFC_READ_TABLE      + OR  syntax
                  The winning combination is used for all subsequent tables.

Date filter
-----------
  AUFK.ERDAT is used to restrict extraction to the chosen period.
  Set DATE_FROM / DATE_TO as YYYYMMDD strings (or env vars).
  Leave DATE_FROM empty ("") to skip the lower date boundary.
  Leave DATE_TO   empty ("") to skip the upper date boundary.
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

# Plant filter — AUFK.WERKS. Use [""] to match blank plant or all plants
# if your RFC variant ignores empty strings.  Set e.g. ["D001"] to restrict.
PLANTS: List[str] = ["1202"]

# Order type filter — AUFK.AUART.  Leave [] for all types.
# Common PP types: PP01 = standard production, PP02 = repetitive, etc.
ORDER_TYPES: List[str] = []

# Date range on AUFK.ERDAT (order creation date) — format YYYYMMDD
# Set both to "" to disable the date filter entirely.
DATE_FROM: str = os.getenv("DATE_FROM", "20251001")   # e.g. "20260101"
DATE_TO:   str = os.getenv("DATE_TO",   "20251231")   # e.g. "20260228"

# Language key for MAKT and TJ02T text lookups
LANGUAGE: str = "E"

# Prefix used when naming staging tables: STG_SAP_COOIS_<TABLE>
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "COOIS")

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_AUFNR_SET: Set[str] = set()   # production order numbers  (AUFK → child tables)
_OBJNR_SET: Set[str] = set()   # object numbers            (AUFK → JEST)
_MATNR_SET: Set[str] = set()   # material numbers          (AFPO → MAKT)
_STAT_SET:  Set[str] = set()   # active status codes       (JEST → TJ02T)

# Active WHERE-syntax mode.
# True  → use IN ('a','b','c')
# False → use field='a' OR field='b' OR ...
_USE_IN_OPERATOR: bool = True

# ---------------------------------------------------------------------------
# WHERE clause builders
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
    """
    Build RFC OPTIONS rows using OR-chained = clauses, chunked to 72 chars.

    Example:
        ( AUFNR = '000001' OR AUFNR = '000002' )
        AND LOEKZ = ''
    """
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
    """
    Build RFC OPTIONS rows using IN (...) clauses, chunked to 72 chars.

    Automatically falls back to OR-syntax when _USE_IN_OPERATOR is False.
    """
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


def _build_aufk_options() -> List[Dict[str, str]]:
    """
    Build WHERE options for the anchor table AUFK.

    Filters applied (all combined with AND):
      - WERKS IN / = (plant list)
      - AUART IN / = (order type list, if specified)
      - ERDAT >= DATE_FROM (if specified)
      - ERDAT <= DATE_TO   (if specified)
      - LOEKZ = ''         (exclude deletion-flagged orders)
    """
    if _USE_IN_OPERATOR:
        # --- IN syntax ---
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

    # --- OR-chained syntax ---
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


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
#
# Each entry drives one complete extract-and-load pipeline pass.
# Keys:
#   table        → SAP transparent table name
#   description  → human-readable label (for logs)
#   columns      → fields to read (must be a subset of the table's columns)
#   options_fn   → callable returning RFC OPTIONS rows (WHERE clause)
#   primary_keys → used only for documentation / future dedup logic
#   key_out      → {SAP_COLUMN: "_GLOBAL_SET_NAME"} — populates runtime key sets
#   is_anchor    → True on the first table; triggers the RFC/syntax probe
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ── PASS 1 ─ AUFK : Production Order Master (anchor) ──────────────────
    # Provides order-level attributes: plant, dates, order type, status link.
    {
        "table": "AUFK",
        "description": "Production Order Master",
        "columns": [
            "AUFNR",   # Production order number
            "AUART",   # Order type
            "WERKS",   # Plant
            "ERDAT",   # Created on
            "KTEXT",   # Order description / short text
            "OBJNR",   # Object number  → JEST status lookup
            "LOEKZ",   # Deletion flag
            "PRCTR",   # Profit center
            "KOSTL",   # Cost center
        ],
        "options_fn": _build_aufk_options,
        "primary_keys": ["AUFNR"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
            "OBJNR": "_OBJNR_SET",
        },
        "is_anchor": True,
    },

    # ── PASS 2 ─ AFKO : Order Header Scheduling Data ───────────────────────
    # One row per production order; provides basic start/finish dates and
    # the total order quantity (which is the same as AFPO.PSMNG for simple
    # single-item orders but stored at header level here).
    {
        "table": "AFKO",
        "description": "Order Header Scheduling Data",
        "columns": [
            "AUFNR",   # Order number (FK to AUFK)
            "GSTRP",   # Basic start date         → "Bsc start" in COOIS
            "GLTRP",   # Basic finish date         → "Basic fin." in COOIS
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
    },

    # ── PASS 3 ─ AFPO : Order Item (Material + Quantities) ─────────────────
    # Production orders may theoretically have multiple items (POSNR), but
    # in standard PP there is always item 0001.  We read all items and let
    # the consumer decide how to handle multi-item orders.
    {
        "table": "AFPO",
        "description": "Production Order Item (Material & Quantities)",
        "columns": [
            "AUFNR",   # Order number (FK)
            "POSNR",   # Item number
            "MATNR",   # Material number           → MAKT description lookup
            "PSMNG",   # Planned (target) quantity → "Target qty" in COOIS
            "WEMNG",   # GR quantity (delivered)   → "Del. qty"   in COOIS
            "MEINS",   # Base unit of measure       → "Unit"       in COOIS
            "DWERK",   # Receiving / production plant
            "LGORT",   # Storage location
            "VERID",   # Production version
            "LTRMP",   # Delivery date from planned order
            "LTRMI",   # Actual delivery / finish date
            "ELIKZ",   # Delivery-completed indicator
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR", "POSNR"],
        "key_out": {
            "MATNR": "_MATNR_SET",
        },
    },

    # ── PASS 4 ─ MAKT : Material Short Texts ───────────────────────────────
    # Driven by _MATNR_SET populated from AFPO.
    {
        "table": "MAKT",
        "description": "Material Descriptions",
        "columns": [
            "MATNR",   # Material number (FK)
            "SPRAS",   # Language key
            "MAKTX",   # Material description (short text) → "Material description"
        ],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR", "SPRAS"],
    },

    # ── PASS 5 ─ JEST : Individual Object Status (active only) ─────────────
    # Driven by _OBJNR_SET from AUFK.OBJNR.
    # INACT = '' means the status flag is currently active (not deactivated).
    {
        "table": "JEST",
        "description": "Order Status (Active)",
        "columns": [
            "OBJNR",   # Object number (FK to AUFK.OBJNR)
            "STAT",    # Status code (e.g. I0002 = REL, I0009 = TECO)
            "INACT",   # Inactive flag — we filter INACT = '' (active)
        ],
        "options_fn": lambda: _build_in_options(
            "OBJNR", _OBJNR_SET, extra="INACT = ''"
        ),
        "primary_keys": ["OBJNR", "STAT"],
        "key_out": {
            "STAT": "_STAT_SET",
        },
    },

    # ── PASS 6 ─ TJ02T : System Status Texts ───────────────────────────────
    # Translates numeric STAT codes (e.g. I0002) to readable text (REL, TECO …).
    # TXT04 = 4-char short code shown in COOIS "System Status" column.
    # TXT30 = 30-char long description.
    {
        "table": "TJ02T",
        "description": "System Status Texts",
        "columns": [
            "ISTAT",   # System status code (= JEST.STAT)
            "SPRAS",   # Language key
            "TXT04",   # Short status text (e.g. REL, TECO, DLV)  → COOIS display
            "TXT30",   # Long status text (e.g. Released, Technically completed)
        ],
        "options_fn": lambda: _build_in_options(
            "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["ISTAT", "SPRAS"],
    },
]

# ---------------------------------------------------------------------------
# Extraction tuning
# ---------------------------------------------------------------------------
@dataclass
class ExtractionConfig:
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "10000")))
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
    log_file = os.path.join(log_dir, f"sap_coois_staging_{run_id}.log")

    log = logging.getLogger(f"sap_coois_staging.{run_id}")
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
    """
    Wraps a pyrfc Connection and manages the active RFC read-table function.

    Fallback order is defined by RFC_FUNCTION_PRIORITY.  Call
    switch_rfc_function() to advance to the next candidate.
    """

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
# Column chunker
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
    table   = table_def["table"].upper()
    columns = list(dict.fromkeys(table_def["columns"]))
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

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

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table = loader.create_staging_table(table, columns)
    extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log)

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
    Try to extract the anchor table (AUFK) using up to four combinations:

        Probe 1  BBP_RFC_READ_TABLE + IN  syntax
        Probe 2  BBP_RFC_READ_TABLE + OR  syntax
        Probe 3  RFC_READ_TABLE     + IN  syntax
        Probe 4  RFC_READ_TABLE     + OR  syntax

    Stops as soon as a non-zero row count is achieved.
    If all probes return 0 rows the filter criteria returned no data.
    """
    global _USE_IN_OPERATOR

    table = anchor_def["table"].upper()

    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))   # IN  syntax
        attempts.append((False, fn_idx))   # OR  syntax

    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap._fn_index    = fn_idx

        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info(
            "─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        # Clear anchor key sets so a retry starts fresh
        _AUFNR_SET.clear()
        _OBJNR_SET.clear()

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
    log.info("SAP COOIS Production Order Staging Pipeline  [run: %s]", run_id)
    log.info("Plants         : %s", PLANTS)
    log.info("Order types    : %s", ORDER_TYPES if ORDER_TYPES else "ALL")
    log.info("Date from      : %s", DATE_FROM   if DATE_FROM   else "unbounded")
    log.info("Date to        : %s", DATE_TO     if DATE_TO     else "unbounded")
    log.info("Language       : %s", LANGUAGE)
    log.info("Staging prefix : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority   : %s", RFC_FUNCTION_PRIORITY)
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
                            "Child tables will be skipped (no order keys to filter on)."
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
    log.info("    OBJNR : %d object numbers",           len(_OBJNR_SET))
    log.info("    MATNR : %d material numbers",         len(_MATNR_SET))
    log.info("    STAT  : %d active status codes",      len(_STAT_SET))
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