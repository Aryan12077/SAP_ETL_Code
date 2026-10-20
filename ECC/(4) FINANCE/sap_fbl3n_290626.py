"""
SAP FBL3N G/L Account Line Items — Staging Extractor
=====================================================
Extracts columns matching the FBL3N (G/L Account Line Items) transaction
report from SAP ECC FI accounting tables and loads them into PostgreSQL
staging tables.

FBL3N Report Column → SAP Field Mapping
-----------------------------------------
St           (Item Status)               → Derived: BSIS rows = 'Open', BSAS rows = 'Cleared'
Clrng doc.   (Clearing Document)         → BSIS/BSAS.AUGBL
Company Code                             → BSIS/BSAS.BUKRS
Reference    (Ref. Doc. Number)          → BSIS/BSAS.XBLNR
Assignment                               → BSIS/BSAS.ZUONR
DocumentNo   (Accounting Document)       → BSIS/BSAS.BELNR
Type         (Document Type)             → BSIS/BSAS.BLART
Doc. Date    (Document Date)             → BSIS/BSAS.BLDAT
Pstng Date   (Posting Date)             → BSIS/BSAS.BUDAT
PK           (Posting Key)              → BSIS/BSAS.BSCHL
Amount in local cur.                     → BSIS/BSAS.DMBTR
Cost Ctr     (Cost Center)              → BSIS/BSAS.KOSTL
Profit Ctr                               → BSIS/BSAS.PRCTR
Amount in doc. curr.                     → BSIS/BSAS.WRBTR
Curr.        (Document Currency)         → BSIS/BSAS.WAERS
Due net      (Net Due Date)             → BSIS/BSAS.ZFBDT
Text         (Item Text)                → BSIS/BSAS.SGTXT
Year         (Fiscal Year)              → BSIS/BSAS.GJAHR
Year/month   (Fiscal Year + Period)     → Derived: GJAHR + MONAT

Source tables (extraction order):
  Pass 1 — BSIS : G/L Secondary Index — Open Items (anchor).
            Filtered by BUDAT date range + BUKRS company code.
            Collects _BUKRS_SET for downstream T001 lookup.
  Pass 2 — BSAS : G/L Secondary Index — Cleared Items.
            Same BUDAT + BUKRS filter as BSIS.
  Pass 3 — T001 : Company Code Master. Driven by _BUKRS_SET from BSIS/BSAS.

Anchor table concept (mirrors COOIS extractor pattern)
------------------------------------------------------
  BSIS is the anchor table. The probe attempts to extract BSIS using up to
  four RFC-function × WHERE-syntax combinations. The first combination that
  yields rows is locked in for all subsequent tables. No re-probing occurs.

  Probe order:
    1. BBP_RFC_READ_TABLE + IN  syntax
    2. BBP_RFC_READ_TABLE + OR  syntax
    3. RFC_READ_TABLE     + IN  syntax
    4. RFC_READ_TABLE     + OR  syntax

  The winning probe performs a FULL extraction of BSIS (not merely a row-
  existence check), so no data is fetched twice.

Date filter
-----------
  BUDAT (posting date) is used to restrict extraction.
  Set DATE_FROM / DATE_TO as YYYYMMDD strings (or env vars).

Filters retained
----------------
  - BUDAT posting date range  (DATE_FROM / DATE_TO)
  - BUKRS company code list   (COMPANY_CODES)

All other filters (G/L account, document type, open/cleared toggle) have
been removed to keep WHERE clauses tight and extraction fast.

Design note — Purchase Document (EBELN):
  BSIS and BSAS are secondary index tables and do NOT carry EBELN.
  EBELN lives in BSEG. To populate Purch.Doc. seen in FBL3N:
    1. Extract BSIS/BSAS as below.
    2. In SQL, build a distinct BELNR+GJAHR key set.
    3. Run a targeted BSEG read keyed by BELNR IN (...), batching ~1 000
       keys at a time, selecting only EBELN + KUNNR/LIFNR fields.
"""

from __future__ import annotations

import csv
import io
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
from datetime import datetime, timedelta
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
# CONFIGURATION — edit this section only
# ---------------------------------------------------------------------------

# Posting date range applied to BSIS.BUDAT / BSAS.BUDAT  (format YYYYMMDD)
# Leave DATE_FROM empty ("") to skip the lower date boundary.
# Leave DATE_TO   empty ("") to skip the upper date boundary.
DATE_FROM: str = os.getenv("DATE_FROM", "20260601")
DATE_TO:   str = os.getenv("DATE_TO",   "20260629")

# Company codes — applied to BUKRS.  Use [""] to match blank or all codes.
COMPANY_CODES: List[str] = ["1200"]

# Prefix used when naming staging tables: stg_sap_fbl3n_<TABLE>
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "FBL3N_v2")

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_BUKRS_SET: Set[str] = set()   # Company codes (BSIS/BSAS → T001)

# Active WHERE-syntax mode.
# True  → use IN ('a','b','c')
# False → use field='a' OR field='b' OR ...
_USE_IN_OPERATOR: bool = True

# Date partition overrides (set per-partition in extract_table)
_DATE_OVERRIDE_FROM: str = ""
_DATE_OVERRIDE_TO:   str = ""

# Fast clean mode flag (set from ExtractionConfig at startup)
_FAST_CLEAN_MODE: bool = True

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
        ( BUKRS = '1200' OR BUKRS = '1300' )
        AND WAERS = 'USD'
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


def _build_gl_line_item_options() -> List[Dict[str, str]]:
    """
    Build WHERE options for BSIS / BSAS.

    Filters applied (all combined with AND):
      - BUDAT >= DATE_FROM (if specified)
      - BUDAT <= DATE_TO   (if specified)
      - BUKRS IN / = (company code list)

    Supports both IN and OR WHERE syntax.
    Uses date partition overrides when set by extract_table.
    """
    date_from = _DATE_OVERRIDE_FROM or DATE_FROM
    date_to   = _DATE_OVERRIDE_TO   or DATE_TO

    if _USE_IN_OPERATOR:
        # --- IN syntax ---
        codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
        where = f"BUKRS IN ({codes})"
        if date_from:
            where += f" AND BUDAT GE '{date_from}'"
        if date_to:
            where += f" AND BUDAT LE '{date_to}'"
        return _format_options(where)

    # --- OR-chained syntax ---
    options: List[Dict[str, str]] = []

    code_clauses = " OR ".join(f"BUKRS = '{c}'" for c in COMPANY_CODES)
    options.append({"TEXT": f"( {code_clauses} )"})

    if date_from:
        options.append({"TEXT": f"AND BUDAT GE '{date_from}'"})
    if date_to:
        options.append({"TEXT": f"AND BUDAT LE '{date_to}'"})

    return options


# ---------------------------------------------------------------------------
# Common column list shared by BSIS and BSAS
# ---------------------------------------------------------------------------
# IMPORTANT — BSIS and BSAS are SECONDARY INDEX tables. They store a limited
# subset of BSEG fields for open-item-managed G/L accounts. Fields that exist
# only in BSEG (e.g. AUGGJ, DMBE2, PROJK, FKBER, SEGMENT) are intentionally
# excluded here to avoid FIELD_NOT_VALID RFC errors.
# ---------------------------------------------------------------------------
_GL_LINE_ITEM_COLUMNS: List[str] = [
    # --- FBL3N visible columns ---
    "BUKRS",      # Company Code
    "HKONT",      # G/L Account Number           (G/L Acct)
    "BELNR",      # Accounting Document Number    (DocumentNo)
    "GJAHR",      # Fiscal Year                  (Year)
    "BUZEI",      # Line Item Number within Document
    "MONAT",      # Fiscal Period                (for Year/month derivation)
    "BUDAT",      # Posting Date                 (Pstng Date)
    "BLDAT",      # Document Date                (Doc. Date)
    "BLART",      # Document Type                (Type)
    "XBLNR",      # Reference Document Number    (Reference)
    "ZUONR",      # Assignment Number            (Assignment)
    "BSCHL",      # Posting Key                  (PK)
    "SHKZG",      # Debit/Credit Indicator       ('S'=Debit / 'H'=Credit)
    "DMBTR",      # Amount in Local Currency     (Amount in local cur.)
    "WRBTR",      # Amount in Document Currency  (Amount in doc. curr.)
    "WAERS",      # Document Currency            (Curr. / LCurr)
    "SGTXT",      # Item Text                    (Text)
    "ZFBDT",      # Baseline Date                (Due net basis)
    "KOSTL",      # Cost Center                  (Cost Ctr)
    "PRCTR",      # Profit Center                (Profit Ctr)
    # --- Additional confirmed BSIS/BSAS secondary index fields ---
    "GSBER",      # Business Area
    "MWSKZ",      # Tax Code
    "AUGDT",      # Clearing Date               (blank in BSIS; populated in BSAS)
    "AUGBL",      # Clearing Document Number    (blank in BSIS; populated in BSAS)
    "MWSTS",      # Tax Amount in Local Currency
    "WMWST",      # Tax Amount in Document Currency
]


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
#
# Each entry drives one complete extract-and-load pipeline pass.
# Keys:
#   table           → SAP transparent table name
#   description     → human-readable label (for logs)
#   columns         → fields to read (subset of the table's columns)
#   options_fn      → callable returning RFC OPTIONS rows (WHERE clause)
#   primary_keys    → for documentation / future dedup logic
#   key_out         → {SAP_COLUMN: "_GLOBAL_SET_NAME"} — populates runtime key sets
#   is_anchor       → True on BSIS; triggers the RFC/syntax probe
#   date_partitioned → True on BSIS/BSAS; enables date window partitioning
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ── PASS 1 ─ BSIS : Open G/L Line Items (anchor) ──────────────────────
    # Filtered by BUDAT date range + BUKRS company code.
    # Collects _BUKRS_SET to drive T001 in Pass 3.
    # The anchor probe (four RFC/syntax combos) is run here; the winning
    # combination is re-used for all subsequent tables.
    {
        "table": "BSIS",
        "description": "G/L Account Open Line Items (BUDAT + BUKRS filter)",
        "columns": _GL_LINE_ITEM_COLUMNS,
        "options_fn": _build_gl_line_item_options,
        "primary_keys": ["BUKRS", "HKONT", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {
            "BUKRS": "_BUKRS_SET",
        },
        "is_anchor": True,
        "date_partitioned": True,
    },

    # ── PASS 2 ─ BSAS : Cleared G/L Line Items ────────────────────────────
    # Same BUDAT + BUKRS filter as BSIS.
    # AUGDT and AUGBL will be populated here (clearing info).
    # Uses the RFC function and WHERE syntax determined by the BSIS probe.
    {
        "table": "BSAS",
        "description": "G/L Account Cleared Line Items (BUDAT + BUKRS filter)",
        "columns": _GL_LINE_ITEM_COLUMNS,
        "options_fn": _build_gl_line_item_options,
        "primary_keys": ["BUKRS", "HKONT", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {
            "BUKRS": "_BUKRS_SET",
        },
        "date_partitioned": True,
    },

    # ── PASS 3 ─ T001 : Company Code Master ───────────────────────────────
    # Driven by _BUKRS_SET populated from BSIS + BSAS.
    # Provides company name and local currency for reporting joins.
    {
        "table": "T001",
        "description": "Company Code Master — codes seen in this G/L line item set",
        "columns": [
            "BUKRS",   # Company Code
            "BUTXT",   # Company Name
            "ORT01",   # City
            "LAND1",   # Country
            "WAERS",   # Local Currency Key
            "KTOPL",   # Chart of Accounts
            "PERIV",   # Fiscal Year Variant
        ],
        "options_fn": lambda: _build_in_options("BUKRS", _BUKRS_SET),
        "primary_keys": ["BUKRS"],
        "date_partitioned": False,
    },
]


# ---------------------------------------------------------------------------
# Extraction tuning  (override via .env)
# ---------------------------------------------------------------------------
@dataclass
class ExtractionConfig:
    # Batch size matched to COOIS for RFC stability and throughput.
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "10000")))
    max_buffer_width: int   = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries:      int   = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s:    float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    # Commit every 100 000 rows (same as COOIS) to minimise PostgreSQL overhead.
    commit_every:     int   = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema:   str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA_ECC",   "sap_stg_ecc"))
    # Performance: UNLOGGED skips WAL writes; COPY is faster than INSERT VALUES.
    use_unlogged_tables: bool = field(default_factory=lambda: os.getenv("USE_UNLOGGED_TABLES", "True").lower() in ("true", "1", "yes"))
    use_copy_loader:     bool = field(default_factory=lambda: os.getenv("USE_COPY_LOADER",     "True").lower() in ("true", "1", "yes"))
    # Date partitioning splits large date ranges into N windows (reduces per-RFC row set).
    date_partitions:  int   = field(default_factory=lambda: int(os.getenv("DATE_PARTITIONS",   "1")))
    # Fast clean skips NUL-byte stripping for maximum throughput on clean SAP data.
    fast_clean_mode:  bool  = field(default_factory=lambda: os.getenv("FAST_CLEAN_MODE", "True").lower() in ("true", "1", "yes"))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")
        if self.date_partitions < 1:
            raise ValueError("date_partitions must be >= 1.")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_fbl3n_staging_{run_id}.log")

    log = logging.getLogger(f"sap_fbl3n_staging.{run_id}")
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


def build_pg_config() -> Dict[str, Any]:
    return {
        "host":     _require_env("PG_HOST"),
        "port":     os.getenv("PG_PORT", "5432"),
        "database": _require_env("PG_DATABASE"),
        "user":     _require_env("PG_USER"),
        "password": _require_env("PG_PASSWORD"),
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
        self._sap   = sap
        self._log   = log
        self._cache: Dict[str, Dict[str, int]] = {}

    def get_widths(self, table: str, columns: List[str]) -> Dict[str, int]:
        if table in self._cache:
            self._log.info("[%s] Returning DD03L widths from cache.", table)
            return self._cache[table]

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
        self._cache[table] = widths
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
    if _FAST_CLEAN_MODE:
        return value.strip()
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
# SAP Batch Extractor — with early stop and adaptive batch sizing
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
        offset             = 0
        total              = 0
        current_batch_size = self._cfg.batch_size

        while True:
            t_start = time.time()
            batch   = self._fetch_offset(offset, current_batch_size)
            fetch_time = time.time() - t_start

            if not batch:
                self._log.info(
                    "[%s] No more rows. Total extracted: %d", self._table, total
                )
                break

            total  += len(batch)
            offset += current_batch_size

            self._log.info(
                "[%s] Fetched %d rows (running total: %d, time: %.1fs).",
                self._table, len(batch), total, fetch_time,
            )
            yield batch

            # Early stop on partial batch — avoids a wasted RFC call at the tail.
            if len(batch) < current_batch_size:
                self._log.info(
                    "[%s] Early stop: partial batch (%d < %d).",
                    self._table, len(batch), current_batch_size,
                )
                break

            # Adaptive batch sizing: halve if a single fetch takes > 120 s.
            if fetch_time > 120 and current_batch_size > 100:
                current_batch_size = max(100, current_batch_size // 2)
                self._log.warning(
                    "[%s] Fetch took %.1fs; halving batch size → %d.",
                    self._table, fetch_time, current_batch_size,
                )

    def _fetch_offset(self, offset: int, batch_size: int) -> List[List[Any]]:
        merged: Dict[int, Dict[str, str]] = {}

        for chunk_idx, chunk in enumerate(self._chunks):
            for attempt in range(1, self._cfg.max_retries + 2):
                try:
                    result = self._sap.read_table(
                        self._table,
                        chunk,
                        options  = self._options,
                        rowcount = batch_size,
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
# PostgreSQL Loader — with COPY and UNLOGGED table support
# ---------------------------------------------------------------------------
class PostgresLoader:
    def __init__(
        self,
        cfg:          Dict[str, Any],
        schema:       str,
        prefix:       str,
        log:          logging.Logger,
        use_unlogged: bool = True,
        use_copy:     bool = True,
    ) -> None:
        self._cfg          = cfg
        self._schema       = schema
        self._prefix       = prefix
        self._log          = log
        self._use_unlogged = use_unlogged
        self._use_copy     = use_copy
        self.conn          = None
        self.cur           = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL...")
        try:
            self.conn = psycopg2.connect(**self._cfg)
            self.cur  = self.conn.cursor()
            self._log.info("PostgreSQL connection established.")
        except Exception:
            self._log.exception("PostgreSQL connection failed.")
            raise

    def _ensure_connection(self) -> None:
        try:
            if self.conn is None or self.conn.closed:
                self._log.warning("PostgreSQL connection lost. Reconnecting...")
                self.connect()
                return
            if self.cur is None or self.cur.closed:
                self._log.warning("PostgreSQL cursor closed. Recreating...")
                self.cur = self.conn.cursor()
        except Exception:
            self._log.warning("PostgreSQL connection check failed. Reconnecting...")
            self.connect()

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        self._ensure_connection()
        stg_name  = f"stg_sap_{self._prefix.lower()}_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_defs  = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
        tbl_type  = "UNLOGGED TABLE" if self._use_unlogged else "TABLE"

        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")
        self.cur.execute(f"""
        DROP TABLE IF EXISTS {self._schema}.{stg_name};
        CREATE {tbl_type} {self._schema}.{stg_name} (
            {col_defs},
            load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """)
        self.conn.commit()
        self._log.info(
            "Staging table %s.%s created (%s).", self._schema, stg_name, tbl_type
        )
        return f'"{self._schema}"."{stg_name}"'

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        if not rows:
            return
        self._ensure_connection()
        if self._use_copy:
            self._load_batch_copy(stg_table, columns, rows)
        else:
            self._load_batch_values(stg_table, columns, rows)

    def _load_batch_values(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        safe_cols = [f'"{c.replace("-", "_").replace("/", "_").lower()}"' for c in columns]
        query     = f"INSERT INTO {stg_table} ({', '.join(safe_cols)}) VALUES %s"
        try:
            extras.execute_values(self.cur, query, rows, page_size=500)
        except psycopg2.OperationalError:
            self._log.warning("PostgreSQL lost during INSERT. Reconnecting...")
            self.connect()
            extras.execute_values(self.cur, query, rows, page_size=500)

    def _load_batch_copy(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_list  = ", ".join(safe_cols)
        copy_sql  = f"COPY {stg_table} ({col_list}) FROM STDIN WITH (FORMAT csv)"

        buf = io.StringIO()
        writer = csv.writer(buf)
        for row in rows:
            writer.writerow([v if v else "" for v in row])
        buf.seek(0)

        try:
            self.cur.copy_expert(copy_sql, buf)
        except psycopg2.OperationalError:
            self._log.warning("PostgreSQL lost during COPY. Reconnecting...")
            self.connect()
            buf.seek(0)
            self.cur.copy_expert(copy_sql, buf)

    def commit(self) -> None:
        if self.conn and not self.conn.closed:
            self.conn.commit()

    def close(self) -> None:
        if self.cur:  self.cur.close()
        if self.conn: self.conn.close()
        self._log.info("PostgreSQL connection closed.")


# ---------------------------------------------------------------------------
# Date partition helper
# ---------------------------------------------------------------------------
def _split_date_range(
    date_from: str, date_to: str, partitions: int
) -> List[Tuple[str, str]]:
    """Split a YYYYMMDD date range into N equal-day-count windows."""
    if partitions <= 1 or not date_from or not date_to:
        return [(date_from, date_to)]

    d_from     = datetime.strptime(date_from, "%Y%m%d")
    d_to       = datetime.strptime(date_to,   "%Y%m%d")
    total_days = (d_to - d_from).days

    if total_days <= 0:
        return [(date_from, date_to)]

    chunk_days = max(1, total_days // partitions)
    windows:   List[Tuple[str, str]] = []
    current    = d_from

    for i in range(partitions):
        win_start = current
        win_end   = d_to if i == partitions - 1 else min(
            current + timedelta(days=chunk_days - 1), d_to
        )
        windows.append((
            win_start.strftime("%Y%m%d"),
            win_end.strftime("%Y%m%d"),
        ))
        current = win_end + timedelta(days=1)
        if current > d_to:
            break

    return windows


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
    global _DATE_OVERRIDE_FROM, _DATE_OVERRIDE_TO

    table   = table_def["table"].upper()
    columns = list(dict.fromkeys(table_def["columns"]))
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

    log.info("=" * 60)
    log.info("Table  : %s  (%s)", table, desc)
    log.info("RFC fn : %s", sap.rfc_function)
    log.info("Syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Fields : %s", columns)

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table = loader.create_staging_table(table, columns)

    is_date_partitioned = table_def.get("date_partitioned", False)
    if cfg.date_partitions > 1 and is_date_partitioned:
        windows = _split_date_range(DATE_FROM, DATE_TO, cfg.date_partitions)
        log.info(
            "[%s] Date partitioning: %d window(s) over %s → %s",
            table, len(windows), DATE_FROM, DATE_TO,
        )
    else:
        windows = [("", "")]

    rows_loaded       = 0
    rows_since_commit = 0

    for win_idx, (win_from, win_to) in enumerate(windows):
        _DATE_OVERRIDE_FROM = win_from
        _DATE_OVERRIDE_TO   = win_to

        if win_from and win_to:
            log.info(
                "[%s] Partition %d/%d: %s → %s",
                table, win_idx + 1, len(windows), win_from, win_to,
            )

        options_fn = table_def.get("options_fn")
        options    = options_fn() if callable(options_fn) else []

        if options:
            preview = " | ".join(r["TEXT"] for r in options[:4])
            suffix  = " | ..." if len(options) > 4 else ""
            log.info("OPTIONS (%d rows): %s%s", len(options), preview, suffix)

        extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log)

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

    _DATE_OVERRIDE_FROM = ""
    _DATE_OVERRIDE_TO   = ""

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
    Try to extract the anchor table (BSIS) using up to four combinations:

        Probe 1  BBP_RFC_READ_TABLE + IN  syntax
        Probe 2  BBP_RFC_READ_TABLE + OR  syntax
        Probe 3  RFC_READ_TABLE     + IN  syntax
        Probe 4  RFC_READ_TABLE     + OR  syntax

    The probe IS the full extraction — no rows are fetched twice.
    Stops as soon as a non-zero row count is achieved.
    The winning RFC function and WHERE syntax are locked in for all
    subsequent table extractions.

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

        # Clear anchor key set so a retry starts fresh.
        _BUKRS_SET.clear()

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
        "Verify company code(s) %s and date range %s–%s in SAP.",
        len(attempts), table, COMPANY_CODES,
        DATE_FROM or "unbounded", DATE_TO or "unbounded",
    )
    return True, 0


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    global _FAST_CLEAN_MODE

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP FBL3N G/L Account Line Items Staging Pipeline  [run: %s]", run_id)
    log.info("Company codes  : %s", COMPANY_CODES)
    log.info("Date from      : %s", DATE_FROM if DATE_FROM else "unbounded")
    log.info("Date to        : %s", DATE_TO   if DATE_TO   else "unbounded")
    log.info("Staging prefix : stg_sap_%s_<TABLE>", STAGING_PREFIX.lower())
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority   : %s", RFC_FUNCTION_PRIORITY)
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
        sap_config = build_sap_config()
        pg_config  = build_pg_config()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    _FAST_CLEAN_MODE = cfg.fast_clean_mode

    log.info(
        "Config: batch_size=%d  commit_every=%d  date_partitions=%d",
        cfg.batch_size, cfg.commit_every, cfg.date_partitions,
    )
    log.info(
        "Config: unlogged=%s  copy_loader=%s  fast_clean=%s",
        cfg.use_unlogged_tables, cfg.use_copy_loader, cfg.fast_clean_mode,
    )

    try:
        sap    = SAPConnection(sap_config, log)
        loader = PostgresLoader(
            pg_config, cfg.staging_schema, STAGING_PREFIX, log,
            use_unlogged=cfg.use_unlogged_tables,
            use_copy=cfg.use_copy_loader,
        )
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
                    results[tbl_name] = (ok, count)
                    if count == 0:
                        log.warning(
                            "Anchor table (%s) returned 0 rows after all probes. "
                            "Child tables will be skipped (no company code keys to filter on).",
                            tbl_name,
                        )
                        break   # Nothing to drive T001 with; abort remaining tables.
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
    log.info("Active RFC function : %s", RFC_FUNCTION_PRIORITY[sap._fn_index])
    log.info("Active WHERE syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Key sets collected:")
    log.info("    BUKRS : %d company codes", len(_BUKRS_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)
    log.info(
        "Downstream hint: UNION stg_sap_fbl3n_bsis (open) + stg_sap_fbl3n_bsas (cleared) "
        "and JOIN stg_sap_fbl3n_t001 ON BUKRS to replicate FBL3N output."
    )
    log.info(
        "Purchase Document (Purch.Doc.) requires an additional BSEG read "
        "keyed by BELNR+GJAHR+BUZEI. See design note in module docstring."
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