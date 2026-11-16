"""
SAP S/4HANA ACDOCA — FBL1N / FBL3N / FBL5N Line Items Staging Extractor
=======================================================================
Extracts the Universal Journal (ACDOCA) columns needed to rebuild the three
line-item reports from one table, plus the master data they join to, and
loads them into PostgreSQL staging tables (schema from STAGING_SCHEMA_HANA).

  FBL1N  Vendor line items     → ACDOCA rows with KOART = 'K'
  FBL3N  G/L account items     → ACDOCA rows with KOART IN ('S','M')
  FBL5N  Customer line items   → ACDOCA rows with KOART = 'D'

All transformation (open/cleared status, report splits, amounts, dates,
currency factors) is done in SQL on top of the staging tables.

ECC field → ACDOCA field mapping (columns used by the three ECC scripts)
------------------------------------------------------------------------
  BUKRS → RBUKRS        HKONT → RACCT         MONAT → POPER (3 digits)
  SHKZG → DRCRK         DMBTR → HSL (+RHCUR)   WRBTR/WAERS → WSL + RWCUR
  KOSTL → RCNTR         GSBER → RBUSA          VBELN → AWREF (AWTYP='VBRK')
  Same name in ACDOCA: BELNR, GJAHR, BUZEI, BUDAT, BLDAT, BLART, ZUONR,
  BSCHL, SGTXT, LIFNR, KUNNR, UMSKZ, REBZG, REBZJ, MWSKZ, PRCTR, WERKS,
  AUGDT, AUGBL.

NOT in ACDOCA (not extracted by this script):
  XBLNR                       → BKPF (document header)
  ZFBDT, ZTERM, ZBD1T/2T/3T,
  ZLSCH, ZLSPR                → BSEG  (ACDOCA.NETDT gives the net due date)
  MWSTS, WMWST                → derive in SQL from tax lines (KTOSL)

Source tables (extraction order)
--------------------------------
  Pass 1 — ACDOCA : anchor. Filtered by ledger (RLDNR), company code
           (RBUKRS), account type (KOART), plant (WERKS, optional), posting
           period <> 000 (excludes balance carry-forward) and BUDAT range.
           Collects RBUKRS / LIFNR / KUNNR key sets.
  Pass 2 — T001   : company codes seen in ACDOCA.
  Pass 3 — LFA1   : vendors seen in ACDOCA.LIFNR   (key lists chunked).
  Pass 4 — KNA1   : customers seen in ACDOCA.KUNNR (key lists chunked).

Anchor probe (same as FBL3N / COOIS)
------------------------------------
  ACDOCA is extracted with up to four RFC-function × WHERE-syntax
  combinations (BBP_RFC_READ_TABLE / RFC_READ_TABLE × IN / OR). The first
  that returns rows is locked in for all later tables. If every probe fails
  with an error, the run is reported as FAILED (exit code 1).

Loading (same as FBL3N)
-----------------------
  Existing staging tables are never dropped: missing columns are added,
  then the table is truncated — or, when PLANTS is set, only this run's
  slice is deleted (ledger + company codes + account types + plants +
  posting-date range for ACDOCA; the extracted keys for the master tables),
  so plant-by-plant runs accumulate in the same tables.

Notes for the SQL layer
-----------------------
  * HSL / WSL are already SIGNED (credit = negative). Do not apply DRCRK
    again.  RFC returns negatives with a TRAILING minus ("1234.56-").
  * Amounts are in SAP internal format: apply the currency-decimal factor
    (e.g. UGX/RWF × 100) per amount's own currency (HSL→RHCUR, WSL→RWCUR).
  * Open item = AUGBL blank; cleared = AUGBL filled.
  * BUZEI = '000' marks lines with no FI (BSEG) line item.
  * LIFNR / KUNNR may also be filled on G/L lines — split reports by KOART.
"""

from __future__ import annotations

import csv
import io
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
from datetime import datetime, timedelta  # noqa: E402
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple  # noqa: E402

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


def _env_list(name: str, default: str = "") -> List[str]:
    """Comma-separated env var → list of trimmed, non-empty values."""
    return [v.strip() for v in (os.getenv(name) or default).split(",") if v.strip()]


# ---------------------------------------------------------------------------
# CONFIGURATION — set via .env (defaults shown)
# ---------------------------------------------------------------------------

# Posting date range applied to ACDOCA.BUDAT (YYYYMMDD). Empty = unbounded.
DATE_FROM: str = os.getenv("DATE_FROM", "20261001")
DATE_TO:   str = os.getenv("DATE_TO",   "20261015")

# Company codes on the HANA system (ACDOCA.RBUKRS). Verify — they may differ
# from the ECC company codes.

# COMPANY_CODES: List[str] = _env_list("HANA_COMPANY_CODES", "1100")

# Leading ledger (FINSC_LEDGER). Every posting exists once per ledger, so
# this filter is mandatory to avoid duplicated amounts.
LEDGER: str = os.getenv("HANA_LEDGER", "0L")

# Account types to extract (ACDOCA.KOART):
#   K = vendor (FBL1N), D = customer (FBL5N), S = G/L, M = material (FBL3N).
# Empty = no KOART filter (also brings asset 'A' and CO-only lines).
ACCOUNT_TYPES: List[str] = _env_list("ACCOUNT_TYPES", "D,K,S,M")

# Plant filter on ACDOCA.WERKS (optional). NOTE: vendor and customer lines
# (KOART K/D) normally have no plant, so a plant-scoped run returns mainly
# material-related G/L lines.
# PLANTS: List[str] = _env_list("PLANTS")

COMPANY_CODES: List[str] = ["1100"]
PLANTS: List[str] = []

# Staging table names: stg_sap_<prefix>_<table>  →  stg_sap_fin_acdoca, ...
# (own env var so it does not collide with STAGING_PREFIX used by others)
STAGING_PREFIX: str = os.getenv("ACDOCA_STAGING_PREFIX", "fin")

# Max keys per RFC call for the master tables (LFA1 / KNA1 / T001).
KEY_CHUNK_SIZE: int = int(os.getenv("KEY_CHUNK_SIZE", "500"))

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_BUKRS_SET: Set[str] = set()   # ACDOCA.RBUKRS → T001
_LIFNR_SET: Set[str] = set()   # ACDOCA.LIFNR  → LFA1
_KUNNR_SET: Set[str] = set()   # ACDOCA.KUNNR  → KNA1

# Active WHERE-syntax mode: True → IN (...), False → OR-chained =
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


def _value_clause(field_name: str, values: List[str]) -> str:
    """One field's filter in the active syntax: =, IN ( ... ) or ( ... OR ... )."""
    vals = sorted(set(values))
    if len(vals) == 1:
        return f"{field_name} = '{vals[0]}'"
    if _USE_IN_OPERATOR:
        return f"{field_name} IN ( " + ", ".join(f"'{v}'" for v in vals) + " )"
    return "( " + " OR ".join(f"{field_name} = '{v}'" for v in vals) + " )"


def _build_acdoca_options() -> List[Dict[str, str]]:
    """
    WHERE options for ACDOCA (all combined with AND):
      RLDNR = ledger, RBUKRS company codes, KOART account types (if set),
      WERKS plants (if set), POPER <> '000', BUDAT range (partition-aware).
    """
    date_from = _DATE_OVERRIDE_FROM or DATE_FROM
    date_to   = _DATE_OVERRIDE_TO   or DATE_TO

    clauses = [f"RLDNR = '{LEDGER}'", _value_clause("RBUKRS", COMPANY_CODES)]
    if ACCOUNT_TYPES:
        clauses.append(_value_clause("KOART", ACCOUNT_TYPES))
    if PLANTS:
        clauses.append(_value_clause("WERKS", PLANTS))
    clauses.append("POPER NE '000'")
    if date_from:
        clauses.append(f"BUDAT GE '{date_from}'")
    if date_to:
        clauses.append(f"BUDAT LE '{date_to}'")
    return _format_options(" AND ".join(clauses))


def _key_options(field_name: str) -> Callable[[List[str]], List[Dict[str, str]]]:
    """Options builder for one chunk of keys of a master table."""
    return lambda keys: _format_options(_value_clause(field_name, keys))


# ---------------------------------------------------------------------------
# Scoped-delete builders (used instead of TRUNCATE when PLANTS is set)
# ---------------------------------------------------------------------------
def _acdoca_delete_scope() -> Optional[Tuple[str, List[Any]]]:
    """Delete exactly the slice this run reloads, so other plants are kept."""
    if not PLANTS:
        return None
    where  = ['"rldnr" = %s', '"rbukrs" = ANY(%s)', '"werks" = ANY(%s)']
    params: List[Any] = [LEDGER, COMPANY_CODES, PLANTS]
    if ACCOUNT_TYPES:
        where.append('"koart" = ANY(%s)')
        params.append(ACCOUNT_TYPES)
    if DATE_FROM:
        where.append('"budat" >= %s')
        params.append(DATE_FROM)
    if DATE_TO:
        where.append('"budat" <= %s')
        params.append(DATE_TO)
    return " AND ".join(where), params


def _key_delete_scope(column: str, set_name: str) -> Callable[[], Optional[Tuple[str, List[Any]]]]:
    """Master tables in plant-scoped runs: replace only the keys seen now."""
    def scope() -> Optional[Tuple[str, List[Any]]]:
        if not PLANTS:
            return None
        keys = sorted(getattr(sys.modules[__name__], set_name))
        return f'"{column}" = ANY(%s)', [keys]
    return scope


# ---------------------------------------------------------------------------
# ACDOCA column list — fields used by FBL1N + FBL3N + FBL5N that exist in
# ACDOCA, plus the control fields the SQL layer needs.
# ---------------------------------------------------------------------------
_ACDOCA_COLUMNS: List[str] = [
    # --- Keys & control ---
    "RLDNR",       # Ledger                          (filter: leading ledger)
    "RBUKRS",      # Company Code                    (ECC BUKRS)
    "GJAHR",       # Fiscal Year
    "BELNR",       # Accounting Document Number
    "DOCLN",       # Ledger line item (6-char key)
    "BUZEI",       # FI line item ('000' = no BSEG line)
    "KOART",       # Account Type  K/D/S/M → FBL1N / FBL5N / FBL3N split
    "BSTAT",       # Document Status
    "AWTYP",       # Reference transaction (VBRK = billing document)
    "AWREF",       # Reference document no.          (ECC VBELN when AWTYP=VBRK)
    "XREVERSING",  # Item reverses another item
    "XREVERSED",   # Item is reversed
    # --- Accounts & partners ---
    "RACCT",       # G/L Account                     (ECC HKONT)
    "LIFNR",       # Vendor                          → LFA1
    "KUNNR",       # Customer                        → KNA1
    "UMSKZ",       # Special G/L Indicator
    # --- Dates & periods ---
    "BUDAT",       # Posting Date
    "BLDAT",       # Document Date
    "POPER",       # Posting Period                  (ECC MONAT, 3 digits)
    "NETDT",       # Net Due Date                    (replaces ZFBDT + terms)
    "AUGDT",       # Clearing Date
    "AUGBL",       # Clearing Document               (blank = open item)
    "AUGGJ",       # Clearing Fiscal Year
    # --- Document attributes ---
    "BLART",       # Document Type
    "BSCHL",       # Posting Key
    "ZUONR",       # Assignment
    "SGTXT",       # Item Text
    "REBZG",       # Invoice reference
    "REBZJ",       # Invoice reference fiscal year
    "MWSKZ",       # Tax Code
    "KTOSL",       # Transaction Key (identifies tax lines)
    "DRCRK",       # Debit/Credit Indicator          (ECC SHKZG)
    "XOPVW",       # Open item managed account
    # --- Amounts (signed) ---
    "HSL",         # Amount in company code currency (ECC DMBTR)
    "RHCUR",       # Company code currency
    "WSL",         # Amount in transaction currency  (ECC WRBTR)
    "RWCUR",       # Transaction currency            (ECC WAERS)
    # --- Org ---
    "RCNTR",       # Cost Center                     (ECC KOSTL)
    "PRCTR",       # Profit Center
    "RBUSA",       # Business Area                   (ECC GSBER)
    "WERKS",       # Plant
]


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
# Keys:
#   table            → SAP table name
#   description      → label for logs
#   columns          → fields to read
#   options_fn       → () → RFC OPTIONS rows                (anchor)
#   key_filter       → (SAP field, "_SET_NAME") → chunked key extraction
#   key_options_fn   → (keys chunk) → RFC OPTIONS rows      (key tables)
#   delete_scope_fn  → () → (WHERE, params) | None  — scoped delete instead
#                      of TRUNCATE when PLANTS is set
#   key_out          → {SAP field: "_SET_NAME"} harvested from extracted rows
#   is_anchor        → runs the RFC/syntax probe
#   date_partitioned → splits the BUDAT range into DATE_PARTITIONS windows
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ── PASS 1 ─ ACDOCA : Universal Journal line items (anchor) ───────────
    {
        "table": "ACDOCA",
        "description": "Universal Journal — FBL1N/FBL3N/FBL5N line items",
        "columns": _ACDOCA_COLUMNS,
        "options_fn": _build_acdoca_options,
        "delete_scope_fn": _acdoca_delete_scope,
        "primary_keys": ["RLDNR", "RBUKRS", "GJAHR", "BELNR", "DOCLN"],
        "key_out": {
            "RBUKRS": "_BUKRS_SET",
            "LIFNR":  "_LIFNR_SET",
            "KUNNR":  "_KUNNR_SET",
        },
        "is_anchor": True,
        "date_partitioned": True,
    },

    # ── PASS 2 ─ T001 : Company Code Master ───────────────────────────────
    {
        "table": "T001",
        "description": "Company Code Master — codes seen in ACDOCA",
        "columns": ["BUKRS", "BUTXT", "ORT01", "LAND1", "WAERS", "KTOPL", "PERIV"],
        "key_filter": ("BUKRS", "_BUKRS_SET"),
        "key_options_fn": _key_options("BUKRS"),
        "delete_scope_fn": _key_delete_scope("bukrs", "_BUKRS_SET"),
        "primary_keys": ["BUKRS"],
    },

    # ── PASS 3 ─ LFA1 : Vendor Master (as in FBL1N) ───────────────────────
    {
        "table": "LFA1",
        "description": "Vendor Master — vendors seen in ACDOCA",
        "columns": ["LIFNR", "NAME1", "NAME2", "ORT01", "LAND1", "STRAS", "PSTLZ", "KTOKK"],
        "key_filter": ("LIFNR", "_LIFNR_SET"),
        "key_options_fn": _key_options("LIFNR"),
        "delete_scope_fn": _key_delete_scope("lifnr", "_LIFNR_SET"),
        "primary_keys": ["LIFNR"],
    },

    # ── PASS 4 ─ KNA1 : Customer Master (as in FBL5N) ─────────────────────
    {
        "table": "KNA1",
        "description": "Customer Master — customers seen in ACDOCA",
        "columns": ["KUNNR", "NAME1", "NAME2", "ORT01", "LAND1", "KTOKD"],
        "key_filter": ("KUNNR", "_KUNNR_SET"),
        "key_options_fn": _key_options("KUNNR"),
        "delete_scope_fn": _key_delete_scope("kunnr", "_KUNNR_SET"),
        "primary_keys": ["KUNNR"],
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
    staging_schema:   str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))
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
    log_file = os.path.join(log_dir, f"sap_acdoca_staging_{run_id}.log")

    log = logging.getLogger(f"sap_acdoca_staging.{run_id}")
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
# SAP connection wrapper — with RFC function fallback
# ---------------------------------------------------------------------------
class SAPConnection:
    """
    Wraps a pyrfc Connection and manages the active RFC read-table function.

    Fallback order is defined by RFC_FUNCTION_PRIORITY.  Call
    switch_rfc_function() to advance to the next candidate.
    """

    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system   = system
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
                self._log.info("Connecting to SAP %s (attempt %d/3)...", self._system, attempt)
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
        schema:       str,
        prefix:       str,
        log:          logging.Logger,
        use_unlogged: bool = True,
        use_copy:     bool = True,
    ) -> None:
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
            self.conn = get_postgres_connection()
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

    def _table_exists(self, schema: str, table: str) -> bool:
        self.cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return self.cur.fetchone() is not None

    def _add_missing_columns(self, stg_name: str, safe_cols: List[str]) -> None:
        """Add any configured column that the existing staging table lacks (as TEXT)."""
        self.cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (self._schema, stg_name),
        )
        existing = {r[0] for r in self.cur.fetchall()}
        for col in safe_cols:
            if col not in existing:
                self.cur.execute(
                    f'ALTER TABLE "{self._schema}"."{stg_name}" ADD COLUMN "{col}" TEXT;'
                )
                self._log.warning(
                    "Staging table %s.%s: added missing column \"%s\".",
                    self._schema, stg_name, col,
                )

    def create_staging_table(
        self,
        sap_table:    str,
        columns:      List[str],
        delete_scope: Optional[Tuple[str, List[Any]]] = None,
    ) -> str:
        """
        Create the staging table if missing. If it exists: add missing columns,
        then delete only `delete_scope` (plant-scoped runs) or TRUNCATE.
        """
        self._ensure_connection()
        stg_name  = f"stg_sap_acdoca_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_defs  = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
        tbl_type  = "UNLOGGED TABLE" if self._use_unlogged else "TABLE"

        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        if self._table_exists(self._schema, stg_name):
            self._add_missing_columns(stg_name, safe_cols)
            if delete_scope is not None:
                where, params = delete_scope
                self.cur.execute(
                    f'DELETE FROM "{self._schema}"."{stg_name}" WHERE {where}', params
                )
                deleted = self.cur.rowcount
                self.conn.commit()
                self._log.info(
                    "Staging table %s.%s: deleted %d rows for this run's slice; "
                    "rows outside it are kept.",
                    self._schema, stg_name, deleted,
                )
                return f'"{self._schema}"."{stg_name}"'

            self.cur.execute(f'TRUNCATE TABLE "{self._schema}"."{stg_name}";')
            self.conn.commit()
            self._log.info("Staging table %s.%s truncated.", self._schema, stg_name)
            return f'"{self._schema}"."{stg_name}"'

        self.cur.execute(f"""
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
    if len(chunks) > 1 and table_def.get("is_anchor"):
        log.warning(
            "[%s] Columns split into %d RFC calls per batch. Rows from separate "
            "calls are matched by position, and HANA does not guarantee the same "
            "row order between calls — check the DD03L width lookup above, and "
            "keep the column list within MAX_BUFFER_WIDTH so it fits one call.",
            table, len(chunks),
        )

    scope_fn  = table_def.get("delete_scope_fn")
    stg_table = loader.create_staging_table(
        table, columns, scope_fn() if callable(scope_fn) else None
    )

    # Build the list of option sets to run: date windows (anchor) or key chunks.
    option_sets: List[Tuple[str, List[Dict[str, str]]]] = []
    key_filter = table_def.get("key_filter")

    if key_filter:
        field_name, set_name = key_filter
        keys = sorted(getattr(sys.modules[__name__], set_name))
        if not keys:
            log.info("[%s] No %s keys collected — nothing to extract.", table, field_name)
            loader.commit()
            return True, 0
        build = table_def["key_options_fn"]
        n_chunks = (len(keys) + KEY_CHUNK_SIZE - 1) // KEY_CHUNK_SIZE
        log.info("[%s] %d %s keys → %d chunk(s) of up to %d.",
                 table, len(keys), field_name, n_chunks, KEY_CHUNK_SIZE)
        for i in range(0, len(keys), KEY_CHUNK_SIZE):
            option_sets.append((
                f"key chunk {i // KEY_CHUNK_SIZE + 1}/{n_chunks}",
                build(keys[i:i + KEY_CHUNK_SIZE]),
            ))
    else:
        if cfg.date_partitions > 1 and table_def.get("date_partitioned", False):
            windows = _split_date_range(DATE_FROM, DATE_TO, cfg.date_partitions)
            log.info("[%s] Date partitioning: %d window(s) over %s → %s",
                     table, len(windows), DATE_FROM, DATE_TO)
        else:
            windows = [("", "")]
        options_fn = table_def.get("options_fn")
        for win_idx, (win_from, win_to) in enumerate(windows):
            _DATE_OVERRIDE_FROM = win_from
            _DATE_OVERRIDE_TO   = win_to
            label = (f"partition {win_idx + 1}/{len(windows)}: {win_from} → {win_to}"
                     if win_from and win_to else "full range")
            option_sets.append((label, options_fn() if callable(options_fn) else []))
        _DATE_OVERRIDE_FROM = ""
        _DATE_OVERRIDE_TO   = ""

    rows_loaded       = 0
    rows_since_commit = 0

    for label, options in option_sets:
        if len(option_sets) > 1:
            log.info("[%s] %s", table, label)
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

    elapsed = time.time() - t0
    rate    = rows_loaded / elapsed if elapsed > 0 else 0
    log.info("[%s] Done — %d rows in %.1fs (%.0f rows/s).", table, rows_loaded, elapsed, rate)
    return True, rows_loaded


# ---------------------------------------------------------------------------
# Anchor-table probe: try all RFC fn × WHERE syntax combinations
# ---------------------------------------------------------------------------
def _clear_key_sets() -> None:
    _BUKRS_SET.clear()
    _LIFNR_SET.clear()
    _KUNNR_SET.clear()


def _probe_anchor_table(
    anchor_def: Dict[str, Any],
    sap:        SAPConnection,
    loader:     PostgresLoader,
    cfg:        ExtractionConfig,
    log:        logging.Logger,
) -> Tuple[bool, int]:
    """
    Extract the anchor table (ACDOCA) with up to four combinations:
        1 BBP_RFC_READ_TABLE + IN   2 BBP_RFC_READ_TABLE + OR
        3 RFC_READ_TABLE     + IN   4 RFC_READ_TABLE     + OR
    The probe IS the full extraction. The winning RFC function and syntax
    are locked in for all later tables. All probes raising → (False, 0).
    """
    global _USE_IN_OPERATOR

    table = anchor_def["table"].upper()

    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))   # IN  syntax
        attempts.append((False, fn_idx))   # OR  syntax

    probes_raised = 0

    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap._fn_index    = fn_idx

        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info(
            "─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        _clear_key_sets()   # a retry starts fresh

        try:
            ok, count = extract_table(anchor_def, sap, loader, cfg, log)
        except Exception:
            probes_raised += 1
            log.exception("Probe %d raised an exception — trying next combination.", probe_num)
            # A failed SQL statement aborts the PostgreSQL transaction;
            # roll back so the next probe (and later tables) can continue.
            try:
                if loader.conn is not None and not loader.conn.closed:
                    loader.conn.rollback()
            except Exception:
                log.warning("PostgreSQL rollback after failed probe also failed.")
            continue

        if count > 0:
            log.info(
                "✓ Anchor probe succeeded on attempt %d — RFC=%s | syntax=%s | rows=%d",
                probe_num, sap.rfc_function, syntax_label, count,
            )
            return ok, count

        log.warning(
            "Probe %d returned 0 rows (RFC=%s | syntax=%s).",
            probe_num, sap.rfc_function, syntax_label,
        )

    if probes_raised == len(attempts):
        log.error(
            "All %d anchor probes FAILED with errors for %s (see tracebacks above). "
            "This is an error, not an empty result.",
            len(attempts), table,
        )
        return False, 0

    log.error(
        "All %d anchor probes returned 0 rows for %s. Verify ledger %s, company "
        "code(s) %s, account types %s, plant(s) %s and date range %s–%s in SAP.",
        len(attempts), table, LEDGER, COMPANY_CODES, ACCOUNT_TYPES or "ALL",
        PLANTS or "ALL", DATE_FROM or "unbounded", DATE_TO or "unbounded",
    )
    return True, 0


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    global _FAST_CLEAN_MODE

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)
    _clear_key_sets()

    log.info("=" * 60)
    log.info("SAP ACDOCA (FBL1N/FBL3N/FBL5N) Staging Pipeline  [run: %s]", run_id)
    log.info("SAP system     : %s", SAP_SYSTEM)
    log.info("Ledger         : %s", LEDGER)
    log.info("Company codes  : %s", COMPANY_CODES)
    log.info("Account types  : %s", ACCOUNT_TYPES if ACCOUNT_TYPES else "ALL (no KOART filter)")
    log.info("Plants         : %s", PLANTS if PLANTS else "ALL (no plant filter)")
    log.info("Date from      : %s", DATE_FROM if DATE_FROM else "unbounded")
    log.info("Date to        : %s", DATE_TO   if DATE_TO   else "unbounded")
    log.info("Staging tables : stg_sap_%s_<TABLE>", STAGING_PREFIX.lower())
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority   : %s", RFC_FUNCTION_PRIORITY)
    log.info("=" * 60)

    if not COMPANY_CODES:
        log.critical("No company codes configured (HANA_COMPANY_CODES) — aborting.")
        sys.exit(1)

    try:
        cfg = ExtractionConfig()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    _FAST_CLEAN_MODE = cfg.fast_clean_mode

    log.info("Config: schema=%s  batch_size=%d  commit_every=%d  date_partitions=%d  key_chunk=%d",
             cfg.staging_schema, cfg.batch_size, cfg.commit_every, cfg.date_partitions, KEY_CHUNK_SIZE)
    log.info("Config: unlogged=%s  copy_loader=%s  fast_clean=%s",
             cfg.use_unlogged_tables, cfg.use_copy_loader, cfg.fast_clean_mode)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(
            cfg.staging_schema, STAGING_PREFIX, log,
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
                            "Anchor table (%s) returned no rows — master tables skipped "
                            "(no keys to filter on).", tbl_name,
                        )
                        break
                else:
                    ok, count = extract_table(tbl_def, sap, loader, cfg, log)
                    results[tbl_name] = (ok, count)

            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)
                try:
                    if loader.conn is not None and not loader.conn.closed:
                        loader.conn.rollback()
                except Exception:
                    pass

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
    log.info("    RBUKRS : %d company codes", len(_BUKRS_SET))
    log.info("    LIFNR  : %d vendors", len(_LIFNR_SET))
    log.info("    KUNNR  : %d customers", len(_KUNNR_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)
    p = STAGING_PREFIX.lower()
    log.info(
        "Downstream: stg_sap_%s_acdoca split by KOART — K → FBL1N (join stg_sap_%s_lfa1), "
        "D → FBL5N (join stg_sap_%s_kna1), S/M → FBL3N; join stg_sap_%s_t001 ON rbukrs.",
        p, p, p, p,
    )
    log.info("XBLNR (BKPF) and payment terms (BSEG) are not in ACDOCA — not extracted.")
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")