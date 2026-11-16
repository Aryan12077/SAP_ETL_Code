"""
SAP COOIS — Production Order Information System Staging Extractor
=================================================================
Version 1.1.0

Extracts the SAP tables behind the COOIS (Production Order Information
System) report from SAP S/4HANA (client 500) and loads them into PostgreSQL
staging tables  sap_stg_hana.stg_sap_coois_<table>  (all columns TEXT,
plus load_ts and extraction_id). Staging tables are truncated, never dropped.

Change log
----------
1.0.0  Original (connections.py integrated). Old commented-out copy of the
       script removed in 1.1.0.
1.1.0  >>> CHANGED (v1.1-1) _build_in_options / _build_or_options wrap all
           IN / OR groups in parentheses before "AND <extra>".
           Before: F IN (..) OR F IN (..) AND extra -> extra only applied to
           the last group (pitfall P1: other languages in MAKT/TJ02T/CEPCT,
           inactive JEST rows).
       >>> CHANGED (v1.1-2) BATCH_SIZE default and cap raised to 100,000 so
           each table is read in one RFC call; WARNING logged if a table
           needs more than one batch (pitfall P2: ROWSKIPS paging is not
           order-stable -> duplicates AND missing rows).
       >>> CHANGED (v1.1-3) AUFK: new field AUTYP (order category) and new
           filter ORDER_CATEGORIES = ['10']. Before, AUFK had no category
           filter, so PM orders (cat. 30) were staged. Confirmed in SE16N:
           plant 1102, Oct 2026 has categories 10 and 30 only.
       >>> CHANGED (v1.1-4) New pass 8: T003P order type texts (replaces the
           hardcoded CASE in the query, pitfall P7). AUART key set added.
       >>> CHANGED (v1.1-5) Loader runs ALTER TABLE .. ADD COLUMN IF NOT
           EXISTS for every column, so new fields (AUTYP) reach existing
           staging tables (pitfall P21).
       >>> CHANGED (v1.1-6) Default window set to the validation month
           (01.10.2026-31.10.2026). The orchestrator / .env still override it;
           always check the "Date from / Date to" lines in the log (P3).

Source tables (extraction order)
--------------------------------
  Pass 1  AUFK  : Order master (anchor). WERKS + ERDAT + AUTYP + LOEKZ = ''.
                  Collects AUFNR, OBJNR, PRCTR, AUART.
  Pass 2  AFKO  : Order header scheduling data (dates, GAMNG, GMEIN).
  Pass 3  AFPO  : Order item (material, PSMNG, WEMNG, MEINS). Collects MATNR.
  Pass 4  MAKT  : Material short texts (SPRAS = E).
  Pass 5  JEST  : Object status, active only (INACT = ''). Collects STAT.
  Pass 6  TJ02T : System status texts (SPRAS = E).
  Pass 7  CEPCT : Profit centre texts (SPRAS = E).
  Pass 8  T003P : Order type texts (SPRAS = E).

Fallback behaviour
------------------
  The anchor table is probed with BBP_RFC_READ_TABLE / RFC_READ_TABLE and
  IN / OR syntax; the first combination returning rows is used for all
  following tables.
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

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Plant filter — AUFK.WERKS
PLANTS: List[str] = ["1102"]

# Order type filter — AUFK.AUART. Leave [] for all types of the categories below.
ORDER_TYPES: List[str] = []

# >>> CHANGED (v1.1-3): order category filter — AUFK.AUTYP.
#   10 = production order (COOIS), 40 = process order (COOISPI),
#   20 = network/PS, 30 = maintenance order (IW39).
# Confirmed (A): SE16N AUFK, WERKS 1102, ERDAT 01.10-31.10.2026 shows only
# categories 10 and 30. 30 = PM orders (IW39), so COOIS = category 10.
ORDER_CATEGORIES: List[str] = ["10"]

# >>> CHANGED (v1.1-6): defaults = validation window. Env vars override.
DATE_FROM: str = os.getenv("DATE_FROM", "20261001")
DATE_TO:   str = os.getenv("DATE_TO",   "20261031")

# Language key for MAKT, TJ02T, CEPCT and T003P
LANGUAGE: str = "E"

# Staging tables: stg_sap_coois_<table>
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "COOIS")

RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_AUFNR_SET: Set[str] = set()   # order numbers        (AUFK -> AFKO, AFPO)
_OBJNR_SET: Set[str] = set()   # object numbers       (AUFK -> JEST)
_MATNR_SET: Set[str] = set()   # material numbers     (AFPO -> MAKT)
_STAT_SET:  Set[str] = set()   # active status codes  (JEST -> TJ02T)
_PRCTR_SET: Set[str] = set()   # profit centres       (AUFK -> CEPCT)
_AUART_SET: Set[str] = set()   # order types          (AUFK -> T003P)  >>> CHANGED (v1.1-4)

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

    >>> CHANGED (v1.1-1): all groups are wrapped in one outer pair of
    parentheses so that "AND <extra>" applies to every group:
        ( ( AUFNR = '1' OR AUFNR = '2' )
        OR ( AUFNR = '3' )
        )
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
        prefix = "( ( " if first_group else "OR ( "
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

    options.append({"TEXT": ")"})
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

    >>> CHANGED (v1.1-1): groups wrapped in parentheses:
        ( AUFNR IN ('1','2')
        OR AUFNR IN ('3')
        )
        AND INACT = ''
    Falls back to OR syntax when _USE_IN_OPERATOR is False.
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
        prefix   = f"( {field_name} IN (" if first_clause else f"OR {field_name} IN ("
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

    options.append({"TEXT": ")"})
    if extra:
        options.append({"TEXT": f"AND {extra}"})

    return options


def _build_aufk_options() -> List[Dict[str, str]]:
    """
    WHERE options for the anchor table AUFK (all combined with AND):
      WERKS (plants), AUART (types, optional), AUTYP (categories, optional),
      ERDAT window, LOEKZ = ''.
    """
    if _USE_IN_OPERATOR:
        plants_str = ",".join(f"'{p}'" for p in PLANTS)
        where = f"WERKS IN ({plants_str})"

        if ORDER_TYPES:
            types_str = ",".join(f"'{t}'" for t in ORDER_TYPES)
            where += f" AND AUART IN ({types_str})"

        # >>> CHANGED (v1.1-3): order category filter
        if ORDER_CATEGORIES:
            cats_str = ",".join(f"'{c}'" for c in ORDER_CATEGORIES)
            where += f" AND AUTYP IN ({cats_str})"

        if DATE_FROM:
            where += f" AND ERDAT GE '{DATE_FROM}'"
        if DATE_TO:
            where += f" AND ERDAT LE '{DATE_TO}'"

        # PENDING (B): deletion-flagged orders are excluded at source, so the
        # final columns "Del. Flag" / "Deletion Flagged" are always blank / N.
        # Keep in line with the COOIS selection used on the SAP side.
        where += " AND LOEKZ = ''"
        return _format_options(where)

    options: List[Dict[str, str]] = []

    plant_clauses = " OR ".join(f"WERKS = '{p}'" for p in PLANTS)
    options.append({"TEXT": f"( {plant_clauses} )"})

    if ORDER_TYPES:
        type_clauses = " OR ".join(f"AUART = '{t}'" for t in ORDER_TYPES)
        options.append({"TEXT": f"AND ( {type_clauses} )"})

    # >>> CHANGED (v1.1-3)
    if ORDER_CATEGORIES:
        cat_clauses = " OR ".join(f"AUTYP = '{c}'" for c in ORDER_CATEGORIES)
        options.append({"TEXT": f"AND ( {cat_clauses} )"})

    if DATE_FROM:
        options.append({"TEXT": f"AND ERDAT GE '{DATE_FROM}'"})
    if DATE_TO:
        options.append({"TEXT": f"AND ERDAT LE '{DATE_TO}'"})

    options.append({"TEXT": "AND LOEKZ = ''"})
    return options


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
#   table        -> SAP transparent table name
#   description  -> label for logs
#   columns      -> fields to read
#   options_fn   -> callable returning RFC OPTIONS rows (WHERE clause)
#   primary_keys -> documentation
#   key_out      -> {SAP_COLUMN: "_GLOBAL_SET_NAME"} fills runtime key sets
#   is_anchor    -> True on the first table; triggers the RFC/syntax probe
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # PASS 1 - AUFK : Order master (anchor)
    {
        "table": "AUFK",
        "description": "Order Master",
        "columns": [
            "AUFNR",   # Order number
            "AUART",   # Order type
            "AUTYP",   # Order category  >>> CHANGED (v1.1-3)
            "WERKS",   # Plant
            "ERDAT",   # Created on
            "KTEXT",   # Order description
            "OBJNR",   # Object number -> JEST
            "LOEKZ",   # Deletion flag
            "PRCTR",   # Profit center
            "KOSTL",   # Cost center
        ],
        "options_fn": _build_aufk_options,
        "primary_keys": ["AUFNR"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
            "OBJNR": "_OBJNR_SET",
            "PRCTR": "_PRCTR_SET",
            "AUART": "_AUART_SET",   # >>> CHANGED (v1.1-4)
        },
        "is_anchor": True,
    },

    # PASS 2 - AFKO : Order header scheduling data
    {
        "table": "AFKO",
        "description": "Order Header Scheduling Data",
        "columns": [
            "AUFNR",   # Order number
            "GSTRP",   # Basic start date
            "GLTRP",   # Basic finish date
            "GSTRS",   # Scheduled start
            "GLTRS",   # Scheduled finish
            "GSTRI",   # Actual start date
            "GLTRI",   # Actual finish date
            "GAMNG",   # Total order quantity
            "GMEIN",   # Unit of measure (header)
            "FEVOR",   # Production supervisor
            "PLGRP",   # Planner group
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR"],
    },

    # PASS 3 - AFPO : Order item (material + quantities)
    {
        "table": "AFPO",
        "description": "Order Item (Material & Quantities)",
        "columns": [
            "AUFNR",   # Order number
            "POSNR",   # Item number
            "MATNR",   # Material
            "PSMNG",   # Order item quantity  -> "Target qty"
            "WEMNG",   # Goods received qty   -> "Del. qty"
            "MEINS",   # Unit of measure      -> "Unit"
            "DWERK",   # Production plant
            "LGORT",   # Storage location
            "VERID",   # Production version
            "LTRMP",   # Planned delivery date
            "LTRMI",   # Actual delivery / finish date
            "ELIKZ",   # Delivery completed indicator
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR", "POSNR"],
        "key_out": {
            "MATNR": "_MATNR_SET",
        },
    },

    # PASS 4 - MAKT : Material short texts
    {
        "table": "MAKT",
        "description": "Material Descriptions",
        "columns": ["MATNR", "SPRAS", "MAKTX"],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR", "SPRAS"],
    },

    # PASS 5 - JEST : Object status (active only)
    {
        "table": "JEST",
        "description": "Order Status (Active)",
        "columns": ["OBJNR", "STAT", "INACT"],
        "options_fn": lambda: _build_in_options(
            "OBJNR", _OBJNR_SET, extra="INACT = ''"
        ),
        "primary_keys": ["OBJNR", "STAT"],
        "key_out": {
            "STAT": "_STAT_SET",
        },
    },

    # PASS 6 - TJ02T : System status texts
    {
        "table": "TJ02T",
        "description": "System Status Texts",
        "columns": ["ISTAT", "SPRAS", "TXT04", "TXT30"],
        "options_fn": lambda: _build_in_options(
            "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["ISTAT", "SPRAS"],
    },

    # PASS 7 - CEPCT : Profit centre texts
    {
        "table": "CEPCT",
        "description": "Profit Centre Text Table",
        "columns": ["SPRAS", "PRCTR", "DATBI", "KOKRS", "KTEXT", "LTEXT", "MCTXT"],
        "options_fn": lambda: _build_in_options(
            "PRCTR",
            {p for p in _PRCTR_SET if p},
            extra=f"SPRAS = '{LANGUAGE}'",
        ),
        "primary_keys": ["SPRAS", "PRCTR", "DATBI", "KOKRS"],
    },

    # PASS 8 - T003P : Order type texts  >>> CHANGED (v1.1-4)
    {
        "table": "T003P",
        "description": "Order Type Texts",
        "columns": ["SPRAS", "AUART", "TXT"],
        "options_fn": lambda: _build_in_options(
            "AUART", _AUART_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["SPRAS", "AUART"],
    },
]

# ---------------------------------------------------------------------------
# Extraction tuning
# ---------------------------------------------------------------------------
_MAX_BATCH_SIZE = 100_000   # >>> CHANGED (v1.1-2): was 10,000


@dataclass
class ExtractionConfig:
    # >>> CHANGED (v1.1-2): default 100,000 -> one RFC call per table
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "100000")))
    max_buffer_width: int   = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries:      int   = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s:    float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every:     int   = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema:   str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))

    def __post_init__(self) -> None:
        if self.batch_size > _MAX_BATCH_SIZE:
            raise ValueError(f"batch_size should not exceed {_MAX_BATCH_SIZE:,} for RFC stability.")
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
# SAP connection wrapper — with RFC function fallback
# ---------------------------------------------------------------------------
class SAPConnection:
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
            self._log.warning("Switching RFC read function -> %s", self.rfc_function)
            return True
        self._log.error("All RFC read functions exhausted (%s).", RFC_FUNCTION_PRIORITY)
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

        level = logging.INFO if len(result) == 1 else logging.WARNING
        self._log.log(
            level,
            "Column chunking: %d columns -> %d chunk(s) (buffer limit %d)%s",
            len(self._columns), len(result), self._max_buffer,
            "" if len(result) == 1 else
            " -- rows from separate calls are merged by position; verify no mixed rows.",
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
                "Collected %d new '%s' keys (total %d) -> %s",
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
        offset    = 0
        total     = 0
        batch_num = 0

        while True:
            batch = self._fetch_offset(offset)
            if not batch:
                self._log.info("[%s] No more rows. Total extracted: %d", self._table, total)
                if batch_num > 1:
                    # >>> CHANGED (v1.1-2): paging warning
                    self._log.warning(
                        "[%s] Read in %d batches -- paging may duplicate or drop rows "
                        "(ROWSKIPS is not order-stable). Use a smaller date window.",
                        self._table, batch_num,
                    )
                break

            batch_num += 1
            total     += len(batch)
            offset    += self._cfg.batch_size
            self._log.info(
                "[%s] Fetched %d rows (running total: %d).",
                self._table, len(batch), total,
            )
            yield batch

            # A batch smaller than ROWCOUNT is the last one; skip the extra call.
            if len(batch) < self._cfg.batch_size:
                self._log.info("[%s] No more rows. Total extracted: %d", self._table, total)
                if batch_num > 1:
                    self._log.warning(
                        "[%s] Read in %d batches -- paging may duplicate or drop rows "
                        "(ROWSKIPS is not order-stable). Use a smaller date window.",
                        self._table, batch_num,
                    )
                break

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
# PostgreSQL staging loader
# ---------------------------------------------------------------------------
class PostgresLoader:
    def __init__(self, schema: str, prefix: str, log: logging.Logger) -> None:
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]  = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL...")
        try:
            self.conn    = get_postgres_connection()
            self._cursor = self.conn.cursor()
            self._log.info("PostgreSQL connection established.")
        except Exception:
            self._log.exception("PostgreSQL connection failed.")
            raise

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg_name  = f"stg_sap_coois_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

        self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        col_defs = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self._schema}.{stg_name} (
            {col_defs},
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """
        self._log.info("Creating staging table %s.%s...", self._schema, stg_name)
        self._cursor.execute(ddl)

        # >>> CHANGED (v1.1-5): add columns missing on an existing table (P21)
        for c in safe_cols:
            self._cursor.execute(
                f'ALTER TABLE {self._schema}.{stg_name} ADD COLUMN IF NOT EXISTS "{c}" TEXT;'
            )

        self._cursor.execute(f"TRUNCATE TABLE {self._schema}.{stg_name};")
        self.conn.commit()
        self._log.info("Staging table %s.%s ready and truncated.", self._schema, stg_name)
        return f'"{self._schema}"."{stg_name}"'

    def load_batch(self, stg_table: str, columns: List[str], rows: List[List[Any]]) -> None:
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
        head   = " | ".join(r["TEXT"] for r in options[:3])
        tail   = " | ".join(r["TEXT"] for r in options[-2:]) if len(options) > 3 else ""
        log.info("OPTIONS (%d rows): %s%s", len(options), head, f" | ... | {tail}" if tail else "")

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
    log.info("[%s] Done — %d rows in %.1fs (%.0f rows/s).", table, rows_loaded, elapsed, rate)
    return True, rows_loaded

# ---------------------------------------------------------------------------
# Anchor-table probe
# ---------------------------------------------------------------------------
def _probe_anchor_table(
    anchor_def: Dict[str, Any],
    sap:        SAPConnection,
    loader:     PostgresLoader,
    cfg:        ExtractionConfig,
    log:        logging.Logger,
) -> Tuple[bool, int]:
    """Try RFC function x WHERE syntax combinations until AUFK returns rows."""
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
            "--- Anchor probe %d/%d: RFC=%s | syntax=%s ---",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        _AUFNR_SET.clear()
        _OBJNR_SET.clear()
        _PRCTR_SET.clear()
        _AUART_SET.clear()

        try:
            ok, count = extract_table(anchor_def, sap, loader, cfg, log)
        except Exception:
            log.warning("Probe %d raised an exception — trying next combination.", probe_num)
            continue

        if count > 0:
            log.info(
                "Anchor probe succeeded on attempt %d — RFC=%s | syntax=%s | rows=%d",
                probe_num, sap.rfc_function, syntax_label, count,
            )
            return ok, count

        log.warning(
            "Probe %d returned 0 rows (RFC=%s | syntax=%s).",
            probe_num, sap.rfc_function, syntax_label,
        )

    log.error(
        "All %d anchor probes returned 0 rows for %s. Verify plant(s) %s, "
        "order types %s, categories %s and date range %s–%s in SAP.",
        len(attempts), table, PLANTS, ORDER_TYPES or "ALL", ORDER_CATEGORIES or "ALL",
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
    log.info("Categories     : %s", ORDER_CATEGORIES if ORDER_CATEGORIES else "ALL")
    log.info("Date range     : %s -> %s", DATE_FROM or "unbounded", DATE_TO or "unbounded")
    log.info("Language       : %s", LANGUAGE)
    log.info("Staging prefix : stg_sap_%s_<table>", STAGING_PREFIX.lower())
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority   : %s", RFC_FUNCTION_PRIORITY)
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
        log.info("Batch size     : %d", cfg.batch_size)
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, STAGING_PREFIX, log)
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
                            "Child tables will be empty (no order keys to filter on)."
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
    log.info("    AUFNR : %d order numbers",      len(_AUFNR_SET))
    log.info("    OBJNR : %d object numbers",     len(_OBJNR_SET))
    log.info("    MATNR : %d material numbers",   len(_MATNR_SET))
    log.info("    STAT  : %d active status codes", len(_STAT_SET))
    log.info("    PRCTR : %d profit centres",     len(_PRCTR_SET))
    log.info("    AUART : %d order types",        len(_AUART_SET))
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