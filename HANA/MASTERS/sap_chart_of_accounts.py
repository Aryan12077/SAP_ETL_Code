"""
SAP S/4HANA Chart of Accounts + Financial Statement Version — Staging Extractor
===============================================================================
Extracts everything needed to build the chart of accounts with its financial
statement version (FSV) hierarchy, and loads it into PostgreSQL staging
tables (schema from STAGING_SCHEMA_HANA, tables stg_sap_coa_<table>).
The hierarchy itself (recursive tree, levels, texts, account intervals) is
built in SQL on top of these tables.

Source tables (extraction order)
--------------------------------
  Pass 1 — T001        : company codes (COA_COMPANY_CODES) → their chart of
                         accounts (KTOPL) is collected for the next passes.
  Pass 2 — SKB1        : G/L accounts per company code (currency, open item
                         management, line item display, reconciliation type).
  Pass 3 — SKA1        : G/L accounts per chart (balance sheet / P&L, account
                         group, P&L type, deletion / blocking flags).
  Pass 4 — SKAT        : G/L account texts (language COA_LANGUAGE).
  Pass 5 — FAGL_011ZC  : account ranges per FSV node (all fields, incl.
                         debit/credit balance flags) for the CHARTS found in
                         pass 1 → collects the FSV versions that apply.
  Pass 6 — FAGL_011PC  : FSV tree nodes (all fields) for those versions.
  Pass 7 — FAGL_011QT  : node texts (all fields) for those versions.
  Pass 8 — T011T       : FSV names (as in OB58) for those versions.

  Nothing client-specific is hardcoded: company codes → charts of accounts
  (T001) → FSV versions (FAGL_011ZC). The FSV setting only restricts.

Field lists
-----------
  Each table's fields are checked against the SAP data dictionary (DD03L)
  at run time. The three FSV tables are read with ALL their fields, so
  release-specific fields (debit/credit flags, text type / line, tree
  pointers) are captured whatever they are called. For the other tables a
  field that does not exist in this release is skipped with a WARNING
  instead of failing the RFC call. Key fields come from DD03L too and are
  used for the duplicate check (expect 0) and for matching rows when a wide
  table is read in more than one RFC call.

Anchor probe (same as ACDOCA / FBL3N / COOIS)
---------------------------------------------
  T001 is read with up to four RFC-function × WHERE-syntax combinations
  (BBP_RFC_READ_TABLE / RFC_READ_TABLE × IN / OR). The first that returns
  rows is locked in for all later tables.

Loading
-------
  Full refresh: existing staging tables are never dropped — missing columns
  are added, then the table is truncated and reloaded.
  These tables are small (a few thousand rows); each is read in one RFC call
  of BATCH_SIZE rows. If a table ever needs a second call, a WARNING is
  logged (paging is not order-stable on HANA).

Configuration (.env, defaults shown)
------------------------------------
  COA_COMPANY_CODES = 1100            (all company codes together, e.g. 1100,3100 —
                                       each run replaces the staging data)
  COA_FSV_VERSIONS  = (empty)  automatic — every FSV with account ranges for
                               the companies' charts; set a list only to
                               restrict (e.g. JB01,JB04)
  COA_EXTRA_CHARTS  = (empty)  extra charts of accounts beyond those of the
                               company codes, e.g. a group chart
  COA_LANGUAGE      = E

  Fields the SQL layer uses are always created in staging; if this release
  lacks one, it stays empty and a WARNING is logged.
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
from datetime import datetime  # noqa: E402
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
COMPANY_CODES: List[str] = _env_list("COA_COMPANY_CODES", "1100, 3100")
# Empty (default) = automatic: every FSV that has account ranges for the
# companies' charts of accounts. Set it only to restrict to certain versions.
FSV_VERSIONS:  List[str] = _env_list("COA_FSV_VERSIONS",  "")
EXTRA_CHARTS:  List[str] = _env_list("COA_EXTRA_CHARTS",  "")
LANGUAGE:      str       = os.getenv("COA_LANGUAGE", "E")

# Staging table names: stg_sap_<prefix>_<table>  →  stg_sap_coa_ska1, ...
STAGING_PREFIX: str = "coa"

# Max keys per RFC call for key-filtered tables.
KEY_CHUNK_SIZE: int = int(os.getenv("KEY_CHUNK_SIZE", "500"))

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------
_KTOPL_SET: Set[str] = set()   # T001.KTOPL (+ COA_EXTRA_CHARTS) → SKA1 / SKAT / FAGL_011ZC
_VERSN_SET: Set[str] = set()   # FAGL_011ZC.VERSN (FSVs found for those charts) → PC / QT / T011T

# Active WHERE-syntax mode: True → IN (...), False → OR-chained =
_USE_IN_OPERATOR: bool = True

# Fast clean mode flag (set from ExtractionConfig at startup)
_FAST_CLEAN_MODE: bool = True

# Marker: read every field of the table that DD03L lists
ALL_FIELDS = "ALL"

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




def _key_options_with(
    field_name: str,
    extra: "str | Callable[[], str]" = "",
) -> Callable[[List[str]], List[Dict[str, str]]]:
    """Key-chunk options, optionally AND-ed with a condition (string or callable,
    evaluated at read time so it uses the WHERE syntax the probe locked in)."""
    def build(keys: List[str]) -> List[Dict[str, str]]:
        clause = _value_clause(field_name, keys)
        cond = extra() if callable(extra) else extra
        if cond:
            clause = f"( {clause} ) AND {cond}"
        return _format_options(clause)
    return build


def _fsv_restriction() -> str:
    """Optional VERSN restriction from the FSV_VERSIONS setting."""
    return f"( {_value_clause('VERSN', FSV_VERSIONS)} )" if FSV_VERSIONS else ""


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
# Keys:
#   table          → SAP table name
#   description    → label for logs
#   columns        → field list, or ALL_FIELDS (every DD03L field)
#   options_fn     → () → RFC OPTIONS rows
#   key_filter     → (SAP field, "_SET_NAME") → chunked key extraction
#   key_options_fn → (keys chunk) → RFC OPTIONS rows
#   key_out        → {SAP field: "_SET_NAME"} harvested from extracted rows
#   is_anchor      → runs the RFC/syntax probe
#   fallback_keys  → key fields if DD03L cannot be read
#
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ── PASS 1 ─ T001 : company codes → chart of accounts (anchor) ────────
    {
        "table": "T001",
        "description": "Company codes → chart of accounts",
        "columns": ["BUKRS", "BUTXT", "ORT01", "LAND1", "WAERS", "KTOPL",
                    "KTOP2", "PERIV", "SPRAS"],
        "options_fn": lambda: _format_options(_value_clause("BUKRS", COMPANY_CODES)),
        "key_out": {"KTOPL": "_KTOPL_SET"},
        "is_anchor": True,
        "fallback_keys": ["BUKRS"],
    },

    # ── PASS 2 ─ SKB1 : G/L account master, company code level ───────────
    {
        "table": "SKB1",
        "description": "G/L accounts per company code",
        "columns": ["BUKRS", "SAKNR", "WAERS", "XOPVW", "XKRES", "MITKZ",
                    "XSPEB", "XLOEB", "FSTAG", "ZUAWA", "XINTB", "MWSKZ",
                    "XMWNO", "XSALH", "ALTKT", "HBKID", "HKTID", "ERDAT", "ERNAM"],
        "options_fn": lambda: _format_options(_value_clause("BUKRS", COMPANY_CODES)),
        "fallback_keys": ["BUKRS", "SAKNR"],
    },

    # ── PASS 3 ─ SKA1 : G/L account master, chart level ──────────────────
    {
        "table": "SKA1",
        "description": "G/L accounts per chart of accounts",
        "columns": ["KTOPL", "SAKNR", "XBILK", "GVTYP", "KTOKS", "BILKT",
                    "XLOEV", "XSPEA", "XSPEB", "XSPEP", "FUNC_AREA",
                    "GLACCOUNT_TYPE", "MAIN_SAKNR", "ERDAT", "ERNAM"],
        "key_filter": ("KTOPL", "_KTOPL_SET"),
        "key_options_fn": _key_options_with("KTOPL"),
        "fallback_keys": ["KTOPL", "SAKNR"],
    },

    # ── PASS 4 ─ SKAT : G/L account texts ────────────────────────────────
    {
        "table": "SKAT",
        "description": "G/L account texts",
        "columns": ["SPRAS", "KTOPL", "SAKNR", "TXT20", "TXT50", "MCOD1"],
        "key_filter": ("KTOPL", "_KTOPL_SET"),
        "key_options_fn": _key_options_with("KTOPL", extra=f"SPRAS = '{LANGUAGE}'"),
        "fallback_keys": ["SPRAS", "KTOPL", "SAKNR"],
    },

    # ── PASS 5 ─ FAGL_011ZC : account ranges per FSV node, by CHART ──────
    #    Finds the FSV versions that apply: every version with ranges for the
    #    companies' charts (optionally restricted by FSV_VERSIONS).
    {
        "table": "FAGL_011ZC",
        "description": "FSV account ranges for the charts (finds the FSV versions)",
        "columns": ALL_FIELDS,
        "fallback_columns": ["VERSN", "ERGSL", "KTOPL", "VONKT", "BISKT", "XSOLL", "XHABN", "XVERD"],
        "required_columns": ["VERSN", "ERGSL", "KTOPL", "VONKT", "BISKT", "XSOLL", "XHABN", "XVERD"],
        "key_filter": ("KTOPL", "_KTOPL_SET"),
        "key_options_fn": _key_options_with("KTOPL", extra=_fsv_restriction),
        "key_out": {"VERSN": "_VERSN_SET"},
        "fallback_keys": ["VERSN", "ERGSL", "KTOPL", "VONKT"],
    },

    # ── PASS 6 ─ FAGL_011PC : FSV hierarchy nodes of those versions ──────
    {
        "table": "FAGL_011PC",
        "description": "FSV hierarchy nodes",
        "columns": ALL_FIELDS,
        "fallback_columns": ["VERSN", "ID", "TYPE", "ERGSL", "PARENT", "CHILD", "NEXTN",
                             "STUFE", "SUMME", "SIGN"],
        "required_columns": ["VERSN", "ID", "TYPE", "ERGSL", "PARENT", "CHILD", "NEXTN", "SIGN"],
        "key_filter": ("VERSN", "_VERSN_SET"),
        "key_options_fn": _key_options_with("VERSN"),
        "fallback_keys": ["VERSN", "ID"],
    },

    # ── PASS 7 ─ FAGL_011QT : FSV node texts ─────────────────────────────
    {
        "table": "FAGL_011QT",
        "description": "FSV node texts",
        "columns": ALL_FIELDS,
        "fallback_columns": ["VERSN", "SPRAS", "ERGSL", "TXTYP", "ZEILE", "TXT45"],
        "required_columns": ["VERSN", "SPRAS", "ERGSL", "TXTYP", "ZEILE", "TXT45"],
        "key_filter": ("VERSN", "_VERSN_SET"),
        "key_options_fn": _key_options_with("VERSN", extra=lambda: f"SPRAS = '{LANGUAGE}'"),
        "fallback_keys": ["VERSN", "SPRAS", "ERGSL", "TXTYP", "ZEILE"],
    },

    # ── PASS 8 ─ T011T : FSV names (as shown in OB58) ────────────────────
    {
        "table": "T011T",
        "description": "FSV names",
        "columns": ["VERSN", "SPRAS", "VSTXT"],
        "key_filter": ("VERSN", "_VERSN_SET"),
        "key_options_fn": _key_options_with("VERSN", extra=lambda: f"SPRAS = '{LANGUAGE}'"),
        "fallback_keys": ["VERSN", "SPRAS"],
    },
]

# ---------------------------------------------------------------------------
# Extraction tuning  (override via .env)
# ---------------------------------------------------------------------------
@dataclass
class ExtractionConfig:
    # Rows per RFC call. Large on purpose: each table should fit in ONE call
    # (paging with ROWSKIPS is not order-stable on HANA).
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "100000")))
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
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1.")
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
    log_file = os.path.join(log_dir, f"sap_coa_staging_{run_id}.log")

    log = logging.getLogger(f"sap_coa_staging.{run_id}")
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
# Data dictionary (DD03L): fields, widths and key flags per table
# ---------------------------------------------------------------------------
class TableMetadata:
    """
    Reads DD03L once per table. Returns ordered field info:
      [{"name", "width", "key"}] — includes (.INCLUDE rows), MANDT and the
      client field are left out. Empty list if DD03L cannot be read.
    """
    _FALLBACK_WIDTH = 100
    _SKIP = {"MANDT", "CLIENT", "RCLNT"}

    def __init__(self, sap: SAPConnection, log: logging.Logger) -> None:
        self._sap   = sap
        self._log   = log
        self._cache: Dict[str, List[Dict[str, Any]]] = {}

    def fields(self, table: str) -> List[Dict[str, Any]]:
        if table in self._cache:
            return self._cache[table]
        self._log.info("[%s] Reading field list from DD03L...", table)
        try:
            result = self._sap.read_table(
                "DD03L",
                ["FIELDNAME", "LENG", "KEYFLAG", "POSITION", "INTTYPE"],
                [{"TEXT": f"TABNAME = '{table.upper()}' AND AS4LOCAL = 'A'"}],
                rowcount=2000,
            )
        except Exception:
            self._log.warning("[%s] DD03L lookup failed — using the configured field list.", table)
            self._cache[table] = []
            return []

        out: Dict[str, Dict[str, Any]] = {}
        for row in result.get("DATA", []):
            parts = [p.strip() for p in row["WA"].split("|")]
            if len(parts) < 5:
                continue
            name, leng, keyflag, position, inttype = parts[:5]
            if not name or name.startswith(".") or name in self._SKIP or name in out:
                continue
            try:
                width = max(1, int(leng))
            except ValueError:
                width = self._FALLBACK_WIDTH
            # Packed / integer / float fields print wider than LENG (sign, separators).
            if inttype in ("P", "I", "b", "s", "F", "8"):
                width = width * 2 + 2
            try:
                pos = int(position)
            except ValueError:
                pos = 9999
            out[name] = {"name": name, "width": width, "key": keyflag == "X", "pos": pos}

        fields = sorted(out.values(), key=lambda f: f["pos"])
        self._log.info("[%s] DD03L: %d fields (%d key fields).",
                       table, len(fields), sum(f["key"] for f in fields))
        self._cache[table] = fields
        return fields


def _resolve_columns(
    table_def: Dict[str, Any],
    dd_fields: List[Dict[str, Any]],
    log:       logging.Logger,
) -> Tuple[List[str], Dict[str, int], List[str]]:
    """
    Final (columns, widths, key fields) for a table:
      * ALL_FIELDS → every DD03L field (or fallback_columns if DD03L failed)
      * explicit list → only fields DD03L knows (missing ones skipped + WARNING)
    """
    table   = table_def["table"]
    wanted  = table_def["columns"]
    dd_map  = {f["name"]: f for f in dd_fields}

    if not dd_fields:
        cols = table_def.get("fallback_columns", []) if wanted == ALL_FIELDS else list(wanted)
        return cols, {c: 100 for c in cols}, [k for k in table_def.get("fallback_keys", []) if k in cols]

    if wanted == ALL_FIELDS:
        cols = [f["name"] for f in dd_fields]
    else:
        cols    = [c for c in dict.fromkeys(wanted) if c in dd_map]
        missing = [c for c in wanted if c not in dd_map]
        if missing:
            log.warning("[%s] Fields not in this release, skipped: %s", table, missing)

    widths = {c: dd_map[c]["width"] for c in cols}
    keys   = [f["name"] for f in dd_fields if f["key"] and f["name"] in cols]
    if not keys:
        keys = [k for k in table_def.get("fallback_keys", []) if k in cols]
    return cols, widths, keys


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
        key_cols: Optional[List[str]] = None,
    ) -> None:
        self._sap     = sap
        self._table   = table
        self._columns = columns
        self._chunks  = chunks
        self._options = options
        self._cfg     = cfg
        self._log     = log
        # When set (and there is more than one chunk), every chunk carries these
        # key columns and rows are matched by key, not by position (v1.1).
        self._key_cols = key_cols if (key_cols and len(chunks) > 1) else None
        # Window probe: a full first chunk means "split the window" — skip the rest.
        self.stop_if_full = False

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
            if offset > current_batch_size:
                self._log.warning(
                    "[%s] Page %d read with ROWSKIPS=%d. Paging is not order-stable "
                    "on HANA: rows can repeat or be missed. Narrow the filter.",
                    self._table, offset // current_batch_size, offset - current_batch_size,
                )

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
        if self._key_cols:
            return self._fetch_offset_by_key(offset, batch_size)
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


    def _read_chunk(self, chunk: List[str], offset: int, batch_size: int, chunk_idx: int) -> List[Dict[str, str]]:
        for attempt in range(1, self._cfg.max_retries + 2):
            try:
                result = self._sap.read_table(
                    self._table, chunk, options=self._options,
                    rowcount=batch_size, rowskips=offset,
                )
                out = []
                for row in result.get("DATA", []):
                    values = row["WA"].split("|")
                    out.append({col: _clean(val) for col, val in zip(chunk, values)})
                return out
            except Exception as exc:
                if attempt > self._cfg.max_retries:
                    self._log.error("[%s] Chunk %d exhausted retries at offset %d.",
                                    self._table, chunk_idx + 1, offset)
                    raise
                self._log.warning("[%s] Chunk %d attempt %d/%d failed: %s — retrying in %.0fs...",
                                  self._table, chunk_idx + 1, attempt,
                                  self._cfg.max_retries, exc, self._cfg.retry_delay_s)
                time.sleep(self._cfg.retry_delay_s)
        return []

    def _fetch_offset_by_key(self, offset: int, batch_size: int) -> List[List[Any]]:
        """Multi-chunk read where each chunk includes the key columns (v1.1)."""
        keys = self._key_cols
        merged: Dict[Tuple[str, ...], Dict[str, str]] = {}
        order:  List[Tuple[str, ...]] = []
        for chunk_idx, chunk in enumerate(self._chunks):
            rows = self._read_chunk(chunk, offset, batch_size, chunk_idx)
            if chunk_idx == 0 and not rows:
                return []
            if chunk_idx == 0 and self.stop_if_full and len(rows) >= batch_size:
                return [[r.get(col, "") for col in self._columns] for r in rows]
            unmatched = 0
            for r in rows:
                k = tuple(r.get(c, "") for c in keys)
                if chunk_idx == 0:
                    if k not in merged:
                        order.append(k)
                        merged[k] = {}
                elif k not in merged:
                    unmatched += 1
                    continue
                merged[k].update(r)
            if unmatched:
                self._log.error("[%s] Chunk %d: %d rows had no key match in chunk 1 "
                                "(their values are lost).", self._table, chunk_idx + 1, unmatched)
        return [[merged[k].get(col, "") for col in self._columns] for k in order]


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
        stg_name  = f"stg_sap_{self._prefix}_{sap_table.lower()}"
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


def _log_duplicate_check(
    table_def: Dict[str, Any],
    stg_table: str,
    loader:    PostgresLoader,
    log:       logging.Logger,
) -> None:
    """Count rows sharing a primary key in the staging table (expect 0)."""
    keys = [k.lower() for k in table_def.get("primary_keys", [])]
    if not keys:
        return
    key_list = ", ".join(f'"{k}"' for k in keys)
    try:
        loader.cur.execute(
            f"SELECT COUNT(*) - COUNT(DISTINCT ({key_list})) FROM {stg_table}"
        )
        dups = loader.cur.fetchone()[0]
    except Exception:
        log.warning("Duplicate check on %s failed.", stg_table)
        loader.conn.rollback()
        return
    if dups:
        log.error("%s: %d duplicate rows on key (%s). Re-run the extraction.",
                  stg_table, dups, key_list)
    else:
        log.info("%s: duplicate check OK (0 rows share a key).", stg_table)



# ---------------------------------------------------------------------------
# Per-table pipeline
# ---------------------------------------------------------------------------
def extract_table(
    table_def: Dict[str, Any],
    sap:       SAPConnection,
    loader:    PostgresLoader,
    meta:      TableMetadata,
    cfg:       ExtractionConfig,
    log:       logging.Logger,
) -> Tuple[bool, int]:
    """Extract one SAP table into its staging counterpart. Returns (ok, row_count)."""
    table   = table_def["table"].upper()
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

    log.info("=" * 60)
    log.info("Table  : %s  (%s)", table, desc)
    log.info("RFC fn : %s", sap.rfc_function)
    log.info("Syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")

    t0 = time.time()

    columns, widths, pkeys = _resolve_columns(table_def, meta.fields(table), log)
    if not columns:
        log.error("[%s] No fields to read.", table)
        return False, 0
    log.info("Fields : %s", columns)
    log.info("Keys   : %s", pkeys or "(none known)")

    chunks = ColumnChunker(columns, widths, cfg.max_buffer_width, log).chunks()
    key_cols: Optional[List[str]] = None
    if len(chunks) > 1 and pkeys:
        key_w  = sum(widths.get(k, 100) + 1 for k in pkeys)
        others = [c for c in columns if c not in pkeys]
        sub    = ColumnChunker(others, widths, cfg.max_buffer_width - key_w, log).chunks()
        chunks = [pkeys + c for c in sub]
        key_cols = pkeys
        log.info("[%s] %d RFC calls per read; each includes the key %s (rows matched by key).",
                 table, len(chunks), pkeys)
    elif len(chunks) > 1:
        log.warning("[%s] %d RFC calls per read and no key known — rows are matched "
                    "by position (HANA row order is not guaranteed).", table, len(chunks))

    # Configured fields this release lacks are still created (left empty), so
    # the same SQL runs on ECC and HANA (e.g. SKA1.GLACCOUNT_TYPE is S/4 only).
    configured  = table_def["columns"] if table_def["columns"] != ALL_FIELDS \
        else table_def.get("required_columns", [])
    absent      = [c for c in configured if c not in columns]
    if absent and table_def["columns"] == ALL_FIELDS:
        log.warning("[%s] Fields the SQL layer uses are not in this release: %s — "
                    "created empty; check the field names in SE11.", table, absent)
    stg_columns = columns + absent
    stg_table = loader.create_staging_table(table, stg_columns)

    # Option sets: key chunks, or one fixed filter
    option_sets: List[Tuple[str, List[Dict[str, str]]]] = []
    key_filter = table_def.get("key_filter")
    if key_filter:
        field_name, set_name = key_filter
        keys = sorted(getattr(sys.modules[__name__], set_name))
        if not keys:
            log.warning("[%s] No %s keys collected — nothing to extract.", table, field_name)
            loader.commit()
            return True, 0
        build    = table_def["key_options_fn"]
        n_chunks = (len(keys) + KEY_CHUNK_SIZE - 1) // KEY_CHUNK_SIZE
        for i in range(0, len(keys), KEY_CHUNK_SIZE):
            option_sets.append((f"key chunk {i // KEY_CHUNK_SIZE + 1}/{n_chunks}",
                                build(keys[i:i + KEY_CHUNK_SIZE])))
    else:
        options_fn = table_def.get("options_fn")
        option_sets.append(("full", options_fn() if callable(options_fn) else []))

    rows_loaded = 0
    for label, options in option_sets:
        if options:
            preview = " | ".join(r["TEXT"] for r in options[:4])
            log.info("OPTIONS (%d rows): %s%s", len(options), preview,
                     " | ..." if len(options) > 4 else "")
        extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log, key_cols)
        for batch in extractor.batches():
            if key_out:
                _collect_keys(batch, columns, key_out, log)
            loader.load_batch(stg_table, columns, batch)
            rows_loaded += len(batch)

    loader.commit()
    if pkeys:
        _log_duplicate_check({"primary_keys": pkeys}, stg_table, loader, log)

    elapsed = time.time() - t0
    log.info("[%s] Done — %d rows in %.1fs.", table, rows_loaded, elapsed)
    return True, rows_loaded


# ---------------------------------------------------------------------------
# Anchor-table probe: try all RFC fn × WHERE syntax combinations
# ---------------------------------------------------------------------------
def _probe_anchor_table(
    anchor_def: Dict[str, Any],
    sap:        SAPConnection,
    loader:     PostgresLoader,
    meta:       TableMetadata,
    cfg:        ExtractionConfig,
    log:        logging.Logger,
) -> Tuple[bool, int]:
    global _USE_IN_OPERATOR

    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))
        attempts.append((False, fn_idx))

    probes_raised = 0
    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap._fn_index    = fn_idx
        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info("─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
                 probe_num, len(attempts), sap.rfc_function, syntax_label)
        _KTOPL_SET.clear()
        try:
            ok, count = extract_table(anchor_def, sap, loader, meta, cfg, log)
        except Exception:
            probes_raised += 1
            log.exception("Probe %d raised an exception — trying next combination.", probe_num)
            try:
                if loader.conn is not None and not loader.conn.closed:
                    loader.conn.rollback()
            except Exception:
                log.warning("PostgreSQL rollback after failed probe also failed.")
            continue
        if count > 0:
            log.info("✓ Anchor probe succeeded on attempt %d — RFC=%s | syntax=%s | rows=%d",
                     probe_num, sap.rfc_function, syntax_label, count)
            return ok, count
        log.warning("Probe %d returned 0 rows.", probe_num)

    if probes_raised == len(attempts):
        log.error("All %d anchor probes FAILED with errors (see tracebacks above).", len(attempts))
        return False, 0
    log.error("All anchor probes returned 0 rows — check company codes %s in T001.", COMPANY_CODES)
    return True, 0


# ---------------------------------------------------------------------------
# Post-load summary checks (logged only)
# ---------------------------------------------------------------------------
def _post_checks(loader: PostgresLoader, schema: str, log: logging.Logger) -> None:
    p = f'"{schema}".stg_sap_{STAGING_PREFIX}_'
    checks = [
        ("Accounts per chart (SKA1)",
         f"SELECT ktopl, COUNT(*) FROM {p}ska1 GROUP BY 1 ORDER BY 1"),
        ("Accounts per company code (SKB1)",
         f"SELECT bukrs, COUNT(*) FROM {p}skb1 GROUP BY 1 ORDER BY 1"),
        ("FSV nodes per version (FAGL_011PC)",
         f"SELECT versn, COUNT(*) FROM {p}fagl_011pc GROUP BY 1 ORDER BY 1"),
        ("Account ranges per version / chart (FAGL_011ZC)",
         f"SELECT versn, ktopl, COUNT(*) FROM {p}fagl_011zc GROUP BY 1, 2 ORDER BY 1, 2"),
    ]
    for title, sql in checks:
        try:
            loader.cur.execute(sql)
            rows = loader.cur.fetchall()
            log.info("%s: %s", title, ", ".join(" / ".join(str(v) for v in r) for r in rows) or "none")
        except Exception:
            loader.conn.rollback()
            log.info("%s: not available.", title)


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    global _FAST_CLEAN_MODE

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)
    _KTOPL_SET.clear()
    _VERSN_SET.clear()

    log.info("=" * 60)
    log.info("SAP Chart of Accounts + FSV Staging Pipeline  [run: %s]", run_id)
    log.info("SAP system     : %s", SAP_SYSTEM)
    log.info("Company codes  : %s", COMPANY_CODES)
    log.info("FSV versions   : %s", FSV_VERSIONS or "automatic (all FSVs with ranges for the charts)")
    log.info("Extra charts   : %s", EXTRA_CHARTS or "none")
    log.info("Language       : %s", LANGUAGE)
    log.info("Staging tables : stg_sap_%s_<TABLE>", STAGING_PREFIX)
    log.info("=" * 60)

    if not COMPANY_CODES:
        log.critical("No company codes configured (COA_COMPANY_CODES) — aborting.")
        sys.exit(1)

    try:
        cfg = ExtractionConfig()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)
    _FAST_CLEAN_MODE = cfg.fast_clean_mode

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, STAGING_PREFIX, log,
                                use_unlogged=cfg.use_unlogged_tables,
                                use_copy=cfg.use_copy_loader)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    meta = TableMetadata(sap, log)
    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            name = tbl_def["table"].upper()
            try:
                if tbl_def.get("is_anchor"):
                    ok, count = _probe_anchor_table(tbl_def, sap, loader, meta, cfg, log)
                    results[name] = (ok, count)
                    if count == 0:
                        log.warning("Anchor (%s) returned no rows — remaining tables skipped.", name)
                        break
                    if EXTRA_CHARTS:
                        _KTOPL_SET.update(EXTRA_CHARTS)
                    log.info("Charts of accounts for SKA1 / SKAT: %s", sorted(_KTOPL_SET))
                else:
                    results[name] = extract_table(tbl_def, sap, loader, meta, cfg, log)
                    if name == "FAGL_011ZC":
                        if _VERSN_SET:
                            log.info("FSV versions found for charts %s: %s",
                                     sorted(_KTOPL_SET), sorted(_VERSN_SET))
                        else:
                            log.warning("No FSV has account ranges for charts %s — the "
                                        "hierarchy will be empty (all accounts 'Not assigned').",
                                        sorted(_KTOPL_SET))
            except Exception:
                log.exception("Table %s FAILED.", name)
                results[name] = (False, 0)
                try:
                    if loader.conn is not None and not loader.conn.closed:
                        loader.conn.rollback()
                except Exception:
                    pass
        _post_checks(loader, cfg.staging_schema, log)
    finally:
        sap.close()
        loader.close()

    succeeded = [(t, n) for t, (ok, n) in results.items() if ok]
    failed    = [t for t, (ok, _) in results.items() if not ok]

    log.info("=" * 60)
    log.info("Run complete in %.1fs", time.time() - overall_start)
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-12s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)

    if failed or not succeeded:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")