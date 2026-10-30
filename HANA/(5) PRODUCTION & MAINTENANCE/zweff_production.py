"""
SAP ZWEFF (Weaving Efficiency — daily entry) — Staging Extractor
=================================================================
Extracts the tables / columns used by the custom ABAP dialog program ZPPMP_WEAVING_EFFICIENCY
(t-code ZWEFF) from SAP and loads them into PostgreSQL staging tables
(one staging table per SAP table).

ZWEFF is a data-ENTRY transaction (screen 0100 table control), not an ALV report. For a
Plant + Date + Section it shows one line per loom:
  * existing rows are read from ZWEFF;
  * if none exist for that Plant / Date / Section, the lines are seeded from ZLOOM
    (WERKS, SECTIONNAME, LOOMNO, RPM);
  * SECTIONNAME text comes from domain ZSACTIONNAME (FM DOMAIN_VALUE_GET → DD07T);
  * MAKTX comes from MAKT (SPRAS 'E');
  * operator codes are validated against ZOPERATORN.OPERATORNAME;
  * the entry screen then saves with MODIFY ZWEFF.
The two reports launched from screen 0200 (ZPPR_WEAVING_EFFICIENCY and
ZPPR_WEAVING_CUMMULATIVE) read the data stored in ZWEFF, so ZWEFF is the table to stage.

Selection / screen → config mapping
-------------------------------------
WA_HEAD-WERKS        (ZWEFF-WERKS)        → PLANTS
WA_HEAD-DATUM        (ZWEFF-DATUM)        → DATE_FROM / DATE_TO  (screen = single date;
                                            set both equal for one day, a range suits the
                                            cumulative report)
WA_HEAD-SECTIONNAME  (ZWEFF-SECTIONNAME)  → SECTIONS  ([] = all)
Hardcoded in ABAP    → MAKT.SPRAS 'E', domain 'ZSACTIONNAME'

Screen field → SAP source
---------------------------
WERKS / DATUM / SECTIONNAME / LOOMNO / RPM / WIDTH / PPI / MATNR /
OPERATORCODE1..6 / shift A-B-C metres, picks, efficiency / losses / CMPX   → ZWEFF
SECTIONNAMEd (section text)                                               → DD07T.DDTEXT
MAKTX                                                                     → MAKT.MAKTX
Default RPM for new rows                                                  → ZLOOM.RPM
Operator validation                                                       → ZOPERATORN.OPERATORNAME

Calculations done on the screen (replicate downstream in SQL if you need to re-derive;
the results are also stored in ZWEFF, so the staged columns can simply be used):
  MCN          = RPM * 60 * 12 * WIDTH
  PICKS_x_MTR  = SHIFT_x_MTR * 39.37 * PPI                      (x = A, B, C)
  EFFI_x_MTR   = PICKS_x_MTR / MCN * 100
  TOTAL_PROD   = SHIFT_A_MTR + SHIFT_B_MTR + SHIFT_C_MTR
  TOTAL_PICK   = PICKS_A_MTR + PICKS_B_MTR + PICKS_C_MTR
  TOTAL_EFFI   = TOTAL_PICK / ( MCN * 2 ) * 100
  RUN_MIN      = TOTAL_PICK / RPM
  BREAK_CMPX   = 1440 - ( RUN_MIN + KTG_GTG + POWER_FAIL + MAINT_ISSUE + NO_PLAN
                          + RAW_MAT_ISSUE + SPARE_ISSUE + OTHERS )
  OPER_EFF     = RUN_MIN / ( 1440 - ( KTG_GTG + POWER_FAIL + MAINT_ISSUE + NO_PLAN
                          + RAW_MAT_ISSUE + SPARE_ISSUE + OTHERS ) ) * 100

Source tables (extraction order):
  Pass 1 — ZWEFF       : Daily efficiency entries. WERKS / DATUM / SECTIONNAME filters.
                         Collects MATNR and OPERATORCODE1..6.
                         Columns are validated against DD03L (the column list is derived
                         from the screen code, so any column not in the table is skipped).
  Pass 2 — ZLOOM       : Loom master (default RPM for days with no ZWEFF row).
  Pass 3 — DD07T       : Domain texts of ZSACTIONNAME (section names).
  Pass 4 — MAKT        : Material descriptions. MATNR IN set + SPRAS.
  Pass 5 — ZOPERATORN  : Operator master. OPERATORNAME IN set.

Notes
-----
  - Replicating the "seed from ZLOOM" behaviour downstream: for each
    (WERKS, DATUM, SECTIONNAME) that has NO row in ZWEFF, the screen lists the ZLOOM looms
    of that WERKS + SECTIONNAME.
  - The ABAP VALIDATE module compares ZOPERATORN.OPERATORNAME with the operator CODE fields,
    so OPERATORNAME is staged as the key. Add further ZOPERATORN columns if you need them.
  - DD07T is additionally filtered AS4LOCAL = 'A' (active texts only).

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
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple  # noqa: E402

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


# -------------------------------------------------------------
# CONFIGURATION — edit this section only
# -------------------------------------------------------------

# Entry date range applied to ZWEFF.DATUM (screen: single date → set both equal)
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260630")

# Plant(s) — ZWEFF.WERKS / ZLOOM.WERKS
PLANTS: List[str] = ["1102"]

# Section(s) — ZWEFF.SECTIONNAME / ZLOOM.SECTIONNAME. [] = all
SECTIONS: List[str] = []

# Language key for text tables (MAKT, DD07T)
LANGUAGE: str = "E"

# Domain holding the section names (ABAP: I_DOMNAME = 'ZSACTIONNAME')
SECTION_DOMAIN: str = "ZSACTIONNAME"

# Staging table name prefix → stg_sap_<prefix>_<table>
STAGING_PREFIX: str = "ZWEFF"


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_MATNR_SET:    Set[str] = set()   # Materials       (ZWEFF → MAKT)
_OPERATOR_SET: Set[str] = set()   # Operator codes  (ZWEFF.OPERATORCODE1..6 → ZOPERATORN)


# -------------------------------------------------------------
# WHERE clause builders
# -------------------------------------------------------------

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


def _quoted_list(values: Iterable[str]) -> str:
    """['A','B'] -> "'A','B'" (sorted, for use inside an IN ( ... ) clause)."""
    return ",".join(f"'{v}'" for v in sorted(values))


def _build_in_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """
    Builds RFC OPTIONS rows for a large IN filter.
    Each row TEXT <=72 characters. Values packed greedily per row.
    Subsequent rows preceded by 'OR'. Extra appended as 'AND ...' rows.

    When 'extra' is supplied, the OR-chain is wrapped in parentheses so that the
    extra condition applies to ALL value rows (AND binds tighter than OR otherwise).
    """
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.extend(_format_options(f"AND {extra}"))
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    first_clause = True
    wrap = bool(extra)
    i = 0

    while i < len(sorted_vals):
        if first_clause:
            prefix = ("( " if wrap else "") + f"{field_name} IN ("
        else:
            prefix = f"OR {field_name} IN ("
        max_content = 72 - len(prefix) - 1 - (2 if wrap else 0)
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

    if wrap:
        options[-1]["TEXT"] += " )"
        options.extend(_format_options(f"AND {extra}"))

    return options


def _join_and(parts: List[str]) -> str:
    return " AND ".join(p for p in parts if p)


def _in(field_name: str, values: Iterable[str]) -> str:
    """'WERKS', ['A','B'] -> "WERKS IN ('A','B')" ; empty values -> '' (no filter)."""
    values = list(values)
    return f"{field_name} IN ({_quoted_list(values)})" if values else ""


def _build_zweff_options() -> List[Dict[str, str]]:
    """ABAP: WHERE WERKS = WA_HEAD-WERKS AND DATUM = WA_HEAD-DATUM AND SECTIONNAME = ..."""
    return _format_options(_join_and([
        _in("WERKS", PLANTS),
        f"DATUM >= '{DATE_FROM}' AND DATUM <= '{DATE_TO}'",
        _in("SECTIONNAME", SECTIONS),
    ]))


def _build_zloom_options() -> List[Dict[str, str]]:
    """ABAP: WHERE WERKS = WA_HEAD-WERKS AND SECTIONNAME = WA_HEAD-SECTIONNAME."""
    return _format_options(_join_and([
        _in("WERKS", PLANTS), _in("SECTIONNAME", SECTIONS),
    ]))


def _build_dd07t_options() -> List[Dict[str, str]]:
    return _format_options(
        f"DOMNAME = '{SECTION_DOMAIN}' AND DDLANGUAGE = '{LANGUAGE}' AND AS4LOCAL = 'A'"
    )


# -------------------------------------------------------------
# TABLE DEFINITIONS
# Execution order is critical — later passes depend on keys
# collected by earlier passes.
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Daily weaving efficiency entries (anchor)
    # Produces: MATNR (_MATNR_SET), OPERATORCODE1..6 (_OPERATOR_SET).
    # validate_columns: list is taken from the screen code; DD03L decides what exists.
    # =========================================================
    {
        "table": "ZWEFF",
        "validate_columns": True,
        "description": "Weaving efficiency — per loom / day / section entries",
        "columns": [
            "WERKS", "DATUM", "SECTIONNAME", "LOOMNO",      # key (inferred from MODIFY ZWEFF)
            "MATNR",                                         # → _MATNR_SET
            "RPM", "WIDTH", "PPI", "MCN",
            "SHIFT_A_MTR", "PICKS_A_MTR", "EFFI_A_MTR",
            "SHIFT_B_MTR", "PICKS_B_MTR", "EFFI_B_MTR",
            "SHIFT_C_MTR", "PICKS_C_MTR", "EFFI_C_MTR",
            "TOTAL_PROD", "TOTAL_PICK", "TOTAL_EFFI",
            "RUN_MIN", "KTG_GTG", "POWER_FAIL", "MAINT_ISSUE", "NO_PLAN",
            "RAW_MAT_ISSUE", "SPARE_ISSUE", "OTHERS", "BREAK_CMPX",
            "SHIFT_A_WEFT_CMPX", "SHIFT_A_WARP_CMPX",
            "SHIFT_B_WEFT_CMPX", "SHIFT_B_WARP_CMPX",
            "SHIFT_C_WEFT_CMPX", "SHIFT_C_WARP_CMPX",
            "OPER_EFF",
            "OPERATORCODE1", "OPERATORCODE2", "OPERATORCODE3",   # → _OPERATOR_SET
            "OPERATORCODE4", "OPERATORCODE5", "OPERATORCODE6",
        ],
        "options_fn": _build_zweff_options,
        "primary_keys": ["WERKS", "DATUM", "SECTIONNAME", "LOOMNO"],   # adjust if table key differs
        "key_out": {
            "MATNR": "_MATNR_SET",
            "OPERATORCODE1": "_OPERATOR_SET", "OPERATORCODE2": "_OPERATOR_SET",
            "OPERATORCODE3": "_OPERATOR_SET", "OPERATORCODE4": "_OPERATOR_SET",
            "OPERATORCODE5": "_OPERATOR_SET", "OPERATORCODE6": "_OPERATOR_SET",
        },
    },

    # =========================================================
    # PASS 2 — Loom master (default rows / RPM when ZWEFF has no entry)
    # =========================================================
    {
        "table": "ZLOOM",
        "description": "Loom master — section, loom number, default RPM",
        "columns": ["WERKS", "SECTIONNAME", "LOOMNO", "RPM"],
        "options_fn": _build_zloom_options,
        "primary_keys": ["WERKS", "SECTIONNAME", "LOOMNO"],
    },

    # =========================================================
    # PASS 3 — Domain texts for section names (FM DOMAIN_VALUE_GET)
    # =========================================================
    {
        "table": "DD07T",
        "description": "Domain value texts — ZSACTIONNAME (SECTIONNAMEd)",
        "columns": ["DOMNAME", "DDLANGUAGE", "AS4LOCAL", "VALPOS", "DOMVALUE_L", "DDTEXT"],
        "options_fn": _build_dd07t_options,
        "primary_keys": ["DOMNAME", "DDLANGUAGE", "AS4LOCAL", "VALPOS"],
    },

    # =========================================================
    # PASS 4 — Material descriptions (module GET_MAKTX)
    # =========================================================
    {
        "table": "MAKT",
        "description": "Material descriptions (language-dependent)",
        "columns": ["MATNR", "SPRAS", "MAKTX"],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR", "SPRAS"],
    },

    # =========================================================
    # PASS 5 — Operator master (module VALIDATE)
    # =========================================================
    {
        "table": "ZOPERATORN",
        "description": "Operator master — operator name / code validation",
        "columns": ["OPERATORNAME"],
        "options_fn": lambda: _build_in_options("OPERATORNAME", _OPERATOR_SET),
        "primary_keys": ["OPERATORNAME"],
    },
]


# -------------------------------------------------------------
# Extraction tuning  (override via .env)
# -------------------------------------------------------------
@dataclass
class ExtractionConfig:
    batch_size: int       = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "10000")))
    max_buffer_width: int = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries: int      = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s: float  = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every: int     = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))
    staging_schema: str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")


# -------------------------------------------------------------
# Logging
# -------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_zweff_staging_{run_id}.log")

    log = logging.getLogger(f"sap_zweff_staging.{run_id}")
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


# -------------------------------------------------------------
# SAP connection wrapper
# -------------------------------------------------------------
class SAPConnection:
    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system = system
        self._log    = log
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP %s (attempt %d/3)...", self._system, attempt)
                self.conn = get_sap_connection(self._system)
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


# -------------------------------------------------------------
# Column metadata (field lengths from DD03L)
# -------------------------------------------------------------
class ColumnMetadata:
    _FALLBACK_WIDTH = 100

    def __init__(self, sap: SAPConnection, log: logging.Logger) -> None:
        self._sap = sap
        self._log = log

    def existing_columns(self, table: str, columns: List[str]) -> List[str]:
        """
        Returns only those columns that exist in the active DDIC definition of `table`
        (order preserved). Used for dynamically named custom tables where the ABAP
        relies on TRY/CATCH because not every column is guaranteed to exist.
        If the DD03L lookup itself fails, all columns are returned unchanged.
        """
        try:
            result = self._sap.read_table(
                "DD03L",
                ["FIELDNAME"],
                [{"TEXT": f"TABNAME = '{table.upper()}' AND AS4LOCAL = 'A'"}],
                rowcount=2000,
            )
        except Exception:
            self._log.warning(
                "[%s] DD03L column-existence check failed -- using all columns.", table
            )
            return list(columns)

        present = {row["WA"].split("|")[0].strip() for row in result.get("DATA", [])}
        found   = [c for c in columns if c in present]
        missing = [c for c in columns if c not in present]
        if missing:
            self._log.warning("[%s] Columns not found in DD03L, skipped: %s", table, missing)
        return found

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


# -------------------------------------------------------------
# Column chunker (respects RFC buffer width limit)
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# Data sanitiser
# -------------------------------------------------------------
def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


# -------------------------------------------------------------
# Key collector
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# Dynamic table expansion (ZCUSTOM_TAB.TMG_NAME → concrete tables)
# -------------------------------------------------------------
def _expand_table_defs(
    defs: List[Dict[str, Any]],
    log: logging.Logger,
) -> Generator[Dict[str, Any], None, None]:
    """
    Yields the table definitions in order. A definition carrying 'dynamic_table_set'
    is expanded into one concrete definition per table name collected in that set.
    This is a lazy generator, so the set is already populated by earlier passes
    when the dynamic definition is reached.
    """
    current_module = sys.modules[__name__]
    for tbl_def in defs:
        enabled = tbl_def.get("enabled")
        if callable(enabled) and not enabled():
            log.info("Skipping %s -- disabled for the current report mode.",
                     tbl_def.get("stg_alias", tbl_def["table"]))
            continue
        set_name = tbl_def.get("dynamic_table_set")
        if not set_name:
            yield tbl_def
            continue

        names = sorted(getattr(current_module, set_name))
        if not names:
            log.warning("No dynamic table names collected in %s -- nothing to extract.", set_name)
        for name in names:
            concrete = dict(tbl_def)
            concrete["table"]       = name
            concrete["description"] = f"{tbl_def.get('description', name)} [{name}]"
            yield concrete


# -------------------------------------------------------------
# SAP batch generator
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# PostgreSQL Loader
# -------------------------------------------------------------
class PostgresLoader:
    def __init__(
        self,
        schema:   str,
        prefix:   str,
        log:      logging.Logger,
    ) -> None:
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
        safe_tbl  = sap_table.lower().replace("-", "_").replace("/", "_")
        stg_name  = f"stg_sap_{self._prefix.lower()}_{safe_tbl}"
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


# -------------------------------------------------------------
# Per-table pipeline
# -------------------------------------------------------------
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

    meta = ColumnMetadata(sap, log)

    # Dynamically named custom tables: keep only the columns that really exist
    if table_def.get("validate_columns"):
        columns = meta.existing_columns(table, columns)
        if not columns:
            log.warning("[%s] None of the expected columns exist -- table skipped.", table)
            return True, 0
        log.info("Fields (validated): %s", columns)

    widths = meta.get_widths(table, columns)

    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table = loader.create_staging_table(table_def.get("stg_alias", table), columns)

    extractor         = BatchExtractor(sap, table, columns, chunks, options, cfg, log)
    rows_loaded       = 0
    rows_since_commit = 0

    for batch in extractor.batches():
        if key_out:
            _collect_keys(batch, columns, key_out, log)
        hook = table_def.get("on_batch")
        if hook:
            hook(batch, columns)

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


# -------------------------------------------------------------
# Main orchestrator
# -------------------------------------------------------------
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP ZWEFF Weaving Efficiency Staging Pipeline  [run: %s]", run_id)
    log.info("Plants            : %s", PLANTS if PLANTS else "ALL")
    log.info("Entry date range  : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Sections          : %s", SECTIONS if SECTIONS else "ALL")
    log.info("Language          : %s", LANGUAGE)
    log.info("Staging prefix    : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Table definitions : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, STAGING_PREFIX, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections -- aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in _expand_table_defs(STAGING_TABLE_DEFS, log):
            tbl_name = tbl_def.get("stg_alias", tbl_def["table"]).upper()
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
    log.info("    MATNR    : %d materials", len(_MATNR_SET))
    log.info("    OPERATOR : %d operator codes", len(_OPERATOR_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-24s  %d rows", tbl, count)
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