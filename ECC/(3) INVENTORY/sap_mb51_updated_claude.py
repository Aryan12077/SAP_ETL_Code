"""
SAP MB51 Selective Column Staging Extractor
============================================
Extracts only specified columns from SAP ECC tables and loads them
into SQL Server staging tables — replicating the VA05 extractor pattern
for Material Document (MB51) data.

Source tables:
  MKPF  — Material Document Header
  MSEG  — Material Document Items
  MAKT  — Material Descriptions (master data, EN only)

FIX (v1.1.0) — Root cause of DBSQL_SQL_ERROR / SQL-3113 on MSEG:
  The original design filtered MSEG via MBLNR IN (...) built from 18 000+
  document numbers.  That produced ~4 500 OPTIONS rows which exceeds SAP's
  internal SQL generation limit for BBP_RFC_READ_TABLE, crashing the query.

  Solution: MSEG carries denormalized header fields directly on every item
  row (BUDAT_MKPF, BUKRS).  We therefore filter MSEG with the same simple
  date + company-code clause used for MKPF — no IN list needed at all.
  This is both correct and dramatically more efficient.

  MKPF is still extracted first (it supplies header-level fields like
  BKTXT, USNAM, CPUTM, BLDAT that are NOT on MSEG).  MBLNR keys are still
  collected from MKPF so downstream consumers can JOIN the two staging
  tables.  But MSEG is now filtered independently.

Extraction strategy (v1.1):
  Pass 1 — MKPF  filtered by BUDAT date range.
            MBLNR keys collected (reference only).
  Pass 2 — MSEG  filtered by BUDAT_MKPF date range + BUKRS IN (...).
            MATNR keys collected for Pass 3.
  Pass 3 — MAKT  filtered by MATNR IN (...) AND SPRAS = 'E'.

MB51 report columns mapped to SAP fields:
  Material Document    -> MKPF.MBLNR  /  MSEG.MBLNR
  Document Date        -> MKPF.BLDAT
  Posting Date         -> MKPF.BUDAT  /  MSEG.BUDAT_MKPF
  Document Header Text -> MKPF.BKTXT
  User name            -> MKPF.USNAM
  Time of Entry        -> MKPF.CPUTM
  Plant                -> MSEG.WERKS
  Company Code         -> MSEG.BUKRS
  Storage Location     -> MSEG.LGORT
  Movement Type        -> MSEG.BWART
  Material             -> MSEG.MATNR
  Batch                -> MSEG.CHARG
  Qty in Un. of Entry  -> MSEG.ERFMG
  Unit of Entry        -> MSEG.ERFME
  Amount in LC         -> MSEG.DMBTR
  Material Desc.       -> MAKT.MAKTX

Author  : [Your Name]
Version : 1.1.0
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import pyodbc
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


# -------------------------------------------------------------
# CONFIGURATION -- edit this section only
# -------------------------------------------------------------

# Posting date range applied to both MKPF.BUDAT and MSEG.BUDAT_MKPF
DATE_FROM = os.getenv("DATE_FROM", "20251001")
DATE_TO   = os.getenv("DATE_TO",   "20251231")

# Company codes -- applied to MSEG.BUKRS (MKPF has no BUKRS field)
COMPANY_CODES: List[str] = ["1200"]

# Movement types to include (leave empty [] to include all)
# Example: ["101", "102", "261", "262"]
MOVEMENT_TYPES: List[str] = []


# -------------------------------------------------------------
# Module-level key sets -- populated at runtime during extraction
# -------------------------------------------------------------
_MBLNR_SET: Set[str] = set()   # Filled after MKPF extraction (informational)
_MATNR_SET: Set[str] = set()   # Filled after MSEG extraction -> used by MAKT


# -------------------------------------------------------------
# WHERE clause builders
# -------------------------------------------------------------

def _format_options(where: str) -> List[Dict[str, str]]:
    """
    Converts a simple WHERE string into RFC OPTIONS rows (<=72 chars each).
    Words are never split across rows.
    Use for short, static clauses only.
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
        max_content = 72 - len(prefix) - 1   # -1 for closing ")"
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


def _build_mseg_options() -> List[Dict[str, str]]:
    """
    Builds the OPTIONS rows for MSEG.

    KEY DESIGN (v1.1 fix):
    ----------------------
    MSEG has BUDAT_MKPF (posting date, denormalized from MKPF) and BUKRS
    (company code) directly on every item row.  We filter MSEG with a
    simple date-range + company-code clause instead of a giant MBLNR IN
    list. This avoids the SQL-3113 error caused by OPTIONS tables with
    thousands of rows exceeding SAP's internal SQL generation limits.

    Resulting WHERE clause example:
        BUDAT_MKPF >= '20251001' AND BUDAT_MKPF <= '20251231'
        AND BUKRS IN ('1200')
        [AND BWART IN ('101','102')]  <- only if MOVEMENT_TYPES configured
    """
    codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
    where = (
        f"BUDAT_MKPF >= '{DATE_FROM}' AND BUDAT_MKPF <= '{DATE_TO}'"
        f" AND BUKRS IN ({codes})"
    )
    if MOVEMENT_TYPES:
        types = ",".join(f"'{t}'" for t in MOVEMENT_TYPES)
        where += f" AND BWART IN ({types})"
    return _format_options(where)


# -------------------------------------------------------------
# TABLE DEFINITIONS
# -------------------------------------------------------------
# Execution order matters:
#   1. MKPF  -- header rows, collects MBLNR keys
#   2. MSEG  -- item rows filtered by date + BUKRS (NOT MBLNR IN)
#              collects MATNR keys
#   3. MAKT  -- material descriptions filtered by MATNR IN (...)
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 -- Material Document Header
    # =========================================================
    {
        "table": "MKPF",
        "description": "Material Document Header (posting date filter)",
        "columns": [
            "MBLNR",   # Material Document Number    -> _MBLNR_SET
            "MJAHR",   # Material Document Year
            "BLDAT",   # Document Date
            "BUDAT",   # Posting Date
            "USNAM",   # User Name
            "CPUTM",   # Time of Entry
            "BKTXT",   # Document Header Text
            "TCODE",   # Transaction Code
        ],
        "options_fn": lambda: _format_options(
            f"BUDAT >= '{DATE_FROM}' AND BUDAT <= '{DATE_TO}'"
        ),
        "primary_keys": ["MBLNR", "MJAHR"],
        "key_out": {
            "MBLNR": "_MBLNR_SET",
        },
    },

    # =========================================================
    # PASS 2 -- Material Document Items
    #
    # Filtered directly by BUDAT_MKPF + BUKRS.
    # BUDAT_MKPF is a denormalized copy of the posting date on
    # every MSEG row; BUKRS (company code) is also on MSEG.
    # This keeps OPTIONS tiny regardless of document volume.
    # =========================================================
    {
        "table": "MSEG",
        "description": "Material Document Items (BUDAT_MKPF + BUKRS filter -- no MBLNR IN)",
        "columns": [
            "MBLNR",        # Material Document Number  (FK to MKPF)
            "MJAHR",        # Material Document Year
            "ZEILE",        # Item in Material Document
            "BWART",        # Movement Type
            "MATNR",        # Material Number           -> _MATNR_SET
            "WERKS",        # Plant
            "LGORT",        # Storage Location
            "CHARG",        # Batch Number
            "SHKZG",        # Debit/Credit Indicator
            "DMBTR",        # Amount in Local Currency
            "MENGE",        # Quantity in Base UoM
            "MEINS",        # Base Unit of Measure
            "ERFMG",        # Quantity in Unit of Entry
            "ERFME",        # Unit of Entry
            "BUKRS",        # Company Code
            "BUDAT_MKPF",   # Posting Date (denormalized from MKPF)
            "SGTXT",        # Item Text
            "EBELN",        # Purchase Order Number
            "EBELP",        # PO Item Number
            "AUFNR",        # Order Number
            "KOSTL",        # Cost Center
            "PRCTR",        # Profit Center
        ],
        "options_fn": _build_mseg_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {
            "MATNR": "_MATNR_SET",
        },
    },

    # =========================================================
    # PASS 3 -- Material Descriptions (English only)
    # =========================================================
    {
        "table": "MAKT",
        "description": "Material Description -- English, only materials in this document set",
        "columns": [
            "MATNR",   # Material Number
            "MAKTX",   # Material Short Description
        ],
        "options_fn": lambda: _build_in_options("MATNR", _MATNR_SET, extra="SPRAS = 'E'"),
        "primary_keys": ["MATNR"],
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
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA",       "dbo"))

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
    log_file = os.path.join(log_dir, f"sap_mb51_staging_{run_id}.log")

    log = logging.getLogger(f"sap_mb51_staging.{run_id}")
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
# Environment / connection config
# -------------------------------------------------------------
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


def build_sql_conn_str() -> str:
    return (
        f"Driver={{{_require_env('SQL_DRIVER')}}};"
        f"Server={_require_env('SQL_SERVER')};"
        f"Database={_require_env('SQL_DATABASE')};"
        f"UID={_require_env('SQL_USERNAME')};"
        f"PWD={_require_env('SQL_PASSWORD')};"
        "TrustServerCertificate=yes;"
    )


# -------------------------------------------------------------
# SAP connection wrapper
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# Column metadata (field lengths from DD03L)
# -------------------------------------------------------------
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
# SQL table manager
# -------------------------------------------------------------
class SQLLoader:
    def __init__(self, conn_str: str, schema: str, log: logging.Logger) -> None:
        self._conn_str = conn_str
        self._schema   = schema
        self._log      = log
        self.conn: Optional[pyodbc.Connection] = None
        self._cursor: Optional[pyodbc.Cursor]  = None

    def connect(self) -> None:
        self._log.info("Connecting to SQL Server...")
        try:
            self.conn    = pyodbc.connect(self._conn_str)
            self._cursor = self.conn.cursor()
            self._log.info("SQL Server connection established.")
        except pyodbc.Error:
            self._log.exception("SQL Server connection failed.")
            raise

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg       = f"[{self._schema}].[STG_SAP_MB51_{sap_table.upper()}]"
        safe_cols = [c.replace("-", "_").replace("/", "_") for c in columns]
        col_defs  = ",\n    ".join(f"[{c}] NVARCHAR(MAX)" for c in safe_cols)

        ddl = f"""
        IF OBJECT_ID('{stg}', 'U') IS NOT NULL DROP TABLE {stg};
        CREATE TABLE {stg} (
            {col_defs},
            [LOAD_TS]        DATETIME2        DEFAULT SYSUTCDATETIME(),
            [EXTRACTION_ID]  UNIQUEIDENTIFIER DEFAULT NEWID()
        );
        """
        self._log.info("Creating staging table %s...", stg)
        self._cursor.execute(ddl)
        self.conn.commit()
        self._log.info("Staging table %s created.", stg)
        return stg

    def load_batch(
        self,
        stg_table: str,
        columns: List[str],
        rows: List[List[Any]],
    ) -> None:
        if not rows:
            return

        safe_cols    = [c.replace("-", "_").replace("/", "_") for c in columns]
        placeholders = ", ".join(["?"] * len(columns))
        col_list     = ", ".join(f"[{c}]" for c in safe_cols)
        sql          = f"INSERT INTO {stg_table} ({col_list}) VALUES ({placeholders})"

        self._cursor.fast_executemany = True
        try:
            self._cursor.executemany(sql, rows)
        except pyodbc.DataError:
            self._log.warning("Batch insert failed -- falling back to row-by-row mode.")
            self._cursor.fast_executemany = False
            skipped = 0
            for i, row in enumerate(rows):
                try:
                    self._cursor.execute(sql, row)
                except Exception as exc:
                    self._log.error(
                        "Row %d skipped: %s | First 5 values: %s", i, exc, row[:5]
                    )
                    skipped += 1
            if skipped:
                self._log.warning("Skipped %d / %d rows in this batch.", skipped, len(rows))

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self.conn:
            self.conn.close()
        self._log.info("SQL Server connection closed.")


# -------------------------------------------------------------
# Per-table pipeline
# -------------------------------------------------------------
def extract_table(
    table_def: Dict[str, Any],
    sap: SAPConnection,
    loader: SQLLoader,
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

    meta   = ColumnMetadata(sap, log)
    widths = meta.get_widths(table, columns)

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


# -------------------------------------------------------------
# Main orchestrator
# -------------------------------------------------------------
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP MB51 Material Document Staging Pipeline  [run: %s]", run_id)
    log.info("Company codes  : %s", COMPANY_CODES)
    log.info("Posting dates  : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Movement types : %s", MOVEMENT_TYPES if MOVEMENT_TYPES else "ALL")
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg          = ExtractionConfig()
        sap_config   = build_sap_config()
        sql_conn_str = build_sql_conn_str()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(sap_config, log)
        loader = SQLLoader(sql_conn_str, cfg.staging_schema, log)
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
    log.info("    MBLNR : %d material documents", len(_MBLNR_SET))
    log.info("    MATNR : %d materials",          len(_MATNR_SET))
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