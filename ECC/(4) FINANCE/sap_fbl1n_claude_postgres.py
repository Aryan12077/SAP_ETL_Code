"""
SAP FBL1N Vendor Line Items — Staging Extractor
================================================
Extracts only specified columns from SAP ECC vendor line-item tables
and loads them into PostgreSQL staging tables, following the same
production pattern as the VF05N and VA05 extractors.

FBL1N Report Column → SAP Field Mapping
-----------------------------------------
CoCd         (Company Code)             → BSIK/BSAK.BUKRS
Vendor       (Vendor Account)           → BSIK/BSAK.LIFNR
Doc. No.     (Accounting Document)      → BSIK/BSAK.BELNR
Year         (Fiscal Year)              → BSIK/BSAK.GJAHR
Itm          (Line Item)                → BSIK/BSAK.BUZEI
Pstng Date   (Posting Date)            → BSIK/BSAK.BUDAT
Doc. Date    (Document Date)            → BSIK/BSAK.BLDAT
Doc.Type     (Document Type)            → BSIK/BSAK.BLART
Reference    (Reference Document)       → BSIK/BSAK.XBLNR
Assignment   (Assignment Number)        → BSIK/BSAK.ZUONR
G/L Acct     (G/L Account)             → BSIK/BSAK.HKONT
Amount       (Amount in Doc. Currency)  → BSIK/BSAK.WRBTR
Curr.        (Document Currency)        → BSIK/BSAK.WAERS
Amt.in LC    (Amount in Local Currency) → BSIK/BSAK.DMBTR
Text         (Item Text)                → BSIK/BSAK.SGTXT
Clrng doc.   (Clearing Document)        → BSIK/BSAK.REBZG

Source tables:
  BSIK  — Vendor Open Items        (open line items)
  BSAK  — Vendor Cleared Items     (cleared/paid line items)
  LFA1  — Vendor Master General    (name, country, city)

Extraction strategy:
  Pass 1 — BSIK filtered by BUDAT date range + BUKRS company code.
            LIFNR keys collected into _LIFNR_SET.
  Pass 2 — BSAK filtered by BUDAT date range + BUKRS company code.
            LIFNR keys collected into _LIFNR_SET.
  Pass 3 — LFA1 filtered by LIFNR IN (_LIFNR_SET) — vendor master
            for every unique vendor seen across Pass 1 + Pass 2.

  Either of Pass 1 / Pass 2 can be disabled independently via
  EXTRACT_OPEN_ITEMS and EXTRACT_CLEARED_ITEMS flags.
  Pass 3 (LFA1) runs automatically using whatever keys were collected
  from the enabled passes.

Design note:
  Both BSIK and BSAK carry BUKRS and BUDAT directly, so their WHERE
  clause is a simple static filter with no IN-list scaling concern.
  LFA1 uses _build_in_options() for safe chunked IN-list filtering,
  exactly as KNA1 is handled in the VF05N extractor.

Author  : [Your Name]
Version : 1.1.0 (PostgreSQL Edition)
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
# CONFIGURATION — edit this section only
# -------------------------------------------------------------

# Posting date range applied to BSIK/BSAK.BUDAT
DATE_FROM = os.getenv("DATE_FROM", "20260601")
DATE_TO   = os.getenv("DATE_TO",   "20260630")

# Company codes — applied to BSIK/BSAK.BUKRS
COMPANY_CODES: List[str] = ["1200"]

# Toggle which item buckets to extract
EXTRACT_OPEN_ITEMS:    bool = True   # BSIK — vendor open items
EXTRACT_CLEARED_ITEMS: bool = True   # BSAK — vendor cleared/paid items

# Document type filter — leave empty [] to include all
# Example: ["KR", "RE", "KZ"] for specific vendor doc types
DOCUMENT_TYPES: List[str] = []


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_LIFNR_SET: Set[str] = set()   # Vendor account keys collected from BSIK + BSAK


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


def _build_bsix_options() -> List[Dict[str, str]]:
    """
    Builds the OPTIONS rows for BSIK and BSAK (shared filter logic).

    Filters by:
      BUDAT  — Posting date range
      BUKRS  — Company code(s)
      BLART  — Document type(s)   [optional]

    Both BSIK and BSAK carry BUKRS and BUDAT directly, so this is a
    simple static clause with no IN-list scaling concern.
    """
    codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
    where = (
        f"BUDAT >= '{DATE_FROM}' AND BUDAT <= '{DATE_TO}'"
        f" AND BUKRS IN ({codes})"
    )
    if DOCUMENT_TYPES:
        types = ",".join(f"'{t}'" for t in DOCUMENT_TYPES)
        where += f" AND BLART IN ({types})"
    return _format_options(where)


# -------------------------------------------------------------
# TABLE DEFINITIONS
# -------------------------------------------------------------
# Execution order matters:
#   1. BSIK  — vendor open items      (enabled by EXTRACT_OPEN_ITEMS)
#              → harvests LIFNR keys into _LIFNR_SET
#   2. BSAK  — vendor cleared items   (enabled by EXTRACT_CLEARED_ITEMS)
#              → harvests LIFNR keys into _LIFNR_SET
#   3. LFA1  — vendor master          (always runs)
#              → filtered by LIFNR IN (_LIFNR_SET) from passes 1+2
#              → mirrors exactly how VF05N fetches KNA1 after VBRK
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Vendor Open Items
    # Filtered by BUDAT date range + BUKRS.
    # =========================================================
    {
        "table":       "BSIK",
        "description": "Vendor Open Items (BUDAT + BUKRS filter)",
        "enabled":     EXTRACT_OPEN_ITEMS,
        "columns": [
            "BUKRS",   # Company Code
            "LIFNR",   # Vendor Account                  ← harvested → _LIFNR_SET
            "BELNR",   # Accounting Document Number
            "GJAHR",   # Fiscal Year
            "BUZEI",   # Line Item Number
            "BUDAT",   # Posting Date
            "BLDAT",   # Document Date
            "BLART",   # Document Type
            "XBLNR",   # Reference Document Number
            "ZUONR",   # Assignment Number
            "HKONT",   # G/L Account
            "WRBTR",   # Amount in Document Currency
            "WAERS",   # Document Currency
            "DMBTR",   # Amount in Local Currency
            "SGTXT",   # Item Text
            "REBZG",   # Clearing Document Number
            "ZFBDT",
            "ZBD1T",
            "ZBD2T",
            "ZBD3T",
            "SHKZG",
        ],
        "options_fn":   _build_bsix_options,
        "primary_keys": ["BUKRS", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {
            "LIFNR": "_LIFNR_SET",
        },
    },

    # =========================================================
    # PASS 2 — Vendor Cleared Items
    # Filtered by BUDAT date range + BUKRS.
    # =========================================================
    {
        "table":       "BSAK",
        "description": "Vendor Cleared Items (BUDAT + BUKRS filter)",
        "enabled":     EXTRACT_CLEARED_ITEMS,
        "columns": [
            "BUKRS",   # Company Code
            "LIFNR",   # Vendor Account                  ← harvested → _LIFNR_SET
            "BELNR",   # Accounting Document Number
            "GJAHR",   # Fiscal Year
            "BUZEI",   # Line Item Number
            "BUDAT",   # Posting Date
            "BLDAT",   # Document Date
            "BLART",   # Document Type
            "XBLNR",   # Reference Document Number
            "ZUONR",   # Assignment Number
            "HKONT",   # G/L Account
            "WRBTR",   # Amount in Document Currency
            "WAERS",   # Document Currency
            "DMBTR",   # Amount in Local Currency
            "SGTXT",   # Item Text
            "REBZG",   # Clearing Document Number
            "ZFBDT",   # Baseline Date
            "ZBD1T",   # Net Days
            "AUGDT",   # Added for cleared items: Clearing Date
            "SHKZG",
        ],
        "options_fn":   _build_bsix_options,
        "primary_keys": ["BUKRS", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {
            "LIFNR": "_LIFNR_SET",
        },
    },

    # =========================================================
    # PASS 3 — Vendor Master
    # Filtered by LIFNR IN (_LIFNR_SET) collected from Pass 1 + 2.
    # Mirrors exactly how VF05N fetches KNA1 after VBRK.
    # =========================================================
    {
        "table":       "LFA1",
        "description": "Vendor Master — all vendors seen in this line-item set",
        "enabled":     True,
        "columns": [
            "LIFNR",   # Vendor Account Number
            "NAME1",   # Vendor Name 1
            "NAME2",   # Vendor Name 2
            "ORT01",   # City
            "LAND1",   # Country Key
            "STRAS",   # Street Address
            "PSTLZ",   # Postal Code
            "KTOKK",   # Vendor Account Group
        ],
        "options_fn":   lambda: _build_in_options("LIFNR", _LIFNR_SET),
        "primary_keys": ["LIFNR"],
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
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA",       "sap_stg_ecc"))

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
    log_file = os.path.join(log_dir, f"sap_fbl1n_staging_{run_id}.log")

    log = logging.getLogger(f"sap_fbl1n_staging.{run_id}")
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
    def __init__(self, cfg: Dict[str, Any], schema: str, log: logging.Logger) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._log    = log
        self.conn: Optional[psycopg2.extensions.connection] = None
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
        stg_name  = f"stg_sap_fbl1n_v2_{sap_table.lower()}"
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
        return f'"{self._schema}"."{stg_name}"'

    def load_batch(
        self,
        stg_table: str,
        columns: List[str],
        rows: List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [f'"{c.replace("-", "_").replace("/", "_").lower()}"' for c in columns]
        # execute_values is the high-performance bulk method for psycopg2
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
    log.info("SAP FBL1N Vendor Line Items Staging Pipeline  [run: %s]", run_id)
    log.info("Company codes    : %s", COMPANY_CODES)
    log.info("Posting dates    : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Open items       : %s", "YES" if EXTRACT_OPEN_ITEMS    else "SKIPPED")
    log.info("Cleared items    : %s", "YES" if EXTRACT_CLEARED_ITEMS else "SKIPPED")
    log.info("Document types   : %s", DOCUMENT_TYPES if DOCUMENT_TYPES else "ALL")
    active = [t for t in STAGING_TABLE_DEFS if t.get("enabled", True)]
    log.info("Active tables    : %d", len(active))
    log.info("=" * 60)

    try:
        cfg        = ExtractionConfig()
        sap_config = build_sap_config()
        # PostgreSQL connection parameters from environment
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
        loader = PostgresLoader(pg_config, cfg.staging_schema, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections -- aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            if not tbl_def.get("enabled", True):
                log.info("Table %s skipped (disabled by config).", tbl_name)
                continue
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
    log.info("    LIFNR : %d unique vendor accounts (used to filter LFA1)", len(_LIFNR_SET))
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