"""
SAP Selective Column Staging Extractor
========================================
Extracts only specified columns from SAP ECC tables and loads them
into PostgreSQL staging tables.

Two-pass extraction strategy:
  Pass 1 — VBAK filtered by company code + date range; VBELN/KUNNR keys collected
  Pass 2 — Child tables (VBAP, VBEP, VBPA, VBUK, VBUP, VBFA) filtered by VBELN IN (...)
  Pass 3 — Master data (KNA1, MAKT) filtered by KUNNR/MATNR keys collected from Pass 2

Author  : [Your Name]
Version : 2.2.0 (PostgreSQL Edition — Orchestrator Compatible)

CHANGES vs 2.1.0
─────────────────
  • COMPANY_CODES now read from env var COMPANY_CODES (comma-separated).
    Fallback: "2600" (unchanged behaviour when run standalone).
  • Module-level key sets are explicitly cleared at the start of main() so that
    the script is safe to import and call multiple times in the same process.
  • Exit code 0  → success, 1 → failure (for subprocess orchestration).
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

SAP_SYSTEM: str = "ECC"


# ─────────────────────────────────────────────────────────────
# ★  CONFIGURATION  (all values read from environment)
# ─────────────────────────────────────────────────────────────

# Date range — set by orchestrator via env vars; fallback keeps standalone behaviour
DATE_FROM: str = os.getenv("DATE_FROM", "20260901")
DATE_TO:   str = os.getenv("DATE_TO",   "20260930")

# ── NEW ──────────────────────────────────────────────────────
# Company codes now read from env var (comma-separated list).
# Example env value: "1200,1400,2600,2700,dapl,hecl,hpdl,pfl"
COMPANY_CODES: List[str] = [
    c.strip().upper()
    for c in os.getenv("COMPANY_CODES", "1200").split(",")
    if c.strip()
]

# ─────────────────────────────────────────────────────────────
# Module-level key sets — populated at runtime
# ─────────────────────────────────────────────────────────────
_VBELN_SET: Set[str] = set()
_KUNNR_SET: Set[str] = set()
_MATNR_SET: Set[str] = set()
_PERNR_SET: Set[str] = set()


# ─────────────────────────────────────────────────────────────
# WHERE clause builders
# ─────────────────────────────────────────────────────────────
def _build_bukrs_filter() -> str:
    return "BUKRS_VF IN (" + ",".join(f"'{c}'" for c in COMPANY_CODES) + ")"


def _build_in_options(
    field: str,
    values: Set[str],
    extra: str = "",
    chunk_size: int = 50,
) -> List[Dict[str, str]]:
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field} = '__NO_MATCH__'"}]
        if extra:
            rows.append({"TEXT": f"AND {extra}"})
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    first_clause = True

    i = 0
    while i < len(sorted_vals):
        prefix      = f"{field} IN (" if first_clause else f"OR {field} IN ("
        max_row_len = 72 - len(prefix) - 1
        row_vals: List[str] = []
        row_len = 0
        while i < len(sorted_vals):
            quoted = f"'{sorted_vals[i]}'"
            need = len(quoted) + (1 if row_vals else 0)
            if row_vals and row_len + need > max_row_len:
                break
            row_vals.append(quoted)
            row_len += need
            i += 1

        row_text = prefix + ",".join(row_vals) + ")"
        options.append({"TEXT": row_text})
        first_clause = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})

    return options


def _in_where(field: str, values: Set[str], extra: str = "") -> str:
    if not values:
        return f"{field} = '__NO_MATCH__'" + (f" AND {extra}" if extra else "")
    sample = sorted(values)[:5]
    sample_str = ",".join(f"'{v}'" for v in sample)
    suffix = ",…" if len(values) > 5 else ""
    where = f"{field} IN ({sample_str}{suffix})"
    if extra:
        where += f" AND {extra}"
    return where


# ─────────────────────────────────────────────────────────────
# ★  TABLE DEFINITIONS
# ─────────────────────────────────────────────────────────────

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # ══════════════════════════════════════════════════════════
    # PASS 1 — Anchor table: filtered by company code + date
    # ══════════════════════════════════════════════════════════
    {
        "table": "VBAK",
        "description": "Sales Document Header (anchor — company + date filter)",
        "columns": [
            "VBELN",
            "AUDAT",
            "BSTNK",
            "WAERK",
            "VKGRP",
            "KUNNR",
            "BUKRS_VF",
        ],
        "options_fn": lambda: _format_options(
            f"{_build_bukrs_filter()} AND AUDAT >= '{DATE_FROM}' AND AUDAT <= '{DATE_TO}'"
        ),
        "primary_keys": ["VBELN"],
        "key_out": {
            "VBELN": "_VBELN_SET",
            "KUNNR": "_KUNNR_SET",
        },
    },

    # ══════════════════════════════════════════════════════════
    # PASS 2 — Child tables: filtered by VBELN IN (...)
    # ══════════════════════════════════════════════════════════
    {
        "table": "VBAP",
        "description": "Sales Document Item",
        "columns": [
            "VBELN",
            "POSNR",
            "MATNR",
            "ARKTX",
            "KWMENG",
            "KBMENG",
            "NETWR",
            "VRKME",
            "LGORT",
            "WERKS",
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET),
        "primary_keys": ["VBELN", "POSNR"],
        "key_out": {"MATNR": "_MATNR_SET"},
    },

    {
        "table": "VBEP",
        "description": "Schedule Lines (Delivery Date)",
        "columns": [
            "VBELN",
            "POSNR",
            "EDATU",
            "ETENR",
            "WMENG",
            "BMENG",
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET),
        "primary_keys": ["VBELN", "POSNR", "ETENR"],
    },

    {
        "table": "VBPA",
        "description": "Partner Functions — Sold-to Party only (PARVW = AG)",
        "columns": [
            "VBELN",
            "POSNR",
            "PARVW",
            "KUNNR"
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET, extra="PARVW = ''"),
        "primary_keys": ["VBELN", "POSNR", "PARVW"],
        "key_out": {"KUNNR": "_KUNNR_SET"},
    },

    {
        "table": "VBPA",
        "stg_name_override": "vbpa_er",
        "description": "Partner Functions — Employee Responsible only (PARVW = ER)",
        "columns": [
            "VBELN",
            "POSNR",
            "PARVW",
            "PERNR",
            "KUNNR"
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET, extra="PARVW = 'ZM'"),
        "primary_keys": ["VBELN", "POSNR", "PARVW"],
        "key_out": {"PERNR": "_PERNR_SET"},
    },

    {
        "table": "VBUK",
        "description": "Sales Document Header Status",
        "columns": [
            "VBELN",
            "GBSTK",
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET),
        "primary_keys": ["VBELN"],
    },

    {
        "table": "VBUP",
        "description": "Sales Document Item Status",
        "columns": [
            "VBELN",
            "POSNR",
            "GBSTA",
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET),
        "primary_keys": ["VBELN", "POSNR"],
    },

    {
        "table": "VBFA",
        "description": "Document Flow (Returns / Invoices) — filtered by VBELV",
        "columns": [
            "VBELV",
            "POSNV",
            "VBELN",
            "POSNN",
            "VBTYP_N",
        ],
        "options_fn": lambda: _build_in_options("VBELV", _VBELN_SET),
        "primary_keys": ["VBELV", "POSNV", "VBELN"],
    },

    # ══════════════════════════════════════════════════════════
    # PASS 3 — Master data: filtered by keys from Pass 1+2
    # ══════════════════════════════════════════════════════════
    {
        "table": "KNA1",
        "description": "Customer Master — only customers seen in this order set",
        "columns": [
            "KUNNR",
            "NAME1",
        ],
        "options_fn": lambda: _build_in_options("KUNNR", _KUNNR_SET),
        "primary_keys": ["KUNNR"],
    },

    {
        "table": "MAKT",
        "description": "Material Description — English, only materials in this order set",
        "columns": [
            "MATNR",
            "MAKTX",
        ],
        "options_fn": lambda: _build_in_options("MATNR", _MATNR_SET, extra="SPRAS = 'E'"),
        "primary_keys": ["MATNR"],
    },

    {
        "table": "LIPS",
        "description": "Delivery Item Data",
        "columns": [
            "VBELN",
            "POSNR",
            "LGORT",
        ],
        "options_fn": lambda: _build_in_options("VGBEL", _VBELN_SET),
        "primary_keys": ["VBELN", "POSNR"],
    },

    {
        "table": "PA0002",
        "description": "HR Master Records — Personal Data (PERNR IN, from VBPA PARVW=ER)",
        "columns": [
            "PERNR",
            "VORNA",
            "NACHN",
            "GBORT",
            "NATIO",
            "VNAMC",
            "NCHMC",
        ],
        "options_fn": lambda: _build_in_options(
            "PERNR", _PERNR_SET,
            extra=f"BEGDA <= '{DATE_TO}' AND ENDDA >= '{DATE_TO}'"
        ),
        "primary_keys": ["PERNR"],
    },
]


# ─────────────────────────────────────────────────────────────
# Extraction tuning
# ─────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────
# Logging / Env Utils
# ─────────────────────────────────────────────────────────────
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_staging_{run_id}.log")

    log = logging.getLogger(f"sap_staging.{run_id}")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"))

    log.addHandler(fh)
    log.addHandler(ch)
    log.info("Log file: %s", log_file)
    return log


# ─────────────────────────────────────────────────────────────
# SAP connection wrapper
# ─────────────────────────────────────────────────────────────
class SAPConnection:
    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system = system
        self._log    = log
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP %s (attempt %d/3)…", self._system, attempt)
                self.conn = get_sap_connection(self._system)
                self._log.info("SAP connection established.")
                return
            except LogonError:
                self._log.exception("SAP logon failed — check credentials.")
                raise
            except CommunicationError as exc:
                self._log.warning("Attempt %d failed: %s", attempt, exc)
                if attempt == 3: raise
                time.sleep(5)

    def read_table(self, table, fields, options=None, rowcount=5000, rowskips=0) -> Dict[str, Any]:
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
            self._log.warning("Communication error — attempting reconnect…")
            self._connect()
            raise

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self._log.info("SAP connection closed.")


# ─────────────────────────────────────────────────────────────
# Column Metadata / Chunker / Cleaning
# ─────────────────────────────────────────────────────────────
class ColumnMetadata:
    def __init__(self, sap: SAPConnection, log: logging.Logger) -> None:
        self._sap, self._log = sap, log

    def get_widths(self, table: str, columns: List[str]) -> Dict[str, int]:
        self._log.info("[%s] Fetching widths from DD03L…", table)
        try:
            result = self._sap.read_table(
                "DD03L",
                ["FIELDNAME", "LENG"],
                [{"TEXT": f"TABNAME = '{table.upper()}' AND AS4LOCAL = 'A'"}],
            )
            col_set = set(columns)
            widths: Dict[str, int] = {}
            for row in result.get("DATA", []):
                parts = row["WA"].split("|")
                if len(parts) >= 2 and parts[0].strip() in col_set:
                    widths[parts[0].strip()] = max(1, int(parts[1].strip()))
            return {c: widths.get(c, 100) for c in columns}
        except Exception:
            return {c: 100 for c in columns}


class ColumnChunker:
    def __init__(self, columns, widths, max_buffer, log):
        self._columns, self._widths, self._max_buffer, self._log = (
            columns, widths, max_buffer, log
        )

    def chunks(self) -> List[List[str]]:
        res, curr, used = [], [], 0
        for col in self._columns:
            w = self._widths.get(col, 100) + 1
            if curr and used + w > self._max_buffer:
                res.append(curr); curr = [col]; used = w
            else:
                curr.append(col); used += w
        if curr:
            res.append(curr)
        return res


def _clean(value: str) -> str:
    v = value.strip()
    return "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r") if v else ""


def _format_options(where: str) -> List[Dict[str, str]]:
    if not where:
        return []
    lines, current = [], ""
    for word in where.split():
        if len(current) + len(word) + 1 > 72:
            lines.append({"TEXT": current.rstrip()})
            current = word + " "
        else:
            current += word + " "
    if current.strip():
        lines.append({"TEXT": current.rstrip()})
    return lines


def _collect_keys(
    rows: List[List[Any]],
    columns: List[str],
    key_out: Dict[str, str],
    log: logging.Logger,
) -> None:
    current_module = sys.modules[__name__]
    col_idx = {col: i for i, col in enumerate(columns)}
    for col, set_name in key_out.items():
        idx = col_idx[col]
        target_set: Set[str] = getattr(current_module, set_name)
        for row in rows:
            if row[idx]:
                target_set.add(row[idx].strip())


# ─────────────────────────────────────────────────────────────
# SAP Batch Extractor
# ─────────────────────────────────────────────────────────────
class BatchExtractor:
    def __init__(self, sap, table, columns, chunks, options, cfg, log):
        self._sap      = sap
        self._table    = table
        self._columns  = columns
        self._chunks   = chunks
        self._options  = options
        self._cfg      = cfg
        self._log      = log

    def batches(self) -> Generator[List[List[Any]], None, None]:
        offset, total = 0, 0
        while True:
            merged: Dict[int, Dict[str, str]] = {}
            for chunk_idx, chunk in enumerate(self._chunks):
                for attempt in range(1, self._cfg.max_retries + 2):
                    try:
                        res = self._sap.read_table(
                            self._table, chunk, self._options,
                            self._cfg.batch_size, offset,
                        )
                        rows = res.get("DATA", [])
                        if chunk_idx == 0 and not rows:
                            return
                        for idx, row in enumerate(rows):
                            vals = row["WA"].split("|")
                            if idx not in merged:
                                merged[idx] = {}
                            for col, val in zip(chunk, vals):
                                merged[idx][col] = _clean(val)
                        break
                    except Exception:
                        if attempt > self._cfg.max_retries:
                            raise
                        time.sleep(self._cfg.retry_delay_s)

            batch = [
                [merged[i].get(col, "") for col in self._columns]
                for i in sorted(merged)
            ]
            total  += len(batch)
            offset += self._cfg.batch_size
            self._log.info(
                "[%s] Fetched %d rows (total: %d).", self._table, len(batch), total
            )
            yield batch


# ─────────────────────────────────────────────────────────────
# PostgreSQL Loader
# ─────────────────────────────────────────────────────────────
class PostgresLoader:
    def __init__(self, schema: str, log: logging.Logger, cfg: Optional[Dict[str, Any]] = None) -> None:
        self._schema = schema
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]     = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL…")
        try:
            self.conn    = get_postgres_connection()
            self._cursor = self.conn.cursor()
            self._log.info("PostgreSQL connection established.")
        except Exception:
            self._log.exception("PostgreSQL connection failed.")
            raise

    # def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
    #     stg_name  = f"stg_sap_va05_v2_{(name_override or sap_table).lower()}"
    #     safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

    #     self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")
    #     col_defs = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
    #     ddl = f"""
    #     DROP TABLE IF EXISTS {self._schema}.{stg_name};
    #     CREATE TABLE {self._schema}.{stg_name} (
    #         {col_defs},
    #         load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         extraction_id UUID      DEFAULT gen_random_uuid()
    #     );
    #     """
    #     self._log.info("Creating staging table %s.%s…", self._schema, stg_name)
    #     self._cursor.execute(ddl)
    #     self.conn.commit()
    #     return f'"{self._schema}"."{stg_name}"'

    def _table_exists(self, schema: str, table: str) -> bool:
        self._cursor.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return self._cursor.fetchone() is not None

    def create_staging_table(
        self,
        sap_table: str,
        columns: List[str],
        name_override: Optional[str] = None,
        ) -> str:
        stg_name  = f"stg_sap_va05_{(name_override or sap_table).lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

        self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        if self._table_exists(self._schema, stg_name):
            self._cursor.execute(f'TRUNCATE TABLE "{self._schema}"."{stg_name}";')
            self.conn.commit()
            self._log.info("Staging table %s.%s truncated.", self._schema, stg_name)
            return f'"{self._schema}"."{stg_name}"'

        col_defs = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
        ddl = f"""
        CREATE TABLE {self._schema}.{stg_name} (
            {col_defs},
            load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID      DEFAULT gen_random_uuid()
        );
        """
        self._log.info("Creating staging table %s.%s…", self._schema, stg_name)
        self._cursor.execute(ddl)
        self.conn.commit()
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


# ─────────────────────────────────────────────────────────────
# Main Pipeline Orchestrator
# ─────────────────────────────────────────────────────────────
def extract_table(table_def, sap, loader, cfg, log) -> Tuple[bool, int]:
    table    = table_def["table"].upper()
    columns  = list(dict.fromkeys(table_def["columns"]))
    desc     = table_def.get("description", table)
    key_out  = table_def.get("key_out", {})

    options_fn = table_def.get("options_fn")
    options    = options_fn() if callable(options_fn) else []

    log.info("━" * 60)
    log.info("Table : %s  (%s)", table, desc)
    log.info("Fields: %s", columns)
    if "primary_keys" in table_def:
        log.info("Primary Keys (Metadata): %s", table_def["primary_keys"])

    meta   = ColumnMetadata(sap, log)
    widths = meta.get_widths(table, columns)
    chunks = ColumnChunker(columns, widths, cfg.max_buffer_width, log).chunks()
    # stg_table = loader.create_staging_table(table, columns)
    stg_table = loader.create_staging_table(table, columns, table_def.get("stg_name_override"))

    extractor        = BatchExtractor(sap, table, columns, chunks, options, cfg, log)
    rows_loaded      = 0
    rows_since_commit = 0

    for batch in extractor.batches():
        if key_out:
            _collect_keys(batch, columns, key_out, log)
        loader.load_batch(stg_table, columns, batch)
        rows_loaded       += len(batch)
        rows_since_commit += len(batch)
        if rows_since_commit >= cfg.commit_every:
            loader.commit()
            rows_since_commit = 0

    loader.commit()
    log.info("[%s] Done — %d rows loaded.", table, rows_loaded)
    return True, rows_loaded


def main() -> None:
    # ── ★ Reset key sets so this function is safe to re-enter ──
    global _VBELN_SET, _KUNNR_SET, _MATNR_SET
    _VBELN_SET.clear()
    _KUNNR_SET.clear()
    _MATNR_SET.clear()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("DATE_FROM     : %s", DATE_FROM)
    log.info("DATE_TO       : %s", DATE_TO)
    log.info("COMPANY_CODES : %s", COMPANY_CODES)

    try:
        cfg = ExtractionConfig()
    except Exception as exc:
        log.critical("Config error: %s", exc)
        sys.exit(1)

    sap    = None
    loader = None
    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, log)
        loader.connect()

        results: Dict[str, Tuple[bool, int]] = {}
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                ok, count = extract_table(tbl_def, sap, loader, cfg, log)
                results[tbl_name] = (ok, count)
            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)

    except Exception:
        log.exception("Fatal error in extraction pipeline.")
        sys.exit(1)
    finally:
        if sap:    sap.close()
        if loader: loader.close()

    log.info("═" * 60)
    log.info(
        "Key sets collected: VBELN:%d  KUNNR:%d  MATNR:%d",
        len(_VBELN_SET), len(_KUNNR_SET), len(_MATNR_SET),
    )
    any_failed = any(not ok for ok, _ in results.values())
    log.info("Overall result: %s", "FAILED" if any_failed else "SUCCESS")
    log.info("═" * 60)

    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()