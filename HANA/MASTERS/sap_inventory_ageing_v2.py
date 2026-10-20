"""
SAP Inventory Ageing — Staging Extractor (HANA-safe, key-driven)
================================================================
Loads MBEW, MBEWH, MARA and MAKT into PostgreSQL staging tables named

    <schema>.<STAGING_PREFIX>_MM_<table>        e.g. stg_sap_MM_mbew

WHY THIS VERSION EXISTS
-----------------------
The previous version paged through each table with ROWSKIPS / ROWCOUNT and
glued the column chunks together by row position. On HANA, a SELECT without
ORDER BY has no stable row order, so:
  * page N could repeat rows already returned and skip others  -> duplicates
    and missing rows;
  * column chunk 2 could come back in a different row order than chunk 1
    -> columns silently attached to the WRONG row.

HOW THIS VERSION WORKS
----------------------
  1. ANCHOR (MBEW): one call returns ALL primary keys for the plant filter.
     No paging, no row skipping, so the key list is complete by construction.
  2. Materials are processed in batches (MATNR IN (...)). Each call returns
     ALL rows for those materials in one go. Every column chunk carries the
     primary key, and chunks are merged BY KEY, never by position.
  3. Every chunk must return exactly the same key set. Any difference stops
     the run with an error instead of loading bad data.
  4. SUPPORTING tables (MBEWH, MARA, MAKT) are fetched for the materials
     found in MBEW (same anchor / supporting idea as the other extractors).
  5. Staging tables get a UNIQUE index on the primary key, and every table
     is reconciled at the end (loaded rows == SAP key count).

Environment variables (all optional)
  PLANTS               comma list of plants (BWKEY), default 1102
  NONZERO_STOCK_ONLY   1 = MBEW rows with LBKUM <> 0 only (default), 0 = all
  KEY_BATCH_SIZE       materials per batch, default 150
  STAGING_PREFIX       default stg_sap
  MAX_BUFFER_WIDTH, MAX_RETRIES, RETRY_DELAY_S, MAX_ROWS_PER_CALL, LOG_DIR
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
from typing import Any, Dict, List, Optional, Set, Tuple  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2 import extras  # noqa: E402
from pyrfc import (  # noqa: E402
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")


# =====================================================================
# 🎯 CONFIG — EDIT THIS SECTION
# =====================================================================
SAP_SYSTEM: str = "HANA"
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "stg_sap")
STAGING_SCHEMA: str = get_postgres_schema(SAP_SYSTEM)

# Staging table name = {STAGING_PREFIX}_{MODULE_TAG}_{table}
# e.g. <hana_schema>.stg_sap_MM_mbew
STAGING_MODULE_TAG: str = "MM"


def staging_table_name(table_name: str) -> str:
    base_prefix = STAGING_PREFIX.lower() if STAGING_PREFIX else "stg_sap"
    return f"{base_prefix}_{STAGING_MODULE_TAG}_{table_name.lower()}"


# Plants (BWKEY) to extract. Empty list = all plants.
PLANTS: List[str] = [p.strip() for p in os.getenv("PLANTS").split(",") if p.strip()]

# True  -> MBEW rows with LBKUM <> 0 only (what the ageing query uses anyway).
# False -> every MBEW row of the plant.
NONZERO_STOCK_ONLY: bool = _env_flag("NONZERO_STOCK_ONLY", "1")

# Order matters: the anchor (MBEW) must come first, the rest are driven by its MATNR list.
TABLES_TO_EXTRACT: List[str] = ["MBEW", "MBEWH"]

# pk            primary key columns (used to merge column chunks and to de-duplicate)
# key_col       column used to batch the extraction (MATNR IN (...))
# anchor        True = key list comes from a single full key scan of this table
# plant_col     column the PLANTS filter applies to
# extra         extra WHERE conditions (one per entry)
# report_missing  log materials that have no row in this table
TABLE_CONFIG: Dict[str, Dict[str, Any]] = {
    "MBEW": dict(pk=["MATNR", "BWKEY", "BWTAR"], key_col="MATNR", anchor=True,
                 plant_col="BWKEY", extra=[], report_missing=False),
    "MBEWH": dict(pk=["MATNR", "BWKEY", "BWTAR", "LFGJA", "LFMON"], key_col="MATNR",
                  anchor=False, plant_col="BWKEY", extra=[], report_missing=False),
    "MARA": dict(pk=["MATNR"], key_col="MATNR", anchor=False,
                 plant_col=None, extra=[], report_missing=True),
    "MAKT": dict(pk=["MATNR", "SPRAS"], key_col="MATNR", anchor=False,
                 plant_col=None, extra=["SPRAS = 'E'"], report_missing=True),
}

# Output width SAP needs per datatype on top of DD03L's LENG (delimiter, sign, decimal point)
_WIDTH_PAD: Dict[str, int] = {
    "DEC": 3, "CURR": 3, "QUAN": 3, "FLTP": 8,
    "INT1": 2, "INT2": 3, "INT4": 3, "INT8": 4,
}

# MATNR lists produced by anchor tables, keyed by key column name
_ANCHOR_KEYS: Dict[str, List[str]] = {}


# =====================================================================
# 📊 EXTRACTION CONFIG
# =====================================================================
@dataclass
class ExtractionConfig:
    key_batch_size: int = field(default_factory=lambda: int(os.getenv("KEY_BATCH_SIZE", "150")))
    max_buffer_width: int = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_delay: int = field(default_factory=lambda: int(os.getenv("RETRY_DELAY_S", "5")))
    # Hard ceiling per RFC call. If a call returns this many rows it is treated as truncated.
    max_rows_per_call: int = field(default_factory=lambda: int(os.getenv("MAX_ROWS_PER_CALL", "1000000")))

    def __post_init__(self):
        if self.key_batch_size < 1:
            raise ValueError("KEY_BATCH_SIZE must be >= 1")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512")


class ConsistencyError(RuntimeError):
    """SAP returned inconsistent data (chunks disagree, truncated call, key mismatch)."""


# =====================================================================
# 🎯 LOGGING SETUP
# =====================================================================
def setup_logging(run_id: str) -> logging.Logger:
    log_dir = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_multi_extract_{run_id}.log")

    logger = logging.getLogger(f"SAP_MultiExtractor_{run_id}")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger


# =====================================================================
# 🧱 WHERE-CLAUSE BUILDERS (RFC OPTIONS rows, max 72 chars each)
# =====================================================================
_ROW_LIMIT = 72


def _plant_part(col: str, plants: List[str]) -> List[str]:
    if not plants:
        return []
    if len(plants) == 1:
        return [f"{col} = '{plants[0]}'"]
    rows = [f"( {col} = '{plants[0]}'"]
    rows += [f"OR {col} = '{p}'" for p in plants[1:]]
    rows.append(")")
    return rows


def _in_part(col: str, values: List[str]) -> List[str]:
    """( COL IN ('a','b',..) OR COL IN (..) ) split into rows that fit the 72-char limit."""
    width = 62
    groups: List[str] = []
    i = 0
    while i < len(values):
        prefix = f"{col} IN (" if not groups else f"OR {col} IN ("
        room = width - len(prefix) - 1
        toks: List[str] = []
        used = 0
        while i < len(values):
            tok = "'" + values[i].replace("'", "''") + "'"
            need = len(tok) + (1 if toks else 0)
            if toks and used + need > room:
                break
            toks.append(tok)
            used += need
            i += 1
        groups.append(prefix + ",".join(toks) + ")")
    groups[0] = "( " + groups[0]
    groups.append(")")
    return groups


def _compose_where(parts: List[List[str]]) -> List[Dict[str, str]]:
    rows: List[str] = []
    for part in parts:
        if not part:
            continue
        part = list(part)
        if rows:
            part[0] = "AND " + part[0]
        rows.extend(part)
    for r in rows:
        if len(r) > _ROW_LIMIT:
            raise ValueError(f"WHERE row longer than {_ROW_LIMIT} chars: {r!r}")
    return [{"TEXT": r} for r in rows]


def _base_parts(table: str) -> List[List[str]]:
    cfg = TABLE_CONFIG[table]
    parts: List[List[str]] = []
    if cfg.get("plant_col"):
        parts.append(_plant_part(cfg["plant_col"], PLANTS))
    if table == "MBEW" and NONZERO_STOCK_ONLY:
        parts.append(["LBKUM <> 0"])
    for cond in cfg.get("extra", []):
        parts.append([cond])
    return [p for p in parts if p]


def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


# =====================================================================
# 🔌 SAP CONNECTION CLASS
# =====================================================================
class SAPConnection:
    """SAP connection with reconnect. Uses BBP_RFC_READ_TABLE (as before)."""

    def __init__(self, system: str, logger: logging.Logger):
        self.system = system
        self.logger = logger
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self):
        for attempt in range(1, 4):
            try:
                self.logger.info(f"Connecting to SAP (attempt {attempt}/3)...")
                self.conn = get_sap_connection(self.system)
                self.logger.info("✅ SAP connection established")
                return
            except LogonError as e:
                self.logger.error(f"SAP logon failed: {e}")
                raise
            except CommunicationError as e:
                self.logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt == 3:
                    raise
                time.sleep(5)

    def read_table(self, table: str, fields: List[str], options: Optional[List[Dict]] = None,
                   rowcount: int = 1000000) -> Dict[str, Any]:
        try:
            return self.conn.call(
                "BBP_RFC_READ_TABLE",
                QUERY_TABLE=table,
                DELIMITER="|",
                FIELDS=[{"FIELDNAME": f} for f in fields],
                OPTIONS=options or [],
                ROWCOUNT=rowcount,
                ROWSKIPS=0,
            )
        except (ABAPApplicationError, ABAPRuntimeError) as e:
            self.logger.error(f"ABAP error reading {table}: {e}")
            raise
        except CommunicationError as e:
            self.logger.error(f"Communication error: {e}")
            self._connect()
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("SAP connection closed")


# =====================================================================
# 📋 METADATA + COLUMN CHUNKING
# =====================================================================
class TableMetadata:
    def __init__(self, sap: SAPConnection, logger: logging.Logger):
        self.sap = sap
        self.logger = logger

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Columns in DICTIONARY POSITION order (DD03L itself returns them alphabetically)."""
        self.logger.info(f"Fetching metadata for {table_name}...")
        result = self.sap.read_table(
            "DD03L",
            ["FIELDNAME", "POSITION", "LENG", "DATATYPE"],
            [{"TEXT": f"TABNAME = '{table_name}' AND AS4LOCAL = 'A'"}],
            rowcount=5000,
        )
        raw = []
        seen = set()
        for row in result.get("DATA", []):
            parts = row["WA"].split("|")
            if len(parts) < 4:
                continue
            name = parts[0].strip()
            datatype = parts[3].strip()
            if (not name or name.startswith(".") or "INCLUDE" in name.upper()
                    or not datatype or name in seen):
                continue
            try:
                pos = int(parts[1].strip() or 0)
            except ValueError:
                pos = 0
            try:
                length = int(parts[2].strip() or 255)
            except ValueError:
                length = 255
            raw.append({"name": name, "position": pos, "length": length, "datatype": datatype})
            seen.add(name)

        raw.sort(key=lambda c: c["position"])
        self.logger.info(f"✅ Found {len(raw)} valid columns")
        return raw


def _width(col: Dict[str, Any]) -> int:
    return col["length"] + _WIDTH_PAD.get(col["datatype"].upper(), 1)


class ColumnChunker:
    """Split columns into RFC-sized chunks. The primary key is repeated in EVERY chunk."""

    def __init__(self, columns: List[Dict[str, Any]], pk: List[str],
                 config: ExtractionConfig, logger: logging.Logger):
        self.columns = columns
        self.pk = pk
        self.config = config
        self.logger = logger

    def create_chunks(self) -> List[List[str]]:
        by_name = {c["name"]: c for c in self.columns}
        missing = [k for k in self.pk if k not in by_name]
        if missing:
            raise ValueError(f"Primary key column(s) not found in DD03L: {missing}")

        budget = self.config.max_buffer_width - sum(_width(by_name[k]) for k in self.pk)
        if budget <= 0:
            raise ValueError("Primary key alone exceeds the RFC buffer width")

        chunks: List[List[str]] = []
        cur: List[str] = []
        used = 0
        for col in self.columns:
            if col["name"] in self.pk:
                continue
            w = _width(col)
            if cur and used + w > budget:
                chunks.append(self.pk + cur)
                cur, used = [col["name"]], w
            else:
                cur.append(col["name"])
                used += w
        if cur or not chunks:
            chunks.append(self.pk + cur)

        self.logger.info(f"✅ Created {len(chunks)} column chunk(s), primary key in each")
        return chunks


# =====================================================================
# 💾 POSTGRES TABLE MANAGER
# =====================================================================
class PostgresTableManager:
    def __init__(self, schema: str, logger: logging.Logger):
        self.schema = schema
        self.logger = logger
        self.conn = None
        self.cursor = None

    def connect(self):
        self.logger.info("Connecting to PostgreSQL...")
        self.conn = get_postgres_connection()
        self.cursor = self.conn.cursor()
        self.cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}";')
        self.conn.commit()
        self.logger.info(f'✅ PostgreSQL connected, schema "{self.schema}" ready')

    @staticmethod
    def _safe_col(name: str) -> str:
        return name.replace("-", "_").replace("/", "_").lower()

    def _table_exists(self, table_name: str) -> bool:
        self.cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s)",
            (self.schema, table_name),
        )
        return bool(self.cursor.fetchone()[0])

    def ensure_table(self, table_name: str, columns: List[Dict[str, Any]], pk: List[str]) -> str:
        """Create if missing, truncate if present, then guarantee a UNIQUE index on the key."""
        qualified = f'"{self.schema}"."{table_name}"'
        try:
            if self._table_exists(table_name):
                self.logger.info(f"Table {qualified} exists — truncating...")
                self.cursor.execute(f"TRUNCATE TABLE {qualified};")
            else:
                self.logger.info(f"Table {qualified} does not exist — creating...")
                col_defs = ",\n    ".join(f'"{self._safe_col(c["name"])}" TEXT' for c in columns)
                self.cursor.execute(f"""
                    CREATE TABLE {qualified} (
                        {col_defs},
                        load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        extraction_id UUID DEFAULT gen_random_uuid()
                    );""")
            idx = f"ux_{table_name}_pk"[:63]
            pk_sql = ", ".join(f'"{self._safe_col(k)}"' for k in pk)
            self.cursor.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx}" ON {qualified} ({pk_sql});')
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(f"ensure_table failed for {qualified}: {e}")
            raise
        return qualified

    def insert_batch(self, qualified: str, columns: List[str], data: List[List[Any]]) -> int:
        """
        Insert one batch and COMMIT it. Returns rows actually inserted.
        Because every batch is committed on its own, a rollback can only affect the
        current batch (the old version lost all uncommitted batches on a rollback).
        """
        if not data:
            return 0
        col_sql = ", ".join(f'"{self._safe_col(c)}"' for c in columns)
        sql = f"INSERT INTO {qualified} ({col_sql}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            extras.execute_values(self.cursor, sql, data, page_size=len(data))
            inserted = self.cursor.rowcount
            self.conn.commit()
            return inserted
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.warning(f"Batch insert failed for {qualified}, retrying row by row: {e}")

        one_sql = (f"INSERT INTO {qualified} ({col_sql}) VALUES ({', '.join(['%s'] * len(columns))}) "
                   f"ON CONFLICT DO NOTHING")
        inserted, failed = 0, 0
        for idx, row in enumerate(data):
            self.cursor.execute("SAVEPOINT row_sp")
            try:
                self.cursor.execute(one_sql, row)
                inserted += self.cursor.rowcount
                self.cursor.execute("RELEASE SAVEPOINT row_sp")
            except psycopg2.Error as row_err:
                self.cursor.execute("ROLLBACK TO SAVEPOINT row_sp")
                self.logger.error(f"Row {idx} failed in {qualified}: {row_err}")
                failed += 1
        self.conn.commit()
        if failed:
            raise ConsistencyError(f"{failed} row(s) could not be loaded into {qualified}")
        return inserted

    def count_rows(self, qualified: str) -> int:
        self.cursor.execute(f"SELECT COUNT(*) FROM {qualified}")
        return int(self.cursor.fetchone()[0])

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("PostgreSQL connection closed")


# =====================================================================
# 📥 DATA EXTRACTOR
# =====================================================================
class DataExtractor:
    def __init__(self, sap: SAPConnection, config: ExtractionConfig, logger: logging.Logger):
        self.sap = sap
        self.config = config
        self.logger = logger

    def _call(self, table: str, fields: List[str], options: List[Dict[str, str]]) -> List[Dict[str, str]]:
        attempt = 0
        while True:
            try:
                result = self.sap.read_table(table, fields, options, rowcount=self.config.max_rows_per_call)
                rows = result.get("DATA", [])
                if len(rows) >= self.config.max_rows_per_call:
                    raise ConsistencyError(
                        f"[{table}] call returned {len(rows):,} rows = MAX_ROWS_PER_CALL, result may be "
                        f"truncated. Lower KEY_BATCH_SIZE or raise MAX_ROWS_PER_CALL.")
                return rows
            except ConsistencyError:
                raise
            except Exception as e:
                attempt += 1
                if attempt > self.config.max_retries:
                    self.logger.error(f"[{table}] failed after {self.config.max_retries} retries")
                    raise
                self.logger.warning(f"[{table}] retry {attempt}/{self.config.max_retries}: {e}")
                time.sleep(self.config.retry_delay)

    @staticmethod
    def _parse(wa: str, ncols: int) -> List[str]:
        vals = wa.split("|")
        if len(vals) > ncols:
            raise ConsistencyError(
                f"Row has {len(vals)} fields but {ncols} were requested "
                f"(a value probably contains the '|' delimiter): {wa[:120]!r}")
        return [_clean(v) for v in vals] + [""] * (ncols - len(vals))

    def scan_keys(self, table: str, pk: List[str], parts: List[List[str]]) -> List[Tuple[str, ...]]:
        """ONE call returning every primary key. No paging, no row skipping."""
        rows = self._call(table, pk, _compose_where(parts))
        return [tuple(self._parse(r["WA"], len(pk))) for r in rows]

    def fetch_by_materials(
        self, table: str, chunks: List[List[str]], pk: List[str], key_col: str,
        materials: List[str], parts: List[List[str]], column_names: List[str],
    ) -> Tuple[List[List[Any]], Set[Tuple[str, ...]]]:
        """All rows for a set of materials. Chunks are merged by primary key and must agree."""
        where = _compose_where(parts + [_in_part(key_col, materials)])
        merged: Dict[Tuple[str, ...], Dict[str, str]] = {}
        reference: Optional[Set[Tuple[str, ...]]] = None

        for ci, chunk in enumerate(chunks):
            keys_seen: List[Tuple[str, ...]] = []
            for r in self._call(table, chunk, where):
                rec = dict(zip(chunk, self._parse(r["WA"], len(chunk))))
                key = tuple(rec[c] for c in pk)
                keys_seen.append(key)
                merged.setdefault(key, {}).update(rec)

            key_set = set(keys_seen)
            if len(keys_seen) != len(key_set):
                raise ConsistencyError(f"[{table}] chunk {ci + 1}: duplicate keys inside ONE call "
                                       f"({len(keys_seen) - len(key_set)} extra rows)")
            if reference is None:
                reference = key_set
            elif key_set != reference:
                raise ConsistencyError(
                    f"[{table}] chunk {ci + 1} returned a different key set than chunk 1 "
                    f"({len(key_set)} vs {len(reference)} rows, "
                    f"{len(key_set ^ reference)} keys differ)")

        data = [[merged[k].get(c, "") for c in column_names] for k in sorted(merged)]
        return data, set(merged)


# =====================================================================
# 🎯 SINGLE-TABLE PIPELINE
# =====================================================================
def extract_one_table(table_name: str, sap: SAPConnection, pg: PostgresTableManager,
                      config: ExtractionConfig, logger: logging.Logger) -> Tuple[bool, int]:
    table = table_name.upper()
    if table not in TABLE_CONFIG:
        raise ValueError(f"{table} is not defined in TABLE_CONFIG")
    cfg = TABLE_CONFIG[table]
    pk, key_col = cfg["pk"], cfg["key_col"]
    pg_table = staging_table_name(table)

    logger.info("=" * 60)
    logger.info(f"Extracting {table} -> {STAGING_SCHEMA}.{pg_table}")
    logger.info(f"Plants: {PLANTS or 'ALL'} | key: {pk} | batch: {config.key_batch_size} materials")
    logger.info("=" * 60)
    t0 = time.time()

    columns = TableMetadata(sap, logger).get_columns(table)
    if not columns:
        raise ValueError(f"No columns found for {table}")
    chunks = ColumnChunker(columns, pk, config, logger).create_chunks()
    column_names = [c["name"] for c in columns]
    qualified = pg.ensure_table(pg_table, columns, pk)
    parts = _base_parts(table)
    extractor = DataExtractor(sap, config, logger)

    expected_by_mat: Dict[str, Set[Tuple[str, ...]]] = {}
    expected_rows: Optional[int] = None

    if cfg["anchor"]:
        keys = extractor.scan_keys(table, pk, parts)
        unique = set(keys)
        if len(keys) != len(unique):
            raise ConsistencyError(f"[{table}] key scan returned {len(keys) - len(unique)} duplicate keys")
        if not unique:
            raise ValueError(f"[{table}] key scan returned 0 rows, check PLANTS / filters")
        expected_rows = len(unique)
        mi = pk.index(key_col)
        for k in unique:
            expected_by_mat.setdefault(k[mi], set()).add(k)
        materials = sorted(expected_by_mat)
        _ANCHOR_KEYS[key_col] = materials
        logger.info(f"[{table}] key scan: {expected_rows:,} rows, {len(materials):,} distinct {key_col}")
    else:
        if key_col not in _ANCHOR_KEYS:
            raise ValueError(f"[{table}] needs the {key_col} list of the anchor table; "
                             f"put the anchor (MBEW) in TABLES_TO_EXTRACT before it")
        materials = _ANCHOR_KEYS[key_col]

    total_loaded = 0
    total_fetched = 0
    found_materials: Set[str] = set()
    mi = pk.index(key_col)
    n_batches = (len(materials) + config.key_batch_size - 1) // config.key_batch_size

    for b in range(n_batches):
        batch = materials[b * config.key_batch_size:(b + 1) * config.key_batch_size]
        data, key_set = extractor.fetch_by_materials(table, chunks, pk, key_col, batch, parts, column_names)

        if expected_rows is not None:
            wanted: Set[Tuple[str, ...]] = set().union(*(expected_by_mat[m] for m in batch))
            if key_set != wanted:
                raise ConsistencyError(
                    f"[{table}] batch {b + 1}: returned {len(key_set)} keys, key scan expected {len(wanted)} "
                    f"({len(key_set ^ wanted)} differ). Data changed during extraction? Re-run.")

        found_materials.update(k[mi] for k in key_set)
        total_fetched += len(data)
        total_loaded += pg.insert_batch(qualified, column_names, data)

        if (b + 1) % 20 == 0 or b + 1 == n_batches:
            logger.info(f"[{table}] batch {b + 1}/{n_batches} | fetched {total_fetched:,} | loaded {total_loaded:,}")

    in_db = pg.count_rows(qualified)
    if total_loaded != total_fetched or in_db != total_fetched:
        raise ConsistencyError(f"[{table}] fetched {total_fetched:,}, inserted {total_loaded:,}, "
                               f"in table {in_db:,}: rows were dropped")
    if expected_rows is not None and in_db != expected_rows:
        raise ConsistencyError(f"[{table}] SAP key scan {expected_rows:,} rows but table has {in_db:,}")

    if cfg.get("report_missing"):
        missing = sorted(set(materials) - found_materials)
        logger.info(f"[{table}] materials without a row in SAP: {len(missing):,} of {len(materials):,}")
        if missing:
            logger.info(f"[{table}] first missing: {missing[:20]}")

    elapsed = time.time() - t0
    logger.info(f"[{table}] ✅ Done: {in_db:,} rows, reconciled, {elapsed:.1f}s")
    return True, in_db


# =====================================================================
# 🎯 ORCHESTRATOR
# =====================================================================
def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = setup_logging(run_id)

    logger.info("=" * 60)
    logger.info("SAP INVENTORY AGEING EXTRACTION -> POSTGRES STAGING")
    logger.info(f"Schema: {STAGING_SCHEMA} | plants: {PLANTS or 'ALL'} | nonzero stock only: {NONZERO_STOCK_ONLY}")
    logger.info(f"Tables ({len(TABLES_TO_EXTRACT)}): {TABLES_TO_EXTRACT}")
    logger.info("=" * 60)

    start = time.time()
    sap: Optional[SAPConnection] = None
    pg: Optional[PostgresTableManager] = None
    results: Dict[str, Tuple[bool, int]] = {}

    try:
        config = ExtractionConfig()
        sap = SAPConnection(SAP_SYSTEM, logger)
        pg = PostgresTableManager(STAGING_SCHEMA, logger)
        pg.connect()

        for table_name in TABLES_TO_EXTRACT:
            try:
                results[table_name.upper()] = extract_one_table(table_name, sap, pg, config, logger)
            except Exception as e:
                logger.error(f"❌ Table {table_name} FAILED: {e}", exc_info=True)
                results[table_name.upper()] = (False, 0)
                if TABLE_CONFIG.get(table_name.upper(), {}).get("anchor"):
                    logger.error("Anchor table failed: stopping, the other tables depend on it.")
                    break
    finally:
        if sap:
            sap.close()
        if pg:
            pg.close()

    ok = [(t, n) for t, (o, n) in results.items() if o]
    failed = [t for t, (o, _) in results.items() if not o]

    logger.info("=" * 60)
    logger.info(f"RUN SUMMARY | duration {time.time() - start:.1f}s")
    for t, n in ok:
        logger.info(f"    {STAGING_SCHEMA}.{staging_table_name(t):<40} {n:>10,} rows (reconciled)")
    if failed:
        logger.error(f"Failed ({len(failed)}): {failed}")
    else:
        logger.info("All tables extracted and reconciled.")
    logger.info("=" * 60)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user")
        sys.exit(1)