"""
SAP Multi-Table Extractor — Postgres Staging Loader
=====================================================
Extracts a LIST of SAP tables (full-table, no IN-clause key chaining)
using the fast generic pattern from the single-table extractor, and
loads each one into PostgreSQL as a standalone staging table named:

    stg_sap_MM_{TABLE_NAME}

Insertion logic per table:
    - If the staging table already EXISTS -> TRUNCATE it (keep schema/indexes).
    - If it does NOT exist               -> CREATE it (all columns as TEXT).

This is intentionally simple/fast: each table in TABLES_TO_EXTRACT is
pulled independently (no MATNR/WERKS/LIFNR key-set chaining like the
Material Master orchestrator). If you need filtered extraction, add an
OPTIONS clause per table in the TABLE_CONFIG dict below.

After all tables land in Postgres, run your existing SQL aggregation
query against these staging tables to build your final combined table.
"""

from __future__ import annotations

import os
import sys

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_THIS_DIR, os.path.dirname(_THIS_DIR), os.path.dirname(os.path.dirname(_THIS_DIR))):
    if _p not in sys.path:
        sys.path.append(_p)

from connections import (
    get_postgres_connection,
    get_postgres_schema,
    get_sap_connection,
)

import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import psycopg2
from psycopg2 import extras
from pyrfc import (
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)

# =====================================================================
# 🎯 CONFIG — EDIT THIS SECTION
# =====================================================================

# List the SAP tables you want extracted. That's it — just add/remove names.
TABLES_TO_EXTRACT: List[str] = [
    "MARA",
    "MAKT",
    "MARC",
    "T001W",
    "T001K",
    "T001",
    "PGMI",
    'T134T',
    'T023T',
    'T006A',
    # 'MBEW',
    'TCURX',
    'TCURM',
    # 'MBEWH'
    'MVKE',
    'TVM1T',
    'TVM2T',
    'TVM3T',
    'TVM4T',
    'TVM5T'
]

# Optional per-table filter (OPTIONS clause), keyed by table name (UPPERCASE).
# Leave a table out of this dict (or give it an empty list) to extract it
# in FULL, unfiltered.
#
# Example:
# TABLE_FILTERS = {
#     "MARA": [{"TEXT": "MTART = 'FERT' AND ERSDA >= '20240101'"}],
# }
TABLE_FILTERS: Dict[str, List[Dict[str, str]]] = {
    # "MBEWH": [{"TEXT": "BWKEY = '1201'"}]
}

SAP_SYSTEM: str = "HANA"
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "stg_sap")
STAGING_SCHEMA: str = get_postgres_schema(SAP_SYSTEM)

# Staging table name = {STAGING_PREFIX}_{MODULE_TAG}_{table}
# e.g. <hana_schema>.stg_sap_MM_mbew
STAGING_MODULE_TAG: str = "MM"


def staging_table_name(table_name: str) -> str:
    base_prefix = STAGING_PREFIX.lower() if STAGING_PREFIX else "stg_sap"
    return f"{base_prefix}_{STAGING_MODULE_TAG}_{table_name.lower()}"


# =====================================================================
# 📊 EXTRACTION CONFIG
# =====================================================================
@dataclass
class ExtractionConfig:
    """Configuration for SAP data extraction"""
    # Max rows per key-window. Every chunk of a window is fetched in ONE call
    # (ROWCOUNT=0, no ROWSKIPS), so there is no paging to go out of sync.
    window_rows: int = field(default_factory=lambda: int(os.getenv("WINDOW_ROWS", "20000")))
    max_buffer_width: int = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_delay: int = field(default_factory=lambda: int(os.getenv("RETRY_DELAY_S", "5")))

    def __post_init__(self):
        if self.window_rows < 1:
            raise ValueError("window_rows must be >= 1")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512")


class DataIntegrityError(Exception):
    """Chunks of one window do not describe the same set of rows."""


def _clean(val: str) -> str:
    val = val.strip()
    if val:
        val = val.replace("\x00", "")
        val = "".join(ch for ch in val if ord(ch) >= 32 or ch in "\t\n\r")
    return val


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

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    ))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger





# =====================================================================
# 🔌 SAP CONNECTION CLASS
# =====================================================================
class SAPConnection:
    """Robust SAP connection with retry logic"""

    def __init__(self, system: str, logger: logging.Logger):
        self.system = system
        self.logger = logger
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self):
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.info(f"Connecting to SAP (attempt {attempt}/{max_attempts})...")
                self.conn = get_sap_connection(self.system)
                self.logger.info("✅ SAP connection established")
                return
            except LogonError as e:
                self.logger.error(f"SAP logon failed: {e}")
                raise
            except CommunicationError as e:
                self.logger.warning(f"Connection attempt {attempt} failed: {e}")
                if attempt < max_attempts:
                    time.sleep(5)
                else:
                    raise

    def read_table(self, table: str, fields: List[str], options: Optional[List[Dict]] = None,
                    rowcount: int = 0, rowskips: int = 0) -> Dict[str, Any]:
        """Raw RFC call. No DELIMITER: output is fixed-width, parsed via FIELDS offsets."""
        try:
            return self.conn.call(
                "BBP_RFC_READ_TABLE",
                QUERY_TABLE=table,
                FIELDS=[{"FIELDNAME": f} for f in fields],
                OPTIONS=options or [],
                ROWCOUNT=rowcount,
                ROWSKIPS=rowskips,
            )
        except ABAPApplicationError as e:
            self.logger.error(f"ABAP Application Error: {e}")
            raise
        except ABAPRuntimeError as e:
            self.logger.error(f"ABAP Runtime Error: {e}")
            raise
        except CommunicationError as e:
            self.logger.error(f"Communication Error: {e}")
            self._connect()
            raise

    def read_rows(self, table: str, fields: List[str], options: Optional[List[Dict]] = None,
                  rowcount: int = 0, raw_fields=()) -> List[Dict[str, str]]:
        """
        Read rows as dicts, sliced by the OFFSET/LENGTH SAP reports.
        Fields in `raw_fields` (the primary key) are NOT stripped or cleaned: only the trailing
        blank padding is removed. SAP can hold keys that differ only by a leading blank or an
        invisible character (e.g. 'X0080' and ' X0080'); stripping them would merge two real rows.
        """
        raw = set(raw_fields)
        result = self.read_table(table, fields, options, rowcount=rowcount)
        layout = [(m["FIELDNAME"].strip(), int(m["OFFSET"]), int(m["LENGTH"]))
                  for m in result.get("FIELDS", [])]
        rows = []
        for rec in result.get("DATA", []):
            wa = rec["WA"]
            rows.append({
                n: (wa[o:o + l].rstrip(" ").replace("\x00", "\\x00") if n in raw else _clean(wa[o:o + l]))
                for n, o, l in layout
            })
        return rows

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("SAP connection closed")


# =====================================================================
# 📋 METADATA FETCHING
# =====================================================================
class TableMetadata:
    """Fetch and manage SAP table metadata"""

    def __init__(self, sap: SAPConnection, logger: logging.Logger):
        self.sap = sap
        self.logger = logger

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        self.logger.info(f"Fetching metadata for {table_name}...")
        rows = self.sap.read_rows(
            "DD03L", ["FIELDNAME", "POSITION", "KEYFLAG", "LENG", "DATATYPE"],
            [{"TEXT": f"TABNAME = '{table_name}' AND AS4LOCAL = 'A'"}],
            rowcount=2000,
        )

        def _int(v, default):
            try:
                return int(v)
            except (TypeError, ValueError):
                return default

        columns, seen = [], set()
        for r in rows:
            name = r["FIELDNAME"]
            if not name or name.startswith('.') or 'INCLUDE' in name.upper() or name in seen:
                continue
            columns.append({
                "name": name,
                "position": _int(r["POSITION"], 0),
                "key": r["KEYFLAG"] == "X",
                "length": _int(r["LENG"], 255),
                "datatype": r["DATATYPE"] or "CHAR",
            })
            seen.add(name)

        columns.sort(key=lambda c: c["position"])   # DD03L is NOT returned in table order
        self.logger.info(f"✅ Found {len(columns)} valid columns "
                         f"({sum(c['key'] for c in columns)} key fields)")
        return columns


# =====================================================================
# 🔪 SMART COLUMN CHUNKING
# =====================================================================
class ColumnChunker:
    """Split columns into <=512-byte chunks. EVERY chunk starts with the full primary key."""

    def __init__(self, columns: List[Dict[str, Any]], config: ExtractionConfig, logger: logging.Logger):
        self.columns = columns
        self.config = config
        self.logger = logger

    def create_chunks(self) -> List[List[str]]:
        key_cols = [c for c in self.columns if c["key"]]
        if not key_cols:
            raise ValueError("Table has no key fields in DD03L; cannot merge chunks safely")
        key_names = [c["name"] for c in key_cols]
        key_width = sum(c["length"] + 1 for c in key_cols)
        budget = self.config.max_buffer_width
        if key_width >= budget:
            raise ValueError("Primary key alone exceeds the RFC buffer width")

        chunks, current, width = [], [], key_width
        for col in (c for c in self.columns if not c["key"]):
            w = col["length"] + 1
            if width + w > budget and current:
                chunks.append(key_names + current)
                current, width = [col["name"]], key_width + w
            else:
                current.append(col["name"])
                width += w
        if current or not chunks:
            chunks.append(key_names + current)

        self.logger.info(f"✅ Created {len(chunks)} column chunks (max buffer: {budget}, keys in each)")
        return chunks


# =====================================================================
# 💾 POSTGRES TABLE MANAGER  (create-or-truncate)
# =====================================================================
class PostgresTableManager:
    """Manage PostgreSQL staging table operations: create if missing, truncate if exists"""

    def __init__(self, schema: str, logger: logging.Logger):
        self.schema = schema
        self.logger = logger
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.logger.info("Connecting to PostgreSQL...")
            self.conn = get_postgres_connection()
            self.cursor = self.conn.cursor()
            self.logger.info("✅ PostgreSQL connected")

            self.cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}";')
            self.conn.commit()
            self.logger.info(f"✅ Schema \"{self.schema}\" ready")
        except Exception as e:
            self.logger.error(f"PostgreSQL connection failed: {e}")
            raise

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

    def _has_pk(self, table_name: str) -> bool:
        self.cursor.execute(
            "SELECT 1 FROM information_schema.table_constraints "
            "WHERE table_schema = %s AND table_name = %s AND constraint_type = 'PRIMARY KEY'",
            (self.schema, table_name),
        )
        return self.cursor.fetchone() is not None

    def ensure_table(self, table_name: str, columns: List[Dict[str, Any]]) -> str:
        """Create (all TEXT) or TRUNCATE the staging table, and make sure it has the SAP primary key."""
        qualified = f'"{self.schema}"."{table_name}"'
        key_sql = ", ".join(f'"{self._safe_col(c["name"])}"' for c in columns if c["key"])
        try:
            if self._table_exists(table_name):
                self.logger.info(f"Table {qualified} exists — truncating...")
                self.cursor.execute(f'TRUNCATE TABLE {qualified};')
            else:
                self.logger.info(f"Table {qualified} does not exist — creating...")
                col_defs = ",\n    ".join(f'"{self._safe_col(c["name"])}" TEXT' for c in columns)
                self.cursor.execute(f"""
                CREATE TABLE {qualified} (
                    {col_defs},
                    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    extraction_id UUID DEFAULT gen_random_uuid()
                );""")
            if not self._has_pk(table_name):
                self.cursor.execute(f'ALTER TABLE {qualified} ADD PRIMARY KEY ({key_sql});')
                self.logger.info(f"✅ Primary key ({key_sql}) added")
            self.conn.commit()
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(f"ensure_table failed for {qualified}: {e}")
            raise
        return qualified

    def insert_batch(self, qualified_table: str, columns: List[str], data: List[List[Any]]):
        """Bulk insert. Any failure (including a duplicate key) raises: no silent row skipping."""
        if not data:
            return
        col_sql = ", ".join(f'"{self._safe_col(c)}"' for c in columns)
        try:
            extras.execute_values(self.cursor, f'INSERT INTO {qualified_table} ({col_sql}) VALUES %s',
                                  data, page_size=1000)
        except psycopg2.Error:
            self.conn.rollback()
            raise

    def count_rows(self, qualified_table: str) -> int:
        self.cursor.execute(f"SELECT COUNT(*) FROM {qualified_table}")
        return int(self.cursor.fetchone()[0])

    def commit(self):
        if self.conn:
            self.conn.commit()

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
    """
    Deterministic extraction:
      1. one call reads ONLY the key fields (no WHERE, no paging) -> the authoritative key set
         (verified against SE16N row counts)
      2. keys are grouped into half-open key windows [lo, next_lo) of <= window_rows rows
      3. per window, every column chunk is fetched in ONE call (ROWCOUNT=0, no ROWSKIPS)
      4. chunks are merged on the FULL, RAW primary key and verified to hold identical key sets
      5. at the end the loaded key set must equal the key scan exactly
    """

    def __init__(self, sap: SAPConnection, config: ExtractionConfig, logger: logging.Logger):
        self.sap = sap
        self.config = config
        self.logger = logger

    def _with_retries(self, label: str, fn):
        attempt = 0
        while True:
            try:
                return fn()
            except Exception as e:
                attempt += 1
                if attempt > self.config.max_retries:
                    self.logger.error(f"{label} failed after {self.config.max_retries} retries: {e}")
                    raise
                self.logger.warning(f"{label} failed, retry {attempt}/{self.config.max_retries}: {e}")
                time.sleep(self.config.retry_delay)

    @staticmethod
    def _is_clean(s: str) -> bool:
        """Plain printable ASCII with no leading/trailing blank: safe inside a WHERE literal."""
        return s.isascii() and s.isprintable() and s == s.strip()

    def _build_windows(self, counts: Counter) -> List[Tuple[Optional[str], Optional[str], int]]:
        """
        Half-open windows (lo, hi, rows) meaning lo <= value < hi. The first window has no lower
        bound and the last none on top, so nothing can fall in a gap. A window only starts on a
        clean value, so odd keys (leading blank, NBSP, tab ...) are never written into a filter.
        """
        starts, sizes, n = [None], [], 0
        for v in sorted(counts):
            c = counts[v]
            if n and n + c > self.config.window_rows and self._is_clean(v):
                starts.append(v); sizes.append(n); n = 0
            n += c
        sizes.append(n)
        return [(lo, starts[i + 1] if i + 1 < len(starts) else None, sizes[i])
                for i, lo in enumerate(starts)]

    @staticmethod
    def _window_options(base, fld, lo, hi) -> List[Dict[str, str]]:
        q = lambda x: x.replace("'", "''")
        rng = []
        if lo is not None:
            rng.append({"TEXT": f"{fld} >= '{q(lo)}'"})
        if hi is not None:
            rng.append({"TEXT": ("AND " if rng else "") + f"{fld} < '{q(hi)}'"})
        if not rng:
            return list(base or [])
        if base:
            return [{"TEXT": "("}] + list(base) + [{"TEXT": ") AND"}] + rng
        return rng

    def _fetch_window(self, table: str, chunks: List[List[str]], key_names: List[str], options):
        merged: Dict[tuple, Dict[str, str]] = {}
        key_sets = []
        for i, chunk in enumerate(chunks, 1):
            rows = self.sap.read_rows(table, chunk, options, rowcount=0, raw_fields=key_names)
            ks = set()
            for r in rows:
                k = tuple(r[n] for n in key_names)
                if k in ks:
                    raise DataIntegrityError(f"{table}: duplicate key {k!r} inside chunk {i}")
                ks.add(k)
                merged.setdefault(k, {}).update(r)
            key_sets.append(ks)
        for i, ks in enumerate(key_sets[1:], 2):
            if ks != key_sets[0]:
                raise DataIntegrityError(
                    f"{table}: chunk {i} returned a different row set than chunk 1 "
                    f"({len(ks)} vs {len(key_sets[0])} rows)")
        return merged

    def extract(self, table_name: str, columns: List[Dict[str, Any]],
                column_chunks: List[List[str]], options: Optional[List[Dict[str, str]]] = None):
        """Yields one list of rows (ordered like `columns`) per key window."""
        key_names = [c["name"] for c in columns if c["key"]]
        all_names = [c["name"] for c in columns]
        window_field = next((k for k in key_names if k.upper() != "MANDT"), None)

        scan = self._with_retries(
            f"[{table_name}] key scan",
            lambda: self.sap.read_rows(table_name, key_names, options, rowcount=0, raw_fields=key_names))
        scan_list = [tuple(r[n] for n in key_names) for r in scan]
        scan_set = set(scan_list)
        del scan
        if len(scan_set) != len(scan_list):
            raise DataIntegrityError(
                f"{table_name}: key scan returned {len(scan_list) - len(scan_set)} repeated raw keys")
        expected_total = len(scan_list)

        odd = sorted(k for k in scan_set if not all(self._is_clean(x) for x in k))
        if odd:
            self.logger.warning(
                f"[{table_name}] {len(odd)} key(s) contain a leading blank, trailing/invisible or "
                f"non-ASCII character. Kept exactly as in SAP (they are distinct SAP rows), e.g. "
                f"{[tuple(repr(x) for x in k) for k in odd[:5]]}")

        if window_field is None or expected_total <= self.config.window_rows:
            windows = [(None, None, expected_total)]
        else:
            wi = key_names.index(window_field)
            windows = self._build_windows(Counter(k[wi] for k in scan_set))
        self.logger.info(f"[{table_name}] {expected_total:,} rows, {len(windows)} window(s), "
                         f"{len(column_chunks)} chunk(s) per window")

        loaded = set()
        for w, (lo, hi, n) in enumerate(windows, 1):
            opts = self._window_options(options, window_field, lo, hi)
            merged = self._with_retries(
                f"[{table_name}] window {w}/{len(windows)}",
                lambda: self._fetch_window(table_name, column_chunks, key_names, opts))
            if len(merged) != n:
                self.logger.warning(f"[{table_name}] window {w}: key scan expected {n} rows, got {len(merged)}")
            loaded.update(merged)
            batch = [[merged[k].get(c, "") for c in all_names] for k in sorted(merged)]
            self.logger.info(f"[{table_name}] window {w}/{len(windows)}: {len(batch):,} rows "
                             f"(total {len(loaded):,})")
            yield batch

        if loaded != scan_set:
            missing, extra = sorted(scan_set - loaded), sorted(loaded - scan_set)
            raise DataIntegrityError(
                f"{table_name}: loaded key set differs from the key scan: {len(missing):,} missing "
                f"(e.g. {missing[:3]!r}), {len(extra):,} unexpected (e.g. {extra[:3]!r}). "
                f"Re-run; if it persists, key ordering differs between SAP and Python.")


# =====================================================================
# 🎯 SINGLE-TABLE PIPELINE
# =====================================================================
def extract_one_table(
    table_name: str,
    sap: SAPConnection,
    pg: PostgresTableManager,
    config: ExtractionConfig,
    logger: logging.Logger,
) -> Tuple[bool, int]:
    table_name = table_name.upper()
    pg_table_name = staging_table_name(table_name)
    options = TABLE_FILTERS.get(table_name)

    logger.info("=" * 60)
    logger.info(f"Extracting table: {table_name} -> {STAGING_SCHEMA}.{pg_table_name}")
    logger.info("=" * 60)
    t0 = time.time()

    columns = TableMetadata(sap, logger).get_columns(table_name)
    if not columns:
        raise ValueError(f"No columns found for table {table_name}")
    column_chunks = ColumnChunker(columns, config, logger).create_chunks()
    qualified_table = pg.ensure_table(pg_table_name, columns)

    extractor = DataExtractor(sap, config, logger)
    column_names = [c["name"] for c in columns]

    total_inserted = 0
    for batch in extractor.extract(table_name, columns, column_chunks, options):
        pg.insert_batch(qualified_table, column_names, batch)
        pg.commit()
        total_inserted += len(batch)

    in_db = pg.count_rows(qualified_table)
    if in_db != total_inserted:
        raise DataIntegrityError(f"{table_name}: inserted {total_inserted:,} but table holds {in_db:,}")

    elapsed = time.time() - t0
    logger.info(f"[{table_name}] ✅ Done — {total_inserted:,} rows in {elapsed:.1f}s")
    return True, total_inserted


# =====================================================================
# 🎯 ORCHESTRATOR
# =====================================================================
def main():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    logger = setup_logging(run_id)

    logger.info("=" * 60)
    logger.info("SAP MULTI-TABLE EXTRACTION -> POSTGRES STAGING")
    logger.info(f"Schema: {STAGING_SCHEMA}")
    logger.info(f"Tables ({len(TABLES_TO_EXTRACT)}): {TABLES_TO_EXTRACT}")
    logger.info("=" * 60)

    start_time = time.time()
    sap: Optional[SAPConnection] = None
    pg: Optional[PostgresTableManager] = None
    results: Dict[str, Tuple[bool, int]] = {}

    try:
        sap = SAPConnection(SAP_SYSTEM, logger)

        pg = PostgresTableManager(STAGING_SCHEMA, logger)
        pg.connect()

        config = ExtractionConfig()

        for table_name in TABLES_TO_EXTRACT:
            try:
                ok, count = extract_one_table(table_name, sap, pg, config, logger)
                results[table_name.upper()] = (ok, count)
            except Exception as e:
                logger.error(f"❌ Table {table_name} FAILED: {e}", exc_info=True)
                results[table_name.upper()] = (False, 0)
                # continue with remaining tables rather than aborting the whole run
                continue

    except Exception as e:
        logger.error(f"❌ FATAL ERROR: {e}", exc_info=True)
        raise

    finally:
        if sap:
            sap.close()
        if pg:
            pg.close()

    elapsed = time.time() - start_time
    succeeded = [(t, n) for t, (ok, n) in results.items() if ok]
    failed = [t for t, (ok, _) in results.items() if not ok]

    logger.info("=" * 60)
    logger.info("RUN SUMMARY")
    logger.info(f"Duration: {elapsed:.1f}s")
    logger.info(f"Succeeded ({len(succeeded)}):")
    for tbl, count in succeeded:
        logger.info(f"    {STAGING_SCHEMA}.{staging_table_name(tbl):<40}  {count:,} rows")
    if failed:
        logger.error(f"Failed ({len(failed)}): {failed}")
    else:
        logger.info("All tables extracted successfully.")
    logger.info("=" * 60)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Extraction cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        sys.exit(1)