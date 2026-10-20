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
    # "MARA",
    # "MAKT",
    "MARC",
    # "T001W",
    # "T001K",
    # "T001",
    # "PGMI",
    # 'T134T',
    # 'T023T',
    # 'T006A',
    # 'MBEW',
    # 'TCURX',
    # 'TCURM'
    # 'MBEWH'
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
    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "5000")))
    max_buffer_width: int = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    retry_delay: int = field(default_factory=lambda: int(os.getenv("RETRY_DELAY_S", "5")))
    commit_frequency: int = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY", "20000")))

    def __post_init__(self):
        if self.batch_size > 10000:
            raise ValueError("batch_size should not exceed 10000 for stability")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512")


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
                    rowcount: int = 5000, rowskips: int = 0) -> Dict[str, Any]:
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
        try:
            result = self.sap.read_table(
                "DD03L",
                ["FIELDNAME", "LENG", "DECIMALS", "DATATYPE"],
                [{"TEXT": f"TABNAME = '{table_name}' AND AS4LOCAL = 'A'"}],
                rowcount=2000,
            )
        except Exception as e:
            self.logger.error(f"Failed to fetch metadata: {e}")
            raise

        columns = []
        seen = set()

        for row in result.get("DATA", []):
            parts = row["WA"].split("|")
            if len(parts) < 1:
                continue

            col_name = parts[0].strip()

            if (not col_name or
                    col_name.startswith('.') or
                    'INCLUDE' in col_name.upper() or
                    col_name in seen):
                continue

            try:
                length = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 255
            except ValueError:
                length = 255

            columns.append({
                "name": col_name,
                "length": length,
                "datatype": parts[3].strip() if len(parts) > 3 else "CHAR",
            })
            seen.add(col_name)

        self.logger.info(f"✅ Found {len(columns)} valid columns")
        return columns


# =====================================================================
# 🔪 SMART COLUMN CHUNKING
# =====================================================================
class ColumnChunker:
    """Intelligently chunk columns to avoid buffer overflow"""

    def __init__(self, columns: List[Dict[str, Any]], config: ExtractionConfig, logger: logging.Logger):
        self.columns = columns
        self.config = config
        self.logger = logger

    def create_chunks(self) -> List[List[str]]:
        chunks = []
        current_chunk = []
        current_width = 0

        for col in self.columns:
            col_name = col["name"]
            col_length = col["length"]
            estimated_width = col_length + 1

            if current_width + estimated_width > self.config.max_buffer_width and current_chunk:
                chunks.append(current_chunk)
                self.logger.debug(f"Chunk {len(chunks)}: {len(current_chunk)} cols, ~{current_width} chars")
                current_chunk = [col_name]
                current_width = estimated_width
            else:
                current_chunk.append(col_name)
                current_width += estimated_width

        if current_chunk:
            chunks.append(current_chunk)
            self.logger.debug(f"Chunk {len(chunks)}: {len(current_chunk)} cols, ~{current_width} chars")

        self.logger.info(f"✅ Created {len(chunks)} column chunks (max buffer: {self.config.max_buffer_width})")
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

    def ensure_table(self, table_name: str, columns: List[Dict[str, Any]]) -> str:
        """
        If the staging table exists -> TRUNCATE it.
        If it does not exist         -> CREATE it (all columns as TEXT).
        Returns the schema-qualified table name to use for inserts.
        """
        pg_table = table_name  # already includes prefix, passed in by caller
        qualified = f'"{self.schema}"."{pg_table}"'

        try:
            if self._table_exists(pg_table):
                self.logger.info(f"Table {qualified} exists — truncating...")
                self.cursor.execute(f'TRUNCATE TABLE {qualified};')
                self.conn.commit()
                self.logger.info(f"✅ Table {qualified} truncated")
            else:
                self.logger.info(f"Table {qualified} does not exist — creating...")
                col_defs = ",\n    ".join(
                    f'"{self._safe_col(c["name"])}" TEXT' for c in columns
                )
                create_sql = f"""
                CREATE TABLE {qualified} (
                    {col_defs},
                    load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    extraction_id UUID DEFAULT gen_random_uuid()
                );
                """
                self.cursor.execute(create_sql)
                self.conn.commit()
                self.logger.info(f"✅ Table {qualified} created")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.error(f"ensure_table failed for {qualified}: {e}")
            raise

        return qualified

    def insert_batch(self, qualified_table: str, columns: List[str], data: List[List[Any]]):
        """Bulk insert using execute_values, with row-by-row fallback on failure"""
        if not data:
            return

        safe_columns = [f'"{self._safe_col(c)}"' for c in columns]
        col_sql = ", ".join(safe_columns)
        insert_sql = f'INSERT INTO {qualified_table} ({col_sql}) VALUES %s'

        try:
            extras.execute_values(self.cursor, insert_sql, data, page_size=1000)
            self.logger.debug(f"Inserted {len(data)} rows into {qualified_table}")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.logger.warning(f"Batch insert failed for {qualified_table}, attempting row-by-row: {e}")

            single_insert_sql = f'INSERT INTO {qualified_table} ({col_sql}) VALUES ({", ".join(["%s"] * len(columns))})'
            failed_rows = 0
            for idx, row in enumerate(data):
                try:
                    self.cursor.execute(single_insert_sql, row)
                except Exception as row_error:
                    self.conn.rollback()
                    self.logger.error(f"Row {idx} failed in {qualified_table}: {row_error}")
                    failed_rows += 1
                    continue

            if failed_rows:
                self.logger.warning(f"Skipped {failed_rows} problematic rows out of {len(data)} in {qualified_table}")

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
    """Extract data from SAP with chunking and retry logic"""

    def __init__(self, sap: SAPConnection, config: ExtractionConfig, logger: logging.Logger):
        self.sap = sap
        self.config = config
        self.logger = logger

    def extract_with_chunks(self, table_name: str, column_chunks: List[List[str]],
                             options: Optional[List[Dict[str, str]]] = None):
        offset = 0
        all_column_names = [col for chunk in column_chunks for col in chunk]
        total_extracted = 0

        self.logger.info(f"[{table_name}] Starting extraction with {len(column_chunks)} column chunks")

        while True:
            try:
                batch_data = self._extract_batch(table_name, column_chunks, all_column_names, offset, options)

                if not batch_data:
                    self.logger.info(f"[{table_name}] ✅ Extraction complete. Total rows: {total_extracted:,}")
                    break

                total_extracted += len(batch_data)
                self.logger.info(f"[{table_name}] Extracted: {total_extracted:,} rows")

                yield batch_data
                offset += self.config.batch_size

            except Exception as e:
                self.logger.error(f"[{table_name}] Extraction failed at offset {offset}: {e}")
                raise

    def _extract_batch(self, table_name: str, column_chunks: List[List[str]],
                        all_columns: List[str], offset: int,
                        options: Optional[List[Dict[str, str]]]) -> List[List[Any]]:
        self.logger.debug(f"[{table_name}] Fetching batch at offset {offset}")

        row_data = {}

        for chunk_idx, chunk in enumerate(column_chunks):
            retry_count = 0

            while retry_count <= self.config.max_retries:
                try:
                    result = self.sap.read_table(
                        table_name,
                        chunk,
                        options=options or [],
                        rowcount=self.config.batch_size,
                        rowskips=offset,
                    )

                    rows = result.get("DATA", [])

                    if not rows and chunk_idx == 0:
                        return []

                    for idx, row in enumerate(rows):
                        values = row["WA"].split("|")

                        if idx not in row_data:
                            row_data[idx] = {}

                        for col, val in zip(chunk, values):
                            clean_val = val.strip()
                            if clean_val:
                                clean_val = clean_val.replace('\x00', '')
                                clean_val = ''.join(
                                    char for char in clean_val if ord(char) >= 32 or char in '\t\n\r'
                                )
                            row_data[idx][col] = clean_val

                    break

                except Exception as e:
                    retry_count += 1
                    if retry_count > self.config.max_retries:
                        self.logger.error(
                            f"[{table_name}] Chunk {chunk_idx + 1} failed after {self.config.max_retries} retries"
                        )
                        raise
                    self.logger.warning(
                        f"[{table_name}] Chunk {chunk_idx + 1} failed, retry "
                        f"{retry_count}/{self.config.max_retries}: {e}"
                    )
                    time.sleep(self.config.retry_delay)

        batch = [
            [row.get(col, "") for col in all_columns]
            for row in [row_data[i] for i in sorted(row_data.keys())]
        ]
        return batch


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

    metadata = TableMetadata(sap, logger)
    columns = metadata.get_columns(table_name)
    if not columns:
        raise ValueError(f"No columns found for table {table_name}")

    chunker = ColumnChunker(columns, config, logger)
    column_chunks = chunker.create_chunks()

    qualified_table = pg.ensure_table(pg_table_name, columns)

    extractor = DataExtractor(sap, config, logger)
    column_names = [c["name"] for c in columns]

    total_inserted = 0
    for batch_data in extractor.extract_with_chunks(table_name, column_chunks, options):
        pg.insert_batch(qualified_table, column_names, batch_data)
        total_inserted += len(batch_data)

        if total_inserted % config.commit_frequency == 0:
            pg.commit()
            logger.info(f"[{table_name}] 💾 Committed {total_inserted:,} rows")

    pg.commit()

    elapsed = time.time() - t0
    rate = total_inserted / elapsed if elapsed > 0 else 0
    logger.info(f"[{table_name}] ✅ Done — {total_inserted:,} rows in {elapsed:.1f}s ({rate:.0f} rows/sec)")

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