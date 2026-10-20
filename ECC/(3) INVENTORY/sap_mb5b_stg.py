"""
SAP MB5B Staging Extraction Pipeline
=====================================
Extracts base tables for MB5B (Material Document List) from SAP ECC
and loads them into SQL Server staging tables.

Author  : [Your Name]
Version : 2.0.0
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from pyrfc import Connection, RFCError

# ─────────────────────────────────────────────
# Logging setup  (call BEFORE any other import-time code)
# ─────────────────────────────────────────────
LOG_DIR = os.getenv("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

_log_file = os.path.join(
    LOG_DIR,
    f"sap_mb5b_staging_{datetime.now():%Y%m%d_%H%M%S}.log",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("sap_mb5b")


# ─────────────────────────────────────────────
# RFC SDK path registration
# ─────────────────────────────────────────────
RFC_SDK_PATH = os.getenv("RFC_SDK_PATH", r"D:\For ESS\nwrfc750P_18-70002755\nwrfcsdk\lib")
if os.path.isdir(RFC_SDK_PATH):
    if sys.platform == "win32":
        os.add_dll_directory(RFC_SDK_PATH)
    else:
        os.environ["PATH"] = RFC_SDK_PATH + os.pathsep + os.environ.get("PATH", "")
    logger.info("RFC SDK registered: %s", RFC_SDK_PATH)
else:
    logger.warning("RFC SDK path not found: %s — continuing anyway.", RFC_SDK_PATH)


# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
load_dotenv()


def _require_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is missing or empty.")
    return value


@dataclass
class SAPConfig:
    user: str = field(default_factory=lambda: _require_env("SAP_USER"))
    passwd: str = field(default_factory=lambda: _require_env("SAP_PASSWORD"))
    ashost: str = field(default_factory=lambda: _require_env("SAP_ASHOST"))
    sysnr: str = field(default_factory=lambda: _require_env("SAP_SYSNR"))
    client: str = field(default_factory=lambda: _require_env("SAP_CLIENT"))
    lang: str = field(default_factory=lambda: os.getenv("SAP_LANG", "EN"))

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class SQLConfig:
    driver: str = field(default_factory=lambda: _require_env("SQL_DRIVER"))
    server: str = field(default_factory=lambda: _require_env("SQL_SERVER"))
    database: str = field(default_factory=lambda: _require_env("SQL_DATABASE"))
    username: str = field(default_factory=lambda: _require_env("SQL_USERNAME"))
    password: str = field(default_factory=lambda: _require_env("SQL_PASSWORD"))

    def connection_string(self) -> str:
        return (
            f"Driver={{{self.driver}}};"
            f"Server={self.server};"
            f"Database={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            "TrustServerCertificate=yes;"
        )


@dataclass
class ExtractionConfig:
    company_codes: list[str] = field(default_factory=lambda: [
        "1200", "1400", "2600", "2700", "DAPL", "HECL", "HPDL", "PFL"
    ])
    date_from: str = field(default_factory=lambda: os.getenv("DATE_FROM", "20251001"))
    date_to: str = field(default_factory=lambda: os.getenv("DATE_TO", "20251231"))
    rfc_max_rows: int = field(default_factory=lambda: int(os.getenv("RFC_MAX_ROWS", "1000000000")))
    rfc_retry_attempts: int = field(default_factory=lambda: int(os.getenv("RFC_RETRY_ATTEMPTS", "3")))
    rfc_retry_delay_s: float = field(default_factory=lambda: float(os.getenv("RFC_RETRY_DELAY_S", "5")))
    sql_batch_size: int = field(default_factory=lambda: int(os.getenv("SQL_BATCH_SIZE", "10000")))
    staging_schema: str = field(default_factory=lambda: os.getenv("STAGING_SCHEMA", "dbo"))

    def bukrs_filter(self) -> str:
        codes = ",".join(f"'{c}'" for c in self.company_codes)
        return f"BUKRS IN ({codes})"


# ─────────────────────────────────────────────
# Table definitions
# ─────────────────────────────────────────────
def build_table_config(cfg: ExtractionConfig) -> list[dict]:
    bukrs = cfg.bukrs_filter()
    return [
        {
            "table": "MKPF",
            "fields": ["MBLNR", "MJAHR", "BUDAT", "CPUDT", "CPUTM", "USNAM", "BKTXT"],
            "where": f"BUDAT <= '{cfg.date_to}'",
            "primary_keys": ["MBLNR", "MJAHR"],
        },
        {
            "table": "MSEG",
            "fields": [
                "MBLNR", "MJAHR", "ZEILE", "MATNR", "WERKS", "LGORT",
                "BWART", "SHKZG", "MENGE", "MEINS", "DMBTR", "WAERS",
                "BWTAR", "BUKRS",
            ],
            "where": bukrs,
            "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        },
        {
            "table": "MAKT",
            "fields": ["MATNR", "MAKTX", "SPRAS"],
            "where": "SPRAS = 'E'",
            "primary_keys": ["MATNR"],
        },
        {
            "table": "MARA",
            "fields": ["MATNR", "MEINS", "MTART", "MATKL"],
            "where": "",
            "primary_keys": ["MATNR"],
        },
        {
            "table": "T001W",
            "fields": ["WERKS", "NAME1", "BWKEY"],
            "where": "",
            "primary_keys": ["WERKS"],
        },
        {
            "table": "T001K",
            "fields": ["BWKEY", "BUKRS"],
            "where": bukrs,
            "primary_keys": ["BWKEY"],
        },
        {
            "table": "T001",
            "fields": ["BUKRS", "BUTXT", "WAERS"],
            "where": bukrs,
            "primary_keys": ["BUKRS"],
        },
        {
            "table": "T156",
            "fields": ["BWART", "SHKZG", "KZBEW"],
            "where": "",
            "primary_keys": ["BWART"],
        },
        {
            "table": "T156T",
            "fields": ["BWART", "BTEXT", "SPRAS"],
            "where": "SPRAS = 'E'",
            "primary_keys": ["BWART"],
        },
        {
            "table": "MBEW",
            "fields": ["MATNR", "BWKEY", "LBKUM", "SALK3", "VERPR", "STPRS", "PEINH"],
            "where": "",
            "primary_keys": ["MATNR", "BWKEY"],
        },
    ]


# ─────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────
class SAPStagingPipeline:
    """
    Connects to SAP ECC via RFC and loads staging tables into SQL Server.
    Each table is fetched field-by-field to work around SAP's alignment
    restriction in RFC_READ_TABLE.
    """

    def __init__(
        self,
        sap_cfg: SAPConfig,
        sql_cfg: SQLConfig,
        ext_cfg: ExtractionConfig,
    ) -> None:
        self._sap_cfg = sap_cfg
        self._sql_cfg = sql_cfg
        self._ext_cfg = ext_cfg
        self._sap_conn: Optional[Connection] = None
        self._db_conn: Optional[pyodbc.Connection] = None

    # ── Connection management ─────────────────

    def connect(self) -> None:
        logger.info("Connecting to SAP ECC [%s]…", self._sap_cfg.ashost)
        try:
            self._sap_conn = Connection(**self._sap_cfg.as_dict())
            logger.info("SAP connection established.")
        except RFCError as exc:
            logger.exception("Failed to connect to SAP.")
            raise RuntimeError("SAP connection failed.") from exc

        logger.info(
            "Connecting to SQL Server [%s] / [%s]…",
            self._sql_cfg.server,
            self._sql_cfg.database,
        )
        try:
            self._db_conn = pyodbc.connect(self._sql_cfg.connection_string())
            logger.info("SQL Server connection established.")
        except pyodbc.Error as exc:
            logger.exception("Failed to connect to SQL Server.")
            raise RuntimeError("SQL Server connection failed.") from exc

    def disconnect(self) -> None:
        if self._sap_conn:
            try:
                self._sap_conn.close()
                logger.info("SAP connection closed.")
            except Exception:
                logger.warning("Error closing SAP connection.", exc_info=True)
            self._sap_conn = None

        if self._db_conn:
            try:
                self._db_conn.close()
                logger.info("SQL Server connection closed.")
            except Exception:
                logger.warning("Error closing SQL Server connection.", exc_info=True)
            self._db_conn = None

    def __enter__(self) -> "SAPStagingPipeline":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    # ── SAP RFC helpers ───────────────────────

    def _prepare_options(self, options_str: str) -> list[dict]:
        """Splits a WHERE clause into ≤72-char chunks for RFC_READ_TABLE OPTIONS."""
        if not options_str:
            return []
        lines: list[dict] = []
        current = ""
        for word in options_str.split():
            if len(current) + len(word) + 1 > 72:
                lines.append({"TEXT": current.rstrip()})
                current = word + " "
            else:
                current += word + " "
        if current.strip():
            lines.append({"TEXT": current.rstrip()})
        return lines

    def _call_rfc_with_retry(
        self,
        table: str,
        fields: list[str],
        where: str,
    ) -> pd.DataFrame:
        """Calls RFC_READ_TABLE with retry logic on transient failures."""
        options = self._prepare_options(where)
        attempts = self._ext_cfg.rfc_retry_attempts

        for attempt in range(1, attempts + 1):
            try:
                res = self._sap_conn.call(
                    "RFC_READ_TABLE",
                    QUERY_TABLE=table,
                    DELIMITER="|",
                    FIELDS=[{"FIELDNAME": f} for f in fields],
                    OPTIONS=options,
                    ROWCOUNT=self._ext_cfg.rfc_max_rows,
                )
                rows = [r["WA"].split("|")[: len(fields)] for r in res["DATA"]]
                return pd.DataFrame(rows, columns=fields)
            except RFCError as exc:
                if attempt < attempts:
                    logger.warning(
                        "RFC call failed (attempt %d/%d) for %s fields %s: %s. Retrying in %.1fs…",
                        attempt, attempts, table, fields, exc,
                        self._ext_cfg.rfc_retry_delay_s,
                    )
                    time.sleep(self._ext_cfg.rfc_retry_delay_s)
                else:
                    logger.error(
                        "RFC call exhausted retries for %s fields %s.", table, fields
                    )
                    raise

    def fetch_table(
        self,
        table: str,
        fields: list[str],
        primary_keys: list[str],
        where: str = "",
    ) -> pd.DataFrame:
        """
        Fetches a SAP table field-by-field and merges on primary keys.
        Fields that fail due to SAP alignment errors are filled with None
        and do NOT abort the overall extraction.
        """
        fields = list(dict.fromkeys(fields))           # deduplicate, preserve order
        other_fields = [f for f in fields if f not in primary_keys]

        logger.info("[%s] Fetching primary key(s): %s", table, primary_keys)
        base_df = self._call_rfc_with_retry(table, primary_keys, where)
        logger.info("[%s] Base key rows: %d", table, len(base_df))

        failed_fields: list[str] = []
        for i, col in enumerate(other_fields, start=1):
            logger.debug("[%s] (%d/%d) fetching field: %s", table, i, len(other_fields), col)
            try:
                chunk = self._call_rfc_with_retry(table, primary_keys + [col], where)
                base_df = base_df.merge(chunk, on=primary_keys, how="left")
            except Exception:
                logger.warning(
                    "[%s] Field [%s] skipped — SAP alignment or RFC error.", table, col
                )
                base_df[col] = None
                failed_fields.append(col)

        if failed_fields:
            logger.warning("[%s] %d field(s) filled with NULL: %s", table, len(failed_fields), failed_fields)

        return base_df

    # ── SQL helpers ───────────────────────────

    def _staging_table_name(self, sap_table: str) -> str:
        return f"[{self._ext_cfg.staging_schema}].[STG_SAP_{sap_table}]"

    def push_to_staging(self, df: pd.DataFrame, sap_table: str) -> None:
        if df.empty:
            logger.warning("[%s] DataFrame is empty — skipping push.", sap_table)
            return

        stg = self._staging_table_name(sap_table)
        cursor = self._db_conn.cursor()

        # Recreate staging table
        col_defs = ", ".join(f"[{c}] NVARCHAR(MAX)" for c in df.columns)
        cursor.execute(f"IF OBJECT_ID('{stg}', 'U') IS NOT NULL DROP TABLE {stg}")
        cursor.execute(
            f"CREATE TABLE {stg} ({col_defs}, [LOAD_TS] DATETIME2 DEFAULT SYSUTCDATETIME())"
        )

        # Batch insert
        placeholders = ", ".join(["?"] * len(df.columns))
        col_list = ", ".join(f"[{c}]" for c in df.columns)
        insert_sql = f"INSERT INTO {stg} ({col_list}) VALUES ({placeholders})"

        cursor.fast_executemany = True
        batch_size = self._ext_cfg.sql_batch_size
        total = len(df)
        loaded = 0

        for start in range(0, total, batch_size):
            batch = df.iloc[start : start + batch_size].values.tolist()
            cursor.executemany(insert_sql, batch)
            self._db_conn.commit()
            loaded += len(batch)
            logger.debug("[%s] Committed %d / %d rows.", sap_table, loaded, total)

        logger.info("[%s] Staged %d rows into %s", sap_table, total, stg)

    # ── Orchestration ─────────────────────────

    def run(self, table_config: list[dict]) -> dict[str, bool]:
        """
        Runs the full extraction and load cycle.
        Returns a dict mapping table name → success flag.
        """
        results: dict[str, bool] = {}
        overall_start = time.time()

        for item in table_config:
            tbl = item["table"]
            t0 = time.time()
            logger.info("═══ Processing [%s] ═══", tbl)
            try:
                df = self.fetch_table(
                    table=tbl,
                    fields=item["fields"],
                    primary_keys=item["primary_keys"],
                    where=item.get("where", ""),
                )
                self.push_to_staging(df, tbl)
                elapsed = time.time() - t0
                logger.info("[%s] Completed in %.1fs — %d rows.", tbl, elapsed, len(df))
                results[tbl] = True
            except Exception:
                elapsed = time.time() - t0
                logger.exception("[%s] FAILED after %.1fs.", tbl, elapsed)
                results[tbl] = False

        # ── Summary ───────────────────────────
        total_elapsed = time.time() - overall_start
        succeeded = [t for t, ok in results.items() if ok]
        failed    = [t for t, ok in results.items() if not ok]

        logger.info("─" * 60)
        logger.info("Run complete in %.1fs", total_elapsed)
        logger.info("Succeeded (%d): %s", len(succeeded), succeeded)
        if failed:
            logger.error("Failed    (%d): %s", len(failed), failed)
        else:
            logger.info("All tables extracted successfully.")
        logger.info("Log file: %s", _log_file)

        return results


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
def main() -> None:
    logger.info("SAP MB5B Staging Pipeline — starting")

    try:
        sap_cfg = SAPConfig()
        sql_cfg = SQLConfig()
        ext_cfg = ExtractionConfig()
    except EnvironmentError as exc:
        logger.critical("Configuration error: %s", exc)
        sys.exit(1)

    table_config = build_table_config(ext_cfg)

    logger.info(
        "Extraction window: %s → %s | Company codes: %s",
        ext_cfg.date_from, ext_cfg.date_to, ext_cfg.company_codes,
    )

    with SAPStagingPipeline(sap_cfg, sql_cfg, ext_cfg) as pipeline:
        results = pipeline.run(table_config)

    failed = [t for t, ok in results.items() if not ok]
    if failed:
        logger.error("Pipeline finished with errors.")
        sys.exit(1)

    logger.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()