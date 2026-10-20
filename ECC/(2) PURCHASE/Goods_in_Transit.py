"""
SAP Selective Column Staging Extractor — Goods In Transit (ZFI_GIT)
====================================================================
Python equivalent of the data model behind ABAP report ZFIR_GOODS_IN_TRANSIT
(tcode ZFI_GIT).  Extracts only the required columns from SAP and loads them
into PostgreSQL staging tables.

ABAP logic being replaced
─────────────────────────
  SELECT DISTINCT ZGIT a  JOIN LFA1 (vendor name)  JOIN BKPF (doc type)
         WHERE a~VBELN IN s_vbeln AND a~EBELN IN s_ebeln
  SELECT BSEG (DMBTR, RFCCUR) WHERE SHKZG = 'S'    FOR ALL ENTRIES IN it
  SELECT SINGLE BKPF-STBLG  (reversal document, if ZGIT-REV_BELNR is empty)

Extraction strategy (joins are done downstream in SQL on the staging tables)
  Pass 1 — ZGIT  filtered by EBELN / VBELN (optional) ; collects BELNR/BUKRS/GJAHR/LIFNR
  Pass 2 — BKPF  (BLART, STBLG)  and  BSEG (DMBTR, RFCCUR, SHKZG='S')
           filtered by the accounting-document keys collected from ZGIT
  Pass 3 — LFA1  (NAME1) filtered by LIFNR collected from ZGIT

Version : 1.0.0 (PostgreSQL Edition — Orchestrator Compatible)
Exit code 0 → success, 1 → failure.
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

SAP_SYSTEM: str = os.getenv("SAP_SYSTEM", "ECC")


# ─────────────────────────────────────────────────────────────
# ★  CONFIGURATION  (all values read from environment)
# ─────────────────────────────────────────────────────────────
def _env_list(name: str, upper: bool = True) -> List[str]:
    """Comma-separated env var -> list. Empty / unset -> []."""
    out = []
    for v in os.getenv(name, "").split(","):
        v = v.strip()
        if v:
            out.append(v.upper() if upper else v)
    return out


# Equivalent of the ABAP selection screen (SELECT-OPTIONS EBELN / VBELN).
# Leave empty to extract ALL rows of ZGIT.
#   e.g.  EBELN_LIST="4500000001,4500000002"   VBELN_LIST="180000123"
EBELN_LIST: List[str] = _env_list("EBELN_LIST")
VBELN_LIST: List[str] = _env_list("VBELN_LIST")


# Optional extra filter on ZGIT-BUKRS (not in the ABAP report; empty = no filter)
COMPANY_CODES: List[str] = _env_list("COMPANY_CODES")

# ─────────────────────────────────────────────────────────────
# Module-level key sets — populated at runtime
# ─────────────────────────────────────────────────────────────
_BELNR_SET: Set[str] = set()
_BUKRS_SET: Set[str] = set()
_GJAHR_SET: Set[str] = set()
_LIFNR_SET: Set[str] = set()
_EBELN_SET: Set[str] = set()
_CONTAINER_SET: Set[str] = set()

# ─────────────────────────────────────────────────────────────
# WHERE clause builders (RFC OPTIONS rows are max 72 chars)
# ─────────────────────────────────────────────────────────────
def _in_group(field: str, values: Set[str], chunk_len: int = 72) -> List[str]:
    """
    Build a parenthesised group of rows:  ( F IN ('a','b') OR F IN ('c') )
    Parentheses guarantee correct AND/OR precedence when more groups follow.
    """
    if not values:
        return [f"{field} = '__NO_MATCH__'"]

    sorted_vals = sorted(values)
    rows: List[str] = []
    first = True
    i = 0
    while i < len(sorted_vals):
        prefix = f"( {field} IN (" if first else f"OR {field} IN ("
        max_row_len = chunk_len - len(prefix) - 3      # room for ")" and group-closing " )"
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
        rows.append(prefix + ",".join(row_vals) + ")")
        first = False
    rows[-1] += " )"                                   # spaces around brackets are mandatory in ABAP
    return rows


def _build_and_options(
    filters: List[Tuple[str, Set[str]]],
    extra: str = "",
) -> List[Dict[str, str]]:
    """
    AND-combine several 'FIELD IN (values)' groups plus an optional free-text
    clause.  Returns RFC OPTIONS rows.
    """
    rows: List[str] = []
    for n, (field, values) in enumerate(filters):
        grp = _in_group(field, values)
        if n > 0:
            grp[0] = "AND " + grp[0]
        rows.extend(grp)
    options = [{"TEXT": r} for r in rows]
    if extra:
        options.extend(_format_options(("AND " if rows else "") + extra))
    return options


def _build_in_options(field: str, values: Set[str], extra: str = "") -> List[Dict[str, str]]:
    return _build_and_options([(field, values)], extra)


def _zgit_options() -> List[Dict[str, str]]:
    """ZGIT anchor filter = ABAP selection screen (EBELN / VBELN) [+ optional BUKRS]."""
    filters: List[Tuple[str, Set[str]]] = []
    if EBELN_LIST:
        filters.append(("EBELN", set(EBELN_LIST)))
    if VBELN_LIST:
        filters.append(("VBELN", set(VBELN_LIST)))
    if COMPANY_CODES:
        filters.append(("BUKRS", set(COMPANY_CODES)))
    return _build_and_options(filters) if filters else []


def _acc_doc_filters() -> List[Tuple[str, Set[str]]]:
    """Accounting-document key (BELNR + BUKRS + GJAHR) collected from ZGIT."""
    return [("BELNR", _BELNR_SET), ("BUKRS", _BUKRS_SET), ("GJAHR", _GJAHR_SET)]


STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Anchor: Shipment item / invoice data
    # =========================================================
    {
        "table": "ZSHIPMENT02",
        "description": "Waiting Port Entry — PO item, material and invoice data",
        "columns": [
            "EBELN",
            "POITEMSEQ",
            "MATNR",
            "MAKTX",
            "MENGE",
            "MEINS",
            "WEIGHT",
            "WT_UOM",
            "VOLUM",
            "VOL_UOM",
            "INV_NO",
            "INV_DATE",
            "CONTAINER",
            "COMP",
            "ACTIVE2",
        ],
        "options_fn": lambda: _build_and_options([
            ("EBELN", set(EBELN_LIST)),
            ("COMP", set(COMPANY_CODES)),
        ]) if EBELN_LIST or COMPANY_CODES else [],
        "primary_keys": [
            "EBELN",
            "POITEMSEQ",
            "MATNR",
            "INV_NO",
            "CONTAINER",
        ],
        "key_out": {
            "EBELN": "_EBELN_SET",
            "CONTAINER": "_CONTAINER_SET",
        },
    },

    # =========================================================
    # PASS 2 — Container and shipment tracking
    # =========================================================
    {
        "table": "ZSHIPMENT01",
        "description": "Waiting Port Entry — container and shipment tracking",
        "columns": [
            "CONTAINER",
            "CONT_TYPE",
            "CONT_SIZE",
            "CONTAINER_WT",
            "WT_UNIT",
            "CONTAINER_VOL",
            "VOL_UNIT",
            "BILL_OF_LAD",
            "BL_DATE",
            "VESSEL_NO",
            "VOYAGE_NO",
            "SHIPMENT_NO",
            "DATE_OF_SHIPMENT",
            "SHIPMENT_TYP",
            "SMODE",
            "SHIPPING_LINE",
            "FROM_PORT",
            "TO_PORT",
            "EDA_PORT",
            "DATE_OF_PORT",
            "ARRIVAL_NO1",
            "ED_TRUCKING",
            "ACT_TRUCKING_DT",
            "TRUCK_NO",
            "TRANSPORTER_NM",
            "CONSIGNMENT_NO",
            "CONSIGNMENT_DT",
            "EDA_FACT",
            "IS_FACT",
            "ACT_FACT_DT",
            "ARRIVAL_NO2",
            "DOC_RCVD",
            "DOC_RCVD_DT",
            "GATEPASS_NO",
        ],
        "options_fn": lambda: _build_and_options([
            ("CONTAINER", _CONTAINER_SET)
        ]),
        "primary_keys": [
            "CONTAINER",
        ],
        "key_out": {},
    },

    # =========================================================
    # PASS 3 — Purchase order header
    # =========================================================
    {
        "table": "EKKO",
        "description": "Purchase order header — PO date",
        "columns": [
            "EBELN",
            "AEDAT",
        ],
        "options_fn": lambda: _build_in_options(
            "EBELN", _EBELN_SET
        ),
        "primary_keys": [
            "EBELN",
        ],
        "key_out": {},
    },

    # =========================================================
    # PASS 4 — Purchase order item / requisitioner
    # =========================================================
    {
        "table": "EKPO",
        "description": "Purchase order item — requisitioner lookup",
        "columns": [
            "EBELN",
            "EBELP",
            "MATNR",
            "AFNAM",
        ],
        "options_fn": lambda: _build_in_options(
            "EBELN", _EBELN_SET
        ),
        "primary_keys": [
            "EBELN",
            "EBELP",
        ],
        "key_out": {},
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
# Logging
# ─────────────────────────────────────────────────────────────
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_staging_git_{run_id}.log")

    log = logging.getLogger(f"sap_staging_git.{run_id}")
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
                if attempt == 3:
                    raise
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
            missing = [c for c in columns if c not in widths]
            if missing:
                self._log.warning("[%s] Columns not found in DD03L (check names): %s", table, missing)
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
                res.append(curr)
                curr = [col]
                used = w
            else:
                curr.append(col)
                used += w
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
        stg_name  = f"stg_sap_git_{(name_override or sap_table).lower()}"
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
    log.debug("WHERE rows: %s", [o["TEXT"] for o in options])

    meta   = ColumnMetadata(sap, log)
    widths = meta.get_widths(table, columns)
    chunks = ColumnChunker(columns, widths, cfg.max_buffer_width, log).chunks()
    stg_table = loader.create_staging_table(table, columns, table_def.get("stg_name_override"))

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
            rows_since_commit = 0

    loader.commit()
    log.info("[%s] Done — %d rows loaded.", table, rows_loaded)
    return True, rows_loaded


def main() -> None:
    # Reset key sets so this function is safe to re-enter
    _BELNR_SET.clear()
    _BUKRS_SET.clear()
    _GJAHR_SET.clear()
    _LIFNR_SET.clear()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("EBELN_LIST    : %s", EBELN_LIST or "(all)")
    log.info("VBELN_LIST    : %s", VBELN_LIST or "(all)")
    log.info("COMPANY_CODES : %s", COMPANY_CODES or "(all)")

    try:
        cfg = ExtractionConfig()
    except Exception as exc:
        log.critical("Config error: %s", exc)
        sys.exit(1)

    sap    = None
    loader = None
    results: Dict[str, Tuple[bool, int]] = {}
    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, log)
        loader.connect()

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
        if sap:
            sap.close()
        if loader:
            loader.close()

    log.info("═" * 60)
    log.info(
        "Key sets collected: EBELN:%d  CONTAINER:%d",
        len(_EBELN_SET), len(_CONTAINER_SET),
    )
    any_failed = any(not ok for ok, _ in results.values())
    log.info("Overall result: %s", "FAILED" if any_failed else "SUCCESS")
    log.info("═" * 60)

    sys.exit(1 if any_failed else 0)


if __name__ == "__main__":
    main()