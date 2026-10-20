from __future__ import annotations

import os
import sys

# ---------------------------------------------------------------------------
# connections.py import
# ---------------------------------------------------------------------------
# connections.py must be imported BEFORE pyrfc / psycopg2: on import it loads
# the .env file and registers the SAP RFC SDK and PostgreSQL DLL directories.
# Make sure it is found whether it sits next to this script or one level up.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
for _p in (_THIS_DIR, os.path.dirname(_THIS_DIR), os.path.dirname(os.path.dirname(_THIS_DIR))):
    if _p not in sys.path:
        sys.path.append(_p)

from connections import (  # noqa: E402  (must precede pyrfc / psycopg2)
    get_postgres_connection,
    get_postgres_schema,
    get_sap_connection,
)

import logging  # noqa: E402
import time  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from datetime import datetime  # noqa: E402
from typing import Any, Dict, Generator, List, Optional, Set, Tuple  # noqa: E402

from psycopg2 import extras  # noqa: E402
from pyrfc import (  # noqa: E402
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------


# SAP system target
SAP_SYSTEM: str = "ECC"

COMPANY_CODES: List[str] = []

# Prefix used when naming staging tables: stg_sap_<prefix>_<table>
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "financial_year_master")

# Target schema comes from connections.py (STAGING_SCHEMA_ECC, default sap_stg_ecc)
PG_SCHEMA: str = get_postgres_schema(SAP_SYSTEM)

# RFC functions tried in order during the anchor-table probe.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state (populated during extraction; used as WHERE filters)
# ---------------------------------------------------------------------------
_PERIV_SET: Set[str] = set()

# True  -> IN ('a','b','c')
# False -> field='a' OR field='b' OR ...
_USE_IN_OPERATOR: bool = True

# ---------------------------------------------------------------------------
# WHERE clause builders
# ---------------------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    {
        "table": "T001",
        "description": "T001 Extraction",
        "columns": ['BUKRS', 'PERIV'],
        "options_fn": lambda: [],
        "is_anchor": True,
        "primary_keys": ["BUKRS"],
        "key_out": {'PERIV': '_PERIV_SET'},
    },

    {
        "table": "T009",
        "description": "T009 Extraction",
        "columns": ['PERIV', 'XKALE'],
        "options_fn": lambda: _build_in_options("PERIV", _PERIV_SET),
        "primary_keys": ["PERIV"],
        "key_out": {'PERIV': '_PERIV_SET'},
    },

    {
        "table": "T009B",
        "description": "T009B Extraction",
        "columns": ['PERIV', 'BDATJ', 'BUMON', 'BUTAG', 'POPER', 'RELJR'],
        "options_fn": lambda: _build_in_options("PERIV", _PERIV_SET),
        "primary_keys": ["PERIV"],
    },
]


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


def _build_or_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """Build RFC OPTIONS rows using OR-chained = clauses, chunked to 72 chars."""
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.append({"TEXT": f"AND {extra}"})
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    i = 0
    first_group = True

    while i < len(sorted_vals):
        prefix = "( " if first_group else "OR ( "
        row_parts: List[str] = []
        row_len = len(prefix)

        while i < len(sorted_vals):
            clause = f"{field_name} = '{sorted_vals[i]}'"
            sep_len = len(" OR ") if row_parts else 0
            if row_parts and row_len + sep_len + len(clause) + 2 > 72:
                break
            row_parts.append(clause)
            row_len += sep_len + len(clause)
            i += 1

        options.append({"TEXT": prefix + " OR ".join(row_parts) + " )"})
        first_group = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})
    return options


def _build_in_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """
    Build RFC OPTIONS rows using IN (...) clauses, chunked to 72 chars.
    Falls back to OR-syntax automatically when _USE_IN_OPERATOR is False.
    """
    if not _USE_IN_OPERATOR:
        return _build_or_options(field_name, values, extra)

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
        prefix = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
        max_fill = 72 - len(prefix) - 1
        row_vals: List[str] = []
        row_len = 0

        while i < len(sorted_vals):
            quoted = f"'{sorted_vals[i]}'"
            need = len(quoted) + (1 if row_vals else 0)
            if row_vals and row_len + need > max_fill:
                break
            row_vals.append(quoted)
            row_len += need
            i += 1

        options.append({"TEXT": prefix + ",".join(row_vals) + ")"})
        first_clause = False

    if extra:
        options.append({"TEXT": f"AND {extra}"})
    return options


def _build_t001_options() -> List[Dict[str, str]]:
    """
    WHERE options for the anchor table T001.

    Filters (all optional):
      - BUKRS IN / = (COMPANY_CODES). No filter -> all company codes.
    """
    if not COMPANY_CODES:
        return []
    return _build_in_options("BUKRS", set(COMPANY_CODES))



# ---------------------------------------------------------------------------
# Extraction tuning
# ---------------------------------------------------------------------------
@dataclass
class ExtractionConfig:
    batch_size:       int   = field(default_factory=lambda: int(os.getenv("BATCH_SIZE",       "10000")))
    max_buffer_width: int   = field(default_factory=lambda: int(os.getenv("MAX_BUFFER_WIDTH", "480")))
    max_retries:      int   = field(default_factory=lambda: int(os.getenv("MAX_RETRIES",      "3")))
    retry_delay_s:    float = field(default_factory=lambda: float(os.getenv("RETRY_DELAY_S",  "5")))
    commit_every:     int   = field(default_factory=lambda: int(os.getenv("COMMIT_EVERY",     "100000")))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", os.path.join(_THIS_DIR, "logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_financial_year_staging_{run_id}.log")

    log = logging.getLogger(f"sap_financial_year_staging.{run_id}")
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
# SAP connection wrapper — RFC function fallback, connection via connections.py
# ---------------------------------------------------------------------------
class SAPConnection:
    """
    Wraps the pyrfc Connection returned by connections.get_sap_connection()
    and manages the active RFC read-table function.
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

    def set_rfc_function_index(self, idx: int) -> None:
        self._fn_index = idx

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
# Column metadata
# ---------------------------------------------------------------------------
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
                "[%s] DD03L lookup failed — using fallback width %d.",
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
# SAP Batch Extractor
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
                        options  = self._options,
                        rowcount = self._cfg.batch_size,
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

# ---------------------------------------------------------------------------
# PostgreSQL loader — connection via connections.py
# ---------------------------------------------------------------------------
class PostgresLoader:
    def __init__(self, schema: str, prefix: str, log: logging.Logger) -> None:
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn    = None
        self._cursor = None

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
        base_prefix = self._prefix.lower() if self._prefix else "stg_sap"
        stg_name  = f"{base_prefix}_financial_year_master_{sap_table.lower()}"
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

# ---------------------------------------------------------------------------
# Per-table pipeline
# ---------------------------------------------------------------------------
def extract_table(
    table_def: Dict[str, Any],
    sap:       SAPConnection,
    loader:    PostgresLoader,
    cfg:       ExtractionConfig,
    log:       logging.Logger,
) -> Tuple[bool, int]:
    """Extract one SAP table into its staging counterpart. Returns (ok, row_count)."""
    table   = table_def["table"].upper()
    columns = list(dict.fromkeys(table_def["columns"]))
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

    options_fn = table_def.get("options_fn")
    options    = options_fn() if callable(options_fn) else []

    log.info("=" * 60)
    log.info("Table  : %s  (%s)", table, desc)
    log.info("RFC fn : %s", sap.rfc_function)
    log.info("Syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Fields : %s", columns)
    if options:
        preview = " | ".join(r["TEXT"] for r in options[:4])
        suffix  = " | ..." if len(options) > 4 else ""
        log.info("OPTIONS (%d rows): %s%s", len(options), preview, suffix)
    else:
        log.info("OPTIONS: none (full table)")

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table = loader.create_staging_table(table, columns)
    extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log)

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
        "[%s] Done — %d rows in %.1fs (%.0f rows/s).",
        table, rows_loaded, elapsed, rate,
    )
    return True, rows_loaded

# ---------------------------------------------------------------------------
# Anchor-table probe: try all RFC fn × WHERE syntax combinations
# ---------------------------------------------------------------------------
def _probe_anchor_table(
    anchor_def: Dict[str, Any],
    sap:        SAPConnection,
    loader:     PostgresLoader,
    cfg:        ExtractionConfig,
    log:        logging.Logger,
) -> Tuple[bool, int]:
    """
    Try the anchor table (T001) with up to four combinations:

        Probe 1  BBP_RFC_READ_TABLE + IN  syntax
        Probe 2  BBP_RFC_READ_TABLE + OR  syntax
        Probe 3  RFC_READ_TABLE     + IN  syntax
        Probe 4  RFC_READ_TABLE     + OR  syntax

    Stops as soon as a non-zero row count is achieved; that combination is
    then used for all supporting tables.
    """
    global _USE_IN_OPERATOR

    table = anchor_def["table"].upper()

    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))   # IN  syntax
        attempts.append((False, fn_idx))   # OR  syntax

    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap.set_rfc_function_index(fn_idx)

        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info(
            "─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        # Clear anchor key sets so a retry starts fresh
        _PERIV_SET.clear()

        try:
            ok, count = extract_table(anchor_def, sap, loader, cfg, log)
        except Exception:
            log.warning("Probe %d raised an exception — trying next combination.", probe_num)
            continue

        if count > 0:
            log.info(
                "✓ Anchor probe succeeded on attempt %d — RFC=%s | syntax=%s | rows=%d",
                probe_num, sap.rfc_function, syntax_label, count,
            )
            return ok, count

        log.warning(
            "Probe %d returned 0 rows (RFC=%s | syntax=%s).",
            probe_num, sap.rfc_function, syntax_label,
        )

    log.error(
        "All %d anchor probes returned 0 rows for %s. "
        "Verify company code filter %s in SAP.",
        len(attempts), table, COMPANY_CODES or "ALL",
    )
    return True, 0

# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP Financial Year Master Staging Pipeline  [run: %s]", run_id)
    log.info("SAP system      : %s", SAP_SYSTEM)
    log.info("Company codes   : %s", COMPANY_CODES if COMPANY_CODES else "ALL")
    log.info("Target schema   : %s", PG_SCHEMA)
    base_prefix = STAGING_PREFIX.lower() if STAGING_PREFIX else "stg_sap"
    log.info("Staging prefix  : %s_financial_year_master_<table>", base_prefix)
    log.info("Tables          : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority    : %s", RFC_FUNCTION_PRIORITY)
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
    except ValueError as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(PG_SCHEMA, STAGING_PREFIX, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                if tbl_def.get("is_anchor"):
                    ok, count = _probe_anchor_table(tbl_def, sap, loader, cfg, log)
                    if count == 0:
                        log.warning(
                            "Anchor table (%s) returned 0 rows after all probes. "
                            "Supporting tables will be created empty (no keys to filter on).",
                            tbl_name,
                        )
                else:
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
    log.info("Active RFC function : %s", sap.rfc_function)
    log.info("Active WHERE syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Key sets collected:")
    log.info("    PERIV : %d variants", len(_PERIV_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-10s  %d rows", tbl, count)
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