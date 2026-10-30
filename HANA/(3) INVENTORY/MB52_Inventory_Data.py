"""
SAP MB52 Inventory Analytics Extraction Pipeline
=================================================
Builds a consolidated enterprise inventory analytics dataset from SAP ECC/HANA,
combining current stock, material master, batch stock, valuation, purchase/GR
information, and company/currency data into PostgreSQL staging tables.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OBJECTIVE AND DESIGN RATIONALE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This pipeline does NOT replicate the SAP MB52 screen output exactly.
It produces an enterprise analytics dataset that combines:
  • Current inventory stock    → MARD  (unrestricted stock LABST)
  • Batch-level stock          → MCHB  (batch + stock segment)
  • Material master attributes → MARA  (type, group, base UoM)
  • Valuation                  → MBEW  (inventory value SALK3, per-piece STPRS)
  • Purchase / GR information  → MSEG + MKPF (BWART in PURCHASE_BWART_LIST)
  • Company & currency         → T001W → T001K → T001

INVENTORY_DATE (single date, no range):
  The pipeline accepts exactly one date — INVENTORY_DATE (e.g. 20260516).
  This date is used in ONE way (changed from v1.1.0):

    1. MARD anchor scope:
       MARD holds current stock positions with no date column of its own.
       The INVENTORY_DATE is recorded in the run metadata and log as the
       "as-of" snapshot label.  PLANT_LIST scopes which plants are extracted.

  MKPF BUDAT FILTER REMOVED (v1.2.0):
    Previously, MKPF.BUDAT = INVENTORY_DATE was applied, giving a single-day
    GR view. This has been removed so that ALL historical GR records for the
    materials in scope are extracted. The downstream materialized view
    mv_last_purchase_by_sloc then computes MAX(BUDAT) per MATNR/WERKS/LGORT,
    giving the true "last purchase date" rather than the "GR posted on
    reporting date" that the old filter produced.

  WHY REMOVING THE DATE FILTER IS CORRECT:
    MB52 is a snapshot report; its "Purchase Date" column in reporting tools
    should represent the most recent receipt into stock, not a single-day
    coincidence. An inventory manager looking at stock on 2026-05-16 wants to
    know the last time material was received — which may have been months ago.
    Keeping BUDAT = INVENTORY_DATE made that column useless for ageing analysis,
    reorder decisions, or supplier performance tracking.

MULTI-PASS ARCHITECTURE (key-set driven):
  Pass 1 : MARD   → anchor (WERKS in PLANT_LIST); collects _MATNR_SET, _WERKS_SET, _LGORT_SET
  Pass 2 : MCHB   → batch stock (MATNR in _MATNR_SET); collects _CHARG_SET
  Pass 3 : MARA   → material master (MATNR in _MATNR_SET)
  Pass 4 : T001W  → plant master (WERKS in _WERKS_SET); collects _BWKEY_SET
  Pass 5 : T001K  → valuation area→company (BWKEY in _BWKEY_SET); collects _BUKRS_SET
  Pass 6 : T001   → company master (BUKRS in _BUKRS_SET)
  Pass 7 : MBEW   → valuation (MATNR in _MATNR_SET)
  Pass 8 : MSEG   → GR line items (MATNR in _MATNR_SET, BWART IN PURCHASE_BWART_LIST);
                    collects _MBLNR_SET
             NOTE: ALL historical GR records for scoped materials — no date cap.
  Pass 9 : MKPF   → GR document headers (MBLNR in _MBLNR_SET);
             NOTE: BUDAT filter REMOVED — all dates, all history.

  Post-extraction:
  Pass 10: _create_purchase_analytics() — creates PostgreSQL materialized view
           mv_last_purchase_by_sloc and supporting indexes.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CHANGES IN THIS VERSION (v1.2.0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CHANGE 1 — Remove MKPF date filter (_build_mkpf_options):
  Old: MBLNR IN (...) AND BUDAT = '<INVENTORY_DATE>'
  New: MBLNR IN (...) only — all historical GR headers for scoped materials.

CHANGE 2 — Configurable BWART list (PURCHASE_BWART_LIST):
  New config var PURCHASE_BWART_LIST (default ['101']).
  _build_mseg_options() builds a dynamic IN clause from this list.
  Typical expansion: ['101','105','107','109'] captures returns and reversals.

CHANGE 3 — MSEG columns extended:
  Added CHARG (batch), BWTAR (valuation type) to MSEG column list so
  last-purchase aggregation can join at batch level when needed.

CHANGE 4 — MKPF description updated:
  Reflects removal of single-date filter.

CHANGE 5 — New function _create_purchase_analytics():
  Called from main() after all 9 table passes succeed.
  Creates:
    • Indexes on MSEG and MKPF staging tables (performance-critical).
    • Materialized view mv_last_purchase_by_sloc:
        last_purchase_date, last_purchase_qty, last_po_number,
        last_vendor, last_mblnr, last_dmbtr (unit price proxy).
    • Unique index on the materialized view for fast REFRESH CONCURRENTLY.

CHANGE 6 — main() startup log updated:
  Old line: "Inventory date : ... (MKPF.BUDAT = this date for GR docs)"
  New line: "Inventory date : ... (MARD snapshot label only — MKPF date filter removed)"

All other architecture, retry logic, resume logic, key-set propagation,
SAP reconnect handling, and PostgreSQL loading logic are UNCHANGED from v1.1.0.

Author  : [Your Name]
Version : 1.2.0
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

SAP_SYSTEM: str = "HANA"


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — edit this section only
# ─────────────────────────────────────────────────────────────

INVENTORY_DATE: str = os.getenv("INVENTORY_DATE", "20260101")

PLANT_LIST: List[str] = ["1102"]

COMPANY_CODES: List[str] = ["1100"]

# ── Resume mode ───────────────────────────────────────────────
# When True, a table whose staging table already exists and has rows
# will be resumed from its last committed offset rather than restarted.
# Set to False to always do a full DROP + re-extract (safe re-run).
RESUME_ON_FAILURE: bool = False

# ── CHANGE 2: Configurable purchase movement types ───────────
# Default is ['101'] (standard goods receipt against PO).
# Extend to capture reversals and subsequent GR variants:
#   '101' — GR for purchase order (standard)
#   '105' — GR for purchase order into blocked stock
#   '107' — GR for purchase order (valuated GR blocked stock)
#   '109' — GR for production order (if relevant)
# Environment variable PURCHASE_BWART_LIST accepts comma-separated values,
# e.g. PURCHASE_BWART_LIST=101,105,107
# All values in this list are used in MSEG extraction and in the
# materialized view WHERE clause — keep them consistent.
_bwart_env = os.getenv("PURCHASE_BWART_LIST", "101")
PURCHASE_BWART_LIST: List[str] = [v.strip() for v in _bwart_env.split(",") if v.strip()]
if not PURCHASE_BWART_LIST:
    PURCHASE_BWART_LIST = ["101"]


# ─────────────────────────────────────────────────────────────
# Module-level key sets — populated at runtime during extraction
# ─────────────────────────────────────────────────────────────

_MATNR_SET: Set[str] = set()
_WERKS_SET: Set[str] = set()
_LGORT_SET: Set[str] = set()
_CHARG_SET: Set[str] = set()
_BWKEY_SET: Set[str] = set()
_BUKRS_SET: Set[str] = set()
_MBLNR_SET: Set[str] = set()


# ─────────────────────────────────────────────────────────────
# WHERE clause builders
# ─────────────────────────────────────────────────────────────

def _format_options(where: str) -> List[Dict[str, str]]:
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
    chunk_size: int = 1500,
) -> List[List[Dict[str, str]]]:
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.append({"TEXT": f"AND {extra}"})
        return [rows]

    sorted_vals = list(sorted(values))
    all_options: List[List[Dict[str, str]]] = []

    for chunk_start in range(0, len(sorted_vals), chunk_size):
        chunk_vals = sorted_vals[chunk_start : chunk_start + chunk_size]
        options: List[Dict[str, str]] = []
        first_clause = True
        i = 0

        while i < len(chunk_vals):
            prefix      = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
            max_content = 72 - len(prefix) - 1
            row_vals: List[str] = []
            row_len = 0

            while i < len(chunk_vals):
                quoted = f"'{chunk_vals[i]}'"
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

        all_options.append(options)

    return all_options


def _build_mard_options() -> List[List[Dict[str, str]]]:
    if not PLANT_LIST:
        return [[]]
    return _build_in_options("WERKS", set(PLANT_LIST))


def _build_mchb_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("MATNR", _MATNR_SET)


def _build_mara_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("MATNR", _MATNR_SET)


def _build_t001w_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("WERKS", _WERKS_SET)


def _build_t001k_options() -> List[List[Dict[str, str]]]:
    codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
    return _build_in_options("BWKEY", _BWKEY_SET, extra=f"BUKRS IN ({codes})")


def _build_t001_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("BUKRS", _BUKRS_SET)


def _build_mbew_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("MATNR", _MATNR_SET)


# ── CHANGE 2: _build_mseg_options — dynamic BWART IN clause ──
def _build_mseg_options() -> List[List[Dict[str, str]]]:
    """
    CHANGE 2 (v1.2.0):
      Old: hardcoded  extra="BWART = '101'"
      New: builds     extra="BWART IN ('101')"   — or multi-value when
           PURCHASE_BWART_LIST contains more than one entry.

    This keeps the chunk-based _MATNR_SET IN-clause architecture
    completely unchanged; only the extra= argument is generated
    dynamically from PURCHASE_BWART_LIST.

    The resulting RFC WHERE clause always has the form:
        MATNR IN ('MAT1','MAT2',...) AND BWART IN ('101')
    so SAP can still use the MATNR index on MSEG.
    """
    # Build BWART IN ('101','105',...) from the config list.
    bwart_in = ",".join(f"'{b}'" for b in PURCHASE_BWART_LIST)
    bwart_clause = f"BWART IN ({bwart_in})"
    return _build_in_options("MATNR", _MATNR_SET, extra=bwart_clause)


# ── CHANGE 1: _build_mkpf_options — BUDAT filter REMOVED ─────
def _build_mkpf_options() -> List[List[Dict[str, str]]]:
    """
    CHANGE 1 (v1.2.0):
      Old logic:
        _build_in_options("MBLNR", _MBLNR_SET, extra=f"BUDAT = '{INVENTORY_DATE}'")

      This filtered MKPF to only headers whose posting date equalled the
      single INVENTORY_DATE, which gave "GR posted ON inventory date" rather
      than "most recent purchase date historically."

      New logic:
        _build_in_options("MBLNR", _MBLNR_SET)   — no extra= argument.

      Effect:
        ALL MKPF headers for document numbers in _MBLNR_SET are extracted,
        regardless of BUDAT. This gives the full date history needed for
        MAX(BUDAT) aggregation in mv_last_purchase_by_sloc.

      Performance note:
        The MBLNR IN (...) filter is preserved. MSEG was already scoped by
        _MATNR_SET, so _MBLNR_SET only contains document numbers for the
        materials in scope. There is no risk of a full MKPF table scan.

      SAP RFC risk:
        MKPF has ~1 row per GR document, so removing the BUDAT filter
        increases row volume proportionally to how many historical receipts
        exist per material. In production systems with 5+ years of history,
        _MBLNR_SET could be significantly larger than under the single-date
        filter. Monitor MKPF row counts on first run and adjust
        BATCH_SIZE / chunk_size if needed.
    """
    return _build_in_options("MBLNR", _MBLNR_SET)
    # REMOVED: extra=f"BUDAT = '{INVENTORY_DATE}'"


# ─────────────────────────────────────────────────────────────
# TABLE DEFINITIONS
# ─────────────────────────────────────────────────────────────

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [
    {
        "table": "MARD",
        "description": (
            f"Storage Location Stock — plants {PLANT_LIST} "
            f"(snapshot date: {INVENTORY_DATE})"
        ),
        "columns": [
            "MATNR", "WERKS", "LGORT", "LABST", "EINME", "INSME",
            "RETME", "VMLAB", "UMLME", "LFGJA", "LFMON",
        ],
        "options_fn": _build_mard_options,
        "primary_keys": ["MATNR", "WERKS", "LGORT"],
        "key_out": {
            "MATNR": "_MATNR_SET",
            "WERKS": "_WERKS_SET",
            "LGORT": "_LGORT_SET",
        },
    },
    {
        "table": "MCHB",
        "description": "Batch-Level Stock — materials in scope",
        "columns": [
            "MATNR", "WERKS", "LGORT", "CHARG", "CLABS", "CEINM",
            "CINSM", "CRETM", "SGT_SCAT", "LFGJA", "LFMON",
        ],
        "options_fn": _build_mchb_options,
        "primary_keys": ["MATNR", "WERKS", "LGORT", "CHARG"],
        "key_out": {"CHARG": "_CHARG_SET"},
    },
    {
        "table": "MARA",
        "description": "Material Master — general data for materials in scope",
        "columns": [
            "MATNR", "MEINS", "MTART", "MATKL", "MBRSH",
            "GEWEI", "NTGEW", "BRGEW", "MFRPN",
        ],
        "options_fn": _build_mara_options,
        "primary_keys": ["MATNR"],
    },
    {
        "table": "T001W",
        "description": "Plant Master — plants in scope",
        "columns": ["WERKS", "NAME1", "BWKEY", "LAND1", "REGIO", "ADRNR"],
        "options_fn": _build_t001w_options,
        "primary_keys": ["WERKS"],
        "key_out": {"BWKEY": "_BWKEY_SET"},
    },
    {
        "table": "T001K",
        "description": "Valuation Area → Company Code mapping",
        "columns": ["BWKEY", "BUKRS"],
        "options_fn": _build_t001k_options,
        "primary_keys": ["BWKEY"],
        "key_out": {"BUKRS": "_BUKRS_SET"},
    },
    {
        "table": "T001",
        "description": "Company Code Master — company name and currency",
        "columns": ["BUKRS", "BUTXT", "WAERS", "LAND1", "SPRAS", "ADRNR"],
        "options_fn": _build_t001_options,
        "primary_keys": ["BUKRS"],
    },
    {
        "table": "MBEW",
        "description": "Material Valuation — inventory value and standard price",
        "columns": [
            "MATNR", "BWKEY", "BWTAR", "VPRSV", "VERPR", "STPRS",
            "PEINH", "LBKUM", "SALK3", "BKLAS", "MYPOL",
        ],
        "options_fn": _build_mbew_options,
        "primary_keys": ["MATNR", "BWKEY"],
    },
    {
        # CHANGE 2 + CHANGE 3 (v1.2.0):
        #   description updated to reference PURCHASE_BWART_LIST.
        #   columns extended: added CHARG (batch), BWTAR (valuation type).
        #   These allow last-purchase aggregation at batch level if required,
        #   and preserve full lineage for audit/reconciliation.
        "table": "MSEG",
        "description": (
            f"GR Line Items — BWART IN {PURCHASE_BWART_LIST} "
            f"(all historical GRs for materials in scope)"
        ),
        "columns": [
            "MBLNR", "MJAHR", "ZEILE", "MATNR", "WERKS", "LGORT",
            "CHARG",   # CHANGE 3: added — batch number for batch-level last-purchase join
            "BWART", "BWTAR",  # CHANGE 3: BWTAR added — valuation type
            "SHKZG", "MENGE", "MEINS", "DMBTR", "WAERS",
            "EBELN", "EBELP", "LIFNR", "BUKRS",
        ],
        "options_fn": _build_mseg_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {"MBLNR": "_MBLNR_SET"},
    },
    {
        # CHANGE 1 + CHANGE 4 (v1.2.0):
        #   description updated — no longer mentions INVENTORY_DATE.
        #   options_fn (_build_mkpf_options) no longer applies BUDAT filter.
        "table": "MKPF",
        "description": (
            "GR Document Headers — all historical GR docs for materials in scope "
            "(BUDAT date filter removed; full history for last-purchase aggregation)"
        ),
        "columns": [
            "MBLNR", "MJAHR", "BUDAT", "BLDAT", "CPUDT",
            "CPUTM", "USNAM", "BKTXT", "XBLNR",
        ],
        "options_fn": _build_mkpf_options,
        "primary_keys": ["MBLNR", "MJAHR"],
    },
]


# ─────────────────────────────────────────────────────────────
# Extraction tuning  (override via .env)
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
    log_file = os.path.join(log_dir, f"sap_mb52_staging_{run_id}.log")

    log = logging.getLogger(f"sap_mb52_staging.{run_id}")
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


# ─────────────────────────────────────────────────────────────
# SAP connection wrapper  — unchanged from v1.1.0 (FIX #1 + FIX #3)
# ─────────────────────────────────────────────────────────────

class SAPConnection:
    """
    Wraps pyrfc.Connection with:
      • Retry loop on initial connect (3 attempts, 5 s apart).
      • read_table() that retries internally on CommunicationError,
        reconnecting before each retry. This replaces the old pattern
        where reconnect happened inside read_table but the exception
        was re-raised, causing BatchExtractor's outer retry to call
        read_table on an already-closed connection.
      • ensure_connected() — a lightweight RFC_PING liveness check
        called by main() before each table pass, so a connection that
        died between tables is healed proactively.
    """

    _CONNECT_ATTEMPTS = 3
    _CONNECT_DELAY_S  = 5.0

    def __init__(
        self,
        system: str,
        log: logging.Logger,
        max_retries: int = 3,
        retry_delay_s: float = 5.0,
    ) -> None:
        self._system       = system
        self._log          = log
        self._max_retries  = max_retries
        self._retry_delay  = retry_delay_s
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        """Open a new RFC connection, retrying up to _CONNECT_ATTEMPTS times."""
        for attempt in range(1, self._CONNECT_ATTEMPTS + 1):
            try:
                self._log.info(
                    "Connecting to SAP %s (attempt %d/%d)...",
                    self._system, attempt, self._CONNECT_ATTEMPTS,
                )
                self.conn = get_sap_connection(self._system)
                self._log.info("SAP %s connection established.", self._system)
                return
            except LogonError:
                self._log.exception("SAP logon failed — check credentials.")
                raise
            except CommunicationError as exc:
                self._log.warning("Attempt %d/%d failed: %s",
                                  attempt, self._CONNECT_ATTEMPTS, exc)
                if attempt == self._CONNECT_ATTEMPTS:
                    raise
                time.sleep(self._CONNECT_DELAY_S)

    def ensure_connected(self) -> None:
        """
        FIX #3 — Ping the SAP system; reconnect if the ping fails.

        Called by main() before every table pass so that a connection
        that dropped between tables is healed before the first read_table
        call for that pass, rather than discovered mid-extraction.
        """
        try:
            self.conn.call("RFC_PING")
            self._log.debug("RFC_PING OK — connection is alive.")
        except Exception as exc:
            self._log.warning("RFC_PING failed (%s) — reconnecting...", exc)
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self._connect()

    def read_table(
        self,
        table: str,
        fields: List[str],
        options: Optional[List[Dict]] = None,
        rowcount: int = 5000,
        rowskips: int = 0,
    ) -> Dict[str, Any]:
        """
        FIX #1 — Retry RFC_READ_TABLE internally on CommunicationError.
        Unchanged from v1.1.0.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 2):
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
                self._log.exception("ABAP error reading %s — not retrying.", table)
                raise
            except CommunicationError as exc:
                last_exc = exc
                self._log.warning(
                    "[%s] RFC_READ_TABLE attempt %d/%d failed (offset=%d): %s",
                    table, attempt, self._max_retries + 1, rowskips, exc,
                )
                if attempt > self._max_retries:
                    break
                self._log.info("[%s] Reconnecting before retry %d...", table, attempt + 1)
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                try:
                    self._connect()
                except Exception as conn_exc:
                    self._log.error(
                        "[%s] Reconnect attempt %d failed: %s — will retry RFC after delay.",
                        table, attempt, conn_exc,
                    )
                time.sleep(self._retry_delay)
            except Exception as exc:
                last_exc = exc
                self._log.warning(
                    "[%s] Unexpected RFC error attempt %d/%d (offset=%d): %s",
                    table, attempt, self._max_retries + 1, rowskips, exc,
                )
                if attempt > self._max_retries:
                    break
                self._log.info("[%s] Reconnecting before retry %d...", table, attempt + 1)
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None
                try:
                    self._connect()
                except Exception:
                    pass
                time.sleep(self._retry_delay)

        self._log.error(
            "[%s] All %d RFC attempts exhausted at rowskips=%d.",
            table, self._max_retries + 1, rowskips,
        )
        raise last_exc  # type: ignore[misc]

    def close(self) -> None:
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self._log.info("SAP HANA connection closed.")
            self.conn = None


# ─────────────────────────────────────────────────────────────
# Column metadata  (field lengths from DD03L)
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Column chunker  (respects RFC buffer width limit)
# ─────────────────────────────────────────────────────────────

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
            "Column chunking: %d columns → %d chunk(s) (buffer limit %d).",
            len(self._columns), len(result), self._max_buffer,
        )
        for i, chunk in enumerate(result, 1):
            self._log.debug("  Chunk %d: %s", i, chunk)
        return result


# ─────────────────────────────────────────────────────────────
# Data sanitiser
# ─────────────────────────────────────────────────────────────

def _clean(value: str) -> str:
    v = value.strip()
    if v:
        v = v.replace("\x00", "")
        v = "".join(ch for ch in v if ord(ch) >= 32 or ch in "\t\n\r")
    return v


# ─────────────────────────────────────────────────────────────
# Key collector
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# SAP batch extractor  (paginated, chunk-merged) — unchanged
# ─────────────────────────────────────────────────────────────

class BatchExtractor:
    """
    Paginates through an SAP table via RFC_READ_TABLE in batches.
    Unchanged from v1.1.0.
    """

    def __init__(
        self,
        sap: SAPConnection,
        table: str,
        columns: List[str],
        chunks: List[List[str]],
        options: List[Dict[str, str]],
        cfg: ExtractionConfig,
        log: logging.Logger,
        start_offset: int = 0,
    ) -> None:
        self._sap          = sap
        self._table        = table
        self._columns      = columns
        self._chunks       = chunks
        self._options      = options
        self._cfg          = cfg
        self._log          = log
        self._start_offset = start_offset

    def batches(self) -> Generator[List[List[Any]], None, None]:
        offset = self._start_offset
        total  = offset

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
        """
        Fetch one page across all column chunks and merge by row index.
        No retry logic here — read_table() handles retries internally.
        """
        merged: Dict[int, Dict[str, str]] = {}

        for chunk_idx, chunk in enumerate(self._chunks):
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

        return [
            [merged[i].get(col, "") for col in self._columns]
            for i in sorted(merged)
        ]


# ─────────────────────────────────────────────────────────────
# PostgreSQL loader — unchanged from v1.1.0
# ─────────────────────────────────────────────────────────────

class PostgresLoader:
    def __init__(self, schema: str, log: logging.Logger) -> None:
        self._schema = schema
        self._log    = log
        self.conn: Optional[psycopg2.extensions.connection] = None
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

    def _stg_name(self, sap_table: str) -> str:
        return f"stg_sap_mb52_{sap_table.lower()}"

    def _safe_cols(self, columns: List[str]) -> List[str]:
        return [c.replace("-", "_").replace("/", "_").lower() for c in columns]

    def get_existing_row_count(self, sap_table: str) -> int:
        """
        FIX #2 — Return the number of rows already in the staging table,
        or -1 if the table does not exist.
        """
        stg_name = self._stg_name(sap_table)
        try:
            self._cursor.execute(
                f"SELECT COUNT(*) FROM {self._schema}.{stg_name};"
            )
            count = self._cursor.fetchone()[0]
            return count
        except Exception:
            self.conn.rollback()
            return -1

    def _table_exists(self, schema: str, table: str) -> bool:
        self._cursor.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return self._cursor.fetchone() is not None

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg_name  = self._stg_name(sap_table)
        safe_cols = self._safe_cols(columns)

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
            load_ts      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            inventory_dt TEXT      DEFAULT '{INVENTORY_DATE}',
            extraction_id UUID     DEFAULT gen_random_uuid()
        );
        """
        self._log.info("Creating staging table %s.%s...", self._schema, stg_name)
        self._cursor.execute(ddl)
        self.conn.commit()
        return f'"{self._schema}"."{stg_name}"'

    def resume_staging_table(self, sap_table: str) -> str:
        """FIX #2 — Resume into existing staging table (no DROP/CREATE)."""
        stg_name = self._stg_name(sap_table)
        self._log.info(
            "Resuming into existing staging table %s.%s (no DROP/CREATE).",
            self._schema, stg_name,
        )
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
        query = f"INSERT INTO {stg_table} ({', '.join(safe_cols)}) VALUES %s"
        extras.execute_values(self._cursor, query, rows)

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def execute(self, sql: str) -> None:
        """Execute arbitrary DDL/DML — used by _create_purchase_analytics."""
        self._cursor.execute(sql)

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self.conn:
            self.conn.close()
        self._log.info("PostgreSQL connection closed.")


# ─────────────────────────────────────────────────────────────
# Per-table pipeline  — unchanged from v1.1.0 (FIX #2 resume)
# ─────────────────────────────────────────────────────────────

def extract_table(
    table_def: Dict[str, Any],
    sap: SAPConnection,
    loader: PostgresLoader,
    cfg: ExtractionConfig,
    log: logging.Logger,
    resume: bool = False,
) -> Tuple[bool, int]:
    """
    FIX #2 — Resume mode. Unchanged from v1.1.0.
    See inline comments in v1.1.0 for full resume logic description.
    """
    table   = table_def["table"].upper()
    columns = list(dict.fromkeys(table_def["columns"]))
    desc    = table_def.get("description", table)
    key_out = table_def.get("key_out", {})

    options_fn = table_def.get("options_fn")
    options_list = options_fn() if callable(options_fn) else [[]]
    if options_list and isinstance(options_list[0], dict):
        options_list = [options_list]
    elif not options_list:
        options_list = [[]]

    log.info("=" * 60)
    log.info("Table : %s  (%s)", table, desc)
    log.info("Fields: %s", columns)
    if options_list and options_list[0]:
        preview = " | ".join(r["TEXT"] for r in options_list[0][:4])
        suffix  = " | ..." if len(options_list[0]) > 4 else ""
        log.info("OPTIONS (chunk 1/%d) (%d rows): %s%s",
                 len(options_list), len(options_list[0]), preview, suffix)

    t0 = time.time()

    start_offset = 0
    if resume:
        if len(options_list) > 1:
            log.warning(
                "[%s] Options are chunked. Resume mode is not supported "
                "with chunked options. Starting fresh.", table,
            )
            resume = False
        else:
            existing_count = loader.get_existing_row_count(table)
            if existing_count > 0:
                start_offset = (existing_count // cfg.batch_size) * cfg.batch_size
                log.info(
                    "[%s] RESUME MODE — %d rows already in staging table. "
                    "Starting RFC at offset %d (batch boundary).",
                    table, existing_count, start_offset,
                )
                stg_table = loader.resume_staging_table(table)
            else:
                log.info("[%s] No existing rows found — starting fresh.", table)
                resume = False

    if not resume:
        meta    = ColumnMetadata(sap, log)
        widths  = meta.get_widths(table, columns)
        chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
        chunks  = chunker.chunks()
        stg_table = loader.create_staging_table(table, columns)
    else:
        meta    = ColumnMetadata(sap, log)
        widths  = meta.get_widths(table, columns)
        chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
        chunks  = chunker.chunks()

    rows_loaded       = start_offset
    rows_since_commit = 0

    for opt_idx, options in enumerate(options_list):
        if len(options_list) > 1:
            log.info("[%s] Extracting options chunk %d of %d...",
                     table, opt_idx + 1, len(options_list))

        current_offset = start_offset if opt_idx == 0 else 0
        extractor = BatchExtractor(
            sap, table, columns, chunks, options, cfg, log,
            start_offset=current_offset,
        )

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


# ─────────────────────────────────────────────────────────────
# FIX #2 helper — re-populate key sets from PostgreSQL
# Unchanged from v1.1.0
# ─────────────────────────────────────────────────────────────

def _prepopulate_key_sets_from_postgres(
    loader: PostgresLoader,
    cfg: ExtractionConfig,
    log: logging.Logger,
) -> None:
    """
    Reads key-set values from already-committed staging tables into
    module-level sets. Unchanged from v1.1.0.
    """
    schema = cfg.staging_schema
    current_module = sys.modules[__name__]

    recovery_map = [
        ("mseg", "mblnr", "_MBLNR_SET"),
        ("mard", "matnr", "_MATNR_SET"),
        ("mard", "werks", "_WERKS_SET"),
        ("mard", "lgort", "_LGORT_SET"),
        ("t001w", "bwkey", "_BWKEY_SET"),
        ("t001k", "bukrs", "_BUKRS_SET"),
        ("mchb", "charg", "_CHARG_SET"),
    ]

    for suffix, col, set_name in recovery_map:
        target_set: Set[str] = getattr(current_module, set_name)
        if target_set:
            log.debug(
                "Key set %s already has %d entries — skipping PostgreSQL recovery.",
                set_name, len(target_set),
            )
            continue

        stg_table = f"{schema}.stg_sap_mb52_{suffix}"
        try:
            cursor = loader.conn.cursor()
            cursor.execute(
                f'SELECT DISTINCT "{col}" FROM {stg_table} '
                f'WHERE "{col}" IS NOT NULL AND "{col}" != \'\';'
            )
            rows = cursor.fetchall()
            cursor.close()
            for (val,) in rows:
                if val:
                    target_set.add(val.strip())
            log.info(
                "Recovered %d '%s' values from %s → %s",
                len(rows), col, stg_table, set_name,
            )
        except Exception as exc:
            log.warning(
                "Could not recover %s from %s: %s — proceeding with partial set.",
                set_name, stg_table, exc,
            )
            try:
                loader.conn.rollback()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# CHANGE 5 — Post-extraction: last-purchase analytics
# ─────────────────────────────────────────────────────────────

def _create_purchase_analytics(
    loader: PostgresLoader,
    cfg: ExtractionConfig,
    log: logging.Logger,
) -> None:
    """
    CHANGE 5 (v1.2.0) — Create indexes and the last-purchase materialized view.

    Called once from main() after all 9 table passes have succeeded.
    Safe to re-run: all DDL uses IF NOT EXISTS / OR REPLACE / CREATE OR REPLACE
    where possible; the materialized view is dropped and recreated to pick up
    any schema changes.

    WHAT THIS CREATES
    ─────────────────
    1. Performance indexes on MSEG and MKPF staging tables.
       Without these, the JOIN in the materialized view would perform a
       sequential scan on potentially millions of MSEG rows.

    2. Materialized view mv_last_purchase_by_sloc:
       One row per (MATNR, WERKS, LGORT) — the natural granularity of
       MARD — containing the most recent GR metrics:
         last_purchase_date   MAX(mkpf.budat)  — answers "when was this last received?"
         last_purchase_qty    MSEG.MENGE from the most recent GR line
         last_po_number       MSEG.EBELN from the most recent GR line
         last_vendor          MSEG.LIFNR from the most recent GR line
         last_mblnr           MSEG.MBLNR (GR document number, for audit trail)
         last_dmbtr           MSEG.DMBTR (amount in local currency, unit price proxy)
         last_waers           MSEG.WAERS (currency of DMBTR)
         days_since_purchase  Age in days from last_purchase_date to CURRENT_DATE

    3. Unique index on the materialized view for REFRESH CONCURRENTLY.

    DESIGN DECISIONS
    ────────────────
    • DISTINCT ON (matnr, werks, lgort) ORDER BY budat DESC:
      Standard PostgreSQL idiom for "latest row per group." Avoids a
      subquery + self-join pattern that is harder to read and equally fast.

    • MATERIALIZED VIEW preferred over a plain VIEW:
      The underlying JOIN on MSEG × MKPF can be expensive at high row counts.
      Materializing pins the result so downstream reports are instant.
      Refresh with:  REFRESH MATERIALIZED VIEW CONCURRENTLY
                         <schema>.mv_last_purchase_by_sloc;

    • BWART filter uses the same PURCHASE_BWART_LIST as the extraction,
      ensuring the view is consistent with what was loaded.

    • days_since_purchase is cast to INTEGER to avoid fractional days
      from CURRENT_TIMESTAMP. Negative values would indicate a future
      BUDAT (data quality issue in SAP).

    SAP RISKS
    ─────────
    • DMBTR / WAERS: MSEG.DMBTR is the total line value in local currency,
      not the unit price. Divide by MENGE to get per-unit cost if needed.
      Currency is MSEG.WAERS (company code currency), not MBEW.VPRSV.
    • LIFNR on MSEG: populated for GR against PO (BWART 101). For other
      movement types it may be blank — add a join to EKKO/EKPO via EBELN
      if vendor attribution at non-101 movements is required.
    • BUDAT vs BLDAT: the view uses BUDAT (posting date) not BLDAT (document
      date). For last-purchase-date reporting, BUDAT is canonical.
    """
    schema     = cfg.staging_schema
    mseg_tbl   = f"{schema}.stg_sap_mb52_mseg"
    mkpf_tbl   = f"{schema}.stg_sap_mb52_mkpf"
    mv_name    = f"{schema}.mv_last_purchase_by_sloc"

    # Build the BWART IN clause from the same list used during extraction,
    # so the view is always consistent with loaded data.
    bwart_in = ", ".join(f"'{b}'" for b in PURCHASE_BWART_LIST)

    log.info("=" * 60)
    log.info("CHANGE 5: Creating last-purchase analytics objects...")
    log.info("  Target schema   : %s", schema)
    log.info("  MSEG table      : %s", mseg_tbl)
    log.info("  MKPF table      : %s", mkpf_tbl)
    log.info("  Materialized view: %s", mv_name)
    log.info("  BWART filter    : IN (%s)", bwart_in)

    # ── Step 1: Indexes on MSEG staging table ─────────────────
    # These are the most critical for performance. MSEG can have millions
    # of rows; without an index on MATNR the materialized view refresh
    # will full-scan the table on every REFRESH.
    mseg_indexes = [
        # Primary join key: (MBLNR, MJAHR) → links to MKPF
        (
            f"idx_stg_mseg_mblnr_mjahr",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mseg_mblnr_mjahr '
            f'ON {mseg_tbl} (mblnr, mjahr);',
        ),
        # Material scope filter: MATNR — speeds up re-runs and ad-hoc queries
        (
            f"idx_stg_mseg_matnr",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mseg_matnr '
            f'ON {mseg_tbl} (matnr);',
        ),
        # Compound: (MATNR, WERKS, LGORT) — the GROUP BY key of the view
        (
            f"idx_stg_mseg_matnr_werks_lgort",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mseg_matnr_werks_lgort '
            f'ON {mseg_tbl} (matnr, werks, lgort);',
        ),
        # BWART filter pushdown
        (
            f"idx_stg_mseg_bwart",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mseg_bwart '
            f'ON {mseg_tbl} (bwart);',
        ),
    ]

    # ── Step 2: Indexes on MKPF staging table ─────────────────
    mkpf_indexes = [
        # Primary join key: (MBLNR, MJAHR) — must match MSEG join columns
        (
            f"idx_stg_mkpf_mblnr_mjahr",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mkpf_mblnr_mjahr '
            f'ON {mkpf_tbl} (mblnr, mjahr);',
        ),
        # BUDAT — used in ORDER BY for DISTINCT ON; index enables sort avoidance
        (
            f"idx_stg_mkpf_budat",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mkpf_budat '
            f'ON {mkpf_tbl} (budat);',
        ),
        # Compound covering index: (MBLNR, MJAHR, BUDAT) — index-only scan path
        (
            f"idx_stg_mkpf_mblnr_mjahr_budat",
            f'CREATE INDEX IF NOT EXISTS idx_stg_mkpf_mblnr_mjahr_budat '
            f'ON {mkpf_tbl} (mblnr, mjahr, budat DESC);',
        ),
    ]

    all_indexes = mseg_indexes + mkpf_indexes
    for idx_name, idx_sql in all_indexes:
        try:
            log.info("  Creating index: %s...", idx_name)
            loader.execute(idx_sql)
            loader.commit()
        except Exception as exc:
            log.warning("  Index %s failed (may already exist): %s", idx_name, exc)
            try:
                loader.conn.rollback()
            except Exception:
                pass

    # ── Step 3: Drop + recreate materialized view ─────────────
    # DROP + CREATE (not CREATE OR REPLACE) because PostgreSQL does not
    # support OR REPLACE for materialized views prior to PG 17. Using
    # DROP IF EXISTS + CREATE is universally safe.
    drop_mv_sql = f"DROP MATERIALIZED VIEW IF EXISTS {mv_name};"

    # The core query uses DISTINCT ON ordered by budat DESC to get the
    # single most-recent GR row per (matnr, werks, lgort). This is
    # equivalent to a ROW_NUMBER() PARTITION BY / RANK() approach but
    # more idiomatic in PostgreSQL and typically faster with the index.
    create_mv_sql = f"""
CREATE MATERIALIZED VIEW {mv_name} AS
/*
  mv_last_purchase_by_sloc
  ========================
  Last purchase (GR) metrics per material / plant / storage location.

  Granularity : one row per (matnr, werks, lgort)
  Source      : {mseg_tbl}  JOIN  {mkpf_tbl}
  BWART filter: {bwart_in}
  Refresh cmd : REFRESH MATERIALIZED VIEW CONCURRENTLY {mv_name};

  Columns
  -------
  matnr               SAP material number
  werks               Plant
  lgort               Storage location
  last_purchase_date  Most recent GR posting date (MKPF.BUDAT), YYYYMMDD text
  last_purchase_qty   MSEG.MENGE on the most recent GR line
  last_purchase_uom   MSEG.MEINS unit of measure for last_purchase_qty
  last_po_number      MSEG.EBELN purchasing document (PO) number
  last_vendor         MSEG.LIFNR vendor account number
  last_mblnr          MSEG.MBLNR GR material document number (audit key)
  last_mjahr          MSEG.MJAHR material document year
  last_zeile          MSEG.ZEILE line item within the GR document
  last_dmbtr          MSEG.DMBTR amount in local currency (total line value)
  last_waers          MSEG.WAERS currency of last_dmbtr
  days_since_purchase Days between last_purchase_date and today (INTEGER)
                      Negative = future-dated posting (data quality flag)

  IMPORTANT: last_dmbtr is the TOTAL LINE value, not per-unit.
  For unit price: last_dmbtr / NULLIF(last_purchase_qty::NUMERIC, 0)
*/
SELECT
    s.matnr,
    s.werks,
    s.lgort,
    h.budat                                           AS last_purchase_date,
    s.menge                                           AS last_purchase_qty,
    s.meins                                           AS last_purchase_uom,
    s.ebeln                                           AS last_po_number,
    s.lifnr                                           AS last_vendor,
    s.mblnr                                           AS last_mblnr,
    s.mjahr                                           AS last_mjahr,
    s.zeile                                           AS last_zeile,
    s.dmbtr                                           AS last_dmbtr,
    s.waers                                           AS last_waers,
    -- days_since_purchase: cast budat (YYYYMMDD text) to DATE for subtraction.
    -- NULLIF guards against blank budat values from data quality issues.
    (CURRENT_DATE
        - TO_DATE(NULLIF(h.budat, ''), 'YYYYMMDD')
    )::INTEGER                                        AS days_since_purchase
FROM (
    /*
      DISTINCT ON (matnr, werks, lgort) ordered by budat DESC gives the
      single most-recent GR document line for each storage location.
      The index on MKPF(mblnr, mjahr, budat DESC) makes this sort cheap.
    */
    SELECT DISTINCT ON (ms.matnr, ms.werks, ms.lgort)
        ms.mblnr,
        ms.mjahr,
        ms.zeile,
        ms.matnr,
        ms.werks,
        ms.lgort,
        ms.menge,
        ms.meins,
        ms.ebeln,
        ms.lifnr,
        ms.dmbtr,
        ms.waers,
        mk.budat
    FROM       {mseg_tbl}  ms
    INNER JOIN {mkpf_tbl}  mk
           ON  ms.mblnr = mk.mblnr
           AND ms.mjahr  = mk.mjahr
    WHERE ms.bwart IN ({bwart_in})
      AND ms.matnr IS NOT NULL AND ms.matnr <> ''
      AND mk.budat IS NOT NULL AND mk.budat <> ''
    ORDER BY
        ms.matnr,
        ms.werks,
        ms.lgort,
        mk.budat DESC      -- most recent first → DISTINCT ON picks this row
) s
-- Alias sub-query so budat is accessible at outer SELECT without re-joining
JOIN {mkpf_tbl} h
  ON s.mblnr = h.mblnr
 AND s.mjahr  = h.mjahr
WITH DATA;
"""

    # ── Step 4: Unique index on the materialized view ─────────
    # Required for REFRESH MATERIALIZED VIEW CONCURRENTLY (non-blocking refresh).
    # The natural key is (matnr, werks, lgort) — guaranteed unique by
    # the DISTINCT ON in the view definition.
    mv_unique_idx_sql = f"""
CREATE UNIQUE INDEX IF NOT EXISTS uidx_mv_last_purchase_sloc
    ON {mv_name} (matnr, werks, lgort);
"""

    # ── Step 5: Optional convenience index for date-range queries ──
    # Analysts often query: "show me all stock not received in 180 days"
    # This index makes that filter fast.
    mv_date_idx_sql = f"""
CREATE INDEX IF NOT EXISTS idx_mv_last_purchase_date
    ON {mv_name} (last_purchase_date);
"""

    try:
        log.info("  Dropping existing materialized view (if any)...")
        loader.execute(drop_mv_sql)
        loader.commit()

        log.info("  Creating materialized view %s...", mv_name)
        loader.execute(create_mv_sql)
        loader.commit()
        log.info("  Materialized view created successfully.")

        log.info("  Creating unique index on materialized view...")
        loader.execute(mv_unique_idx_sql)
        loader.commit()

        log.info("  Creating date index on materialized view...")
        loader.execute(mv_date_idx_sql)
        loader.commit()

        log.info("  All analytics objects created successfully.")

    except Exception:
        log.exception("Failed to create purchase analytics materialized view.")
        try:
            loader.conn.rollback()
        except Exception:
            pass
        # Non-fatal: extraction data is still intact; raise to surface in main().
        raise

    log.info("=" * 60)
    log.info("Purchase analytics summary:")
    log.info("  Materialized view : %s", mv_name)
    log.info("  Refresh command   : REFRESH MATERIALIZED VIEW CONCURRENTLY %s;", mv_name)
    log.info("  Unique index      : uidx_mv_last_purchase_sloc  (matnr, werks, lgort)")
    log.info("  Date index        : idx_mv_last_purchase_date   (last_purchase_date)")
    log.info("  MSEG indexes      : %d created", len(mseg_indexes))
    log.info("  MKPF indexes      : %d created", len(mkpf_indexes))
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────
# Startup validation
# ─────────────────────────────────────────────────────────────

def _validate_config(log: logging.Logger) -> None:
    try:
        datetime.strptime(INVENTORY_DATE, "%Y%m%d")
    except ValueError:
        raise EnvironmentError(
            f"INVENTORY_DATE '{INVENTORY_DATE}' is not a valid YYYYMMDD date."
        )

    if not PLANT_LIST:
        log.warning(
            "⚠  PLANT_LIST is empty — MARD will be extracted WITHOUT a plant filter. "
            "This may fetch millions of rows on production systems.  "
            "Set PLANT_LIST in the configuration section to scope the extraction."
        )

    if not COMPANY_CODES:
        raise EnvironmentError(
            "COMPANY_CODES is empty — T001K and T001 cannot be scoped. "
            "Provide at least one company code."
        )

    # CHANGE 2 validation: warn if PURCHASE_BWART_LIST has unexpected values.
    known_gr_bwarts = {"101", "102", "103", "104", "105", "106", "107", "108", "109"}
    unknown = set(PURCHASE_BWART_LIST) - known_gr_bwarts
    if unknown:
        log.warning(
            "⚠  PURCHASE_BWART_LIST contains non-standard GR movement types: %s. "
            "Verify these are intentional before running on production.",
            unknown,
        )


# ─────────────────────────────────────────────────────────────
# Main orchestrator  — CHANGE 6: updated log header + CHANGE 5 call
# ─────────────────────────────────────────────────────────────

def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP MB52 Inventory Analytics Pipeline  [run: %s]", run_id)
    # CHANGE 6 (v1.2.0): updated log line — MKPF date filter has been removed.
    # Old: "Inventory date   : %s  (MKPF.BUDAT = this date for GR docs)"
    # New: reflects that INVENTORY_DATE is now only a snapshot label for MARD.
    log.info(
        "Inventory date   : %s  "
        "(MARD snapshot label only — MKPF BUDAT filter removed in v1.2.0; "
        "full GR history extracted for last-purchase aggregation)",
        INVENTORY_DATE,
    )
    log.info("Plant scope      : %s", PLANT_LIST if PLANT_LIST else "ALL (⚠ no filter)")
    log.info("Company codes    : %s", COMPANY_CODES)
    # CHANGE 2: log the BWART list so operators can verify the scope at a glance.
    log.info("Purchase BWARTs  : %s", PURCHASE_BWART_LIST)
    log.info("Tables           : %d", len(STAGING_TABLE_DEFS))
    log.info("Resume on failure: %s", RESUME_ON_FAILURE)
    log.info("=" * 60)

    try:
        _validate_config(log)
        cfg = ExtractionConfig()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log,
                               max_retries=cfg.max_retries,
                               retry_delay_s=cfg.retry_delay_s)
        loader = PostgresLoader(cfg.staging_schema, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()

            # FIX #3: ping SAP before each table pass
            try:
                sap.ensure_connected()
            except Exception as ping_exc:
                log.error(
                    "Cannot establish SAP connection before table %s: %s — skipping.",
                    tbl_name, ping_exc,
                )
                results[tbl_name] = (False, 0)
                continue

            # FIX #2: key-set recovery before MKPF
            if tbl_name == "MKPF" and RESUME_ON_FAILURE:
                log.info(
                    "Pre-MKPF: recovering key sets from PostgreSQL staging tables..."
                )
                _prepopulate_key_sets_from_postgres(loader, cfg, log)
                log.info(
                    "Post-recovery — _MBLNR_SET: %d entries", len(_MBLNR_SET)
                )

            should_resume = False
            if RESUME_ON_FAILURE:
                existing = loader.get_existing_row_count(tbl_name)
                if existing > 0:
                    log.info(
                        "[%s] Found %d existing rows in staging table — will resume.",
                        tbl_name, existing,
                    )
                    should_resume = True

            try:
                ok, count = extract_table(
                    tbl_def, sap, loader, cfg, log,
                    resume=should_resume,
                )
                results[tbl_name] = (ok, count)
            except Exception:
                log.exception("Table %s FAILED.", tbl_name)
                results[tbl_name] = (False, 0)

        # ── CHANGE 5: post-extraction analytics ───────────────
        # Only run if ALL 9 table passes succeeded. If any table failed,
        # the materialized view would be built on incomplete data, which
        # could produce misleading last-purchase dates.
        failed_tables = [t for t, (ok, _) in results.items() if not ok]
        if not failed_tables:
            log.info(
                "All table passes succeeded — creating last-purchase analytics..."
            )
            try:
                _create_purchase_analytics(loader, cfg, log)
                results["mv_last_purchase_by_sloc"] = (True, 0)
            except Exception:
                log.exception(
                    "Post-extraction analytics (mv_last_purchase_by_sloc) FAILED. "
                    "Staging data is intact; re-run _create_purchase_analytics() "
                    "manually after fixing the error."
                )
                results["mv_last_purchase_by_sloc"] = (False, 0)
        else:
            log.warning(
                "Skipping last-purchase analytics — %d table(s) failed: %s. "
                "Fix failures and re-run, or call _create_purchase_analytics() "
                "manually once data is complete.",
                len(failed_tables), failed_tables,
            )

    finally:
        sap.close()
        loader.close()

    total_elapsed = time.time() - overall_start
    succeeded     = [(t, n) for t, (ok, n) in results.items() if ok]
    failed        = [t for t, (ok, _) in results.items() if not ok]

    log.info("=" * 60)
    log.info("Run complete in %.1fs", total_elapsed)
    log.info("Key sets collected:")
    log.info("    MATNR : %d materials",           len(_MATNR_SET))
    log.info("    WERKS : %d plants",              len(_WERKS_SET))
    log.info("    LGORT : %d storage locations",   len(_LGORT_SET))
    log.info("    CHARG : %d batches",             len(_CHARG_SET))
    log.info("    BWKEY : %d valuation areas",     len(_BWKEY_SET))
    log.info("    BUKRS : %d company codes",       len(_BUKRS_SET))
    log.info("    MBLNR : %d GR doc numbers",      len(_MBLNR_SET))
    log.info("Staging tables created (schema: %s):", cfg.staging_schema)
    log.info("    %-30s → stg_sap_mb52_mard",  "MARD  (stock by sloc)")
    log.info("    %-30s → stg_sap_mb52_mchb",  "MCHB  (batch stock)")
    log.info("    %-30s → stg_sap_mb52_mara",  "MARA  (material master)")
    log.info("    %-30s → stg_sap_mb52_t001w", "T001W (plant master)")
    log.info("    %-30s → stg_sap_mb52_t001k", "T001K (val. area)")
    log.info("    %-30s → stg_sap_mb52_t001",  "T001  (company master)")
    log.info("    %-30s → stg_sap_mb52_mbew",  "MBEW  (valuation)")
    log.info("    %-30s → stg_sap_mb52_mseg",  "MSEG  (all historical GR lines)")
    log.info("    %-30s → stg_sap_mb52_mkpf",  "MKPF  (all historical GR headers)")
    # CHANGE 5: log the materialized view
    log.info("    %-30s → mv_last_purchase_by_sloc", "ANALYTICS (last purchase)")
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted and analytics view created successfully.")
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)