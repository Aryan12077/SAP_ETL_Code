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
  • Purchase / GR information  → MSEG + MKPF (BWART='101' goods receipts)
  • Company & currency         → T001W → T001K → T001

INVENTORY_DATE (single date, no range):
  The pipeline accepts exactly one date — INVENTORY_DATE (e.g. 20260516).
  This date is used in two ways:

    1. MARD anchor scope:
       MARD holds current stock positions with no date column of its own.
       The INVENTORY_DATE is recorded in the run metadata and log as the
       "as-of" snapshot label.  PLANT_LIST scopes which plants are extracted.

    2. MKPF purchase-GR date filter:
       MKPF.BUDAT = INVENTORY_DATE
       Only goods receipts (BWART='101') posted ON the inventory date are
       included in the Purchase Date / Purchase Quantity columns.  This
       gives a single-day GR view aligned with the reporting date rather
       than an open-ended historical accumulation (which would be MB5B logic).

  WHY A DATE RANGE IS NOT USED:
    MB52 is a snapshot report, not a movement report.  The unrestricted
    stock on MARD is always the current balance; it does not change based
    on a date window.  Applying a FROM/TO date range to MKPF would produce
    a multi-day GR list that misrepresents the intent of the "Purchase Date"
    column, which should show the most recent / target-date receipt only.

MULTI-PASS ARCHITECTURE (key-set driven):
  Pass 1 : MARD   → anchor (WERKS in PLANT_LIST); collects _MATNR_SET, _WERKS_SET, _LGORT_SET
  Pass 2 : MCHB   → batch stock (MATNR in _MATNR_SET); collects _CHARG_SET
  Pass 3 : MARA   → material master (MATNR in _MATNR_SET)
  Pass 4 : T001W  → plant master (WERKS in _WERKS_SET); collects _BWKEY_SET
  Pass 5 : T001K  → valuation area→company (BWKEY in _BWKEY_SET); collects _BUKRS_SET
  Pass 6 : T001   → company master (BUKRS in _BUKRS_SET)
  Pass 7 : MBEW   → valuation (MATNR in _MATNR_SET)
  Pass 8 : MSEG   → GR line items (MATNR in _MATNR_SET, BWART='101'); collects _MBLNR_SET
  Pass 9 : MKPF   → GR headers (MBLNR in _MBLNR_SET, BUDAT=INVENTORY_DATE); collects _BUDAT_SET

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIXES IN THIS VERSION (v1.1.0)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Problem observed in production:
  MSEG failed at offset 140,000 with RFC_COMMUNICATION_FAILURE (WSAECONNREFUSED).
  MKPF then failed immediately because the connection was still closed.

Root causes identified:

  1. STALE CONNECTION REFERENCE IN BATCH EXTRACTOR
     SAPConnection._connect() replaces self.conn with a new Connection object
     on reconnect. However, read_table() re-raises CommunicationError after
     reconnecting, so BatchExtractor's retry loop calls read_table() again on
     an already-fresh connection — but if the reconnect itself failed (3 SAP
     attempts exhausted), subsequent retries in _fetch_offset still used the
     dead connection object. The retry in read_table and the retry in
     _fetch_offset were uncoordinated, leading to retries being "used up"
     fighting each other.

     FIX: SAPConnection.read_table() now performs its own full retry loop
     (up to max_retries) with re-connect on CommunicationError, and does NOT
     re-raise until all attempts are exhausted. BatchExtractor._fetch_offset()
     no longer needs a separate retry layer for RFC calls — it trusts that
     read_table() has already retried robustly.

  2. NO RESUME AFTER PARTIAL FAILURE
     When MSEG failed, all 140,000 already-loaded rows were retained in
     PostgreSQL (committed), but the pipeline marked MSEG as FAILED and
     stopped. MKPF also failed because _MBLNR_SET was only partially
     populated.

     FIX: extract_table() now supports RESUME MODE. It detects the row count
     already present in the staging table (via SELECT COUNT(*)), calculates the
     correct starting offset (rounded down to the nearest batch boundary), and
     resumes extraction from that point. This is safe because staging tables
     use TRUNCATE+CREATE only on the first run; subsequent runs with
     resume_on_failure=True skip the DROP/CREATE and resume from last commit.

  3. CONNECTION EXHAUSTION BEFORE MKPF
     After MSEG's reconnect attempts exhausted all 3 SAP retries, the
     SAPConnection was left in a closed state. MKPF's DD03L lookup then
     failed immediately (producing the fallback width warning), and all
     RFC calls failed. This is why MKPF produced 0 rows despite _MBLNR_SET
     being populated from the MSEG rows that did succeed.

     FIX: main() now calls sap.ensure_connected() before each table pass.
     SAPConnection exposes ensure_connected() which checks the connection
     liveness via a lightweight RFC_PING call and reconnects if needed.

Business column → SAP field mapping:
  Material Code      MARD.MATNR
  Plant              MARD.WERKS
  Storage Location   MARD.LGORT
  Batch              MCHB.CHARG
  Base Unit of Meas. MARA.MEINS
  Unrestricted Stock MARD.LABST
  Stock Segment      MCHB.SGT_SCAT
  Currency           T001.WAERS
  Inventory Value    MBEW.SALK3
  Company Name       T001.BUTXT
  Material Type      MARA.MTART
  Material Group     MARA.MATKL
  Purchase Date      MKPF.BUDAT
  Purchase Quantity  MSEG.MENGE  (BWART='101' only)
  Per Piece Value    MBEW.STPRS

Connection parameters: SAP HANA (same env-var names as MB5B pipeline).

Author  : [Your Name]
Version : 1.1.0
"""

from __future__ import annotations

import os
import platform

# ── PostgreSQL DLL registration (Windows) ────────────────────
pg_bin_path = r"C:\Program Files\PostgreSQL\18\bin"

if platform.system() == "Windows":
    if os.path.exists(pg_bin_path):
        os.add_dll_directory(pg_bin_path)
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

# ── Load env ─────────────────────────────────────────────────
load_dotenv()


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — edit this section only
# ─────────────────────────────────────────────────────────────

INVENTORY_DATE: str = os.getenv("INVENTORY_DATE", "20260915")

PLANT_LIST: List[str] = ["1202"]

COMPANY_CODES: List[str] = ["1200"]

# ── Resume mode ───────────────────────────────────────────────
# When True, a table whose staging table already exists and has rows
# will be resumed from its last committed offset rather than restarted.
# Set to False to always do a full DROP + re-extract (safe re-run).
RESUME_ON_FAILURE: bool = False


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
    extra_filter = ""
    if PLANT_LIST:
        plants = ",".join(f"'{p}'" for p in PLANT_LIST)
        extra_filter = f"WERKS IN ({plants})"
    return _build_in_options("MATNR", _MATNR_SET, extra=extra_filter)


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


def _build_mseg_options() -> List[List[Dict[str, str]]]:
    return _build_in_options("MATNR", _MATNR_SET, extra="BWART = '101'")


def _build_mkpf_options() -> List[List[Dict[str, str]]]:
    return _build_in_options(
        "MBLNR",
        _MBLNR_SET,
        extra=f"BUDAT = '{INVENTORY_DATE}'",
    )


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
        "table": "MSEG",
        "description": "GR Line Items — BWART='101' (Goods Receipt) for materials in scope",
        "columns": [
            "MBLNR", "MJAHR", "ZEILE", "MATNR", "WERKS", "LGORT",
            "BWART", "SHKZG", "MENGE", "MEINS", "DMBTR", "WAERS",
            "EBELN", "EBELP", "LIFNR", "BUKRS",
        ],
        "options_fn": _build_mseg_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {"MBLNR": "_MBLNR_SET"},
    },
    {
        "table": "MKPF",
        "description": (
            f"GR Document Headers — BWART='101' docs posted on {INVENTORY_DATE}"
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
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA_ECC", "sap_stg_hana"))

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
# Environment / connection config  (same env-var names as MB5B)
# ─────────────────────────────────────────────────────────────

def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is missing or empty.")
    return val


# def build_sap_config() -> Dict[str, str]:
#     return {
#         "saprouter": os.getenv("HANA_ROUTER"),
#         "ashost":    _require_env("HANA_ASHOST"),
#         "sysnr":     os.getenv("HANA_SYSNR", "00"),
#         "client":    os.getenv("HANA_CLIENT", "500"),
#         "user":      _require_env("HANA_USER"),
#         "passwd":    _require_env("HANA_PASSWD"),
#         "lang":      os.getenv("HANA_LANG", "EN"),
#     }
def build_sap_config() -> Dict[str, str]:
    return {
        "user":   _require_env("SAP_USER"),
        "passwd": _require_env("SAP_PASSWORD"),
        "ashost": _require_env("SAP_ASHOST"),
        "sysnr":  _require_env("SAP_SYSNR"),
        "client": _require_env("SAP_CLIENT"),
        "lang":   os.getenv("SAP_LANG", "EN"),
    }


def build_pg_config() -> Dict[str, Any]:
    return {
        "host":     _require_env("PG_HOST"),
        "port":     os.getenv("PG_PORT", "5432"),
        "database": _require_env("PG_DATABASE"),
        "user":     _require_env("PG_USER"),
        "password": _require_env("PG_PASSWORD"),
    }


# ─────────────────────────────────────────────────────────────
# SAP connection wrapper  — FIX #1 + FIX #3
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
        config: Dict[str, str],
        log: logging.Logger,
        max_retries: int = 3,
        retry_delay_s: float = 5.0,
    ) -> None:
        self._cfg          = config
        self._log          = log
        self._max_retries  = max_retries
        self._retry_delay  = retry_delay_s
        self.conn: Optional[Connection] = None
        self._connect()

    # ── Internal: open a new Connection ──────────────────────

    def _connect(self) -> None:
        """Open a new RFC connection, retrying up to _CONNECT_ATTEMPTS times."""
        for attempt in range(1, self._CONNECT_ATTEMPTS + 1):
            try:
                self._log.info(
                    "Connecting to SAP ECC [%s] (attempt %d/%d)...",
                    self._cfg.get("ashost", "?"), attempt, self._CONNECT_ATTEMPTS,
                )
                self.conn = Connection(**self._cfg)
                self._log.info("SAP ECC connection established.")
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

    # ── Public: liveness check ────────────────────────────────

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

    # ── Public: RFC_READ_TABLE with integrated retry ──────────

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

        Old behaviour:
          • On CommunicationError: call _connect() (replaces self.conn),
            then re-raise — so the caller's retry loop would call
            read_table() again, but self.conn was already the new
            connection (yet the old pyrfc.Connection object used before
            reconnect is gone).  The re-raised exception was caught in
            BatchExtractor._fetch_offset, which re-called read_table()
            up to max_retries times — but by then there was no guarantee
            the new self.conn was still open either, leading to the
            "connection is closed" cascade.

        New behaviour:
          • On CommunicationError: log, attempt reconnect, wait, then
            retry the RFC call directly here — up to max_retries times.
          • Only re-raise after all internal attempts are exhausted.
          • ABAPApplicationError / ABAPRuntimeError are not retried
            (they indicate an SAP-side ABAP problem, not a network drop).
          • BatchExtractor._fetch_offset no longer needs its own retry
            wrapper for individual RFC failures.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 2):   # +1 so attempt 1 is the first try
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
                # Reconnect before retrying
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
                # Unexpected pyrfc error — treat like CommunicationError for robustness
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
# SAP batch extractor  (paginated, chunk-merged)
# — FIX #1: retry logic removed from _fetch_offset; read_table handles it.
# ─────────────────────────────────────────────────────────────

class BatchExtractor:
    """
    Paginates through an SAP table via RFC_READ_TABLE in batches.

    FIX #1 impact:
      _fetch_offset() no longer wraps read_table() in its own retry loop.
      read_table() on SAPConnection already handles CommunicationError with
      reconnect+retry internally.  The old dual-retry pattern (one in
      read_table, one here) caused retry budgets to interfere:
        • read_table reconnected on attempt 1, then re-raised.
        • _fetch_offset counted that as attempt 1 and retried.
        • read_table had no budget left, so it raised immediately.
        • _fetch_offset ran out of attempts before the SAP side recovered.
      Now: read_table retries fully (default 4 total attempts with reconnect),
      and _fetch_offset simply propagates any final exception.
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
        self._start_offset = start_offset   # FIX #2: resume support

    def batches(self) -> Generator[List[List[Any]], None, None]:
        offset = self._start_offset
        total  = offset   # for display purposes: count from start, not from resume point

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
# PostgreSQL loader
# ─────────────────────────────────────────────────────────────

class PostgresLoader:
    def __init__(self, cfg: Dict[str, Any], schema: str, log: logging.Logger) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._log    = log
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]  = None

    def connect(self) -> None:
        self._log.info(
            "Connecting to PostgreSQL [%s:%s] / [%s]...",
            self._cfg.get("host", "?"),
            self._cfg.get("port", "5432"),
            self._cfg.get("database", "?"),
        )
        try:
            self.conn    = psycopg2.connect(**self._cfg)
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
        or -1 if the table does not exist (meaning it must be created fresh).
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
            return -1   # table does not exist

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
        """
        FIX #2 — Return the qualified table name for resuming into an
        existing staging table (no DROP/CREATE).
        """
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

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self.conn:
            self.conn.close()
        self._log.info("PostgreSQL connection closed.")


# ─────────────────────────────────────────────────────────────
# Per-table pipeline  — FIX #2: resume support
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
    FIX #2 — Resume mode:
      When resume=True (RESUME_ON_FAILURE=True and a prior partial run exists):
        1. Query the existing staging table row count.
        2. Round down to the nearest batch boundary to find a safe restart
           offset (the final partial batch may be incomplete, so we re-fetch
           the last full batch to avoid a gap).
        3. Skip DROP/CREATE — append into the existing table.
        4. Pass start_offset to BatchExtractor so RFC_READ_TABLE ROWSKIPS
           correctly fast-forwards past already-loaded rows.

      key_out sets (_MBLNR_SET etc.) are NOT pre-populated from the existing
      staging data during a resume.  This means that for MSEG (key_out:
      MBLNR → _MBLNR_SET), the resume will only collect MBLNR values from
      the rows fetched after the resume point.  If you need a complete
      _MBLNR_SET from both old and new rows, set RESUME_ON_FAILURE=False
      and run a full re-extract.  For the MSEG→MKPF dependency specifically,
      the already-loaded MSEG rows have their MBLNR values in PostgreSQL;
      an optional pre-population step reads them back if key_out sets are empty
      (see _prepopulate_key_sets_from_postgres below).
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
        log.info("OPTIONS (chunk 1/%d) (%d rows): %s%s", len(options_list), len(options_list[0]), preview, suffix)

    t0 = time.time()

    # ── Determine start offset (resume vs fresh) ──────────────
    start_offset = 0
    if resume:
        if len(options_list) > 1:
            log.warning("[%s] Options are chunked. Resume mode is not supported with chunked options. Starting fresh.", table)
            resume = False
        else:
            existing_count = loader.get_existing_row_count(table)
            if existing_count > 0:
                # Round down to nearest batch boundary to avoid a mid-batch gap.
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
        # In resume mode we still need widths/chunks for the extractor
        meta    = ColumnMetadata(sap, log)
        widths  = meta.get_widths(table, columns)
        chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
        chunks  = chunker.chunks()

    rows_loaded       = start_offset   # count from zero; resume just skips fetching those rows
    rows_since_commit = 0

    for opt_idx, options in enumerate(options_list):
        if len(options_list) > 1:
            log.info("[%s] Extracting options chunk %d of %d...", table, opt_idx + 1, len(options_list))
            
        current_offset = start_offset if opt_idx == 0 else 0
        extractor = BatchExtractor(sap, table, columns, chunks, options, cfg, log,
                                   start_offset=current_offset)
                                   
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
# ─────────────────────────────────────────────────────────────

def _prepopulate_key_sets_from_postgres(
    loader: PostgresLoader,
    cfg: ExtractionConfig,
    log: logging.Logger,
) -> None:
    """
    After a failed MSEG run, _MBLNR_SET contains only the MBLNR values
    collected from the batches that succeeded before the failure.  The
    already-committed rows in stg_sap_mb52_mseg may contain additional
    MBLNR values that were loaded but not collected (because key_out
    collection happens in the same loop as loading).

    This function reads MBLNR from the existing MSEG staging table and
    merges those values into _MBLNR_SET before MKPF runs, ensuring MKPF
    gets a complete document-number filter.

    It also re-populates other key sets from their respective tables if
    they are empty (e.g. after a crash before collection was complete).
    """
    schema = cfg.staging_schema
    current_module = sys.modules[__name__]

    recovery_map = [
        # (staging_table_suffix, column_name, set_name)
        ("mseg", "mblnr", "_MBLNR_SET"),
        ("mard", "matnr", "_MATNR_SET"),
        ("mard", "werks", "_WERKS_SET"),
        ("mard", "lgort", "_LGORT_SET"),
        ("t001w", "bwkey", "_BWKEY_SET"),
        ("t001k", "bukrs", "_BUKRS_SET"),

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
                f'SELECT DISTINCT "{col}" FROM {stg_table} WHERE "{col}" IS NOT NULL AND "{col}" != \'\';'
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


# ─────────────────────────────────────────────────────────────
# Main orchestrator  — FIX #2 + FIX #3
# ─────────────────────────────────────────────────────────────

def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP MB52 Inventory Analytics Pipeline  [run: %s]", run_id)
    log.info("Inventory date   : %s  (MKPF.BUDAT = this date for GR docs)", INVENTORY_DATE)
    log.info("Plant scope      : %s", PLANT_LIST if PLANT_LIST else "ALL (⚠ no filter)")
    log.info("Company codes    : %s", COMPANY_CODES)
    log.info("Tables           : %d", len(STAGING_TABLE_DEFS))
    log.info("Resume on failure: %s", RESUME_ON_FAILURE)
    log.info("=" * 60)

    try:
        _validate_config(log)
        cfg        = ExtractionConfig()
        sap_config = build_sap_config()
        pg_config  = build_pg_config()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        # Pass max_retries and retry_delay to SAPConnection so it uses the
        # same tuning parameters as the rest of the pipeline.
        sap    = SAPConnection(sap_config, log,
                               max_retries=cfg.max_retries,
                               retry_delay_s=cfg.retry_delay_s)
        loader = PostgresLoader(pg_config, cfg.staging_schema, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()

            # ── FIX #3: ping SAP before each table pass ───────
            try:
                sap.ensure_connected()
            except Exception as ping_exc:
                log.error(
                    "Cannot establish SAP connection before table %s: %s — skipping.",
                    tbl_name, ping_exc,
                )
                results[tbl_name] = (False, 0)
                continue

            # ── FIX #2: key-set recovery before MKPF ──────────
            # If MSEG succeeded partially or fully in a prior run,
            # recover _MBLNR_SET (and other sets) from PostgreSQL
            # before MKPF's options are built.  This runs once, just
            # before the MKPF pass, and is a no-op if the sets are
            # already populated.
            if tbl_name == "MKPF" and RESUME_ON_FAILURE:
                log.info(
                    "Pre-MKPF: recovering key sets from PostgreSQL staging tables..."
                )
                _prepopulate_key_sets_from_postgres(loader, cfg, log)
                log.info(
                    "Post-recovery — _MBLNR_SET: %d entries", len(_MBLNR_SET)
                )

            # ── Determine whether to resume this table ─────────
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

    log.info("    %-30s → stg_sap_mb52_mara",  "MARA  (material master)")
    log.info("    %-30s → stg_sap_mb52_t001w", "T001W (plant master)")
    log.info("    %-30s → stg_sap_mb52_t001k", "T001K (val. area)")
    log.info("    %-30s → stg_sap_mb52_t001",  "T001  (company master)")
    log.info("    %-30s → stg_sap_mb52_mbew",  "MBEW  (valuation)")
    log.info("    %-30s → stg_sap_mb52_mseg",  "MSEG  (GR line items)")
    log.info("    %-30s → stg_sap_mb52_mkpf",  "MKPF  (GR doc headers)")
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