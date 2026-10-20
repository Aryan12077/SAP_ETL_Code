"""
SAP IW38 Maintenance Order List — Staging Extractor
=====================================================
Extracts columns matching the IW38 (List of Orders) transaction report from
SAP ECC Plant Maintenance tables and loads them into SQL Server staging tables.

IW38 Report Column → SAP Field Mapping
-----------------------------------------
Bas. start date    (Basic Start Date)       → AFKO.GSTRP
Order Type                                  → AUFK.AUART
Order              (Order Number)           → AUFK.AUFNR
Equipment                                   → AFIH.EQUNR
Description        (Order Short Text)       → AUFK.KTEXT
Description        (Equipment Description)  → EQKT.EQKTX
Functional Loc.    (Functional Location)    → IFLOT.TPLNR  (via ILOA.TPLNR)
Description        (Func. Loc. Description) → IFLOTX.PLTXT
MaintenancePlan                             → AFIH.WARPL
Entered by         (Created By)             → AUFK.ERNAM
Plant              (Planning Plant)         → AFIH.IWERK   (or AUFK.WERKS)
Planner group                               → AFIH.INGPR
Total act.costs    (Actual Costs)           → PMCO (sum of WRT01..WRT16 for WRTTP='04')
System status      (User / System Status)   → derived via AUFK.OBJNR → JEST + TJ02T
Changed by                                  → AUFK.AENAM
Created on                                  → AUFK.ERDAT

Source tables (extraction order):
  Pass 1 — AUFK  : Order master, filtered by AUART + ERDAT date range + WERKS.
                   Collects AUFNR keys → _AUFNR_SET
                   Collects OBJNR keys → _OBJNR_SET (for JEST/TJ02T status lookup)
  Pass 2 — AFIH  : PM order header (1:1 with AUFNR for PM orders).
                   Filtered by AUFNR IN (_AUFNR_SET).
                   Collects EQUNR keys → _EQUNR_SET
                   Collects ILOAN keys → _ILOAN_SET
  Pass 3 — AFKO  : Production/order scheduling header (basic dates).
                   Filtered by AUFNR IN (_AUFNR_SET).
  Pass 4 — EQKT  : Equipment short texts (language-dependent).
                   Filtered by EQUNR IN (_EQUNR_SET) AND SPRAS = 'E'.
  Pass 5 — ILOA  : PM location and account assignment (for functional location).
                   Filtered by ILOAN IN (_ILOAN_SET).
                   Collects TPLNR keys → _TPLNR_SET
  Pass 6 — IFLOTX: Functional location descriptions.
                   Filtered by TPLNR IN (_TPLNR_SET) AND SPRAS = 'E'.
  Pass 7 — JEST  : Individual status per object number.
                   Filtered by OBJNR IN (_OBJNR_SET) AND INACT = ''.
                   Collects STAT keys → _STAT_SET
  Pass 8 — TJ02T : System status texts.
                   Filtered by ISTAT IN (_STAT_SET) AND SPRAS = 'E'.
  Pass 9 — PMCO  : PM cost totals per OBJNR.
                   Filtered by OBJNR IN (_OBJNR_SET) AND WRTTP = '04' (actuals).

Design notes:
  - AUFK is the anchor table, filtered by date range + plant + order type.
    All downstream passes use IN-lists collected from prior passes.
  - System status is stored normalised (JEST + TJ02T) so the consumer can
    reconstruct the space-delimited status string (e.g. "TECO GMPS PRC SETC").
  - PMCO actual costs: WRTTP='04' selects actual postings; summing WRT01..WRT16
    gives the fiscal-year total per OBJNR (matches "Total act.costs" in IW38).
  - ILOA is a 1:1 extension to AFIH (keyed by ILOAN = AFIH.ILOAN).

Author  : [Your Name]
Version : 1.0.0
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

# Order creation date range applied to AUFK.ERDAT
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260228")

# Planning plant(s) — applied to AUFK.WERKS
PLANTS: List[str] = ["1202"]

# Order types to include — leave empty [] for all PM order types.
# Typical PM types: PM01=Corrective, PM02=Breakdown, ZPM3=Preventive
ORDER_TYPES: List[str] = []

# Language key for text tables (EQKT, IFLOTX, TJ02T)
LANGUAGE: str = "E"

# PMCO value type for actual costs  ('04' = actuals in SAP CO)
PMCO_WRTTP: str = "04"


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_AUFNR_SET: Set[str] = set()   # Order numbers  (AUFK  → AFIH, AFKO, PMCO)
_OBJNR_SET: Set[str] = set()   # Object numbers (AUFK  → JEST, PMCO)
_EQUNR_SET: Set[str] = set()   # Equipment nos  (AFIH  → EQKT)
_ILOAN_SET: Set[str] = set()   # ILOA numbers   (AFIH  → ILOA)
_TPLNR_SET: Set[str] = set()   # Func. locs     (ILOA  → IFLOTX)
_STAT_SET:  Set[str] = set()   # Status codes   (JEST  → TJ02T)


# -------------------------------------------------------------
# WHERE clause builders
# -------------------------------------------------------------

def _format_options(where: str) -> List[Dict[str, str]]:
    """
    Converts a simple WHERE string into RFC OPTIONS rows (<=72 chars each).
    Words are never split across rows.
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
    """
    if not values:
        return [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    first_clause = True
    i = 0

    while i < len(sorted_vals):
        prefix      = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
        max_content = 72 - len(prefix) - 1
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


def _build_aufk_options() -> List[Dict[str, str]]:
    """
    Anchor table OPTIONS: filter AUFK by ERDAT date range, WERKS, and
    optionally AUART.  AUTYP='30' limits to PM orders (category 30).
    """
    plants = ",".join(f"'{p}'" for p in PLANTS)
    where  = (
        f"ERDAT >= '{DATE_FROM}' AND ERDAT <= '{DATE_TO}'"
        f" AND WERKS IN ({plants})"
        f" AND AUTYP = '30'"          # PM order category
    )
    if ORDER_TYPES:
        types = ",".join(f"'{t}'" for t in ORDER_TYPES)
        where += f" AND AUART IN ({types})"
    return _format_options(where)


def _build_pmco_options() -> List[Dict[str, str]]:
    """PMCO filtered by collected OBJNR set + WRTTP = actuals."""
    return _build_in_options(
        "OBJNR",
        _OBJNR_SET,
        extra=f"WRTTP = '{PMCO_WRTTP}'",
    )


def _build_jest_options() -> List[Dict[str, str]]:
    """JEST: active statuses only (INACT = '' means not inactive)."""
    return _build_in_options(
        "OBJNR",
        _OBJNR_SET,
        extra="INACT = ''",
    )


# -------------------------------------------------------------
# TABLE DEFINITIONS
# Execution order is critical — later passes depend on keys
# collected by earlier passes.
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Order Master Header
    # Anchor: filtered by ERDAT + WERKS (+ AUTYP=30 for PM).
    # Produces: AUFNR keys (_AUFNR_SET) and OBJNR (_OBJNR_SET).
    # =========================================================
    {
        "table": "AUFK",
        "description": "PM Order Master Header (ERDAT + WERKS filter)",
        "columns": [
            "AUFNR",    # Order Number                → _AUFNR_SET
            "AUART",    # Order Type                  (IW38: Order Type)
            "AUTYP",    # Order Category              (30 = PM)
            "KTEXT",    # Order Short Text            (IW38: Description)
            "ERNAM",    # Created By                  (IW38: Entered by)
            "ERDAT",    # Creation Date               (IW38: Created on)
            "AENAM",    # Last Changed By             (IW38: Changed by)
            "AEDAT",    # Last Change Date
            "WERKS",    # Plant                       (IW38: Plant)
            "WAERS",    # Order Currency
            "OBJNR",    # Object Number               → _OBJNR_SET (for JEST/PMCO)
            "PRCTR",    # Profit Center
            "BUKRS",    # Company Code
            "KOSTL",    # Settlement Cost Center
        ],
        "options_fn": _build_aufk_options,
        "primary_keys": ["AUFNR"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
            "OBJNR": "_OBJNR_SET",
        },
    },

    # =========================================================
    # PASS 2 — PM Order Header Extension (maintenance-specific)
    # Filtered by AUFNR IN (_AUFNR_SET).
    # Produces: EQUNR (_EQUNR_SET) and ILOAN (_ILOAN_SET).
    # =========================================================
    {
        "table": "AFIH",
        "description": "PM Order Header — equipment, func. loc., planner group",
        "columns": [
            "AUFNR",    # Order Number               (join key)
            "EQUNR",    # Equipment Number           (IW38: Equipment) → _EQUNR_SET
            "ILOAN",    # ILOA Number                → _ILOAN_SET (for func. loc.)
            "IWERK",    # Maintenance Planning Plant (IW38: Plant)
            "INGPR",    # Planner Group              (IW38: Planner group)
            "WARPL",    # Maintenance Plan           (IW38: MaintenancePlan)
            "PRIOK",    # Priority
            "ILART",    # Maintenance Activity Type
            "QMNUM",    # Notification Number
            "ANLZU",    # System Condition
            "IPHAS",    # Maintenance Processing Phase
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR"],
        "key_out": {
            "EQUNR": "_EQUNR_SET",
            "ILOAN": "_ILOAN_SET",
        },
    },

    # =========================================================
    # PASS 3 — Order Scheduling/Basic Dates
    # Filtered by AUFNR IN (_AUFNR_SET).
    # =========================================================
    {
        "table": "AFKO",
        "description": "Order Scheduling Header — basic start/finish dates",
        "columns": [
            "AUFNR",    # Order Number               (join key)
            "GSTRP",    # Basic Start Date           (IW38: Bas. start date)
            "GLTRP",    # Basic Finish Date
            "GSTRS",    # Scheduled Start Date
            "GLTRS",    # Scheduled Finish Date
            "GSTRI",    # Actual Start Date
            "GLTRI",    # Actual Finish Date
            "FTRMI",    # Actual Release Date
        ],
        "options_fn": lambda: _build_in_options("AUFNR", _AUFNR_SET),
        "primary_keys": ["AUFNR"],
    },

    # =========================================================
    # PASS 4 — Equipment Short Texts
    # Filtered by EQUNR IN (_EQUNR_SET) and language.
    # =========================================================
    {
        "table": "EQKT",
        "description": "Equipment Descriptions (language-dependent)",
        "columns": [
            "EQUNR",    # Equipment Number           (join key)
            "SPRAS",    # Language Key
            "EQKTX",    # Equipment Description      (IW38: Description / equipment)
        ],
        "options_fn": lambda: _build_in_options(
            "EQUNR", _EQUNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["EQUNR", "SPRAS"],
    },

    # =========================================================
    # PASS 5 — Location & Account Assignment (links to func. loc.)
    # Filtered by ILOAN IN (_ILOAN_SET).
    # Produces: TPLNR (_TPLNR_SET).
    # =========================================================
    {
        "table": "ILOA",
        "description": "PM Location & Account Assignment — functional location link",
        "columns": [
            "ILOAN",    # ILOA Number                (join key)
            "TPLNR",    # Functional Location        (IW38: Functional Loc.) → _TPLNR_SET
            "SWERK",     # Plant (from ILOA)
            "KOSTL"
        ],
        "options_fn": lambda: _build_in_options("ILOAN", _ILOAN_SET),
        "primary_keys": ["ILOAN"],
        "key_out": {
            "TPLNR": "_TPLNR_SET",
        },
    },

    # =========================================================
    # PASS 6 — Functional Location Descriptions
    # Filtered by TPLNR IN (_TPLNR_SET) and language.
    # =========================================================
    {
        "table": "IFLOTX",
        "description": "Functional Location Descriptions (language-dependent)",
        "columns": [
            "TPLNR",    # Functional Location        (join key)
            "SPRAS",    # Language Key
            "PLTXT",    # Functional Location Description (IW38: Func. Loc. Description)
        ],
        "options_fn": lambda: _build_in_options(
            "TPLNR", _TPLNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["TPLNR", "SPRAS"],
    },

    # =========================================================
    # PASS 7 — Individual Status per Object Number
    # Filtered by OBJNR IN (_OBJNR_SET) AND INACT = '' (active only).
    # Produces: STAT keys (_STAT_SET) for TJ02T text lookup.
    # Note: JEST stores one row per status code per OBJNR.
    #       Consumer reconstructs the IW38 "System status" string by
    #       aggregating active STAT values per OBJNR.
    # =========================================================
    {
        "table": "JEST",
        "description": "Individual Status per Object (active statuses only)",
        "columns": [
            "OBJNR",    # Object Number              (join key)
            "STAT",     # Status Code                → _STAT_SET
            "INACT",    # Inactive Indicator         ('' = active)
        ],
        "options_fn": _build_jest_options,
        "primary_keys": ["OBJNR", "STAT"],
        "key_out": {
            "STAT": "_STAT_SET",
        },
    },

    # =========================================================
    # PASS 8 — System Status Texts
    # Filtered by ISTAT IN (_STAT_SET) and language.
    # Maps status codes (e.g. 'I0009') to text (e.g. 'TECO').
    # =========================================================
    {
        "table": "TJ02T",
        "description": "System Status Texts (e.g. TECO, REL, GMPS, PRC, SETC)",
        "columns": [
            "ISTAT",    # Internal Status Code       (join key, matches JEST.STAT)
            "SPRAS",    # Language Key
            "TXT04",    # 4-character Status Text    (IW38: System status short code)
            "TXT30",    # 30-character Status Text   (full description)
        ],
        "options_fn": lambda: _build_in_options(
            "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["ISTAT", "SPRAS"],
    },

    # =========================================================
    # PASS 9 — PM Actual Costs per Order (OBJNR)
    # Filtered by OBJNR IN (_OBJNR_SET) AND WRTTP = '04' (actuals).
    # WRT01..WRT16 are period values (months); sum = annual actual cost.
    # Note: PMCO stores costs in controlling-area currency (COCUR).
    # =========================================================
    {
        "table": "PMCO",
        "description": "PM Actual Costs per Order (WRTTP=04, sum WRT01..WRT16)",
        "columns": [
            "OBJNR",    # Object Number              (join key → AUFK.OBJNR)
            "COCUR",    # Controlling Area Currency
            "WRTTP",    # Value Type                 ('04' = actuals)
            "GJAHR",    # Fiscal Year
            "ACPOS",    # Value Category
            "VERSN",    # Planning Version
            # Period values (WRT01=Period1 ... WRT16=Period16)
            # Sum these 16 fields in SQL/downstream to get "Total act.costs"
            "WRT01", "WRT02", "WRT03", "WRT04",
            "WRT05", "WRT06", "WRT07", "WRT08",
            "WRT09", "WRT10", "WRT11", "WRT12",
            "WRT13", "WRT14", "WRT15", "WRT16",
        ],
        "options_fn": _build_pmco_options,
        "primary_keys": ["OBJNR", "GJAHR", "ACPOS", "VERSN"],
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
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA_ECC",   "sap_stg_ecc"))

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
    log_file = os.path.join(log_dir, f"sap_iw38_staging_{run_id}.log")

    log = logging.getLogger(f"sap_iw38_staging.{run_id}")
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
    def __init__(
        self,
        cfg:      Dict[str, Any],
        schema:   str,
        log:      logging.Logger,
    ) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
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
        stg_name  = f"stg_sap_iw38_{sap_table.lower()}"
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

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
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
    log.info("SAP IW38 Maintenance Order Staging Pipeline  [run: %s]", run_id)
    log.info("Plants         : %s", PLANTS)
    log.info("Date range     : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Order types    : %s", ORDER_TYPES if ORDER_TYPES else "ALL PM (AUTYP=30)")
    log.info("Language       : %s", LANGUAGE)
    log.info("PMCO val. type : %s (actuals)", PMCO_WRTTP)
    log.info("Tables         : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg        = ExtractionConfig()
        sap_config = build_sap_config()
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
    log.info("    AUFNR : %d PM orders",              len(_AUFNR_SET))
    log.info("    OBJNR : %d object numbers",         len(_OBJNR_SET))
    log.info("    EQUNR : %d equipment numbers",      len(_EQUNR_SET))
    log.info("    ILOAN : %d location assignments",   len(_ILOAN_SET))
    log.info("    TPLNR : %d functional locations",   len(_TPLNR_SET))
    log.info("    STAT  : %d active status codes",    len(_STAT_SET))
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