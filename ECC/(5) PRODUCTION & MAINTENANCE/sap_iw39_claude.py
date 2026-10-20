"""
SAP IW39 Display PM Orders — Staging Extractor
===============================================
Extracts columns matching the IW39 (Display Orders) transaction report from
SAP ECC Plant Maintenance tables and loads them into SQL Server staging tables.

IW39 is the display-only counterpart to IW38 (Change Orders). Both transactions
share identical column layouts and draw from the same underlying SAP tables.
The key operational difference is that IW39 supports broader order-type
selection (all PM categories) and is typically used for reporting/read-only
access, while IW38 is used for editing. The extraction logic is therefore
identical; only the configuration scope may differ.

IW39 Report Column → SAP Field Mapping
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
Plant              (Maintenance Planning Plt)→ AFIH.IWERK
Planner group                               → AFIH.INGPR
Total act.costs    (Sum WRT01..WRT16)       → PMCO (WRTTP='04')
System status      (Active status string)   → JEST + TJ02T
Changed by                                  → AUFK.AENAM
Created on                                  → AUFK.ERDAT

Source tables (extraction order):
  Pass 1 — AUFK  : Order master header, filtered by ERDAT + WERKS + AUTYP.
                   Collects AUFNR → _AUFNR_SET, OBJNR → _OBJNR_SET.
  Pass 2 — AFIH  : PM order extension (1:1 with AUFK for PM orders).
                   Filtered by AUFNR IN (_AUFNR_SET).
                   Collects EQUNR → _EQUNR_SET, ILOAN → _ILOAN_SET.
  Pass 3 — AFKO  : Order scheduling/dates.
                   Filtered by AUFNR IN (_AUFNR_SET).
  Pass 4 — EQKT  : Equipment short texts (language-dependent).
                   Filtered by EQUNR IN (_EQUNR_SET) + SPRAS.
  Pass 5 — ILOA  : PM location & account assignment (func. loc. link).
                   Filtered by ILOAN IN (_ILOAN_SET).
                   Collects TPLNR → _TPLNR_SET.
  Pass 6 — IFLOTX: Functional location descriptions.
                   Filtered by TPLNR IN (_TPLNR_SET) + SPRAS.
  Pass 7 — JEST  : Individual object status (active statuses only).
                   Filtered by OBJNR IN (_OBJNR_SET) + INACT=''.
                   Collects STAT → _STAT_SET.
  Pass 8 — TJ02T : System status texts (4-char codes, e.g. TECO, REL).
                   Filtered by ISTAT IN (_STAT_SET) + SPRAS.
  Pass 9 — PMCO  : PM actual costs per order (WRTTP='04').
                   Filtered by OBJNR IN (_OBJNR_SET).

IW39 vs IW38 configuration notes:
  - IW39 typically covers ALL PM order categories (AUTYP IN 10,20,30,40).
    Set ORDER_CATEGORIES = [] to include all, or specify as needed.
  - IW38 is usually filtered to corrective/breakdown/preventive (AUTYP=30).
  - Both share the same 9-pass extraction pipeline and staging tables.
  - Staging table names use the prefix STG_SAP_IW39_ to avoid collision
    with IW38 staging tables if both pipelines run in the same database.

Author  : [Your Name]
Version : 1.0.0
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


# -------------------------------------------------------------
# CONFIGURATION — edit this section only
# -------------------------------------------------------------

# Order creation date range applied to AUFK.ERDAT
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260630")

# Maintenance planning plant(s) — applied to AUFK.WERKS
PLANTS: List[str] = ["1202"]

# Order types to include — leave empty [] for all PM order types.
# Typical PM types: PM01=Corrective, PM02=Breakdown, ZPM3=Preventive
ORDER_TYPES: List[str] = ["PM01", "PM02", "ZPM3"]

# Order categories (AUFK.AUTYP) to include — leave empty [] for all categories.
# IW39 default covers all PM categories (AUTYP=10,20,30,40).
# Set to ["30"] to restrict to PM orders only (same behaviour as IW38).
# Common values: '10'=Internal, '20'=Investment, '30'=PM, '40'=Process.
ORDER_CATEGORIES: List[str] = []

# Language key for text tables (EQKT, IFLOTX, TJ02T)
LANGUAGE: str = "E"

# PMCO value type for actual costs ('04' = actuals in SAP CO)
PMCO_WRTTP: str = "04"

# Staging table name prefix — change to avoid collision with IW38 tables
# if both pipelines load into the same database schema simultaneously.
# Set to "IW38" if you want shared staging tables (safe when date ranges differ).
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "IW39")


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
    Subsequent rows preceded by 'OR'. Extra appended as 'AND ...' row.
    """
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
    Anchor table OPTIONS: filter AUFK by ERDAT date range and WERKS.

    IW39 behaviour:
      - ORDER_CATEGORIES = [] → no AUTYP filter (all categories, mirrors IW39
        default which shows all order types across PM categories).
      - ORDER_CATEGORIES = ['30'] → restrict to PM orders only (same as IW38).
      - ORDER_TYPES filter applied if ORDER_TYPES list is populated.
    """
    plants = ",".join(f"'{p}'" for p in PLANTS)
    where  = (
        f"ERDAT >= '{DATE_FROM}' AND ERDAT <= '{DATE_TO}'"
        f" AND WERKS IN ({plants})"
    )
    if ORDER_CATEGORIES:
        cats = ",".join(f"'{c}'" for c in ORDER_CATEGORIES)
        where += f" AND AUTYP IN ({cats})"
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
    # Anchor: filtered by ERDAT + WERKS (+ optional AUTYP/AUART).
    # IW39 differs from IW38 here: no hardcoded AUTYP='30'.
    # Produces: AUFNR keys (_AUFNR_SET) and OBJNR (_OBJNR_SET).
    # =========================================================
    {
        "table": "AUFK",
        "description": "Order Master Header — IW39 scope (all PM order categories)",
        "columns": [
            "AUFNR",    # Order Number                → _AUFNR_SET
            "AUART",    # Order Type                  (IW39: Order Type)
            "AUTYP",    # Order Category
            "KTEXT",    # Order Short Text            (IW39: Description)
            "ERNAM",    # Created By                  (IW39: Entered by)
            "ERDAT",    # Creation Date               (IW39: Created on)
            "AENAM",    # Last Changed By             (IW39: Changed by)
            "AEDAT",    # Last Change Date
            "WERKS",    # Plant
            "WAERS",    # Order Currency
            "OBJNR",    # Object Number               → _OBJNR_SET (JEST/PMCO)
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
        "description": "PM Order Header Extension — equipment, func. loc., planner group",
        "columns": [
            "AUFNR",    # Order Number               (join key)
            "EQUNR",    # Equipment Number           (IW39: Equipment) → _EQUNR_SET
            "ILOAN",    # ILOA Number                → _ILOAN_SET (for func. loc.)
            "IWERK",    # Maintenance Planning Plant (IW39: Plant)
            "INGPR",    # Planner Group              (IW39: Planner group)
            "WARPL",    # Maintenance Plan           (IW39: MaintenancePlan)
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
    # PASS 3 — Order Scheduling / Basic Dates
    # Filtered by AUFNR IN (_AUFNR_SET).
    # =========================================================
    {
        "table": "AFKO",
        "description": "Order Scheduling Header — basic start/finish dates",
        "columns": [
            "AUFNR",    # Order Number               (join key)
            "GSTRP",    # Basic Start Date           (IW39: Bas. start date)
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
    # PASS 4 — Equipment Short Texts (language-dependent)
    # Filtered by EQUNR IN (_EQUNR_SET) + SPRAS.
    # =========================================================
    {
        "table": "EQKT",
        "description": "Equipment Descriptions (language-dependent)",
        "columns": [
            "EQUNR",    # Equipment Number           (join key)
            "SPRAS",    # Language Key
            "EQKTX",    # Equipment Description      (IW39: Description / equipment)
        ],
        "options_fn": lambda: _build_in_options(
            "EQUNR", _EQUNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["EQUNR", "SPRAS"],
    },

    # =========================================================
    # PASS 5 — Location & Account Assignment
    # Links AFIH.ILOAN to functional location (TPLNR).
    # Filtered by ILOAN IN (_ILOAN_SET).
    # Produces: TPLNR (_TPLNR_SET).
    # =========================================================
    {
        "table": "ILOA",
        "description": "PM Location & Account Assignment — functional location link",
        "columns": [
            "ILOAN",    # ILOA Number                (join key)
            "TPLNR",    # Functional Location        (IW39: Functional Loc.) → _TPLNR_SET
            "SWERK",     # Plant (from ILOA)
            "KOSTL",    # Cost Center
            #"PRCTR",    # Profit Center
        ],
        "options_fn": lambda: _build_in_options("ILOAN", _ILOAN_SET),
        "primary_keys": ["ILOAN"],
        "key_out": {
            "TPLNR": "_TPLNR_SET",
        },
    },

    # =========================================================
    # PASS 6 — Functional Location Descriptions (language-dependent)
    # Filtered by TPLNR IN (_TPLNR_SET) + SPRAS.
    # =========================================================
    {
        "table": "IFLOTX",
        "description": "Functional Location Descriptions (language-dependent)",
        "columns": [
            "TPLNR",    # Functional Location        (join key)
            "SPRAS",    # Language Key
            "PLTXT",    # Functional Location Text   (IW39: Func. Loc. Description)
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
    # One row per status per OBJNR — consumer aggregates per order.
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
    # Filtered by ISTAT IN (_STAT_SET) + SPRAS.
    # Maps internal codes (e.g. 'I0009') → 4-char text (e.g. 'TECO').
    # =========================================================
    {
        "table": "TJ02T",
        "description": "System Status Texts (TECO, REL, GMPS, PRC, SETC, NMAT, ...)",
        "columns": [
            "ISTAT",    # Internal Status Code       (join key → JEST.STAT)
            "SPRAS",    # Language Key
            "TXT04",    # 4-character Status Text    (IW39: System status code)
            "TXT30",    # 30-character Status Text   (full description)
        ],
        "options_fn": lambda: _build_in_options(
            "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["ISTAT", "SPRAS"],
    },

    # =========================================================
    # PASS 9 — PM Actual Costs per Order (via OBJNR)
    # Filtered by OBJNR IN (_OBJNR_SET) AND WRTTP = '04' (actuals).
    # Sum WRT01..WRT16 across all periods/fiscal years = Total act.costs.
    # PMCO currency = controlling-area currency (COCUR).
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
            # Period values: WRT01=Period 1 … WRT16=Period 16
            # Sum all 16 in downstream SQL to get IW39 "Total act.costs"
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
    staging_schema: str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))

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
    log_file = os.path.join(log_dir, f"sap_iw39_staging_{run_id}.log")

    log = logging.getLogger(f"sap_iw39_staging.{run_id}")
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
# SAP connection wrapper
# -------------------------------------------------------------
class SAPConnection:
    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system = system
        self._log    = log
        self.conn: Optional[Connection] = None
        self._connect()

    def _connect(self) -> None:
        for attempt in range(1, 4):
            try:
                self._log.info("Connecting to SAP %s (attempt %d/3)...", self._system, attempt)
                self.conn = get_sap_connection(self._system)
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
        schema:   str,
        prefix:   str,
        log:      logging.Logger,
    ) -> None:
        self._schema = schema
        self._prefix = prefix
        self._log    = log
        self.conn:    Optional[psycopg2.extensions.connection] = None
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

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg_name  = f"stg_sap_{self._prefix.lower()}_{sap_table.lower()}"
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
    log.info("SAP IW39 Display Orders Staging Pipeline  [run: %s]", run_id)
    log.info("Plants            : %s", PLANTS)
    log.info("Date range        : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Order types       : %s", ORDER_TYPES       if ORDER_TYPES       else "ALL")
    log.info("Order categories  : %s", ORDER_CATEGORIES  if ORDER_CATEGORIES  else "ALL")
    log.info("Language          : %s", LANGUAGE)
    log.info("PMCO val. type    : %s (actuals)", PMCO_WRTTP)
    log.info("Staging prefix    : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables            : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, STAGING_PREFIX, log)
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
    log.info("    AUFNR : %d orders",               len(_AUFNR_SET))
    log.info("    OBJNR : %d object numbers",        len(_OBJNR_SET))
    log.info("    EQUNR : %d equipment numbers",     len(_EQUNR_SET))
    log.info("    ILOAN : %d location assignments",  len(_ILOAN_SET))
    log.info("    TPLNR : %d functional locations",  len(_TPLNR_SET))
    log.info("    STAT  : %d active status codes",   len(_STAT_SET))
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