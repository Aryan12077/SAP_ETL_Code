"""
SAP VF05N Billing Document List — Staging Extractor
=====================================================
Extracts only specified columns from SAP ECC billing tables and loads
them into PostgreSQL staging tables, replicating the MB51 extractor
pattern for the VF05N (List of Billing Documents) transaction.

VF05N Report Column → SAP Field Mapping
-----------------------------------------
BillT        (Billing Type)              → VBRK.FKART
Sold-to pt   (Sold-to Party)            → VBRK.KUNAG
Payer                                   → VBRK.KUNRG
Bill. Date   (Billing Date)             → VBRK.FKDAT
SOrg.        (Sales Organization)       → VBRK.VKORG
DChl         (Distribution Channel)     → VBRK.VTWEG
BlCat        (Billing Category)         → VBRK.FKTYP
Bill.Doc.    (Billing Document Number)  → VBRK.VBELN
DocCa        (SD Document Category)     → VBRK.VBTYP
PsSt         (Posting Status to FI)     → VBRK.RFBSK
Created      (Created By)               → VBRK.ERNAM
Net Value                               → VBRK.NETWR
Curr.        (Document Currency)        → VBRK.WAERK
Tax amount                              → VBRK.MWSBK

Additional useful fields extracted:
  VBRK.BUKRS   — Company Code
  VBRK.ERDAT   — Creation Date
  VBRK.ERZET   — Creation Time
  VBRK.BELNR   — Accounting Document Number
  VBRK.GJAHR   — Fiscal Year
  VBRK.FKSTO   — Cancellation Flag
  VBRK.SFAKN   — Cancelled Billing Doc Number
  VBRK.BSTNK_VF — Customer PO Number
  KNA1.NAME1   — Sold-to Customer Name (joined via KUNAG)

  ── NEW FIELDS (added) ──────────────────────────────────────
  SALE QUANTITY  → VBRP.FKIMG   Actual Invoiced Quantity
  COGS           → VBRP.WAVWR   Cost in document currency
  PRODUCT CODE   → VBRP.MATNR   Material Number
  BRANCH ID      → VBRP.VKBUR   Sales Office
  WAREHOUSE ID   → VBRP.LGORT   Storage Location
  EXECUTIVE ID   → VBPA.PERNR   Personnel Number (PARVW = 'VE')
  WAREHOUSE NAME → T001L.LGOBE  Storage Location Description
  EXECUTIVE NAME → PA0001.ENAME Formatted Employee Name

Source tables:
  VBRK   — Billing Document Header  (one row per billing document)
  VBRP   — Billing Document Item    (one row per billing item)     ← NEW
  KNA1   — Customer Master General Data
  VBPA   — Partner Functions        (PARVW = 'VE' for executive)   ← NEW
  T001L  — Storage Location Master  (warehouse name lookup)        ← NEW
  PA0001 — HR Infotype 0001         (employee name lookup)         ← NEW

Extraction strategy:
  Pass 1 — VBRK  filtered by FKDAT date range + BUKRS company code.
            Collects: _VBELN_SET (billing doc numbers), _KUNAG_SET (sold-to).
  Pass 2 — VBRP  filtered by VBELN IN (_VBELN_SET).              ← NEW
            Collects: _MATNR_SET, _LGORT_SET, _WERKS_SET.
  Pass 3 — VBPA  filtered by VBELN IN (_VBELN_SET) + PARVW='VE'. ← NEW
            Collects: _PERNR_SET (personnel numbers).
  Pass 4 — KNA1  filtered by KUNNR IN (_KUNAG_SET).
  Pass 5 — T001L filtered by WERKS IN (_WERKS_SET).              ← NEW
            (T001L is keyed WERKS+LGORT; join uses both columns.)
  Pass 6 — PA0001 filtered by PERNR IN (_PERNR_SET)              ← NEW
            AND ENDDA = '99991231'  (current HR record only).

Author  : [Your Name]
Version : 1.2.0 (PostgreSQL Edition)
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

# Billing date range applied to VBRK.FKDAT
DATE_FROM = os.getenv("DATE_FROM", "20260901")
DATE_TO   = os.getenv("DATE_TO",   "20260930")

# Company codes — applied to VBRK.BUKRS
# Read from env var (comma-separated list), fallback to "2600"
COMPANY_CODES: List[str] = [
    c.strip().upper()
    for c in os.getenv("COMPANY_CODES", "1200").split(",")
    if c.strip()
]

# Billing types to include — leave empty [] to include all
# Example: ["ZF29", "ZF2A", "ZRE"] for specific invoice types
BILLING_TYPES: List[str] = []

# Posting status filter — leave empty [] to include all
# RFBSK values: 'C' = Posted to FI, 'A' = Not yet posted, 'D' = Cleared
POSTING_STATUS: List[str] = []

# ── NEW ──────────────────────────────────────────────────────
# Partner function code for the sales executive in VBPA.
# Standard SAP value: 'VE' (Verkäufer / Sales Employee).
# Change to match your system's customising if different.
EXECUTIVE_PARTNER_FUNC: str = os.getenv("EXECUTIVE_PARTNER_FUNC", "VE")
# ─────────────────────────────────────────────────────────────


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_VBELN_SET: Set[str] = set()   # Billing doc numbers  — from VBRK → drives VBRP / VBPA
_KUNAG_SET: Set[str] = set()   # Sold-to party keys   — from VBRK → drives KNA1

# ── NEW key sets ─────────────────────────────────────────────
_MATNR_SET: Set[str] = set()   # Material numbers     — from VBRP (informational / future use)
_LGORT_SET: Set[str] = set()   # Storage location IDs — from VBRP (informational)
_WERKS_SET: Set[str] = set()   # Plant codes          — from VBRP → drives T001L filter
_PERNR_SET: Set[str] = set()   # Personnel numbers    — from VBPA → drives PA0001 filter
# ─────────────────────────────────────────────────────────────


# -------------------------------------------------------------
# WHERE clause builders
# -------------------------------------------------------------

def _format_options(where: str) -> List[Dict[str, str]]:
    """
    Converts a simple WHERE string into RFC OPTIONS rows (<=72 chars each).
    Words are never split across rows.
    Use for short, static clauses only.
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
        max_content = 72 - len(prefix) - 1   # -1 for closing ")"
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


def _build_vbrk_options() -> List[Dict[str, str]]:
    """
    Builds the OPTIONS rows for VBRK.

    Filters by:
      FKDAT  — Billing date range
      BUKRS  — Company code(s)
      FKART  — Billing type(s)          [optional]
      RFBSK  — Posting status           [optional]

    All filters are simple short clauses — no IN-list scaling issue
    since VBRK is the anchor table filtered by date + company, not
    by a collected key set.
    """
    codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
    where = (
        f"FKDAT >= '{DATE_FROM}' AND FKDAT <= '{DATE_TO}'"
        f" AND BUKRS IN ({codes})"
    )
    if BILLING_TYPES:
        types = ",".join(f"'{t}'" for t in BILLING_TYPES)
        where += f" AND FKART IN ({types})"
    if POSTING_STATUS:
        statuses = ",".join(f"'{s}'" for s in POSTING_STATUS)
        where += f" AND RFBSK IN ({statuses})"
    return _format_options(where)


# ── NEW: VBPA options builder ─────────────────────────────────
def _build_vbpa_options() -> List[Dict[str, str]]:
    """
    Builds OPTIONS for VBPA filtered by:
      VBELN IN (_VBELN_SET)           — only documents from this billing run
      PARVW = EXECUTIVE_PARTNER_FUNC  — sales executive partner function
    """
    return _build_in_options(
        "VBELN",
        _VBELN_SET,
        extra=f"PARVW = '{EXECUTIVE_PARTNER_FUNC}'",
    )


# ── NEW: T001L options builder ────────────────────────────────
def _build_t001l_options() -> List[Dict[str, str]]:
    """
    Builds OPTIONS for T001L filtered by WERKS IN (_WERKS_SET).
    T001L is a small plant-configuration table; filtering by plant keeps
    the result set small even if the table has many plants system-wide.
    The final join uses WERKS + LGORT to uniquely resolve warehouse name.
    """
    return _build_in_options("WERKS", _WERKS_SET)


# ── NEW: PA0001 options builder ───────────────────────────────
def _build_pa0001_options() -> List[Dict[str, str]]:
    """
    Builds OPTIONS for PA0001 filtered by:
      PERNR IN (_PERNR_SET)   — only personnel numbers seen in VBPA
      ENDDA = '99991231'      — current (active) HR record only;
                                SAP uses 31-Dec-9999 as open-ended end date
    """
    return _build_in_options(
        "PERNR",
        _PERNR_SET,
        extra="ENDDA = '99991231'",
    )

####
def _build_mbew_options() -> List[Dict[str, str]]:
    
    return _build_in_options(
        "MATNR",
        _MATNR_SET,
        #extra="ENDDA = '99991231'",
    )    
# ─────────────────────────────────────────────────────────────


# -------------------------------------------------------------
# TABLE DEFINITIONS
# -------------------------------------------------------------
# Execution order matters — each pass collects keys used by the next.
#
#   Pass 1 : VBRK  → collects _VBELN_SET, _KUNAG_SET
#   Pass 2 : VBRP  → collects _MATNR_SET, _LGORT_SET, _WERKS_SET   ← NEW
#   Pass 3 : VBPA  → collects _PERNR_SET                            ← NEW
#   Pass 4 : KNA1  → uses _KUNAG_SET
#   Pass 5 : T001L → uses _WERKS_SET                                ← NEW
#   Pass 6 : PA0001→ uses _PERNR_SET                                ← NEW
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Billing Document Header
    # Anchor table: filtered by FKDAT date range + BUKRS.
    # VBRK has BUKRS and FKDAT directly — no IN-list scaling.
    # =========================================================
    {
        "table": "VBRK",
        "description": "Billing Document Header (FKDAT + BUKRS filter)",
        "columns": [
            # --- VF05N visible columns ---
            "VBELN",      # Billing Document Number      (Bill.Doc.)
            "FKART",      # Billing Type                 (BillT)
            "FKTYP",      # Billing Category             (BlCat)
            "VBTYP",      # SD Document Category         (DocCa)
            "FKDAT",      # Billing Date                 (Bill.Date)
            "VKORG",      # Sales Organization           (SOrg.)
            "VTWEG",      # Distribution Channel         (DChl)
            "KUNAG",      # Sold-to Party                (Sold-to pt)  -> _KUNAG_SET
            "KUNRG",      # Payer                        (Payer)
            "NETWR",      # Net Value                    (Net Value)
            "WAERK",      # Document Currency            (Curr.)
            "MWSBK",      # Tax Amount in Doc Currency   (Tax amount)
            "RFBSK",      # Posting Status to FI         (PsSt)
            "ERNAM",      # Created By                   (Created)
            # --- Additional useful fields ---
            "BUKRS",      # Company Code
            "ERDAT",      # Creation Date
            "ERZET",      # Creation Time
            "BELNR",      # Accounting Document Number
            "GJAHR",      # Fiscal Year
            "FKSTO",      # Cancellation Indicator ('X' = cancelled)
            "SFAKN",      # Cancelled Billing Document Number
            "BSTNK_VF",   # Customer Purchase Order Number
            "SPART",      # Division
            "ZTERM",      # Payment Terms
        ],
        "options_fn": _build_vbrk_options,
        "primary_keys": ["VBELN"],
        "key_out": {
            "VBELN": "_VBELN_SET",   # ← collected here, drives Passes 2 + 3
            "KUNAG": "_KUNAG_SET",
        },
    },

    # =========================================================
    # PASS 2 — Billing Document Items                     ← NEW
    # Filtered by VBELN IN (_VBELN_SET) from Pass 1.
    # Provides item-level fields: quantity, COGS, material,
    # sales office (branch), and storage location (warehouse).
    # Also collects _WERKS_SET for T001L lookup in Pass 5.
    # =========================================================
    {
        "table": "VBRP",
        "description": "Billing Document Item — qty, COGS, material, branch, warehouse",
        "columns": [
            "VBELN",   # Billing Document              (join key to VBRK)
            "POSNR",   # Billing Item Number
            "MATNR",   # Material Number               (PRODUCT CODE)
            "ARKTX",   # Item Short Text               (material description fallback)
            "FKIMG",   # Actual Invoiced Quantity       (SALE QUANTITY)
            "VRKME",   # Sales Unit of Measure
            "WAVWR",   # Cost in Document Currency     (COGS)
            "VKBUR",   # Sales Office                  (BRANCH ID)
            "LGORT",   # Storage Location              (WAREHOUSE ID)  → _LGORT_SET
            "WERKS",   # Plant                         → _WERKS_SET (T001L join key)
            "NETWR",   # Net Value of Billing Item
            "GSBER",   # Business Area
        ],
        "options_fn": lambda: _build_in_options("VBELN", _VBELN_SET),
        "primary_keys": ["VBELN", "POSNR"],
        "key_out": {
            "MATNR": "_MATNR_SET",   # collected for future material lookups
            "LGORT": "_LGORT_SET",   # collected for reference / audit
            "WERKS": "_WERKS_SET",   # drives T001L filter in Pass 5
        },
    },

    # =========================================================
    # PASS 3 — Partner Functions (Sales Executive)        ← NEW
    # Filtered by VBELN IN (_VBELN_SET) AND PARVW = 'VE'.
    #
    # PARVW = 'VE' is the standard SAP Sales Employee partner
    # function — it is the only VBPA function that carries PERNR
    # (Personnel Number).  Change EXECUTIVE_PARTNER_FUNC in the
    # config section above if your system uses a different code.
    #
    # Collects _PERNR_SET → drives PA0001 lookup in Pass 6.
    # =========================================================
    {
        "table": "VBPA",
        "description": (
            f"Partner Functions — Sales Executive only "
            f"(PARVW = '{EXECUTIVE_PARTNER_FUNC}')"
        ),
        "columns": [
            "VBELN",   # Billing Document              (join key to VBRK)
            "POSNR",   # Item Number (0 = header level)
            "PARVW",   # Partner Function
            "PERNR",   # Personnel Number              (EXECUTIVE ID)  → _PERNR_SET
        ],
        "options_fn": _build_vbpa_options,
        "primary_keys": ["VBELN", "POSNR", "PARVW"],
        "key_out": {
            "PERNR": "_PERNR_SET",   # drives PA0001 filter in Pass 6
        },
    },

    # =========================================================
    # PASS 4 — Customer Master (Sold-to Party names)
    # Filtered by KUNNR IN (KUNAG values collected from VBRK).
    # =========================================================
    {
        "table": "KNA1",
        "description": "Customer Master — sold-to parties seen in this billing set",
        "columns": [
            "KUNNR",   # Customer Number
            "NAME1",   # Customer Name 1
            "NAME2",   # Customer Name 2
            "ORT01",   # City
            "LAND1",   # Country
        ],
        "options_fn": lambda: _build_in_options("KUNNR", _KUNAG_SET),
        "primary_keys": ["KUNNR"],
    },

    # =========================================================
    # PASS 5 — Storage Location Master (Warehouse names)  ← NEW
    # Filtered by WERKS IN (_WERKS_SET) collected from VBRP.
    #
    # T001L key: WERKS (plant) + LGORT (storage location).
    # Filtering by WERKS keeps the result set small.
    # The downstream join uses BOTH WERKS and LGORT to uniquely
    # resolve LGOBE so that same LGORT codes in different plants
    # are not confused with each other.
    # =========================================================
    {
        "table": "T001L",
        "description": "Storage Location Master — warehouse names for plants in scope",
        "columns": [
            "WERKS",   # Plant                         (join key part 1)
            "LGORT",   # Storage Location              (join key part 2 = WAREHOUSE ID)
            "LGOBE",   # Storage Location Description  (WAREHOUSE NAME)
        ],
        "options_fn": _build_t001l_options,
        "primary_keys": ["WERKS", "LGORT"],
    },

    # =========================================================
    # PASS 6 — HR Infotype 0001 (Employee names)          ← NEW
    # Filtered by PERNR IN (_PERNR_SET) AND ENDDA='99991231'.
    #
    # PA0001 is a time-sliced HR infotype (one row per validity
    # period per employee).  ENDDA = '99991231' selects only the
    # current (open-ended) record so we get one row per employee.
    #
    # ENAME: pre-formatted employee full name (CHAR 40).
    # SNAME: sortable "LASTNAME FIRSTNAME" variant (CHAR 30).
    # Both are fetched so the consumer can choose which to display.
    # =========================================================
    {
        "table": "PA0001",
        "description": "HR Infotype 0001 — formatted employee names (current record only)",
        "columns": [
            "PERNR",   # Personnel Number              (join key = EXECUTIVE ID)
            "ENAME",   # Formatted Employee Name       (EXECUTIVE NAME)
            "SNAME",   # Sortable employee name        (LAST, FIRST)
            "BUKRS",   # Company Code (HR assignment)
            "WERKS",   # Personnel Area
            "BEGDA",   # Record Start Date
            "ENDDA",   # Record End Date (= '99991231' for current)
        ],
        "options_fn": _build_pa0001_options,
        "primary_keys": ["PERNR"],
    },
    {
        "table": "MBEW",
        "description": "Material Valuation",
        "columns": [
            "MATNR",
            "BWKEY",
            "STPRS",
            "VERPR",
        ],
        "options_fn": _build_mbew_options,
        "primary_keys": ["MATNR"],
    }

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
    log_file = os.path.join(log_dir, f"sap_vf05n_staging_{run_id}.log")

    log = logging.getLogger(f"sap_vf05n_staging.{run_id}")
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
# PostgreSQL Loader (replaces SQLLoader / pyodbc)
# -------------------------------------------------------------
class PostgresLoader:
    def __init__(self, schema: str, log: logging.Logger, cfg: Optional[Dict[str, Any]] = None) -> None:
        self._schema = schema
        self._log    = log
        self.conn: Optional[psycopg2.extensions.connection] = None
        self._cursor: Optional[psycopg2.extensions.cursor]  = None

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
        stg_name  = f"stg_sap_vf05n_{(name_override or sap_table).lower()}"
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

    def load_batch(
        self,
        stg_table: str,
        columns: List[str],
        rows: List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [f'"{c.replace("-", "_").replace("/", "_").lower()}"' for c in columns]
        # execute_values is the high-performance bulk method for psycopg2
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
    # ── ★ Reset key sets so this function is safe to re-enter ──
    global _VBELN_SET, _KUNAG_SET, _MATNR_SET, _LGORT_SET, _WERKS_SET, _PERNR_SET
    _VBELN_SET.clear()
    _KUNAG_SET.clear()
    _MATNR_SET.clear()
    _LGORT_SET.clear()
    _WERKS_SET.clear()
    _PERNR_SET.clear()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP VF05N Billing Document Staging Pipeline  [run: %s]", run_id)
    log.info("Company codes        : %s", COMPANY_CODES)
    log.info("Billing dates        : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Billing types        : %s", BILLING_TYPES  if BILLING_TYPES  else "ALL")
    log.info("Posting status       : %s", POSTING_STATUS if POSTING_STATUS else "ALL")
    log.info("Executive partner fn : %s", EXECUTIVE_PARTNER_FUNC)
    log.info("Tables               : %d", len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg = ExtractionConfig()
    except Exception as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    sap    = None
    loader = None
    try:
        sap    = SAPConnection(SAP_SYSTEM, log)
        loader = PostgresLoader(cfg.staging_schema, log)
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
        if sap:
            sap.close()
        if loader:
            loader.close()

    total_elapsed = time.time() - overall_start
    succeeded     = [(t, n) for t, (ok, n) in results.items() if ok]
    failed        = [t for t, (ok, _) in results.items() if not ok]

    log.info("=" * 60)
    log.info("Run complete in %.1fs", total_elapsed)
    log.info("Key sets collected:")
    log.info("    VBELN  : %d billing documents",  len(_VBELN_SET))
    log.info("    KUNAG  : %d sold-to customers",  len(_KUNAG_SET))
    log.info("    MATNR  : %d materials",          len(_MATNR_SET))
    log.info("    LGORT  : %d storage locations",  len(_LGORT_SET))
    log.info("    WERKS  : %d plants",             len(_WERKS_SET))
    log.info("    PERNR  : %d executives (HR)",    len(_PERNR_SET))
    log.info("Staging tables created:")
    log.info("    %-30s → stg_sap_vf05n_vbrk",   "VBRK (header)")
    log.info("    %-30s → stg_sap_vf05n_vbrp",   "VBRP (items)")
    log.info("    %-30s → stg_sap_vf05n_vbpa",   "VBPA (exec partner)")
    log.info("    %-30s → stg_sap_vf05n_kna1",   "KNA1 (customer)")
    log.info("    %-30s → stg_sap_vf05n_t001l",  "T001L (warehouse)")
    log.info("    %-30s → stg_sap_vf05n_pa0001", "PA0001 (exec name)")
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)