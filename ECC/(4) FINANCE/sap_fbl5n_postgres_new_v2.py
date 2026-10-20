"""
SAP FBL5N Customer Line Items — Staging Extractor
==================================================
Extracts only specified columns from SAP ECC customer accounting tables
and loads them into SQL Server staging tables, replicating the VF05N
extractor pattern for the FBL5N (Customer Line Items) transaction.

FBL5N Report Column → SAP Field Mapping
-----------------------------------------
Cleared/open items symbol               → Derived: BSID rows = 'Open', BSAD rows = 'Cleared'
Reference           (Ref. Doc. Number)  → BSID/BSAD.XBLNR
Assignment                              → BSID/BSAD.ZUONR
G/L Account                             → BSID/BSAD.HKONT
Account             (Customer Number)   → BSID/BSAD.KUNNR
Company Code                            → BSID/BSAD.BUKRS
Document Number                         → BSID/BSAD.BELNR
Document Type                           → BSID/BSAD.BLART
Posting Date                            → BSID/BSAD.BUDAT
Document Date                           → BSID/BSAD.BLDAT
Net Due Date        (Baseline Date)     → BSID/BSAD.ZFBDT  (+ ZBD3T net days)
Special G/L ind.                        → BSID/BSAD.UMSKZ
Amount in doc. curr.                    → BSID/BSAD.WRBTR
Document currency                       → BSID/BSAD.WAERS
Amount in local currency                → BSID/BSAD.DMBTR
Text                (Item Text)         → BSID/BSAD.SGTXT
Invoice reference                       → BSID/BSAD.REBZG

Additional useful fields extracted:
  BSID/BSAD.GJAHR    — Fiscal Year
  BSID/BSAD.BUZEI    — Line Item Number within Document
  BSID/BSAD.BSCHL    — Posting Key
  BSID/BSAD.SHKZG    — Debit/Credit Indicator  ('S'=Debit, 'H'=Credit)
  BSID/BSAD.GSBER    — Business Area
  BSID/BSAD.MWSKZ    — Tax Code
  BSID/BSAD.ZTERM    — Payment Terms
  BSID/BSAD.ZBD3T    — Net Payment Terms Period (days to add to ZFBDT)
  BSID/BSAD.ZLSCH    — Payment Method
  BSID/BSAD.ZLSPR    — Payment Block Key
  BSID/BSAD.AUGDT    — Clearing Date              (BSAD: populated; BSID: blank)
  BSID/BSAD.AUGBL    — Clearing Document Number   (BSAD: populated; BSID: blank)
  BSID/BSAD.REBZJ    — Fiscal Year of Invoice Referenced
  BSID/BSAD.VBELN    — Billing Document (SD link)
  BSID/BSAD.SGTXT    — Item Text
  KNA1.NAME1         — Customer Name 1 (joined via KUNNR)

Source tables:
  BSID  — Accounting: Secondary Index for Customers (OPEN items)
  BSAD  — Accounting: Secondary Index for Customers (CLEARED items)
  KNA1  — Customer Master General Data

Extraction strategy:
  Pass 1 — BSID filtered by BUDAT date range + BUKRS company code.
            KUNNR keys collected for Pass 3.
  Pass 2 — BSAD filtered by BUDAT date range + BUKRS company code.
            KUNNR keys merged into same set as Pass 1.
  Pass 3 — KNA1 filtered by KUNNR IN (values collected from passes 1+2).

==========================================================================
FIX — Currency Amount Decimal Placement  (Change #1, #2, #3, #4)
==========================================================================
SAP RFC (BBP_RFC_READ_TABLE / RFC_READ_TABLE) returns CURR and QUAN typed
fields as fixed-width character strings WITHOUT a decimal point.  The raw
value must be divided by 10^N where N is the number of decimal places
defined for that currency in table TCURX (currency decimal places).

Examples:
  USD (2 decimals): raw "45843000" → correct value 458430.00
  UGX (0 decimals): raw "45843000" → correct value 45843000  (no shift)
  JPY (0 decimals): raw "45843000" → correct value 45843000

Root cause observed in the original script:
  The raw string "45843000" was written directly to SQL as "45843000",
  which when interpreted as a number produces 45843000 — BUT when SAP
  displays "458,430" the FBL5N screen already applied the 2-decimal shift.
  Because the staging column is NVARCHAR(MAX) the raw digits are stored
  as-is, so any downstream SUM or display shows ~100x the true value for
  2-decimal currencies, or 1/100 of expected for 0-decimal currencies
  depending on which end the shift was applied.

The fix (Change #1–#4):
  1. At startup, load TCURX from SAP into a dict {currency_code: decimals}.
     Currencies not listed in TCURX default to 2 decimal places (ISO 4217
     standard default for most currencies).
  2. After each batch is fetched for BSID/BSAD, apply _apply_currency_decimals()
     which looks up WAERS per row and shifts WRBTR and DMBTR accordingly.
  3. The corrected values are stored as decimal strings (e.g. "458430.00")
     so SQL can CAST them to DECIMAL without data loss.
  4. TCURX is fetched once per run and reused for both BSID and BSAD passes.
==========================================================================

Author  : [Your Name]
Version : 1.1.0  (currency decimal fix applied)
"""
from __future__ import annotations

import os
import platform

# -------------------------------------------------------------
# PostgreSQL DLL (same as your VA05)
# -------------------------------------------------------------
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
from decimal import Decimal, InvalidOperation
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
# LOAD ENV
# -------------------------------------------------------------
load_dotenv()

# -------------------------------------------------------------
# LOGGER
# -------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_staging_{run_id}.log")

    log = logging.getLogger(f"sap_staging.{run_id}")
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
# ENV HELPERS
# -------------------------------------------------------------
def _require_env(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Missing env: {key}")
    return val


def get_pg_schema() -> str:
    return os.getenv("STAGING_SCHEMA_ECC")


# -------------------------------------------------------------
# HANA CONFIG
# -------------------------------------------------------------
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


# -------------------------------------------------------------
# POSTGRES CONFIG
# -------------------------------------------------------------
def build_pg_config() -> Dict[str, Any]:
    return {
        "host":     _require_env("PG_HOST"),
        "port":     os.getenv("PG_PORT", "5432"),
        "database": _require_env("PG_DATABASE"),
        "user":     _require_env("PG_USER"),
        "password": _require_env("PG_PASSWORD"),
    }

# -------------------------------------------------------------
# CONFIGURATION — edit this section only
# -------------------------------------------------------------

# Posting date range applied to BSID.BUDAT / BSAD.BUDAT
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260228")

# Company codes — applied to BUKRS
COMPANY_CODES: List[str] = ["1200"]

# Document types to include — leave empty [] to include all
DOCUMENT_TYPES: List[str] = []

# Extract open items  (BSID)?
EXTRACT_OPEN_ITEMS: bool = True

# Extract cleared items (BSAD)?
EXTRACT_CLEARED_ITEMS: bool = True

# ── Currency amount fields that need decimal placement applied ──────────────
# These are the CURR-typed fields in BSID/BSAD that RFC returns without a
# decimal point.  Every field listed here will be corrected per-row using
# the WAERS value on the same row.
# Change #1 ── declare which columns carry currency amounts
_CURRENCY_AMOUNT_COLS: List[str] = ["WRBTR", "DMBTR"]

# The column that carries the currency key for each row (used to look up
# decimal places from TCURX).
_CURRENCY_KEY_COL: str = "WAERS"

# ISO 4217 default: currencies not listed in TCURX have 2 decimal places.
# Change #1 (continued) ── default fallback decimal places
_DEFAULT_CURRENCY_DECIMALS: int = 2

# Module-level cache populated by _load_currency_decimals() at run start.
# Change #1 (continued) ── module-level currency decimals dict
_CURRENCY_DECIMALS: Dict[str, int] = {}

# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_KUNNR_SET: Set[str] = set()   # Customer keys collected from BSID+BSAD → used by KNA1


# =============================================================================
# Change #2 — Currency decimal loader
# =============================================================================
# Called once in main() before any table extraction begins.
# Reads TCURX (Currency Decimal Places) from SAP and stores the result in
# the module-level _CURRENCY_DECIMALS dict.
# TCURX schema: CURRKEY (currency code, 5 chars) | CURRDEC (decimals, 1 char)
# Currencies absent from TCURX use 2 decimal places (ISO default).
# =============================================================================
def _load_currency_decimals(sap: "SAPConnection", log: logging.Logger) -> None:
    """
    Populate _CURRENCY_DECIMALS from SAP table TCURX.

    TCURX only lists currencies whose decimal count differs from the ISO
    default of 2.  Typical entries:
        JPY → 0   (Japanese Yen)
        KWD → 3   (Kuwaiti Dinar)
        UGX → 0   (Ugandan Shilling)
        BHD → 3   (Bahraini Dinar)

    After this call, look up any currency with:
        decimals = _CURRENCY_DECIMALS.get(currency_code, _DEFAULT_CURRENCY_DECIMALS)
    """
    global _CURRENCY_DECIMALS
    log.info("[TCURX] Loading currency decimal places from SAP...")
    try:
        result = sap.read_table(
            "TCURX",
            ["CURRKEY", "CURRDEC"],
            options=[],      # read all rows — TCURX is a small table (~200 rows)
            rowcount=500,
        )
        loaded = 0
        for row in result.get("DATA", []):
            parts = row["WA"].split("|")
            if len(parts) < 2:
                continue
            curr = parts[0].strip()
            try:
                dec  = int(parts[1].strip())
            except ValueError:
                continue
            if curr:
                _CURRENCY_DECIMALS[curr] = dec
                loaded += 1
        log.info(
            "[TCURX] Loaded %d non-default currency decimal entries. "
            "Default (ISO) = %d decimals for all others.",
            loaded, _DEFAULT_CURRENCY_DECIMALS,
        )
        # Log a few well-known ones for sanity-check
        for sample in ["USD", "EUR", "GBP", "UGX", "JPY", "KWD"]:
            dec = _CURRENCY_DECIMALS.get(sample, _DEFAULT_CURRENCY_DECIMALS)
            log.debug("  %s → %d decimal(s)", sample, dec)
    except Exception:
        log.warning(
            "[TCURX] Could not read TCURX — will use %d decimal places for ALL "
            "currencies. Amounts may be incorrect for 0- or 3-decimal currencies "
            "(e.g. UGX, JPY, KWD).",
            _DEFAULT_CURRENCY_DECIMALS,
        )


# =============================================================================
# Change #3 — Per-batch currency decimal correction function
# =============================================================================
# Called inside extract_table() immediately after each batch is fetched from
# SAP, BEFORE the batch is passed to loader.load_batch().
#
# For each row:
#   1. Look up the currency code from the WAERS column.
#   2. Determine number of decimal places from _CURRENCY_DECIMALS
#      (defaulting to _DEFAULT_CURRENCY_DECIMALS = 2 if not in TCURX).
#   3. For each column in _CURRENCY_AMOUNT_COLS, divide the raw integer
#      string by 10^decimals and reformat as a decimal string.
#
# Example (UGX, 0 decimals):
#   raw WRBTR = "70623000"  →  corrected = "70623000"   (no shift needed)
#   raw WRBTR = "458430"    →  corrected = "458430"
#
# Example (USD, 2 decimals):
#   raw WRBTR = "70623000"  →  corrected = "706230.00"
#   raw WRBTR = "45843000"  →  corrected = "458430.00"
#
# The function operates on the list-of-lists row format used by BatchExtractor
# and returns a NEW list of lists (does not mutate in-place).
# =============================================================================
def _apply_currency_decimals(
    rows: List[List[Any]],
    columns: List[str],
    log: logging.Logger,
) -> List[List[Any]]:
    """
    Apply SAP currency decimal placement to CURR-typed amount columns.

    Parameters
    ----------
    rows    : batch as returned by BatchExtractor (list of lists, all strings)
    columns : column names matching the positional order of each row's values
    log     : logger for warnings on malformed values

    Returns
    -------
    A new list of lists with WRBTR and DMBTR replaced by correctly-scaled
    decimal strings.  All other columns are unchanged.
    """
    if not rows:
        return rows

    col_idx = {col: i for i, col in enumerate(columns)}

    # Identify which column positions need correction and the currency key col
    amount_positions: List[int] = [
        col_idx[c] for c in _CURRENCY_AMOUNT_COLS if c in col_idx
    ]
    currency_pos: Optional[int] = col_idx.get(_CURRENCY_KEY_COL)

    if not amount_positions:
        # None of the target columns present in this batch — nothing to do
        return rows

    corrected_rows: List[List[Any]] = []
    warn_count = 0

    for row in rows:
        # Determine decimal places for this row's currency
        if currency_pos is not None:
            currency = row[currency_pos].strip()
        else:
            currency = ""

        decimals = _CURRENCY_DECIMALS.get(currency, _DEFAULT_CURRENCY_DECIMALS)

        new_row = list(row)   # shallow copy — strings are immutable so this is safe

        for pos in amount_positions:
            raw: str = row[pos].strip()
            if not raw:
                # blank / null — leave as-is
                continue

            # SAP can prefix negative amounts with '-' and may include spaces.
            # Strip all spaces; the sign is preserved.
            raw_clean = raw.replace(" ", "")

            try:
                if decimals == 0:
                    # Integer currency (UGX, JPY, etc.) — no division needed.
                    # Just validate it is numeric and reformat.
                    new_row[pos] = str(int(Decimal(raw_clean)))
                else:
                    # Shift decimal point left by `decimals` places.
                    # Using Decimal avoids floating-point rounding errors.
                    divisor = Decimal(10) ** decimals
                    value   = Decimal(raw_clean) / divisor
                    # Format with exactly `decimals` places so "458430.00" is
                    # stored rather than "458430" for 2-decimal currencies.
                    new_row[pos] = f"{value:.{decimals}f}"
            except (InvalidOperation, ValueError):
                if warn_count < 10:   # cap log noise
                    log.warning(
                        "Could not apply decimal correction to value %r "
                        "(column pos %d, currency %s, decimals %d). "
                        "Raw value kept.",
                        raw, pos, currency, decimals,
                    )
                warn_count += 1
                # Leave the raw value in place so data is never silently lost
                new_row[pos] = raw_clean

        corrected_rows.append(new_row)

    if warn_count > 10:
        log.warning(
            "%d additional currency decimal warnings were suppressed.", warn_count - 10
        )

    return corrected_rows


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
    Each row TEXT <=72 characters.
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


def _build_line_item_options() -> List[Dict[str, str]]:
    """
    Builds the OPTIONS rows for BSID / BSAD.
    Filters: BUDAT date range, BUKRS company code(s), optional BLART.
    """
    codes = ",".join(f"'{c}'" for c in COMPANY_CODES)
    where = (
        f"BUDAT >= '{DATE_FROM}' AND BUDAT <= '{DATE_TO}'"
        f" AND BUKRS IN ({codes})"
    )
    if DOCUMENT_TYPES:
        types = ",".join(f"'{t}'" for t in DOCUMENT_TYPES)
        where += f" AND BLART IN ({types})"
    return _format_options(where)


# -------------------------------------------------------------
# Common column list shared by BSID and BSAD
# -------------------------------------------------------------
_LINE_ITEM_COLUMNS: List[str] = [
    # --- FBL5N visible columns ---
    "BUKRS",      # Company Code
    "KUNNR",      # Customer Number             (Account)         -> _KUNNR_SET
    "BELNR",      # Accounting Document Number  (Document Number)
    "GJAHR",      # Fiscal Year
    "BUZEI",      # Line Item Number within Document
    "BUDAT",      # Posting Date
    "BLDAT",      # Document Date
    "BLART",      # Document Type
    "XBLNR",      # Reference Document Number   (Reference)
    "ZUONR",      # Assignment Number           (Assignment)
    "HKONT",      # General Ledger Account      (G/L Account)
    "UMSKZ",      # Special G/L Indicator       (Special G/L ind.)
    "WRBTR",      # Amount in Document Currency ← CURR field — decimal fix applied
    "WAERS",      # Currency Key                (Document currency) ← used for lookup
    "DMBTR",      # Amount in Local Currency    ← CURR field — decimal fix applied
    "SGTXT",      # Item Text                   (Text)
    "REBZG",      # Invoice Reference Number    (Invoice reference)
    "ZFBDT",      # Baseline Date for Due Date Calculation
    # --- Additional useful fields ---
    "BSCHL",      # Posting Key
    "SHKZG",      # Debit/Credit Indicator  ('S'=Debit / 'H'=Credit)
    "GSBER",      # Business Area
    "MWSKZ",      # Tax Code
    "ZTERM",      # Payment Terms
    "ZBD3T",      # Net Payment Terms Period
    "ZLSCH",      # Payment Method
    "ZLSPR",      # Payment Block Key
    "AUGDT",      # Clearing Date
    "AUGBL",      # Clearing Document Number
    "REBZJ",      # Fiscal Year of Referenced Invoice
    "VBELN",      # Billing Document (SD link)
    "KOSTL",      # Cost Center
    "PRCTR",      # Profit Center
]


# -------------------------------------------------------------
# TABLE DEFINITIONS
# -------------------------------------------------------------
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Open Customer Line Items (BSID)
    # =========================================================
    {
        "table": "BSID",
        "description": "Customer Open Line Items (BUDAT + BUKRS filter)",
        "columns": _LINE_ITEM_COLUMNS,
        "options_fn": _build_line_item_options,
        "primary_keys": ["BUKRS", "KUNNR", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {"KUNNR": "_KUNNR_SET"},
        # Change #4 ── flag this table for currency decimal correction
        "apply_currency_fix": True,
        "enabled": EXTRACT_OPEN_ITEMS,
    },

    # =========================================================
    # PASS 2 — Cleared Customer Line Items (BSAD)
    # =========================================================
    {
        "table": "BSAD",
        "description": "Customer Cleared Line Items (BUDAT + BUKRS filter)",
        "columns": _LINE_ITEM_COLUMNS,
        "options_fn": _build_line_item_options,
        "primary_keys": ["BUKRS", "KUNNR", "BELNR", "GJAHR", "BUZEI"],
        "key_out": {"KUNNR": "_KUNNR_SET"},
        # Change #4 ── flag this table for currency decimal correction
        "apply_currency_fix": True,
        "enabled": EXTRACT_CLEARED_ITEMS,
    },

    # =========================================================
    # PASS 3 — Customer Master (KNA1)
    # =========================================================
    {
        "table": "KNA1",
        "description": "Customer Master — customers seen in this line item set",
        "columns": [
            "KUNNR",   # Customer Number
            "NAME1",   # Customer Name 1
            "NAME2",   # Customer Name 2
            "ORT01",   # City
            "LAND1",   # Country
            "KTOKD",   # Customer Account Group
        ],
        "options_fn": lambda: _build_in_options("KUNNR", _KUNNR_SET),
        "primary_keys": ["KUNNR"],
        # No currency amounts in KNA1
        "apply_currency_fix": False,
        "enabled": True,
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
    staging_schema: str   = field(default_factory=lambda: os.getenv("STAGING_SCHEMA"))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")





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
# POSTGRES LOADER
# -------------------------------------------------------------
class PostgresLoader:
    def __init__(self, cfg: Dict[str, Any], schema: str, log: logging.Logger) -> None:
        self._cfg    = cfg
        self._schema = schema
        self._log    = log
        self.conn    = None
        self.cur     = None

    def connect(self) -> None:
        self._log.info("Connecting to PostgreSQL...")
        self.conn = psycopg2.connect(**self._cfg)
        self.cur  = self.conn.cursor()
        self._log.info("PostgreSQL connected.")

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg       = f"{self._schema}.stg_sap_fbl5n_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_defs = ",\n".join([f'"{c}" TEXT' for c in safe_cols])

        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")
        self.cur.execute(f"""
        DROP TABLE IF EXISTS {stg};
        CREATE TABLE {stg} (
            {col_defs},
            load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """)
        self.conn.commit()
        self._log.info("Staging table %s created.", stg)
        return stg

    def load_batch(
        self,
        stg_table: str,
        columns:   List[str],
        rows:      List[List[Any]],
    ) -> None:
        if not rows:
            return
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]
        col_list  = ", ".join(safe_cols)
        query     = f"INSERT INTO {stg_table} ({col_list}) VALUES %s"
        extras.execute_values(self.cur, query, rows)
        self.conn.commit()

    def commit(self) -> None:
        if self.conn:
            self.conn.commit()

    def close(self) -> None:
        if self.cur:  self.cur.close()
        if self.conn: self.conn.close()
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

    # Change #4 ── read the flag that controls currency correction for this table
    apply_currency_fix: bool = table_def.get("apply_currency_fix", False)

    options_fn = table_def.get("options_fn")
    options    = options_fn() if callable(options_fn) else []

    log.info("=" * 60)
    log.info("Table : %s  (%s)", table, desc)
    log.info("Fields: %s", columns)
    # Change #4 ── log whether currency fix is active for this table
    log.info(
        "Currency decimal fix : %s  (columns: %s)",
        "ENABLED" if apply_currency_fix else "disabled",
        ", ".join(_CURRENCY_AMOUNT_COLS) if apply_currency_fix else "n/a",
    )
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
        # ── Change #4 ── apply currency decimal correction BEFORE key collection
        #    and BEFORE loading, so corrected values flow into both.
        if apply_currency_fix:
            batch = _apply_currency_decimals(batch, columns, log)

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

    active_tables = [t for t in STAGING_TABLE_DEFS if t.get("enabled", True)]

    log.info("=" * 60)
    log.info("SAP FBL5N Customer Line Items Staging Pipeline  [run: %s]", run_id)
    log.info("Company codes  : %s", COMPANY_CODES)
    log.info("Posting dates  : %s -> %s", DATE_FROM, DATE_TO)
    log.info("Document types : %s", DOCUMENT_TYPES if DOCUMENT_TYPES else "ALL")
    log.info("Open items     : %s", "YES" if EXTRACT_OPEN_ITEMS   else "SKIPPED")
    log.info("Cleared items  : %s", "YES" if EXTRACT_CLEARED_ITEMS else "SKIPPED")
    log.info("Tables         : %d (of %d defined)", len(active_tables), len(STAGING_TABLE_DEFS))
    log.info("=" * 60)

    try:
        cfg        = ExtractionConfig()
        schema     = get_pg_schema()
        sap_config = build_sap_config()
    except (EnvironmentError, ValueError) as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)

    try:
        sap    = SAPConnection(sap_config, log)
        loader = PostgresLoader(build_pg_config(), schema, log)
        loader.connect()
    except Exception:
        log.critical("Failed to establish connections -- aborting.")
        sys.exit(1)

    # ==========================================================================
    # Change #2 ── Load currency decimal places from TCURX BEFORE any extraction
    # This must run after the SAP connection is established and before the first
    # BSID/BSAD batch is processed.
    # ==========================================================================
    _load_currency_decimals(sap, log)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in active_tables:
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
    log.info("    KUNNR : %d unique customers", len(_KUNNR_SET))
    log.info("Currency decimals loaded : %d currencies from TCURX", len(_CURRENCY_DECIMALS))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-20s  %d rows", tbl, count)
    if failed:
        log.error("Failed (%d): %s", len(failed), failed)
    else:
        log.info("All tables extracted successfully.")
    log.info("=" * 60)
    log.info(
        "Downstream hint: UNION STG_SAP_BSID (open) + STG_SAP_BSAD (cleared) "
        "and JOIN STG_SAP_KNA1 ON KUNNR to replicate FBL5N output."
    )
    log.info("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExtraction cancelled by user.")
        sys.exit(1)