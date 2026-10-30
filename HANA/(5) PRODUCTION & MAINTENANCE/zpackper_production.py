"""
SAP ZPACKPER (Packing Percentage / Weaving-to-Packing Report) — Staging Extractor
==================================================================================
Extracts the tables / columns used by the custom ABAP report ZPACKPER from SAP
and loads them into PostgreSQL staging tables (one staging table per SAP table).

The ABAP report shows one ALV row per JOB CARD (= batch, MATDOC.CHARG) of a PACKING
order (order types PK01 / PK02) and compares the metres woven, finished and packed
(1 / 2 / 3 PCS, short length, second, rags, rework), with several percentages.
The original ABAP uses one multi-table JOIN, three FOR ALL ENTRIES selects and a per-row
SELECT SUM( ) on ZPACK. RFC table reads cannot join, so the same data is staged
table-by-table, driven by key sets. The same SAP table is read several times
(MATDOC x4, AFPO x2, ZPACK x2) and each read goes to its own staging table
(see "stg_alias" below).

Selection screen → config mapping
-----------------------------------
S_WERKS (MATDOC-WERKS)   → PLANTS          ([] = all)
S_MATNR (MATDOC-MATNR)   → MATERIALS       ([] = all)  NOTE: in the ABAP SELECT list MATNR
                                            is AFPO.MATNR (MATDOC.MATNR is commented out)
S_AUFNR (MATDOC-AUFNR)   → ORDERS          ([] = all)
S_BUDAT (MATDOC-BUDAT)   → DATE_FROM / DATE_TO   (applied to ZPACK.BUDAT, as in the ABAP;
                                            "" = no date filter)
S_DAUAT (AFPO-DAUAT)     → ORDER_TYPES     ([] = all; combined with PK01 / PK02)
S_CHARG (MATDOC-CHARG)   → BATCHES         ([] = all)  — applied to ZPACK.CHARG
S_SUM   (checkbox)       → no extraction effect (it only de-duplicates / sorts the ALV rows)
Hardcoded in ABAP        → packing order types PK01 / PK02 ; consumption BWART '261' ;
                           weaving order types PH01 / PH02 ; finish GR BWART '101' ;
                           rework BWART '311' + LGORT 'FINS' + UMLGO 'SFDC' + SHKZG 'H' ;
                           CANCELLED = '' ; AUFNR <> '' ; MAKT SPRAS 'E'

ALV output column → SAP source
--------------------------------
WERKS / AUFNR / MATNR / BUDAT / JOBCARDNO  → packing MATDOC (261) + AFPO + ZPACK.BUDAT
MAKTX                                      → MAKT (SPRAS 'E') of the 261 MATDOC.MATNR
WEAVING_MTR  → SUM(MATDOC.MENGE) BWART 261, same CHARG, order type PH01/PH02  (weaving)
FINISH_MTR   → SUM(MATDOC.MENGE) BWART 101, same CHARG                        (finish)
REWORK       → SUM(MATDOC.MENGE) BWART 311, LGORT FINS → UMLGO SFDC, SHKZG H,
               same CHARG + WERKS + MATNR
PCS_1/2/3, SHORT_LENGTH, SECOND, RAGE → SUM over ALL ZPACK rows with CHARG = job card
               (no date filter!)  RAGE = SUM(ZPACK.RAGE) * 3.6
Derived: SHRINK_% = (WEAVING-FINISH)/WEAVING*100 ; TOTAL = PCS1+PCS2+PCS3+SHORT+SECOND+RAGE+REWORK ;
         FRESH_FABRIC = PCS1+PCS2+PCS3 ; plus the "%ge wrt Wvng Mtr / Finish Mtr" columns.

Source tables (extraction order):
  Pass 1 — ZPACK       : Anchor. BUDAT / CHARG / AUFNR filters. Collects AUFNR.
  Pass 2 — AFPO        : Packing order items. AUFNR IN set + DAUAT (PK01/PK02 ∩ S_DAUAT)
                         + MATNR filter. Collects AUFNR (valid packing orders) + MATNR.
  Pass 3 — MATDOC      : alias MATDOC_PACK. BWART 261 consumption on the packing orders.
                         Collects CHARG (job cards), WERKS, MATNR.
  Pass 4 — MAKT        : Descriptions of the collected MATNR set.
  Pass 5 — MATDOC      : alias MATDOC_REWORK. 311 FINS → SFDC, SHKZG 'H', CHARG IN set.
  Pass 6 — MATDOC      : alias MATDOC_WEAVING. 261 on CHARG IN set. Collects AUFNR.
  Pass 7 — MATDOC      : alias MATDOC_FINISH. 101 on CHARG IN set. Collects AUFNR.
  Pass 8 — AFPO        : alias AFPO_JOBCARD. Order types of the weaving / finish orders
                         (DAUAT PH01 / PH02 filter is applied downstream).
  Pass 9 — ZPACK       : alias ZPACK_ALL. ALL ZPACK rows for the job cards (CHARG IN set),
                         i.e. the data behind the per-row SELECT SUM( ) of the ABAP.

Downstream SQL notes
--------------------
  - ZPACK (Pass 1) decides WHICH job cards appear (date / batch filter, like the ABAP join
    D~BUDAT IN S_BUDAT). The packed quantities must be summed from ZPACK_ALL.
  - WEAVING_MTR = SUM over MATDOC_WEAVING joined to AFPO_JOBCARD (DAUAT IN PH01, PH02).
    FINISH_MTR  = SUM over MATDOC_FINISH  joined to AFPO_JOBCARD (any DAUAT; inner join).
  - REWORK must be matched on CHARG + WERKS + MATNR (MATNR = AFPO.MATNR of the packing order):
    that last part is not applied in the RFC read — do it in SQL.
  - The ABAP LEFT JOINs with WHERE on right-hand columns (D~BUDAT, D~CHARG, B~DAUAT)
    behave as INNER JOINs.
  - MATDOC exists only in S/4HANA. Make sure SAP_SYSTEM points to such a system.
  - The ABAP removes duplicate rows by (JOBCARDNO, AUFNR) after the loop.

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
from typing import Any, Dict, Generator, Iterable, List, Optional, Set, Tuple  # noqa: E402

import psycopg2  # noqa: E402
from psycopg2 import extras  # noqa: E402
from pyrfc import (  # noqa: E402
    ABAPApplicationError,
    ABAPRuntimeError,
    CommunicationError,
    Connection,
    LogonError,
)

SAP_SYSTEM: str = "HANA"   # NOTE: MATDOC requires an S/4HANA system — change if needed


# -------------------------------------------------------------
# CONFIGURATION — edit this section only
# -------------------------------------------------------------

# Posting date range applied to ZPACK.BUDAT (S_BUDAT). "" = no date filter
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260630")

# Plant(s) — MATDOC.WERKS (S_WERKS). [] = all
PLANTS: List[str] = ["1102"]

# Material number(s) — AFPO.MATNR (S_MATNR). [] = all
MATERIALS: List[str] = []

# Packing order number(s) — S_AUFNR. [] = all
ORDERS: List[str] = []

# Order type(s) — AFPO.DAUAT (S_DAUAT). [] = all (still limited to PACK_ORDER_TYPES)
ORDER_TYPES: List[str] = []

# Job card / batch number(s) — ZPACK.CHARG (S_CHARG). [] = all
BATCHES: List[str] = []

# Hardcoded in ABAP
PACK_ORDER_TYPES: List[str] = ["PK01", "PK02"]   # packing orders
PACK_MOVEMENT:    str = "261"                    # consumption on packing order
WEAVING_MOVEMENT: str = "261"                    # consumption on weaving order (PH01 / PH02)
FINISH_MOVEMENT:  str = "101"                    # goods receipt of finished fabric
REWORK_MOVEMENT:  str = "311"                    # transfer posting FINS -> SFDC
REWORK_FROM_LGORT: str = "FINS"
REWORK_TO_LGORT:   str = "SFDC"
REWORK_SHKZG:      str = "H"

# Language key for MAKT
LANGUAGE: str = "E"

# Staging table name prefix → stg_sap_<prefix>_<table or alias>
STAGING_PREFIX: str = "ZPACKPER"


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_AUFNR_SET:      Set[str] = set()   # Orders in ZPACK              (ZPACK → AFPO)
_PACK_AUFNR_SET: Set[str] = set()   # Valid packing orders         (AFPO → MATDOC 261)
_CHARG_SET:      Set[str] = set()   # Job cards / batches          (MATDOC 261 → rework/weaving/finish/ZPACK_ALL)
_MATNR_SET:      Set[str] = set()   # Materials                    (AFPO + MATDOC → MAKT)
_WERKS_SET:      Set[str] = set()   # Plants                       (MATDOC 261 → rework)
_JOB_AUFNR_SET:  Set[str] = set()   # Weaving + finish orders      (MATDOC → AFPO_JOBCARD)


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


def _quoted_list(values: Iterable[str]) -> str:
    """['A','B'] -> "'A','B'" (sorted, for use inside an IN ( ... ) clause)."""
    return ",".join(f"'{v}'" for v in sorted(values))


def _build_in_options(
    field_name: str,
    values: Set[str],
    extra: str = "",
) -> List[Dict[str, str]]:
    """
    Builds RFC OPTIONS rows for a large IN filter.
    Each row TEXT <=72 characters. Values packed greedily per row.
    Subsequent rows preceded by 'OR'. Extra appended as 'AND ...' rows.

    When 'extra' is supplied, the OR-chain is wrapped in parentheses so that the
    extra condition applies to ALL value rows (AND binds tighter than OR otherwise).
    """
    if not values:
        rows: List[Dict[str, str]] = [{"TEXT": f"{field_name} = '__NO_MATCH__'"}]
        if extra:
            rows.extend(_format_options(f"AND {extra}"))
        return rows

    sorted_vals = sorted(values)
    options: List[Dict[str, str]] = []
    first_clause = True
    wrap = bool(extra)
    i = 0

    while i < len(sorted_vals):
        if first_clause:
            prefix = ("( " if wrap else "") + f"{field_name} IN ("
        else:
            prefix = f"OR {field_name} IN ("
        max_content = 72 - len(prefix) - 1 - (2 if wrap else 0)
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

    if wrap:
        options[-1]["TEXT"] += " )"
        options.extend(_format_options(f"AND {extra}"))

    return options


def _join_and(parts: List[str]) -> str:
    return " AND ".join(p for p in parts if p)


def _in(field_name: str, values: Iterable[str]) -> str:
    """'WERKS', ['A','B'] -> "WERKS IN ('A','B')" ; empty values -> '' (no filter)."""
    values = list(values)
    return f"{field_name} IN ({_quoted_list(values)})" if values else ""


def _date_filter(field_name: str) -> str:
    if DATE_FROM and DATE_TO:
        return f"{field_name} >= '{DATE_FROM}' AND {field_name} <= '{DATE_TO}'"
    if DATE_FROM:
        return f"{field_name} >= '{DATE_FROM}'"
    return ""


def _build_zpack_anchor_options() -> List[Dict[str, str]]:
    """ABAP: D~BUDAT IN S_BUDAT AND D~CHARG IN S_CHARG AND A~AUFNR IN S_AUFNR (via join)."""
    return _format_options(_join_and([
        _date_filter("BUDAT"), _in("CHARG", BATCHES), _in("AUFNR", ORDERS),
    ]))


def _build_afpo_pack_options() -> List[Dict[str, str]]:
    """
    ABAP: INNER JOIN AFPO ON AUFNR AND DAUAT IN ('PK01','PK02'), plus
          B~DAUAT IN S_DAUAT and B~MATNR IN S_MATNR.
    """
    types = [t for t in ORDER_TYPES if t in PACK_ORDER_TYPES] if ORDER_TYPES else PACK_ORDER_TYPES
    if not types:                       # S_DAUAT excludes every packing type → nothing matches
        types = ["__NONE__"]
    extra = _join_and([_in("DAUAT", types), _in("MATNR", MATERIALS)])
    return _build_in_options("AUFNR", _AUFNR_SET, extra=extra)


def _build_matdoc_pack_options() -> List[Dict[str, str]]:
    """ABAP: A~WERKS IN S_WERKS AND A~BWART IN ('261') AND A~AUFNR <> '' AND A~CANCELLED = ''."""
    extra = _join_and([
        _in("WERKS", PLANTS),
        f"BWART = '{PACK_MOVEMENT}'",
        "CANCELLED = ''",
    ])
    return _build_in_options("AUFNR", _PACK_AUFNR_SET, extra=extra)


def _build_matdoc_rework_options() -> List[Dict[str, str]]:
    """
    ABAP FAE: CHARG / MATNR / WERKS = packing rows ; LGORT 'FINS', UMLGO 'SFDC',
              SHKZG 'H', BWART '311', CANCELLED ''.   (WERKS / MATNR matched downstream)
    """
    extra = _join_and([
        _in("WERKS", _WERKS_SET),
        f"LGORT = '{REWORK_FROM_LGORT}'",
        f"UMLGO = '{REWORK_TO_LGORT}'",
        f"SHKZG = '{REWORK_SHKZG}'",
        f"BWART = '{REWORK_MOVEMENT}'",
        "CANCELLED = ''",
    ])
    return _build_in_options("CHARG", _CHARG_SET, extra=extra)


def _build_matdoc_weaving_options() -> List[Dict[str, str]]:
    """ABAP: A~CHARG = job card, A~BWART = '261', A~CANCELLED = '' (+ DAUAT PH01/PH02 downstream)."""
    return _build_in_options(
        "CHARG", _CHARG_SET,
        extra=f"BWART = '{WEAVING_MOVEMENT}' AND CANCELLED = '' AND AUFNR <> ''",
    )


def _build_matdoc_finish_options() -> List[Dict[str, str]]:
    """ABAP: A~CHARG = job card, A~BWART = '101', A~CANCELLED = ''."""
    return _build_in_options(
        "CHARG", _CHARG_SET,
        extra=f"BWART = '{FINISH_MOVEMENT}' AND CANCELLED = '' AND AUFNR <> ''",
    )


_MATDOC_ROW_COLS: List[str] = [
    "MBLNR", "MJAHR", "ZEILE",
    "KEY1", "KEY2", "KEY3", "KEY4", "KEY5", "KEY6",
    "WERKS", "MATNR", "AUFNR", "CHARG", "BUDAT", "MENGE", "BWART", "CANCELLED",
]


# -------------------------------------------------------------
# TABLE DEFINITIONS
# Execution order is critical — later passes depend on keys
# collected by earlier passes.
# 'stg_alias' = staging table name when the same SAP table is read more than once.
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # PASS 1 — ZPACK anchor (date / batch / order filters) → _AUFNR_SET
    {
        "table": "ZPACK",
        "description": "Packing data — job cards in the selected period (anchor)",
        "columns": [
            "AUFNR", "CHARG", "BUDAT",
            "PCS_1", "PCS_2", "PCS_3", "SHORT_LENGTH", "ZSECOND", "RAGE",
        ],
        "options_fn": _build_zpack_anchor_options,
        "primary_keys": [],    # not visible in ABAP — add the real key fields if you have them
        "key_out": {"AUFNR": "_AUFNR_SET"},
    },

    # PASS 2 — Packing order items (PK01 / PK02) → valid orders + MATNR
    {
        "table": "AFPO",
        "stg_alias": "AFPO_PACK",
        "description": "Order item — packing orders (DAUAT PK01 / PK02)",
        "columns": ["AUFNR", "POSNR", "MATNR", "DAUAT"],
        "options_fn": _build_afpo_pack_options,
        "primary_keys": ["AUFNR", "POSNR"],
        "key_out": {"AUFNR": "_PACK_AUFNR_SET", "MATNR": "_MATNR_SET"},
    },

    # PASS 3 — Packing order consumption (261) → job cards
    {
        "table": "MATDOC",
        "stg_alias": "MATDOC_PACK",
        "description": "Material documents — 261 consumption on packing orders",
        "columns": _MATDOC_ROW_COLS,
        "options_fn": _build_matdoc_pack_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {"CHARG": "_CHARG_SET", "MATNR": "_MATNR_SET", "WERKS": "_WERKS_SET"},
    },

    # PASS 4 — Material descriptions
    {
        "table": "MAKT",
        "description": "Material descriptions (language-dependent)",
        "columns": ["MATNR", "SPRAS", "MAKTX"],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR", "SPRAS"],
    },

    # PASS 5 — Rework: transfer 311 FINS → SFDC
    {
        "table": "MATDOC",
        "stg_alias": "MATDOC_REWORK",
        "description": "Material documents — 311 rework transfers FINS -> SFDC",
        "columns": _MATDOC_ROW_COLS + ["LGORT", "UMLGO", "SHKZG"],
        "options_fn": _build_matdoc_rework_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
    },

    # PASS 6 — Weaving metres: 261 on weaving orders (PH01 / PH02 resolved via AFPO_JOBCARD)
    {
        "table": "MATDOC",
        "stg_alias": "MATDOC_WEAVING",
        "description": "Material documents — 261 consumption per job card (weaving metres)",
        "columns": _MATDOC_ROW_COLS,
        "options_fn": _build_matdoc_weaving_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {"AUFNR": "_JOB_AUFNR_SET"},
    },

    # PASS 7 — Finish metres: GR 101 per job card
    {
        "table": "MATDOC",
        "stg_alias": "MATDOC_FINISH",
        "description": "Material documents — 101 goods receipt per job card (finish metres)",
        "columns": _MATDOC_ROW_COLS,
        "options_fn": _build_matdoc_finish_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {"AUFNR": "_JOB_AUFNR_SET"},
    },

    # PASS 8 — Order types of the weaving / finish orders (DAUAT PH01 / PH02 filter downstream)
    {
        "table": "AFPO",
        "stg_alias": "AFPO_JOBCARD",
        "description": "Order item — order types of weaving / finish orders",
        "columns": ["AUFNR", "POSNR", "MATNR", "DAUAT"],
        "options_fn": lambda: _build_in_options("AUFNR", _JOB_AUFNR_SET),
        "primary_keys": ["AUFNR", "POSNR"],
    },

    # PASS 9 — ALL ZPACK rows of the job cards (no date filter) = ABAP per-row SELECT SUM( )
    {
        "table": "ZPACK",
        "stg_alias": "ZPACK_ALL",
        "description": "Packing data — every row of the job cards (for SUM of PCS / rags / etc.)",
        "columns": [
            "AUFNR", "CHARG", "BUDAT",
            "PCS_1", "PCS_2", "PCS_3", "SHORT_LENGTH", "ZSECOND", "RAGE",
        ],
        "options_fn": lambda: _build_in_options("CHARG", _CHARG_SET),
        "primary_keys": [],
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
    log_file = os.path.join(log_dir, f"sap_zpackper_staging_{run_id}.log")

    log = logging.getLogger(f"sap_zpackper_staging.{run_id}")
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

    def existing_columns(self, table: str, columns: List[str]) -> List[str]:
        """
        Returns only those columns that exist in the active DDIC definition of `table`
        (order preserved). Used for dynamically named custom tables where the ABAP
        relies on TRY/CATCH because not every column is guaranteed to exist.
        If the DD03L lookup itself fails, all columns are returned unchanged.
        """
        try:
            result = self._sap.read_table(
                "DD03L",
                ["FIELDNAME"],
                [{"TEXT": f"TABNAME = '{table.upper()}' AND AS4LOCAL = 'A'"}],
                rowcount=2000,
            )
        except Exception:
            self._log.warning(
                "[%s] DD03L column-existence check failed -- using all columns.", table
            )
            return list(columns)

        present = {row["WA"].split("|")[0].strip() for row in result.get("DATA", [])}
        found   = [c for c in columns if c in present]
        missing = [c for c in columns if c not in present]
        if missing:
            self._log.warning("[%s] Columns not found in DD03L, skipped: %s", table, missing)
        return found

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
# Dynamic table expansion (ZCUSTOM_TAB.TMG_NAME → concrete tables)
# -------------------------------------------------------------
def _expand_table_defs(
    defs: List[Dict[str, Any]],
    log: logging.Logger,
) -> Generator[Dict[str, Any], None, None]:
    """
    Yields the table definitions in order. A definition carrying 'dynamic_table_set'
    is expanded into one concrete definition per table name collected in that set.
    This is a lazy generator, so the set is already populated by earlier passes
    when the dynamic definition is reached.
    """
    current_module = sys.modules[__name__]
    for tbl_def in defs:
        enabled = tbl_def.get("enabled")
        if callable(enabled) and not enabled():
            log.info("Skipping %s -- disabled for the current report mode.",
                     tbl_def.get("stg_alias", tbl_def["table"]))
            continue
        set_name = tbl_def.get("dynamic_table_set")
        if not set_name:
            yield tbl_def
            continue

        names = sorted(getattr(current_module, set_name))
        if not names:
            log.warning("No dynamic table names collected in %s -- nothing to extract.", set_name)
        for name in names:
            concrete = dict(tbl_def)
            concrete["table"]       = name
            concrete["description"] = f"{tbl_def.get('description', name)} [{name}]"
            yield concrete


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
        safe_tbl  = sap_table.lower().replace("-", "_").replace("/", "_")
        stg_name  = f"stg_sap_{self._prefix.lower()}_{safe_tbl}"
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

    meta = ColumnMetadata(sap, log)

    # Dynamically named custom tables: keep only the columns that really exist
    if table_def.get("validate_columns"):
        columns = meta.existing_columns(table, columns)
        if not columns:
            log.warning("[%s] None of the expected columns exist -- table skipped.", table)
            return True, 0
        log.info("Fields (validated): %s", columns)

    widths = meta.get_widths(table, columns)

    chunker = ColumnChunker(columns, widths, cfg.max_buffer_width, log)
    chunks  = chunker.chunks()

    stg_table = loader.create_staging_table(table_def.get("stg_alias", table), columns)

    extractor         = BatchExtractor(sap, table, columns, chunks, options, cfg, log)
    rows_loaded       = 0
    rows_since_commit = 0

    for batch in extractor.batches():
        if key_out:
            _collect_keys(batch, columns, key_out, log)
        hook = table_def.get("on_batch")
        if hook:
            hook(batch, columns)

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
    log.info("SAP ZPACKPER Packing Percentage Staging Pipeline  [run: %s]", run_id)
    log.info("Plants            : %s", PLANTS    if PLANTS    else "ALL")
    log.info("Posting date range: %s -> %s", DATE_FROM or "-", DATE_TO or "-")
    log.info("Materials         : %s", MATERIALS if MATERIALS else "ALL")
    log.info("Orders            : %s", ORDERS    if ORDERS    else "ALL")
    log.info("Order types       : %s (limited to %s)", ORDER_TYPES if ORDER_TYPES else "ALL", PACK_ORDER_TYPES)
    log.info("Batches           : %s", BATCHES   if BATCHES   else "ALL")
    log.info("Language          : %s", LANGUAGE)
    log.info("Staging prefix    : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Table definitions : %d", len(STAGING_TABLE_DEFS))
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
        for tbl_def in _expand_table_defs(STAGING_TABLE_DEFS, log):
            tbl_name = tbl_def.get("stg_alias", tbl_def["table"]).upper()
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
    log.info("    AUFNR (ZPACK)   : %d orders",      len(_AUFNR_SET))
    log.info("    AUFNR (packing) : %d orders",      len(_PACK_AUFNR_SET))
    log.info("    CHARG           : %d job cards",   len(_CHARG_SET))
    log.info("    MATNR           : %d materials",   len(_MATNR_SET))
    log.info("    WERKS           : %d plants",      len(_WERKS_SET))
    log.info("    AUFNR (wvg/fin) : %d orders",      len(_JOB_AUFNR_SET))
    log.info("Succeeded (%d):", len(succeeded))
    for tbl, count in succeeded:
        log.info("    %-24s  %d rows", tbl, count)
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