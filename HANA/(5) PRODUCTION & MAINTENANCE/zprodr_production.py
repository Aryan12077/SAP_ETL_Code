"""
SAP ZPRODR (Production Report) — Staging Extractor
===================================================
Extracts the tables / columns used by the custom ABAP report ZPRODR from SAP
and loads them into PostgreSQL staging tables (one staging table per SAP table).

The ABAP report builds one ALV row per PLANT + MATERIAL from goods receipts
(movement type 101) posted against production orders. The original ABAP does
this with one big multi-table JOIN plus a few per-row SELECTs. RFC table reads
cannot join, so the same data is staged table-by-table, driven by key sets.

Selection screen → config mapping
-----------------------------------
S_WERKS  (MATDOC-WERKS)   → PLANTS
S_MATNR  (MATDOC-MATNR)   → MATERIALS      ([] = all)
S_BUDAT  (MATDOC-BUDAT)   → DATE_FROM / DATE_TO   (posting date, mandatory)
S_DAUAT  (AFPO-DAUAT)     → ORDER_TYPES    ([] = all)  production order type
S_FEVOR  (MARC-FEVOR)     → DIVISIONS      ([] = all)  production scheduler
Hardcoded in ABAP         → MOVEMENT_TYPES ('101'), CANCELLED = '',
                            VKORG '1100', VTWEG '01', SPRAS 'E'

ALV output column → SAP source
--------------------------------
WERKS                     → MATDOC.WERKS
MATNR                     → MATDOC.MATNR
MAKTX (Description)       → MAKT.MAKTX                (SPRAS = 'E')
MEINS (UOM)               → MARA.MEINS
FEVOR (Division)          → MARC.FEVOR
TXT   (Division Desc.)    → T024F.TXT                 (FEVOR + WERKS)
QUALITY                   → <ZCUSTOM_TAB.TMG_NAME>.ZQLTY_SORTD  where ZQLTY_SORT  matches
SHADE                     → <TMG_NAME>.ZDESIGN_NOD || <TMG_NAME>.ZSHADE_NOD
                            where ZDESIGN_NO / ZSHADE_NO match ZCUSTOM_TAB values
MVGR1..5                  → MVKE.MVGR1..5             (VKORG='1100', VTWEG='01')
BEZEI1..5                 → TVM1T..TVM5T.BEZEI        (SPRAS = 'E')
FROM_TO_PROD              → SUM(MATDOC.MENGE / 10) per WERKS + MATNR
TO_PROD                   → SUM(MATDOC.MENGE / 10) per WERKS + MATNR
                            only for rows where MATDOC.BUDAT = DATE_TO

Source tables (extraction order):
  Pass  1 — MATDOC       : Anchor. Filtered by BUDAT/WERKS/MATNR/BWART/CANCELLED.
                           Collects AUFNR, MATNR, WERKS.
  Pass  2 — AFPO         : Order item. AUFNR IN set (+ DAUAT filter).
  Pass  3 — MARC         : Plant data. MATNR IN set + WERKS IN set (+ FEVOR filter).
                           Collects FEVOR.
  Pass  4 — T024F        : Production scheduler texts. FEVOR IN set + WERKS IN set.
  Pass  5 — MAKT         : Material descriptions. MATNR IN set + SPRAS.
  Pass  6 — ZCUSTOM_TAB  : Custom table (quality / shade / design codes + TMG table
                           name). MATNR IN set. Collects TMG_NAME.
  Pass  7 — MVKE         : Sales org data. MATNR IN set + VKORG + VTWEG.
                           Collects MVGR1..MVGR5.
  Pass  8-12 — TVM1T..TVM5T : Material group texts. MVGRn IN set + SPRAS.
  Pass 13 — MARA         : Base UOM. MATNR IN set.
  Pass 14 — <TMG_NAME>   : DYNAMIC. The ABAP reads ZCUSTOM_TAB-TMG_NAME at run time
                           (SELECT ... FROM (W_MATDOC-TMG_NAME)). One staging table is
                           created per distinct TMG_NAME collected in pass 6. Only the
                           columns that really exist in each table (checked via DD03L)
                           are extracted, mirroring the ABAP TRY/CATCH behaviour.

Notes
-----
  - MATDOC exists only in S/4HANA (in ECC the equivalent is MKPF/MSEG). Make sure
    SAP_SYSTEM below points to the system that actually has MATDOC.
  - ABAP uses SELECT SINGLE for lookups, so downstream SQL should take one row per key
    (e.g. DISTINCT ON / MIN) for ZCUSTOM_TAB, MARC, TMG_NAME lookups.
  - The ABAP LEFT JOINs + WHERE on AFPO / MARC columns behave as inner joins, so
    apply the AFPO.DAUAT and MARC.FEVOR filters again as inner joins downstream.

Author  : [Your Name]
Version : 1.1.0
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

# Posting date range applied to MATDOC.BUDAT (S_BUDAT)
DATE_FROM = os.getenv("DATE_FROM", "20260101")
DATE_TO   = os.getenv("DATE_TO",   "20260630")   # also the "To Date Prod." cut-off date

# Plant(s) — applied to MATDOC.WERKS (S_WERKS). [] = all plants
PLANTS: List[str] = ["1102"]

# Material number(s) — applied to MATDOC.MATNR (S_MATNR). [] = all materials.
# Values must be stored exactly as in SAP (e.g. 18-char, zero padded if numeric).
MATERIALS: List[str] = []

# Production order type(s) — applied to AFPO.DAUAT (S_DAUAT). [] = all
ORDER_TYPES: List[str] = []

# Production scheduler / division — applied to MARC.FEVOR (S_FEVOR). [] = all
DIVISIONS: List[str] = []

# Movement types — hardcoded in ABAP: A~BWART IN ( '101' )
MOVEMENT_TYPES: List[str] = ["101"]

# Sales area for MVKE (hardcoded in ABAP)
VKORG: str = "1100"
VTWEG: str = "01"

# Language key for text tables (MAKT, T024F, TVM1T..TVM5T)
LANGUAGE: str = "E"

# Staging table name prefix → stg_sap_<prefix>_<table>
STAGING_PREFIX: str = "ZPRODR"


# -------------------------------------------------------------
# Module-level key sets — populated at runtime during extraction
# -------------------------------------------------------------
_AUFNR_SET:  Set[str] = set()   # Order numbers     (MATDOC → AFPO)
_MATNR_SET:  Set[str] = set()   # Material numbers  (MATDOC → MARC, MAKT, ZCUSTOM_TAB, MVKE, MARA)
_WERKS_SET:  Set[str] = set()   # Plants            (MATDOC → MARC, T024F)
_FEVOR_SET:  Set[str] = set()   # Prod. schedulers  (MARC   → T024F)
_TMG_SET:    Set[str] = set()   # Dynamic tables    (ZCUSTOM_TAB.TMG_NAME → dynamic reads)
_MVGR1_SET:  Set[str] = set()   # Material grp 1    (MVKE → TVM1T)
_MVGR2_SET:  Set[str] = set()   # Material grp 2    (MVKE → TVM2T)
_MVGR3_SET:  Set[str] = set()   # Material grp 3    (MVKE → TVM3T)
_MVGR4_SET:  Set[str] = set()   # Material grp 4    (MVKE → TVM4T)
_MVGR5_SET:  Set[str] = set()   # Material grp 5    (MVKE → TVM5T)


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


def _build_matdoc_options() -> List[Dict[str, str]]:
    """
    Anchor table OPTIONS — mirrors the ABAP WHERE clause on MATDOC:
        A~MATNR IN S_MATNR, A~WERKS IN S_WERKS, A~BUDAT IN S_BUDAT,
        A~BWART IN ('101'), A~CANCELLED = ''
    plus AUFNR <> '' (the ABAP INNER JOIN to AFPO on AUFNR drops rows with no order).
    """
    where_parts = [f"BUDAT >= '{DATE_FROM}' AND BUDAT <= '{DATE_TO}'"]
    if PLANTS:
        where_parts.append(f"WERKS IN ({_quoted_list(PLANTS)})")
    if MOVEMENT_TYPES:
        where_parts.append(f"BWART IN ({_quoted_list(MOVEMENT_TYPES)})")
    where_parts.append("CANCELLED = ''")
    where_parts.append("AUFNR <> ''")
    where = _join_and(where_parts)

    if MATERIALS:
        return _build_in_options("MATNR", set(MATERIALS), extra=where)
    return _format_options(where)


def _build_afpo_options() -> List[Dict[str, str]]:
    """AFPO filtered by collected AUFNR set (+ DAUAT = S_DAUAT when provided)."""
    extra = f"DAUAT IN ({_quoted_list(ORDER_TYPES)})" if ORDER_TYPES else ""
    return _build_in_options("AUFNR", _AUFNR_SET, extra=extra)


def _build_marc_options() -> List[Dict[str, str]]:
    """MARC filtered by MATNR set + plants collected from MATDOC (+ FEVOR = S_FEVOR)."""
    parts: List[str] = []
    if _WERKS_SET:
        parts.append(f"WERKS IN ({_quoted_list(_WERKS_SET)})")
    if DIVISIONS:
        parts.append(f"FEVOR IN ({_quoted_list(DIVISIONS)})")
    return _build_in_options("MATNR", _MATNR_SET, extra=_join_and(parts))


def _build_t024f_options() -> List[Dict[str, str]]:
    """T024F filtered by FEVOR set (from MARC) + plants collected from MATDOC."""
    extra = f"WERKS IN ({_quoted_list(_WERKS_SET)})" if _WERKS_SET else ""
    return _build_in_options("FEVOR", _FEVOR_SET, extra=extra)


def _build_mvke_options() -> List[Dict[str, str]]:
    """MVKE filtered by MATNR set + hardcoded sales area (VKORG / VTWEG)."""
    return _build_in_options(
        "MATNR",
        _MATNR_SET,
        extra=f"VKORG = '{VKORG}' AND VTWEG = '{VTWEG}'",
    )


def _build_no_filter_options() -> List[Dict[str, str]]:
    """No WHERE clause — used for small custom master/config tables read dynamically."""
    return []


def _tvm_def(n: int) -> Dict[str, Any]:
    """Table definition for TVM<n>T (material group <n> texts)."""
    set_name = f"_MVGR{n}_SET"
    return {
        "table": f"TVM{n}T",
        "description": f"Material Group {n} Texts (language-dependent)",
        "columns": [
            "SPRAS",         # Language Key
            f"MVGR{n}",      # Material Group {n}          (join key ← MVKE.MVGR{n})
            "BEZEI",         # Description                 (ALV: BEZEI{n})
        ],
        "options_fn": lambda s=set_name, k=n: _build_in_options(
            f"MVGR{k}", globals()[s], extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["SPRAS", f"MVGR{n}"],
    }


# -------------------------------------------------------------
# TABLE DEFINITIONS
# Execution order is critical — later passes depend on keys
# collected by earlier passes.
# -------------------------------------------------------------

STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # =========================================================
    # PASS 1 — Material Document (anchor)
    # Filtered by BUDAT + WERKS + MATNR + BWART + CANCELLED.
    # Produces: AUFNR (_AUFNR_SET), MATNR (_MATNR_SET), WERKS (_WERKS_SET).
    # =========================================================
    {
        "table": "MATDOC",
        "description": "Material Documents — goods receipts (BWART 101) against production orders",
        "columns": [
            "MBLNR",        # Material Document Number    (PK)
            "MJAHR",        # Material Document Year      (PK)
            "ZEILE",        # Material Document Item      (PK)
            "WERKS",        # Plant                       → _WERKS_SET
            "MATNR",        # Material Number             → _MATNR_SET
            "AUFNR",        # Order Number                → _AUFNR_SET
            "BUDAT",        # Posting Date                (From/To Prod. calc)
            "MENGE",        # Quantity                    (divided by 10 in ABAP)
            "BWART",        # Movement Type               (filter = '101')
            "CANCELLED",    # Cancelled Indicator         (filter = '')
        ],
        "options_fn": _build_matdoc_options,
        "primary_keys": ["MBLNR", "MJAHR", "ZEILE"],
        "key_out": {
            "AUFNR": "_AUFNR_SET",
            "MATNR": "_MATNR_SET",
            "WERKS": "_WERKS_SET",
        },
    },

    # =========================================================
    # PASS 2 — Production Order Item
    # Filtered by AUFNR IN (_AUFNR_SET) [+ DAUAT IN S_DAUAT].
    # ABAP: INNER JOIN AFPO AS B ON B~AUFNR = A~AUFNR, B~DAUAT IN S_DAUAT.
    # =========================================================
    {
        "table": "AFPO",
        "description": "Order Item — production order type (DAUAT)",
        "columns": [
            "AUFNR",        # Order Number                (join key)
            "POSNR",        # Order Item Number           (PK)
            "DAUAT",        # Production Order Type       (filter S_DAUAT)
        ],
        "options_fn": _build_afpo_options,
        "primary_keys": ["AUFNR", "POSNR"],
    },

    # =========================================================
    # PASS 3 — Plant Data for Material
    # Filtered by MATNR IN (_MATNR_SET) + WERKS IN (_WERKS_SET) [+ FEVOR IN S_FEVOR].
    # Produces: FEVOR (_FEVOR_SET).
    # =========================================================
    {
        "table": "MARC",
        "description": "Plant Data for Material — production scheduler (FEVOR)",
        "columns": [
            "MATNR",        # Material Number             (join key)
            "WERKS",        # Plant                       (join key)
            "FEVOR",        # Production Scheduler        (ALV: Division) → _FEVOR_SET
        ],
        "options_fn": _build_marc_options,
        "primary_keys": ["MATNR", "WERKS"],
        "key_out": {
            "FEVOR": "_FEVOR_SET",
        },
    },

    # =========================================================
    # PASS 4 — Production Scheduler Texts
    # Filtered by FEVOR IN (_FEVOR_SET) + WERKS IN (_WERKS_SET).
    # =========================================================
    {
        "table": "T024F",
        "description": "Production Scheduler Texts (Division Desc.)",
        "columns": [
            "WERKS",        # Plant                       (join key)
            "FEVOR",        # Production Scheduler        (join key)
            "TXT",          # Scheduler Description       (ALV: Division Desc.)
        ],
        "options_fn": _build_t024f_options,
        "primary_keys": ["WERKS", "FEVOR"],
    },

    # =========================================================
    # PASS 5 — Material Descriptions (language-dependent)
    # Filtered by MATNR IN (_MATNR_SET) + SPRAS.
    # =========================================================
    {
        "table": "MAKT",
        "description": "Material Descriptions (language-dependent)",
        "columns": [
            "MATNR",        # Material Number             (join key)
            "SPRAS",        # Language Key
            "MAKTX",        # Material Description        (ALV: Description)
        ],
        "options_fn": lambda: _build_in_options(
            "MATNR", _MATNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["MATNR", "SPRAS"],
    },

    # =========================================================
    # PASS 6 — Custom table: quality / shade / design per material
    # Filtered by MATNR IN (_MATNR_SET).
    # Produces: TMG_NAME (_TMG_SET) — names of tables read dynamically in pass 14.
    # =========================================================
    {
        "table": "ZCUSTOM_TAB",
        "description": "Custom Table — quality sort, shade no., design no., text table name",
        "columns": [
            "MATNR",        # Material Number             (join key)
            "ZQLTY_SORT",   # Quality Sort code           (lookup → ZQLTY_SORTD)
            "ZSHADE_NO",    # Shade number                (lookup → ZSHADE_NOD)
            "ZDESIGN_NO",   # Design number               (lookup → ZDESIGN_NOD)
            "TMG_NAME",     # Name of table to read dynamically → _TMG_SET
        ],
        "options_fn": lambda: _build_in_options("MATNR", _MATNR_SET),
        "primary_keys": ["MATNR"],      # ABAP joins on MATNR only — adjust if table key differs
        "key_out": {
            "TMG_NAME": "_TMG_SET",
        },
    },

    # =========================================================
    # PASS 7 — Sales Data for Material (sales org 1100 / dist. channel 01)
    # Filtered by MATNR IN (_MATNR_SET) + VKORG + VTWEG.
    # Produces: MVGR1..MVGR5 sets for the TVMnT text lookups.
    # =========================================================
    {
        "table": "MVKE",
        "description": "Sales Data for Material — material groups 1-5",
        "columns": [
            "MATNR",        # Material Number             (join key)
            "VKORG",        # Sales Organization
            "VTWEG",        # Distribution Channel
            "MVGR1",        # Material Group 1            → _MVGR1_SET
            "MVGR2",        # Material Group 2            → _MVGR2_SET
            "MVGR3",        # Material Group 3            → _MVGR3_SET
            "MVGR4",        # Material Group 4            → _MVGR4_SET
            "MVGR5",        # Material Group 5            → _MVGR5_SET
        ],
        "options_fn": _build_mvke_options,
        "primary_keys": ["MATNR", "VKORG", "VTWEG"],
        "key_out": {
            "MVGR1": "_MVGR1_SET",
            "MVGR2": "_MVGR2_SET",
            "MVGR3": "_MVGR3_SET",
            "MVGR4": "_MVGR4_SET",
            "MVGR5": "_MVGR5_SET",
        },
    },

    # =========================================================
    # PASS 8-12 — Material Group Texts (language-dependent)
    # Filtered by MVGRn IN (_MVGRn_SET) + SPRAS.
    # =========================================================
    _tvm_def(1),
    _tvm_def(2),
    _tvm_def(3),
    _tvm_def(4),
    _tvm_def(5),

    # =========================================================
    # PASS 13 — Material Master General Data (base UOM)
    # Filtered by MATNR IN (_MATNR_SET).
    # =========================================================
    {
        "table": "MARA",
        "description": "Material Master — base unit of measure",
        "columns": [
            "MATNR",        # Material Number             (join key)
            "MEINS",        # Base Unit of Measure        (ALV: UOM)
        ],
        "options_fn": lambda: _build_in_options("MATNR", _MATNR_SET),
        "primary_keys": ["MATNR"],
    },

    # =========================================================
    # PASS 14 — DYNAMIC custom text tables (ABAP: SELECT ... FROM (W_MATDOC-TMG_NAME))
    # One concrete table per distinct ZCUSTOM_TAB.TMG_NAME collected in pass 6.
    # 'validate_columns' → only columns that exist in DD03L for that table are read
    # (the ABAP wraps each SELECT in TRY/CATCH for the same reason).
    # No WHERE clause: these are small master tables; the lookups
    # (ZQLTY_SORT / ZDESIGN_NO / ZSHADE_NO) are resolved downstream.
    # =========================================================
    {
        "table": "<TMG_NAME>",                 # replaced at runtime for each TMG_NAME
        "dynamic_table_set": "_TMG_SET",
        "validate_columns": True,
        "description": "Dynamic custom text table — quality / design / shade descriptions",
        "columns": [
            "ZQLTY_SORT",   # Quality Sort code           (lookup key)
            "ZQLTY_SORTD",  # Quality Sort description    (ALV: Quality)
            "ZDESIGN_NO",   # Design number               (lookup key)
            "ZDESIGN_NOD",  # Design description          (ALV: Shade, part 1)
            "ZSHADE_NO",    # Shade number                (lookup key)
            "ZSHADE_NOD",   # Shade description           (ALV: Shade, part 2)
        ],
        "options_fn": _build_no_filter_options,
        "primary_keys": ["ZQLTY_SORT", "ZDESIGN_NO", "ZSHADE_NO"],
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
    log_file = os.path.join(log_dir, f"sap_zprodr_staging_{run_id}.log")

    log = logging.getLogger(f"sap_zprodr_staging.{run_id}")
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
    log.info("SAP ZPRODR Production Report Staging Pipeline  [run: %s]", run_id)
    log.info("Plants            : %s", PLANTS           if PLANTS           else "ALL")
    log.info("Posting date range: %s -> %s", DATE_FROM, DATE_TO)
    log.info("Materials         : %s", MATERIALS        if MATERIALS        else "ALL")
    log.info("Order types       : %s", ORDER_TYPES      if ORDER_TYPES      else "ALL")
    log.info("Divisions (FEVOR) : %s", DIVISIONS        if DIVISIONS        else "ALL")
    log.info("Movement types    : %s", MOVEMENT_TYPES   if MOVEMENT_TYPES   else "ALL")
    log.info("Sales area        : VKORG %s / VTWEG %s", VKORG, VTWEG)
    log.info("Language          : %s", LANGUAGE)
    log.info("Staging prefix    : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Table definitions : %d (+ dynamic TMG_NAME tables)", len(STAGING_TABLE_DEFS))
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
    log.info("    AUFNR    : %d orders",                  len(_AUFNR_SET))
    log.info("    MATNR    : %d materials",               len(_MATNR_SET))
    log.info("    WERKS    : %d plants",                  len(_WERKS_SET))
    log.info("    FEVOR    : %d production schedulers",   len(_FEVOR_SET))
    log.info("    TMG_NAME : %d dynamic custom tables",   len(_TMG_SET))
    log.info("    MVGR1    : %d values", len(_MVGR1_SET))
    log.info("    MVGR2    : %d values", len(_MVGR2_SET))
    log.info("    MVGR3    : %d values", len(_MVGR3_SET))
    log.info("    MVGR4    : %d values", len(_MVGR4_SET))
    log.info("    MVGR5    : %d values", len(_MVGR5_SET))
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