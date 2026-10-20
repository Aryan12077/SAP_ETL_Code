"""
SAP IH08/IE08 Equipment Register — Staging Extractor
======================================================
Extracts columns matching the IH08/IE08 (Display Equipment) transaction report
from SAP ECC Plant Maintenance master data tables and loads them into PostgreSQL.

Equipment Report Column → SAP Field Mapping
-----------------------------------------
SAP EQUIPMENT NO     → EQUI.EQUNR
EQUIPMENT NAME       → EQKT.EQKTX
EquipCategory        → EQUI.EQTYP
LOCATION             → ILOA.STORT / ILOA.TPLNR
Plant                → EQUZ.IWERK  ← Plant is on EQUZ, NOT EQUI.WERK
Manufacturer         → EQUI.HERST
Country of Origin    → EQUI.HERLD
Model                → EQUI.TYPBZ
CAPACITY             → EQUI.GROES (Size/dimension)
Construction Year    → EQUI.BAUJJ
CONDITION            → JEST.STAT + TJ02T.TXT30 (via EQUI.OBJNR)

Source tables (extraction order):
  Pass 1 — EQUZ  : Equipment time segment (ANCHOR). Filters by IWERK + DATBI.
                   Collects EQUNR and ILOAN for all downstream tables.
  Pass 2 — EQUI  : Core equipment master. Filtered by EQUNR from Pass 1.
                   Collects OBJNR (needed for JEST status lookup).
  Pass 3 — ILOA  : Location & account assignment. Filtered by ILOAN from Pass 1.
  Pass 4 — EQKT  : Equipment short texts. Filtered by EQUNR from Pass 1.
  Pass 5 — JEST  : Individual object status. Filtered by OBJNR from Pass 2.
                   Collects STAT codes for Pass 6.
  Pass 6 — TJ02T : System status texts. Filtered by STAT from Pass 5.
  Pass 7 — S070  : PMIS Breakdown Statistics (MTTR/MTBF/MTBR).
                   Filtered by EQUNR IN (_EQUNR_SET) from Pass 1.
                   Contains pre-aggregated breakdown KPIs per equipment
                   per period: failure counts, downtime, operating time,
                   Mean Time To Repair, Mean Time Between Failures, etc.
  Pass 8 — IFLOTX: Functional location descriptions. Filtered by TPLNR from Pass 3.

WHY EQUZ IS THE ANCHOR (not EQUI)
-----------------------------------
  EQUI.WERK is the equipment's home plant, which may be blank or differ from
  the maintenance plant stored in EQUZ.IWERK.  IH08 filters on EQUZ.IWERK,
  so using EQUI.WERK as the anchor returns 0 rows for many SAP configurations.
  Filtering EQUZ by IWERK + DATBI='99991231' (open-ended validity) replicates
  the exact scope that IH08 shows.

Fallback behaviour
------------------
  WHERE syntax  : First attempts IN (...) operator.  If the anchor table
                  (EQUZ) returns 0 rows, automatically retries using
                  OR-chained = clauses.
  RFC function  : First attempts BBP_RFC_READ_TABLE; falls back to the
                  standard RFC_READ_TABLE.
  Probe order   : Four combinations are tried in sequence:
                    1. BBP_RFC_READ_TABLE  + IN  syntax
                    2. BBP_RFC_READ_TABLE  + OR  syntax
                    3. RFC_READ_TABLE      + IN  syntax
                    4. RFC_READ_TABLE      + OR  syntax
                  The winning combination is used for all remaining tables.
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

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

# Filter by Maintenance Plant (EQUZ.IWERK) — this is what IH08 uses
PLANTS: List[str] = ['1202']

# Equipment categories to include — applied as a post-filter on EQUI.EQTYP.
# Leave empty [] for all (e.g. 'M' = Machines).
# NOTE: EQUZ has no EQTYP column; category filtering happens on EQUI (Pass 2).
EQUIPMENT_CATEGORIES: List[str] = []

LANGUAGE: str = "E"
STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "IH08")

# RFC functions tried in order during the anchor-table probe.
# BBP_RFC_READ_TABLE is tried first; RFC_READ_TABLE is the standard fallback.
RFC_FUNCTION_PRIORITY: List[str] = [
    "BBP_RFC_READ_TABLE",
    "RFC_READ_TABLE",
]

# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

# Key sets populated during extraction — used as WHERE filters for child tables.
# EQUNR and ILOAN are now both collected from the EQUZ anchor table (Pass 1).
# OBJNR is collected from EQUI (Pass 2) and used for JEST (Pass 5).
# STAT  is collected from JEST (Pass 5) and used for TJ02T (Pass 6).
_EQUNR_SET: Set[str] = set()
_OBJNR_SET: Set[str] = set()
_ILOAN_SET: Set[str] = set()
_STAT_SET:  Set[str] = set()
_TPLNR_SET: Set[str] = set()

# Active WHERE-syntax mode — toggled by the anchor-table probe when IN fails.
# True  → use IN ('a','b','c')     syntax
# False → use field='a' OR field='b' syntax
_USE_IN_OPERATOR: bool = True

# ---------------------------------------------------------------------------
# WHERE clause builders
# ---------------------------------------------------------------------------

def _format_options(where: str) -> List[Dict[str, str]]:
    """Split a long WHERE string into 72-character RFC option rows."""
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
    """
    Build RFC OPTIONS rows using OR-chained = clauses.

    Example output rows:
        ( EQUNR = '000001' OR EQUNR = '000002' OR EQUNR = '000003' )
        AND DATBI = '99991231'

    This form is accepted by every RFC_READ_TABLE variant including those
    that do not support the IN (...) operator.
    """
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

    Falls back to OR-chained = clauses automatically when the module-level
    _USE_IN_OPERATOR flag is False (set by the anchor-table probe).
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
        prefix    = f"{field_name} IN (" if first_clause else f"OR {field_name} IN ("
        max_fill  = 72 - len(prefix) - 1
        row_vals: List[str] = []
        row_len   = 0

        while i < len(sorted_vals):
            quoted = f"'{sorted_vals[i]}'"
            need   = len(quoted) + (1 if row_vals else 0)
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


# ---------------------------------------------------------------------------
# Anchor-table WHERE builders  (EQUZ — filtered by IWERK + DATBI)
# ---------------------------------------------------------------------------

def _build_equz_anchor_options() -> List[Dict[str, str]]:
    """
    Build WHERE options for the EQUZ anchor table.

    We filter on:
      • IWERK  — the maintenance plant (matches IH08 plant selection)
      • DATBI  = '99991231' — open-ended time segment (current assignment only)

    IN syntax:
        IWERK IN ('1206') AND DATBI = '99991231'

    OR syntax:
        ( IWERK = '1206' )
        AND DATBI = '99991231'
    """
    if _USE_IN_OPERATOR:
        plants = ",".join(f"'{p}'" for p in PLANTS)
        where  = f"IWERK IN ({plants}) AND DATBI = '99991231'"
        return _format_options(where)

    # OR-syntax mode
    options: List[Dict[str, str]] = []
    plant_clauses = " OR ".join(f"IWERK = '{p}'" for p in PLANTS)
    options.append({"TEXT": f"( {plant_clauses} )"})
    options.append({"TEXT": "AND DATBI = '99991231'"})
    return options


def _build_equi_options() -> List[Dict[str, str]]:
    """
    Build WHERE options for EQUI (Pass 2).

    EQUI is now a child table, filtered by EQUNR values collected from EQUZ.
    Equipment category (EQTYP) filtering is applied here if EQUIPMENT_CATEGORIES
    is configured — this is the only place EQTYP is filtered.

    IN syntax:
        EQUNR IN ('0000001234', ...) AND EQTYP IN ('M', 'U')

    OR syntax:
        ( EQUNR = '0000001234' OR ... )
        AND ( EQTYP = 'M' OR EQTYP = 'U' )
    """
    if not EQUIPMENT_CATEGORIES:
        # No category filter — build purely on EQUNR keys
        return _build_in_options("EQUNR", _EQUNR_SET)

    if _USE_IN_OPERATOR:
        cats = ",".join(f"'{c}'" for c in EQUIPMENT_CATEGORIES)
        extra = f"EQTYP IN ({cats})"
        return _build_in_options("EQUNR", _EQUNR_SET, extra=extra)

    # OR-syntax: build EQUNR block first, then AND the EQTYP block
    options = _build_or_options("EQUNR", _EQUNR_SET)
    if options and options[-1]["TEXT"].startswith("AND "):
        # _build_or_options doesn't add an extra clause here; append manually
        pass
    cat_clauses = " OR ".join(f"EQTYP = '{c}'" for c in EQUIPMENT_CATEGORIES)
    options.append({"TEXT": f"AND ( {cat_clauses} )"})
    return options


# ---------------------------------------------------------------------------
# TABLE DEFINITIONS
# ---------------------------------------------------------------------------
# Extraction order matters: each pass depends on keys collected by the previous.
#
#   Pass 1 EQUZ  → collects EQUNR, ILOAN
#   Pass 2 EQUI  → uses   EQUNR;  collects OBJNR
#   Pass 3 ILOA  → uses   ILOAN
#   Pass 4 EQKT  → uses   EQUNR
#   Pass 5 JEST  → uses   OBJNR;  collects STAT
#   Pass 6 TJ02T → uses   STAT
#   Pass 7 S070  → uses   EQUNR  (PMIS breakdown statistics)
#   Pass 8 IFLOTX→ uses   TPLNR  (Functional location description)
# ---------------------------------------------------------------------------
STAGING_TABLE_DEFS: List[Dict[str, Any]] = [

    # PASS 1 — EQUZ: Equipment Time Segment  (ANCHOR)
    # Filters on IWERK (maintenance plant) + DATBI (current segment only).
    # Produces the EQUNR and ILOAN key sets used by all subsequent passes.
    {
        "table": "EQUZ",
        "description": "Equipment Time Segment — Anchor (plant filter on IWERK)",
        "columns": [
            "EQUNR",   # Equipment number  → feeds _EQUNR_SET
            "DATBI",   # Valid-to date     (always '99991231' after filter)
            "ILOAN",   # Location link     → feeds _ILOAN_SET
            "IWERK",   # Maintenance plant (the field we filter on)
        ],
        "options_fn": _build_equz_anchor_options,
        "primary_keys": ["EQUNR", "DATBI"],
        "key_out": {
            "EQUNR": "_EQUNR_SET",   # used by EQUI, EQKT
            "ILOAN": "_ILOAN_SET",   # used by ILOA
        },
        "is_anchor": True,   # ← triggers the RFC / syntax probe on zero rows
    },

    # PASS 2 — EQUI: Core Equipment Master Data
    # Filtered by EQUNR collected in Pass 1.
    # EQTYP (category) filtering is applied here if EQUIPMENT_CATEGORIES is set.
    # Collects OBJNR for the JEST status lookup in Pass 5.
    {
        "table": "EQUI",
        "description": "Equipment Master Data (filtered by EQUNR from EQUZ)",
        "columns": [
            "EQUNR",   # SAP Equipment No
            "EQTYP",   # Equip Category
            "HERST",   # Manufacturer
            "HERLD",   # Country of Origin
            "TYPBZ",   # Model Number
            "BAUJJ",   # Construction Year
            "GROES",   # Size/Dimension (Capacity)
            "BRGEW",   # Weight
            "WERK",    # Home plant (informational; NOT used as filter)
            "OBJNR",   # Object Number → feeds _OBJNR_SET for JEST
        ],
        "options_fn": _build_equi_options,
        "primary_keys": ["EQUNR"],
        "key_out": {
            "OBJNR": "_OBJNR_SET",   # used by JEST
        },
    },

    # PASS 3 — ILOA: Location and Account Assignment
    # Filtered by ILOAN collected in Pass 1 (from EQUZ).
    {
        "table": "ILOA",
        "description": "PM Location and Account Assignment",
        "columns": [
            "ILOAN",
            "SWERK",
            "STORT",
            "TPLNR",
            "KOSTL",
        ],
        "options_fn": lambda: _build_in_options("ILOAN", _ILOAN_SET),
        "primary_keys": ["ILOAN"],
        "key_out": {
            "TPLNR": "_TPLNR_SET",
        },
    },

    # PASS 4 — EQKT: Equipment Short Texts
    # Filtered by EQUNR collected in Pass 1 (from EQUZ).
    {
        "table": "EQKT",
        "description": "Equipment Descriptions",
        "columns": [
            "EQUNR",
            "SPRAS",
            "EQKTX",
        ],
        "options_fn": lambda: _build_in_options(
            "EQUNR", _EQUNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["EQUNR", "SPRAS"],
    },

    # PASS 5 — JEST: Individual Object Status (Active only)
    # Filtered by OBJNR collected in Pass 2 (from EQUI).
    # Collects STAT codes for TJ02T text lookup in Pass 6.
    {
        "table": "JEST",
        "description": "Equipment Status (Active)",
        "columns": [
            "OBJNR",
            "STAT",
            "INACT",
        ],
        "options_fn": lambda: _build_in_options(
            "OBJNR", _OBJNR_SET, extra="INACT = ''"
        ),
        "primary_keys": ["OBJNR", "STAT"],
        "key_out": {
            "STAT": "_STAT_SET",   # used by TJ02T
        },
    },

    # PASS 6 — TJ02T: System Status Texts
    # Filtered by STAT collected in Pass 5 (from JEST).
    {
        "table": "TJ02T",
        "description": "System Status Texts (e.g. AVLB, INAC)",
        "columns": [
            "ISTAT",
            "SPRAS",
            "TXT04",
            "TXT30",
        ],
        "options_fn": lambda: _build_in_options(
            "ISTAT", _STAT_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["ISTAT", "SPRAS"],
    },

    # PASS 7 — S070: PMIS Breakdown Statistics (MTTR / MTBF / MTBR)
    # Filtered by EQUNR IN (_EQUNR_SET) — reuses the equipment set
    # already collected from EQUZ in Pass 1.
    #
    # S070 is the standard SAP PMIS info structure for breakdown
    # statistics.  It stores pre-aggregated KPIs per equipment
    # per period:
    #   • ANZAU  — Number of breakdowns (failure count)
    #   • AUSFZ  — Breakdown duration (total downtime)
    #   • AUSEH  — Unit for breakdown duration
    #   • INDTZ  — Available / operating time (reference time)
    #   • INDEH  — Unit for available time
    #   • MNSBR  — MTBR (Mean Time Between Repairs)
    #   • MNSRP  — MTTR (Mean Time To Repair)
    #   • MNSBF  — MTBF (Mean Time Between Failures)
    #   • MNSEE  — Unit for MTBR/MTTR/MTBF
    #
    # NOTE: S070 columns can vary by SAP system configuration.
    # If the RFC call fails with "field not found", comment out
    # the offending column(s) and re-run.
    {
        "table": "S070",
        "description": "PMIS Breakdown Statistics — MTTR / MTBF / MTBR per equipment",
        "columns": [
            "MANDT",
            "SSOUR",
            "VRSIO",
            "SPMON",
            "SPTAG",
            "SPWOC",
            "SPBUP",
            "SKLSOB",
            "TPLNR",
            "EQUNR",
            "PERIV",
            "VWDAT",
            "SUNIT",
            "SMSAUS",
            "SAUSZT",
            "SEQTBR",
        ],
        "options_fn": lambda: _build_in_options("EQUNR", _EQUNR_SET),
        "primary_keys": ["EQUNR", "SPMON"],
    },

    # PASS 8 — IFLOTX: Functional Location Descriptions
    # Filtered by TPLNR collected in Pass 3 (from ILOA).
    {
        "table": "IFLOTX",
        "description": "Functional Location Descriptions",
        "columns": [
            "TPLNR",   # Functional Location        (join key)
            "SPRAS",   # Language Key
            "PLTXT",   # Functional Location Description
            "KZLTX",   # Long text exists
            "KZMLA",   # Primary language
            "PLTXU",   # Description in upper case
        ],
        "options_fn": lambda: _build_in_options(
            "TPLNR", _TPLNR_SET, extra=f"SPRAS = '{LANGUAGE}'"
        ),
        "primary_keys": ["TPLNR", "SPRAS"],
    },
]

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
    staging_schema:   str   = field(default_factory=lambda: get_postgres_schema(SAP_SYSTEM))

    def __post_init__(self) -> None:
        if self.batch_size > 10_000:
            raise ValueError("batch_size should not exceed 10 000 for RFC stability.")
        if self.max_buffer_width > 512:
            raise ValueError("max_buffer_width cannot exceed SAP RFC limit of 512.")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _build_logger(run_id: str) -> logging.Logger:
    log_dir  = os.getenv("LOG_DIR", "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"sap_ih08_staging_{run_id}.log")

    log = logging.getLogger(f"sap_ih08_staging.{run_id}")
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
# SAP connection wrapper — with RFC function fallback
# ---------------------------------------------------------------------------
class SAPConnection:
    """
    Wraps a pyrfc Connection and manages the active RFC read-table function.

    Fallback order is defined by RFC_FUNCTION_PRIORITY.  Call
    switch_rfc_function() to advance to the next candidate; returns False
    when the list is exhausted.
    """

    def __init__(self, system: str, log: logging.Logger) -> None:
        self._system   = system
        self._log      = log
        self._fn_index = 0            # index into RFC_FUNCTION_PRIORITY
        self.conn: Optional[Connection] = None
        self._connect()

    # ------------------------------------------------------------------
    # RFC function management
    # ------------------------------------------------------------------
    @property
    def rfc_function(self) -> str:
        return RFC_FUNCTION_PRIORITY[self._fn_index]

    def switch_rfc_function(self) -> bool:
        """
        Advance to the next RFC function in RFC_FUNCTION_PRIORITY.

        Returns True if a new function is now active, False if already at
        the last entry (i.e. no more fallbacks available).
        """
        if self._fn_index < len(RFC_FUNCTION_PRIORITY) - 1:
            self._fn_index += 1
            self._log.warning(
                "Switching RFC read function → %s", self.rfc_function
            )
            return True
        self._log.error(
            "All RFC read functions exhausted (%s). No further fallback.",
            RFC_FUNCTION_PRIORITY,
        )
        return False

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Table read
    # ------------------------------------------------------------------
    def read_table(
        self,
        table:    str,
        fields:   List[str],
        options:  Optional[List[Dict]] = None,
        rowcount: int = 5000,
        rowskips: int = 0,
    ) -> Dict[str, Any]:
        """
        Call the active RFC read-table function.

        Raises ABAPApplicationError / ABAPRuntimeError on ABAP-level problems;
        re-raises CommunicationError after attempting one reconnect so the
        BatchExtractor retry loop can handle it.
        """
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
        widths:  Dict[str, int] = {}
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
            w = self._widths.get(col, 100) + 1   # +1 for "|" delimiter
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
                self._log.info(
                    "[%s] No more rows. Total extracted: %d", self._table, total
                )
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
# PostgreSQL Loader
# ---------------------------------------------------------------------------
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

    # def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
    #     stg_name  = f"stg_sap_eqpreg_{self._prefix.lower()}_{sap_table.lower()}"
    #     safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

    #     self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

    #     col_defs = ",\n    ".join([f'"{c}" TEXT' for c in safe_cols])
    #     ddl = f"""
    #     DROP TABLE IF EXISTS {self._schema}.{stg_name};
    #     CREATE TABLE {self._schema}.{stg_name} (
    #         {col_defs},
    #         load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    #         extraction_id UUID DEFAULT gen_random_uuid()
    #     );
    #     """
    #     self._log.info("Creating staging table %s.%s...", self._schema, stg_name)
    #     self._cursor.execute(ddl)
    #     self.conn.commit()
    #     self._log.info("Staging table %s.%s created.", self._schema, stg_name)
    #     return f'"{self._schema}"."{stg_name}"'

    def create_staging_table(self, sap_table: str, columns: List[str]) -> str:
        stg_name  = f"{self._prefix.lower()}_eqpreg_{sap_table.lower()}"
        safe_cols = [c.replace("-", "_").replace("/", "_").lower() for c in columns]

        self._cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema};")

        col_defs = ",\n            ".join([f'"{c}" TEXT' for c in safe_cols])
        
        # Create the table only if it doesn't already exist
        ddl_create = f"""
        CREATE TABLE IF NOT EXISTS {self._schema}.{stg_name} (
            {col_defs},
            load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            extraction_id UUID DEFAULT gen_random_uuid()
        );
        """
        
        self._log.info("Preparing staging table %s.%s...", self._schema, stg_name)
        self._cursor.execute(ddl_create)
        
        # Truncate to wipe previous run data
        self._cursor.execute(f"TRUNCATE TABLE {self._schema}.{stg_name};")
        self.conn.commit()
        
        self._log.info("Staging table %s.%s truncated and ready.", self._schema, stg_name)
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

    t0 = time.time()

    meta    = ColumnMetadata(sap, log)
    widths  = meta.get_widths(table, columns)
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
    Try to extract the anchor table (EQUZ) using up to four combinations:

        Probe 1  BBP_RFC_READ_TABLE + IN  syntax
        Probe 2  BBP_RFC_READ_TABLE + OR  syntax
        Probe 3  RFC_READ_TABLE     + IN  syntax
        Probe 4  RFC_READ_TABLE     + OR  syntax

    Stops as soon as a non-zero row count is achieved and returns
    (True, row_count) so that the winning combination (RFC function +
    _USE_IN_OPERATOR flag) remains active for all subsequent tables.

    If all four probes return 0 rows the data is genuinely absent (wrong
    plant code), and (True, 0) is returned.
    """
    global _USE_IN_OPERATOR

    table = anchor_def["table"].upper()

    # Combinations: (use_in, rfc_fn_index)
    # For each RFC function try IN syntax first, then OR syntax.
    attempts: List[Tuple[bool, int]] = []
    for fn_idx in range(len(RFC_FUNCTION_PRIORITY)):
        attempts.append((True,  fn_idx))   # IN syntax
        attempts.append((False, fn_idx))   # OR syntax

    for probe_num, (use_in, fn_idx) in enumerate(attempts, start=1):
        _USE_IN_OPERATOR = use_in
        sap._fn_index    = fn_idx

        syntax_label = "IN (...)" if use_in else "OR-chained ="
        log.info(
            "─── Anchor probe %d/%d: RFC=%s | syntax=%s ───",
            probe_num, len(attempts), sap.rfc_function, syntax_label,
        )

        # Clear key sets so a re-run of EQUZ starts fresh
        _EQUNR_SET.clear()
        _ILOAN_SET.clear()
        _TPLNR_SET.clear()

        try:
            ok, count = extract_table(anchor_def, sap, loader, cfg, log)
        except Exception:
            log.warning("Probe %d raised an exception — trying next combination.", probe_num)
            continue

        if count > 0:
            log.info(
                "✓ Anchor probe succeeded on attempt %d — "
                "RFC=%s | syntax=%s | rows=%d",
                probe_num, sap.rfc_function, syntax_label, count,
            )
            return ok, count

        log.warning(
            "Probe %d returned 0 rows (RFC=%s | syntax=%s).",
            probe_num, sap.rfc_function, syntax_label,
        )

    # All probes exhausted
    log.error(
        "All %d anchor probes returned 0 rows for %s. "
        "Verify maintenance plant code(s) %s in SAP (EQUZ.IWERK).",
        len(attempts), table, PLANTS,
    )
    return True, 0

# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def main() -> None:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    log.info("=" * 60)
    log.info("SAP IH08 Equipment Register Staging Pipeline  [run: %s]", run_id)
    log.info("Anchor table      : EQUZ (filter on EQUZ.IWERK)")
    log.info("Plants (IWERK)    : %s", PLANTS)
    log.info("Equip Categories  : %s", EQUIPMENT_CATEGORIES if EQUIPMENT_CATEGORIES else "ALL")
    log.info("Language          : %s", LANGUAGE)
    log.info("Staging prefix    : STG_SAP_%s_<TABLE>", STAGING_PREFIX)
    log.info("Tables            : %d", len(STAGING_TABLE_DEFS))
    log.info("RFC priority      : %s", RFC_FUNCTION_PRIORITY)
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
        log.critical("Failed to establish connections — aborting.")
        sys.exit(1)

    results: Dict[str, Tuple[bool, int]] = {}
    overall_start = time.time()

    try:
        for tbl_def in STAGING_TABLE_DEFS:
            tbl_name = tbl_def["table"].upper()
            try:
                if tbl_def.get("is_anchor"):
                    # --- Anchor table: run the RFC-function / syntax probe ---
                    ok, count = _probe_anchor_table(tbl_def, sap, loader, cfg, log)
                    if count == 0:
                        log.warning(
                            "Anchor table EQUZ returned 0 rows after all probes. "
                            "Verify EQUZ.IWERK = %s exists in SAP. "
                            "Child tables will be skipped (no keys to filter on).",
                            PLANTS,
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
    log.info("Active RFC function : %s", RFC_FUNCTION_PRIORITY[sap._fn_index] if sap else "N/A")
    log.info("Active WHERE syntax : %s", "IN (...)" if _USE_IN_OPERATOR else "OR-chained =")
    log.info("Key sets collected:")
    log.info("    EQUNR : %d equipment numbers    (from EQUZ → used by EQUI, EQKT)", len(_EQUNR_SET))
    log.info("    ILOAN : %d location assignments (from EQUZ → used by ILOA)",       len(_ILOAN_SET))
    log.info("    TPLNR : %d functional locations (from ILOA → used by IFLOTX)",     len(_TPLNR_SET))
    log.info("    OBJNR : %d object numbers       (from EQUI → used by JEST)",        len(_OBJNR_SET))
    log.info("    STAT  : %d active status codes  (from JEST → used by TJ02T)",       len(_STAT_SET))
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