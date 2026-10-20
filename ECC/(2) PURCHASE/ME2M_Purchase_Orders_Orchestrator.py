"""
SAP ME2M Extraction Orchestrator
=================================
Drives ME2M_Purchase_Orders.py in 15-day (fortnightly) batches, one company
code at a time.

Loop structure
──────────────
  Outer loop : company code   (1200 -> 1400 -> 2600 -> ... -> PFL)
  Inner loop : 15-day period  (LOOP_START -> LOOP_END)
  Total      : len(COMPANY_CODES) x len(periods) batches

Each calendar month is split into two periods:
  Period 1 : 1st  -> 15th
  Period 2 : 16th -> last day of month

The 15-day window is applied by the extractor to EKKO.BEDAT (PO date).
The extractor's EKET pre-scan window (EINDT_FROM / EINDT_TO) is pinned to the
same 15-day window for every batch.

After each (company code + 15-day period) batch:
  -> PROCEDURE_CALL is executed (staging -> target load).

!! REQUIRED ONE-TIME PATCH TO ME2M_Purchase_Orders.py !!
────────────────────────────────────────────────────────
In the extractor, COMPANY_CODES and PLANTS are hard-coded lists, so this
orchestrator cannot steer them. Replace those two lines with:

    COMPANY_CODES: List[str] = [
        c.strip() for c in os.getenv("COMPANY_CODES", "1200").split(",") if c.strip()
    ]
    PLANTS: List[str] = [
        p.strip() for p in os.getenv("PLANTS", "1202").split(",") if p.strip()
    ]

Standalone runs keep their old defaults; the orchestrator always sets both
(PLANTS="" means "all plants"). Start-up aborts if the patch is missing.

Open-schedule caveat
────────────────────
With INCLUDE_OPEN_SCHEDULES on, an old PO whose schedule lines fall in
several different fortnights is re-extracted (with ALL its lines) in each of
those batches. The procedure must therefore be idempotent / upsert on
(EBELN, EBELP, ETENR), or set INCLUDE_OPEN_SCHEDULES = False below.

Usage
─────
  python orchestrator_me2m.py                      # normal run (resumes automatically)
  python orchestrator_me2m.py --dry-run            # print plan, no SAP/DB calls
  python orchestrator_me2m.py --only 1200,1400     # run a subset (company codes) -- handy for testing
  python orchestrator_me2m.py --retry-failed       # re-queue FAILED batches, keep COMPLETED
  python orchestrator_me2m.py --reset              # wipe progress and start fresh

Resume behaviour
────────────────
  Progress is persisted to orchestrator_progress_me2m.json.
  Each batch key is  "{COMPANY_CODE}_{DATE_FROM}_{DATE_TO}".
  On restart, COMPLETED batches are skipped automatically.
  FAILED batches are retried up to ORCH_MAX_RETRIES times (per run).

Required env vars (.env or shell)
──────────────────────────────────
  SAP_USER, SAP_PASSWORD, SAP_ASHOST, SAP_SYSNR, SAP_CLIENT
  PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD

Optional env vars
─────────────────
  ORCH_MAX_RETRIES, ORCH_RETRY_DELAY_S, ORCH_EXTRACTOR_TIMEOUT_S, LOG_DIR

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
from connections import get_postgres_connection  # noqa: E402

import argparse  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import re  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
from calendar import monthrange  # noqa: E402
from datetime import date, datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Dict, List, Optional, Tuple  # noqa: E402

import psycopg2  # noqa: E402

# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Inclusive date range (applied to EKKO.BEDAT)
LOOP_START: date = date(2025, 1, 1)
LOOP_END:   date = date(2026, 9, 30)

# Company codes -- processed one at a time, each running all periods
COMPANY_CODES: List[str] = [
    "1200"
    # "1200", "1400", "2600", "2700",
    # "DAPL", "HECL", "HPDL", "PFL",
]
UNITS: List[str] = COMPANY_CODES

# Optional plant restriction per company code (EKPO.WERKS).
# Company not listed (or empty list) -> ALL plants.   e.g. {"1200": ["1202"]}
PLANTS_BY_COMPANY: Dict[str, List[str]] = {}

# Run the extractor's EKET pre-scan for old POs with open schedule lines?
# (see "Open-schedule caveat" in the docstring)
INCLUDE_OPEN_SCHEDULES: bool = True

# PostgreSQL stored procedure called after every (company code + period) batch
# NOTE: placeholder name -- change to your real procedure.
PROCEDURE_CALL: str = "CALL sap_stg_ecc.prc_load_sap_me2m_purchase_orders();"

# Paths -- extractor must sit in the same folder as this script
EXTRACTOR_SCRIPT: Path = Path(__file__).parent / "ME2M_Purchase_Orders.py"
PROGRESS_FILE:    Path = Path(__file__).parent / "orchestrator_progress_me2m.json"
LOG_DIR:          Path = Path(os.getenv("LOG_DIR", "logs"))
LOG_PREFIX:       str  = "orchestrator_me2m"

# Retry / timing
ORCH_MAX_RETRIES:   int   = int(os.getenv("ORCH_MAX_RETRIES",   "2"))
ORCH_RETRY_DELAY_S: float = float(os.getenv("ORCH_RETRY_DELAY_S", "60"))

# Kill a hung extractor after N seconds (0 = no timeout)
ORCH_EXTRACTOR_TIMEOUT_S: int = int(os.getenv("ORCH_EXTRACTOR_TIMEOUT_S", "0"))

# Env vars the extractor MUST read via os.getenv(...) for this orchestrator
# to work. Checked at start-up so a mis-wired extractor fails loudly instead
# of silently re-extracting the same hard-coded scope for every batch.
REQUIRED_EXTRACTOR_ENV_VARS: List[str] = ["DATE_FROM", "DATE_TO", "COMPANY_CODES", "PLANTS"]

UNIT_LABEL: str = "Company code"


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def _build_logger(run_id: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{LOG_PREFIX}_{run_id}.log"

    log = logging.getLogger(f"{LOG_PREFIX}.{run_id}")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fmt_file    = logging.Formatter("%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s")
    fmt_console = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt_file)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt_console)

    log.addHandler(fh)
    log.addHandler(ch)
    log.info("Orchestrator log: %s", log_file)
    return log


# ─────────────────────────────────────────────────────────────
# Progress file helpers
# ─────────────────────────────────────────────────────────────
# Batch key format:  "{COMPANY_CODE}_{DATE_FROM}_{DATE_TO}"
# e.g.              "1200_20250101_20250115"

def _batch_key(unit: str, date_from: str, date_to: str) -> str:
    return f"{unit}_{date_from}_{date_to}"


def _load_progress() -> Dict:
    if PROGRESS_FILE.exists():
        try:
            with PROGRESS_FILE.open(encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass  # Corrupt file -> start fresh
    return {"batches": {}}


def _save_progress(progress: Dict) -> None:
    """Atomic write via temp file + rename -- safe against mid-write crashes."""
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(progress, fh, indent=2, default=str)
    tmp.replace(PROGRESS_FILE)


# ─────────────────────────────────────────────────────────────
# Fortnightly batch generator  (15-day periods)
# ─────────────────────────────────────────────────────────────
# Each calendar month is split into two periods:
#   Period 1 : 1st  -> 15th
#   Period 2 : 16th -> last day of month
#
# Example for January 2025:
#   20250101 -> 20250115
#   20250116 -> 20250131
#
# Both ends are clipped to [start, end], so a LOOP_START / LOOP_END that
# falls mid-period is handled correctly.

def generate_fortnightly_batches(start: date, end: date) -> List[Tuple[str, str]]:
    """Returns (DATE_FROM, DATE_TO) string pairs in YYYYMMDD format."""
    batches: List[Tuple[str, str]] = []
    cursor = date(start.year, start.month, 1)

    while cursor <= end:
        y, m = cursor.year, cursor.month
        last_day = monthrange(y, m)[1]

        for p_start, p_end in (
            (date(y, m, 1),  date(y, m, 15)),
            (date(y, m, 16), date(y, m, last_day)),
        ):
            s = max(p_start, start)
            e = min(p_end, end)
            if s <= e:
                batches.append((s.strftime("%Y%m%d"), e.strftime("%Y%m%d")))

        cursor = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)

    return batches


# ─────────────────────────────────────────────────────────────
# PostgreSQL helper
# ─────────────────────────────────────────────────────────────

def call_staging_procedure(log: logging.Logger, dry_run: bool = False) -> bool:
    """Calls the staging procedure and commits. Returns True on success."""
    if dry_run:
        log.info("[DRY-RUN] Would execute: %s", PROCEDURE_CALL)
        return True

    log.info("Calling staging procedure: %s", PROCEDURE_CALL)
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = get_postgres_connection()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(PROCEDURE_CALL)
        conn.commit()
        log.info("Procedure completed successfully.")
        return True
    except Exception:
        log.exception("Procedure call FAILED.")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# Extractor wiring
# ─────────────────────────────────────────────────────────────

def build_extractor_env(unit: str, date_from: str, date_to: str) -> Dict[str, str]:
    """Environment for one ME2M extractor run (unit = company code)."""
    env = os.environ.copy()
    env["DATE_FROM"]     = date_from
    env["DATE_TO"]       = date_to
    # Pin the pre-scan window to this batch so a stray EINDT_* in the parent
    # shell can't widen every batch to the same global window.
    env["EINDT_FROM"]    = date_from
    env["EINDT_TO"]      = date_to
    env["COMPANY_CODES"] = unit
    env["PLANTS"]        = ",".join(PLANTS_BY_COMPANY.get(unit, []))   # "" = all plants
    env["INCLUDE_OPEN_SCHEDULES"] = "true" if INCLUDE_OPEN_SCHEDULES else "false"
    return env


def verify_extractor_env_hooks() -> List[str]:
    """
    Returns the names in REQUIRED_EXTRACTOR_ENV_VARS that the extractor
    source does NOT read via os.getenv("NAME", ...).
    """
    if not EXTRACTOR_SCRIPT.exists():
        return []  # reported separately
    src = EXTRACTOR_SCRIPT.read_text(encoding="utf-8", errors="replace")
    return [
        v for v in REQUIRED_EXTRACTOR_ENV_VARS
        if not re.search(r"""getenv\(\s*["']%s["']""" % re.escape(v), src)
    ]


def run_extractor(
    unit:      str,
    date_from: str,
    date_to:   str,
    log:       logging.Logger,
    dry_run:   bool = False,
) -> bool:
    """
    Launches the extractor as a subprocess for a single company code and
    15-day period. Returns True if the subprocess exits with code 0.
    """
    env = build_extractor_env(unit, date_from, date_to)
    cmd = [sys.executable, str(EXTRACTOR_SCRIPT)]

    log.info(
        "Launching extractor | %s=%s | DATE_FROM=%s | DATE_TO=%s",
        UNIT_LABEL.upper().replace(" ", "_"), unit, date_from, date_to,
    )

    if dry_run:
        log.info("[DRY-RUN] Would run: %s", EXTRACTOR_SCRIPT.name)
        return True

    if not EXTRACTOR_SCRIPT.exists():
        log.error("Extractor script not found: %s", EXTRACTOR_SCRIPT)
        return False

    try:
        result = subprocess.run(
            cmd, env=env, text=True,
            timeout=ORCH_EXTRACTOR_TIMEOUT_S or None,
        )
        if result.returncode == 0:
            log.info("Extractor finished successfully (exit 0).")
            return True
        log.error("Extractor exited with code %d.", result.returncode)
        return False
    except subprocess.TimeoutExpired:
        log.error("Extractor killed after %ds timeout.", ORCH_EXTRACTOR_TIMEOUT_S)
        return False
    except FileNotFoundError:
        log.exception("Python interpreter not found: %s", sys.executable)
        return False
    except Exception:
        log.exception("Unexpected error launching extractor.")
        return False


# ─────────────────────────────────────────────────────────────
# Main orchestration loop
# ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="SAP ME2M extraction orchestrator -- 15-day periods, one company code at a time."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the execution plan without making any SAP or DB calls.",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Wipe the progress file and start from the beginning.",
    )
    parser.add_argument(
        "--retry-failed", action="store_true",
        help="Give FAILED batches a fresh set of attempts; COMPLETED batches stay skipped.",
    )
    parser.add_argument(
        "--only", metavar="LIST",
        help="Comma-separated subset of company codes to run (useful for testing).",
    )
    args = parser.parse_args()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    # Resolve the company code list
    units: List[str] = list(UNITS)
    if args.only:
        wanted = [u.strip() for u in args.only.split(",") if u.strip()]
        unknown = [u for u in wanted if u not in UNITS]
        if unknown:
            log.critical("--only contains company code(s) not in config: %s", unknown)
            sys.exit(1)
        units = wanted

    # Validate environment / extractor wiring early
    missing_hooks = verify_extractor_env_hooks()
    if missing_hooks:
        msg = (
            "%s does not read %s from the environment (os.getenv). "
            "Every batch would silently run with the extractor's hard-coded "
            "values. Patch the extractor first (see this script's docstring)."
        )
        if args.dry_run:
            log.warning("[DRY-RUN] " + msg, EXTRACTOR_SCRIPT.name, missing_hooks)
        else:
            log.critical(msg, EXTRACTOR_SCRIPT.name, missing_hooks)
            sys.exit(1)

    if not args.dry_run:
        missing = [
            v for v in (
                "SAP_USER", "SAP_PASSWORD", "SAP_ASHOST", "SAP_SYSNR", "SAP_CLIENT",
                "PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD",
            )
            if not os.getenv(v)
        ]
        if missing:
            log.critical("Missing required environment variables: %s", missing)
            sys.exit(1)

    # Progress file
    if args.reset and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        log.info("Progress file reset.")

    progress = _load_progress()

    if args.retry_failed:
        n = 0
        for rec in progress["batches"].values():
            if rec.get("status") == "FAILED":
                rec["status"], rec["attempts"] = "PENDING", 0
                n += 1
        _save_progress(progress)
        log.info("--retry-failed: %d FAILED batch(es) re-queued.", n)

    # Build the full batch plan
    # Outer: company codes  |  Inner: 15-day date-range periods
    periods       = generate_fortnightly_batches(LOOP_START, LOOP_END)
    total_batches = len(units) * len(periods)

    log.info("=" * 70)
    log.info("SAP ME2M EXTRACTION ORCHESTRATOR  --  run %s", run_id)
    log.info("Date range      : %s -> %s", LOOP_START, LOOP_END)
    log.info("Periods         : %d  (2 x 15-day periods per month)", len(periods))
    log.info("%-15s: %s  (%d)", UNIT_LABEL + "s", units, len(units))
    log.info("Total batches   : %d  (one per company code x 15-day period)", total_batches)
    log.info("Loop order      : company code-first -> all periods per company code -> next company code")
    log.info("Extractor       : %s", EXTRACTOR_SCRIPT.name)
    log.info("Procedure       : %s", PROCEDURE_CALL)
    log.info("Dry-run         : %s", args.dry_run)
    log.info("=" * 70)

    # Dry-run: print full plan and exit
    if args.dry_run:
        overall_idx = 0
        for unit in units:
            log.info("")
            log.info("  +-- %s: %s %s", UNIT_LABEL, unit, "-" * 50)
            for date_from, date_to in periods:
                overall_idx += 1
                key    = _batch_key(unit, date_from, date_to)
                status = progress["batches"].get(key, {}).get("status", "PENDING")
                log.info("  |  %4d. %s -> %s  [%s]", overall_idx, date_from, date_to, status)
            log.info("  +%s", "-" * 60)
        log.info("")
        log.info("[DRY-RUN] No data will be extracted or loaded.")
        return

    # Live run
    stats = {"completed": 0, "skipped": 0, "failed": 0}
    overall_idx = 0

    for unit in units:
        log.info(">" * 70)
        log.info("%s: %s", UNIT_LABEL.upper(), unit)
        log.info(">" * 70)

        for date_from, date_to in periods:
            overall_idx += 1
            key = _batch_key(unit, date_from, date_to)
            rec = progress["batches"].setdefault(key, {
                "status":   "PENDING",
                "attempts": 0,
                "error":    None,
            })

            label = f"[{overall_idx:>3}/{total_batches}]  {unit}  {date_from}->{date_to}"

            # Skip completed batches
            if rec["status"] == "COMPLETED":
                log.info("%s  SKIPPED (already completed)", label)
                stats["skipped"] += 1
                continue

            # Check retry limit (counts runs of the orchestrator, not inner retries)
            if rec["attempts"] > ORCH_MAX_RETRIES and rec["status"] == "FAILED":
                log.warning(
                    "%s  SKIPPED -- exceeded max retries (%d). "
                    "Fix the issue and re-run with --retry-failed.",
                    label, ORCH_MAX_RETRIES,
                )
                stats["failed"] += 1
                continue

            log.info("-" * 70)
            log.info("%s  START", label)

            # Mark as RUNNING
            rec["status"]              = "RUNNING"
            rec["attempts"]            = rec.get("attempts", 0) + 1
            rec["extraction_start_ts"] = datetime.now().isoformat()
            rec["extraction_end_ts"]   = None
            rec["procedure_ts"]        = None
            rec["error"]               = None
            _save_progress(progress)

            # Step 1: Run the extractor
            extraction_ok = False
            for attempt in range(1, ORCH_MAX_RETRIES + 2):
                log.info(
                    "%s  Extraction attempt %d/%d...",
                    label, attempt, ORCH_MAX_RETRIES + 1,
                )
                if run_extractor(unit, date_from, date_to, log):
                    extraction_ok = True
                    break
                if attempt <= ORCH_MAX_RETRIES:
                    log.warning(
                        "%s  Extraction failed -- retrying in %.0fs...",
                        label, ORCH_RETRY_DELAY_S,
                    )
                    time.sleep(ORCH_RETRY_DELAY_S)

            rec["extraction_end_ts"] = datetime.now().isoformat()

            if not extraction_ok:
                rec["status"] = "FAILED"
                rec["error"]  = "Extractor subprocess failed (non-zero exit, timeout or launch error)."
                _save_progress(progress)
                log.error(
                    "%s  FAILED -- extraction unsuccessful after %d attempts.",
                    label, ORCH_MAX_RETRIES + 1,
                )
                stats["failed"] += 1
                continue

            log.info("%s  Extraction done -- calling staging procedure...", label)

            # Step 2: Call the PostgreSQL procedure
            if not call_staging_procedure(log):
                rec["status"] = "FAILED"
                rec["error"]  = "Staging procedure call failed."
                _save_progress(progress)
                log.error(
                    "%s  FAILED -- procedure call unsuccessful. "
                    "Staging tables are intact; fix and re-run.",
                    label,
                )
                stats["failed"] += 1
                continue

            # Mark COMPLETED
            rec["status"]       = "COMPLETED"
            rec["procedure_ts"] = datetime.now().isoformat()
            rec["error"]        = None
            _save_progress(progress)

            log.info("%s  COMPLETED", label)
            stats["completed"] += 1

    # Final summary
    log.info("=" * 70)
    log.info("ORCHESTRATION COMPLETE")
    log.info("  Total batches : %d", total_batches)
    log.info("  Completed     : %d", stats["completed"])
    log.info("  Skipped       : %d  (already done in a previous run)", stats["skipped"])
    log.info("  Failed        : %d", stats["failed"])
    log.info("  Progress file : %s", PROGRESS_FILE)
    log.info("=" * 70)

    if stats["failed"]:
        log.warning(
            "%d batch(es) failed. Inspect the logs, fix the issue, "
            "then re-run with --retry-failed -- completed batches are skipped automatically.",
            stats["failed"],
        )
        sys.exit(1)


if __name__ == "__main__":
    main()