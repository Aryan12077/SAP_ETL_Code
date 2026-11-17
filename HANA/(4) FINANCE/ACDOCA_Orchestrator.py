"""
SAP S/4HANA ACDOCA Finance (FBL1N / FBL3N / FBL5N) — Extraction Orchestrator
============================================================================
Drives ACDOCA_Finance.py in 15-day (fortnightly) batches, one company code
at a time, and after every batch loads the three finance reports from the
same staging data.

Loop structure
──────────────
  Outer loop : company code   (COMPANY_CODES list below)
  Inner loop : 15-day period  (LOOP_START -> LOOP_END)
  Total      : len(COMPANY_CODES) x number of periods batches

Each calendar month is split into two periods:
  Period 1 : 1st  -> 15th
  Period 2 : 16th -> last day of month

The date window is applied by the extractor to ACDOCA.BUDAT and BKPF.BUDAT
(posting date).

After each (company code + 15-day period) batch, three procedures are called
in this order, inside ONE transaction:
  1. prc_load_sap_finance_data_fbl1n()   vendor lines   (KOART K)
  2. prc_load_sap_finance_data_fbl3n()   G/L lines      (KOART S/M)
  3. prc_load_sap_finance_data_fbl5n()   customer lines (KOART D)

  All three commit together or not at all. If one fails, the other two are
  rolled back too, so a retry never loads FBL1N (say) twice. The staging
  tables are untouched by a rollback, so the procedures are simply retried
  (up to ORCH_MAX_RETRIES) without re-extracting.

IMPORTANT
  * The extractor TRUNCATEs its staging tables at the start of every run,
    so the procedures MUST consume the data after each batch — append /
    upsert into the final tables. A procedure that TRUNCATEs its final table
    keeps only the last batch.
  * To make re-runs safe (e.g. --reset, or re-running a window), each
    procedure should first delete the final-table rows of the window it is
    about to insert (company code + posting-date range of the staged data),
    then insert.
  * The procedures must not COMMIT internally: the orchestrator commits once
    after all three have run.

Resume behaviour
────────────────
  Progress is persisted to orchestrator_progress_acdoca.json.
  Each batch key is  "{COMPANY_CODE}_{DATE_FROM}_{DATE_TO}".
  On restart, COMPLETED batches are skipped automatically.
  FAILED batches are retried on the next run until ORCH_MAX_RETRIES runs
  have failed; then they are skipped until --reset.

Usage
─────
  python ACDOCA_Finance_Orchestrator.py                # normal run
  python ACDOCA_Finance_Orchestrator.py --dry-run      # print plan, no SAP/DB calls
  python ACDOCA_Finance_Orchestrator.py --reset        # wipe progress and start fresh

Required env vars (.env or shell, read via connections.py)
──────────────────────────────────────────────────────────
  HANA_USER, HANA_PASSWD, HANA_ASHOST, HANA_SYSNR, HANA_CLIENT
  PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD
  (HANA_ROUTER is optional)

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
)

import argparse  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
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

# Inclusive posting-date range
LOOP_START: date = date(2026, 1, 1)
LOOP_END:   date = date(2026, 10, 31)

# Company codes — processed one at a time, each running all periods.
# Passed to the extractor through ACDOCA_COMPANY_CODES.
COMPANY_CODES: List[str] = ["1100"]

# Staging schema of the HANA system (sap_stg_hana)
SCHEMA: str = get_postgres_schema("HANA")

# Procedures called after every batch, in this order, in one transaction.
PROCEDURES: List[Tuple[str, str]] = [
    ("FBL1N", f"{SCHEMA}.prc_load_sap_finance_data_fbl1n"),
    ("FBL3N", f"{SCHEMA}.prc_load_sap_finance_data_fbl3n"),
    ("FBL5N", f"{SCHEMA}.prc_load_sap_finance_data_fbl5n"),
]

# Staging tables filled by the extractor (row counts are logged per batch)
STG_ACDOCA: str = f"{SCHEMA}.stg_sap_acdoca_acdoca"
STG_BKPF:   str = f"{SCHEMA}.stg_sap_acdoca_bkpf"

# Paths — extractor must sit in the same folder as this script
EXTRACTOR_SCRIPT: Path = Path(__file__).parent / "ACDOCA_Finance.py"
PROGRESS_FILE:    Path = Path(__file__).parent / "orchestrator_progress_acdoca.json"
LOG_DIR:          Path = Path(os.getenv("LOG_DIR", "logs"))

# Retry / timing
ORCH_MAX_RETRIES:   int   = int(os.getenv("ORCH_MAX_RETRIES",   "2"))
ORCH_RETRY_DELAY_S: float = float(os.getenv("ORCH_RETRY_DELAY_S", "60"))

REQUIRED_ENV: Tuple[str, ...] = (
    "HANA_USER", "HANA_PASSWD", "HANA_ASHOST", "HANA_SYSNR", "HANA_CLIENT",
    "PG_HOST", "PG_DATABASE", "PG_USER", "PG_PASSWORD",
)


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def _build_logger(run_id: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"orchestrator_acdoca_{run_id}.log"

    log = logging.getLogger(f"orchestrator_acdoca.{run_id}")
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
# e.g.              "1100_20250101_20250115"

def _batch_key(entity: str, date_from: str, date_to: str) -> str:
    return f"{entity}_{date_from}_{date_to}" if entity else f"{date_from}_{date_to}"


def _load_progress() -> Dict:
    if PROGRESS_FILE.exists():
        try:
            with PROGRESS_FILE.open(encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass  # Corrupt file -> start fresh
    return {"batches": {}}


def _save_progress(progress: Dict) -> None:
    """Atomic write via temp file + rename — safe against mid-write crashes."""
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(progress, fh, indent=2, default=str)
    tmp.replace(PROGRESS_FILE)


# ─────────────────────────────────────────────────────────────
# Fortnightly batch generator  (15-day periods)
# ─────────────────────────────────────────────────────────────

def generate_fortnightly_batches(start: date, end: date) -> List[Tuple[str, str]]:
    """
    Returns (DATE_FROM, DATE_TO) string pairs in YYYYMMDD format.
    Each calendar month produces TWO periods (1st–15th, 16th–last day).
    The first period starts at *start* and the last is capped at *end*.
    """
    batches: List[Tuple[str, str]] = []
    cursor = date(start.year, start.month, 1)

    while cursor <= end:
        last_day_of_month = monthrange(cursor.year, cursor.month)[1]
        mid               = date(cursor.year, cursor.month, 15)
        month_end         = date(cursor.year, cursor.month, last_day_of_month)
        p2_start          = date(cursor.year, cursor.month, 16)

        for p_start, p_end in ((cursor, mid), (p2_start, month_end)):
            p_start = max(p_start, start)
            p_end   = min(p_end, end)
            if p_start <= p_end:
                batches.append((p_start.strftime("%Y%m%d"), p_end.strftime("%Y%m%d")))

        cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 \
            else date(cursor.year, cursor.month + 1, 1)

    return batches


# ─────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ─────────────────────────────────────────────────────────────

def check_procedures_exist(log: logging.Logger) -> bool:
    """Pre-flight: all three procedures must exist before any extraction."""
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = get_postgres_connection()
        missing = []
        with conn.cursor() as cur:
            for report, proc in PROCEDURES:
                schema, name = proc.split(".", 1)
                cur.execute(
                    """
                    SELECT 1
                    FROM pg_proc p
                    JOIN pg_namespace n ON n.oid = p.pronamespace
                    WHERE n.nspname = %s AND p.proname = %s AND p.prokind = 'p'
                    """,
                    (schema, name),
                )
                if cur.fetchone() is None:
                    missing.append(proc)
        if missing:
            log.critical("Procedure(s) not found in PostgreSQL: %s", missing)
            return False
        log.info("Pre-flight: all %d procedures found.", len(PROCEDURES))
        return True
    except Exception:
        log.exception("Pre-flight PostgreSQL check FAILED.")
        return False
    finally:
        if conn:
            conn.close()


def staging_counts(log: logging.Logger) -> Dict[str, int]:
    """Row counts of the staged batch, per report (logged + saved in progress)."""
    counts: Dict[str, int] = {}
    conn: Optional[psycopg2.extensions.connection] = None
    try:
        conn = get_postgres_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  COUNT(*)                                                      AS acdoca,
                  COUNT(*) FILTER (WHERE koart = 'K')                           AS fbl1n,
                  COUNT(*) FILTER (WHERE koart IN ('S','M')
                                     AND COALESCE(buzei,'') NOT IN ('','000')
                                     AND COALESCE(bstat,'') <> 'U')             AS fbl3n,
                  COUNT(*) FILTER (WHERE koart = 'D')                           AS fbl5n
                FROM {STG_ACDOCA}
                """
            )
            row = cur.fetchone()
            counts = {"acdoca": row[0], "fbl1n": row[1], "fbl3n": row[2], "fbl5n": row[3]}
            cur.execute(f"SELECT COUNT(*) FROM {STG_BKPF}")
            counts["bkpf"] = cur.fetchone()[0]
        log.info(
            "Staged: ACDOCA %d lines (FBL1N %d | FBL3N %d | FBL5N %d), BKPF %d headers.",
            counts["acdoca"], counts["fbl1n"], counts["fbl3n"], counts["fbl5n"], counts["bkpf"],
        )
    except Exception:
        log.warning("Could not read staging row counts (continuing).", exc_info=True)
    finally:
        if conn:
            conn.close()
    return counts


def call_finance_procedures(
    log:     logging.Logger,
    dry_run: bool = False,
) -> Tuple[bool, Optional[str], Dict[str, float]]:
    """
    Calls the three procedures in ONE transaction and commits once.
    Returns (ok, failed_report, seconds per report).
    On any failure everything is rolled back (no partial load).
    """
    timings: Dict[str, float] = {}
    if dry_run:
        for report, proc in PROCEDURES:
            log.info("[DRY-RUN] Would execute: CALL %s();", proc)
        return True, None, timings

    conn: Optional[psycopg2.extensions.connection] = None
    current: Optional[str] = None
    try:
        conn = get_postgres_connection()
        conn.autocommit = False
        with conn.cursor() as cur:
            for report, proc in PROCEDURES:
                current = report
                log.info("  %s: CALL %s();", report, proc)
                t0 = time.time()
                cur.execute(f"CALL {proc}();")
                timings[report] = round(time.time() - t0, 1)
                log.info("  %s: done in %.1fs.", report, timings[report])
        conn.commit()
        log.info("All %d procedures committed.", len(PROCEDURES))
        return True, None, timings
    except Exception:
        log.exception("Procedure %s FAILED — rolling back all three.", current)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False, current, timings
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
# Extractor subprocess runner
# ─────────────────────────────────────────────────────────────

def run_extractor(
    date_from: str,
    date_to:   str,
    entity:    str,
    log:       logging.Logger,
    dry_run:   bool = False,
) -> bool:
    """
    Launches ACDOCA_Finance.py as a subprocess for one company code and one
    15-day period. Returns True if the subprocess exits with code 0.
    """
    env = os.environ.copy()
    env["DATE_FROM"]            = date_from
    env["DATE_TO"]              = date_to
    env["ACDOCA_COMPANY_CODES"] = entity

    cmd = [sys.executable, str(EXTRACTOR_SCRIPT)]

    log.info(
        "Launching extractor | COMPANY_CODE=%s | DATE_FROM=%s | DATE_TO=%s",
        entity or "-", date_from, date_to,
    )

    if dry_run:
        log.info(
            "[DRY-RUN] Would run: %s  ACDOCA_COMPANY_CODES=%s  DATE_FROM=%s  DATE_TO=%s",
            EXTRACTOR_SCRIPT.name, entity or "-", date_from, date_to,
        )
        return True

    if not EXTRACTOR_SCRIPT.exists():
        log.error("Extractor script not found: %s", EXTRACTOR_SCRIPT)
        return False

    try:
        result = subprocess.run(cmd, env=env, text=True)
        if result.returncode == 0:
            log.info("Extractor finished successfully (exit 0).")
            return True
        log.error("Extractor exited with code %d.", result.returncode)
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
        description="SAP ACDOCA finance (FBL1N/FBL3N/FBL5N) orchestrator — "
                    "15-day periods, one company code at a time."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the execution plan without making any SAP or DB calls.")
    parser.add_argument("--reset", action="store_true",
                        help="Wipe the progress file and start from the beginning.")
    args = parser.parse_args()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    # Validate environment and procedures early
    if not args.dry_run:
        missing = [v for v in REQUIRED_ENV if not os.getenv(v)]
        if missing:
            log.critical("Missing required environment variables: %s", missing)
            sys.exit(1)
        if not check_procedures_exist(log):
            sys.exit(1)

    if args.reset and PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()
        log.info("Progress file reset.")

    progress = _load_progress()

    periods       = generate_fortnightly_batches(LOOP_START, LOOP_END)
    total_batches = len(COMPANY_CODES) * len(periods)

    log.info("=" * 70)
    log.info("SAP ACDOCA FINANCE ORCHESTRATOR (FBL1N / FBL3N / FBL5N)  —  run %s", run_id)
    log.info("Date range      : %s -> %s  (posting date)", LOOP_START, LOOP_END)
    log.info("Periods         : %d  (2 x 15-day periods per month)", len(periods))
    log.info("Company codes   : %s  (%d)", COMPANY_CODES, len(COMPANY_CODES))
    log.info("Loop order      : company code -> all periods -> next company code")
    log.info("Total batches   : %d", total_batches)
    for report, proc in PROCEDURES:
        log.info("Procedure %-6s: %s()", report, proc)
    log.info("Dry-run         : %s", args.dry_run)
    log.info("=" * 70)

    # Dry-run: print full plan and exit
    if args.dry_run:
        overall_idx = 0
        for entity in COMPANY_CODES:
            log.info("")
            log.info("  +-- Company code: %s %s", entity, "-" * 45)
            for date_from, date_to in periods:
                overall_idx += 1
                key    = _batch_key(entity, date_from, date_to)
                status = progress["batches"].get(key, {}).get("status", "PENDING")
                log.info("  |  %4d. %s -> %s  [%s]", overall_idx, date_from, date_to, status)
            log.info("  +%s", "-" * 60)
        log.info("")
        call_finance_procedures(log, dry_run=True)
        log.info("[DRY-RUN] No data will be extracted or loaded.")
        return

    # Live run
    stats = {"completed": 0, "skipped": 0, "failed": 0}
    overall_idx = 0

    for entity in COMPANY_CODES:
        log.info(">" * 70)
        log.info("COMPANY CODE: %s", entity)
        log.info(">" * 70)

        for date_from, date_to in periods:
            overall_idx += 1
            key = _batch_key(entity, date_from, date_to)
            rec = progress["batches"].setdefault(key, {
                "status":   "PENDING",
                "attempts": 0,
                "error":    None,
            })

            label = f"[{overall_idx:>3}/{total_batches}]  {entity}  {date_from}->{date_to}"

            if rec["status"] == "COMPLETED":
                log.info("%s  SKIPPED (already completed)", label)
                stats["skipped"] += 1
                continue

            if rec["status"] == "FAILED" and rec.get("attempts", 0) > ORCH_MAX_RETRIES:
                log.warning(
                    "%s  SKIPPED — exceeded max retries (%d). Fix the issue and reset to reprocess.",
                    label, ORCH_MAX_RETRIES,
                )
                stats["failed"] += 1
                continue

            log.info("-" * 70)
            log.info("%s  START", label)

            rec["status"]              = "RUNNING"
            rec["attempts"]            = rec.get("attempts", 0) + 1
            rec["extraction_start_ts"] = datetime.now().isoformat()
            rec["extraction_end_ts"]   = None
            rec["procedure_ts"]        = None
            rec["error"]               = None
            _save_progress(progress)

            # ── Step 1: extractor (retried) ───────────────────────────
            extraction_ok = False
            for attempt in range(1, ORCH_MAX_RETRIES + 2):
                log.info("%s  Extraction attempt %d/%d...", label, attempt, ORCH_MAX_RETRIES + 1)
                if run_extractor(date_from, date_to, entity, log):
                    extraction_ok = True
                    break
                if attempt <= ORCH_MAX_RETRIES:
                    log.warning("%s  Extraction failed — retrying in %.0fs...", label, ORCH_RETRY_DELAY_S)
                    time.sleep(ORCH_RETRY_DELAY_S)

            rec["extraction_end_ts"] = datetime.now().isoformat()

            if not extraction_ok:
                rec["status"] = "FAILED"
                rec["error"]  = "Extractor subprocess returned non-zero exit code."
                _save_progress(progress)
                log.error("%s  FAILED — extraction unsuccessful after %d attempts.",
                          label, ORCH_MAX_RETRIES + 1)
                stats["failed"] += 1
                continue

            rec["staged_rows"] = staging_counts(log)
            _save_progress(progress)

            # ── Step 2: three procedures in one transaction (retried) ─
            log.info("%s  Extraction done — loading FBL1N / FBL3N / FBL5N...", label)
            procedures_ok, failed_report, timings = False, None, {}
            for attempt in range(1, ORCH_MAX_RETRIES + 2):
                procedures_ok, failed_report, timings = call_finance_procedures(log)
                if procedures_ok:
                    break
                if attempt <= ORCH_MAX_RETRIES:
                    log.warning("%s  Procedures rolled back (%s failed) — retrying in %.0fs "
                                "(staging is intact)...", label, failed_report, ORCH_RETRY_DELAY_S)
                    time.sleep(ORCH_RETRY_DELAY_S)

            rec["procedure_seconds"] = timings

            if not procedures_ok:
                rec["status"] = "FAILED"
                rec["error"]  = f"Procedure for {failed_report} failed; all three rolled back."
                _save_progress(progress)
                log.error("%s  FAILED — %s procedure failed; nothing was loaded for this batch. "
                          "Fix the procedure and re-run.", label, failed_report)
                stats["failed"] += 1
                continue

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
            "%d batch(es) failed. Inspect the logs, fix the issue, then re-run — "
            "completed batches will be skipped automatically.",
            stats["failed"],
        )
        sys.exit(1)


if __name__ == "__main__":
    main()