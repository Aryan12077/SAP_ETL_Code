"""
SAP IW39 Display PM Orders — Extraction Orchestrator
====================================================
Drives sap_iw39_claude.py in 15-day (fortnightly) batches, one plant at a time.

Loop structure
──────────────
  Outer loop : plant           (PLANTS list below)
  Inner loop : 15-day period   (LOOP_START -> LOOP_END)
  Total      : len(PLANTS) x number of periods batches

Each calendar month is split into two periods:
  Period 1 : 1st  -> 15th
  Period 2 : 16th -> last day of month

The date window is applied by the extractor to AUFK.ERDAT (order creation date).

After each (plant + 15-day period) batch:
  -> the staging procedure prc_load_sap_maintenance_order_iw39() is called

IMPORTANT: the extractor TRUNCATEs its staging tables at the start of every
run, so the staging procedure MUST consume (upsert / append to the target
tables) the data after each batch — otherwise the next batch wipes it.

Resume behaviour
────────────────
  Progress is persisted to orchestrator_progress_iw39.json.
  Each batch key is  "{PLANT}_{DATE_FROM}_{DATE_TO}".
  On restart, COMPLETED batches are skipped automatically.
  FAILED batches are retried up to ORCH_MAX_RETRIES times.

Usage
─────
  python IW39_Display_Orders_Orchestrator.py                # normal run
  python IW39_Display_Orders_Orchestrator.py --dry-run      # print plan, no SAP/DB calls
  python IW39_Display_Orders_Orchestrator.py --reset        # wipe progress and start fresh

Required env vars (.env or shell)
──────────────────────────────────
  SAP_USER, SAP_PASSWORD, SAP_ASHOST, SAP_SYSNR, SAP_CLIENT
  PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD
  (SAP_ROUTER is optional; all of these are read via connections.py)

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

# Inclusive date range
LOOP_START: date = date(2025, 1, 1)
LOOP_END:   date = date(2026, 10, 31)

# Plants — processed one at a time, each running all periods.
# Passed to the extractor through the PLANTS env var (comma-separated).
PLANTS: List[str] = ["1102"]
ENTITIES: List[str] = PLANTS

# PostgreSQL stored procedure called after every batch.
# NOTE: confirm this name matches the procedure that exists in your database.
PROCEDURE_CALL: str = f"CALL {get_postgres_schema('HANA')}.prc_load_sap_iw39_pm_orders_v2();"

# Paths — extractor must sit in the same folder as this script
EXTRACTOR_SCRIPT: Path = Path(__file__).parent / "IW39_Maintenance_Orders.py"
PROGRESS_FILE:    Path = Path(__file__).parent / "orchestrator_progress_iw39.json"
LOG_DIR:          Path = Path(os.getenv("LOG_DIR", "logs"))

# Retry / timing
ORCH_MAX_RETRIES:   int   = int(os.getenv("ORCH_MAX_RETRIES",   "2"))
ORCH_RETRY_DELAY_S: float = float(os.getenv("ORCH_RETRY_DELAY_S", "60"))


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def _build_logger(run_id: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"orchestrator_iw39_{run_id}.log"

    log = logging.getLogger(f"orchestrator_iw39.{run_id}")
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
# Batch key format:  "{PLANT}_{DATE_FROM}_{DATE_TO}"
# e.g.              "1202_20250101_20250115"

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
# Each calendar month is split into two periods:
#   Period 1 : 1st  -> 15th
#   Period 2 : 16th -> last day of month
#
# Example for January 2025:
#   20250101 -> 20250115
#   20250116 -> 20250131

def generate_fortnightly_batches(start: date, end: date) -> List[Tuple[str, str]]:
    """
    Returns (DATE_FROM, DATE_TO) string pairs in YYYYMMDD format.
    Each calendar month produces TWO 15-day periods.
    The final period is capped at *end* if it falls mid-month.
    """
    batches: List[Tuple[str, str]] = []
    cursor = date(start.year, start.month, 1)

    while cursor <= end:
        last_day_of_month = monthrange(cursor.year, cursor.month)[1]

        # Period 1: 1st -> 15th
        p1_start = cursor
        p1_end   = min(date(cursor.year, cursor.month, 15), end)
        batches.append((p1_start.strftime("%Y%m%d"), p1_end.strftime("%Y%m%d")))

        # Period 2: 16th -> end of month  (skip if end falls before the 16th)
        p2_start = date(cursor.year, cursor.month, 16)
        if p2_start <= end:
            p2_end = min(date(cursor.year, cursor.month, last_day_of_month), end)
            batches.append((p2_start.strftime("%Y%m%d"), p2_end.strftime("%Y%m%d")))

        # Advance to first day of next month
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    return batches


# ─────────────────────────────────────────────────────────────
# PostgreSQL helpers
# ─────────────────────────────────────────────────────────────

def call_staging_procedure(
    log:     logging.Logger,
    dry_run: bool = False,
) -> bool:
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
    Launches sap_iw39_claude.py as a subprocess for a single plant
    and 15-day period. Returns True if the subprocess exits with code 0.
    """
    env = os.environ.copy()
    env["DATE_FROM"] = date_from
    env["DATE_TO"]   = date_to
    env["PLANTS"]    = entity

    cmd = [sys.executable, str(EXTRACTOR_SCRIPT)]

    log.info(
        "Launching extractor | PLANTS=%s | DATE_FROM=%s | DATE_TO=%s",
        entity or "-", date_from, date_to,
    )

    if dry_run:
        log.info(
            "[DRY-RUN] Would run: %s  PLANTS=%s  DATE_FROM=%s  DATE_TO=%s",
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
        else:
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
    # Argument parsing
    parser = argparse.ArgumentParser(
        description="SAP IW39 Display PM Orders extraction orchestrator — 15-day periods, one plant at a time."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without making any SAP or DB calls.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Wipe the progress file and start from the beginning.",
    )
    args = parser.parse_args()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    # Validate environment early
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

    # Build the full batch plan
    periods       = generate_fortnightly_batches(LOOP_START, LOOP_END)
    total_batches = len(ENTITIES) * len(periods)

    log.info("=" * 70)
    log.info("SAP IW39 DISPLAY PM ORDERS ORCHESTRATOR  —  run %s", run_id)
    log.info("Date range      : %s -> %s", LOOP_START, LOOP_END)
    log.info("Periods         : %d  (2 x 15-day periods per month)", len(periods))
    log.info("Plants          : %s  (%d)", ENTITIES, len(ENTITIES))
    log.info("Loop order      : plant-first -> all periods per plant -> next plant")
    log.info("Total batches   : %d", total_batches)
    log.info("Procedure       : %s", PROCEDURE_CALL)
    log.info("Dry-run         : %s", args.dry_run)
    log.info("=" * 70)

    # Dry-run: print full plan and exit
    if args.dry_run:
        overall_idx = 0
        for entity in ENTITIES:
            log.info("")
            log.info("  +-- Plant: %s %s", entity or "(all)", "-" * 50)
            for date_from, date_to in periods:
                overall_idx += 1
                key    = _batch_key(entity, date_from, date_to)
                status = progress["batches"].get(key, {}).get("status", "PENDING")
                log.info("  |  %4d. %s -> %s  [%s]", overall_idx, date_from, date_to, status)
            log.info("  +%s", "-" * 60)
        log.info("")
        log.info("[DRY-RUN] No data will be extracted or loaded.")
        return

    # Live run
    stats = {"completed": 0, "skipped": 0, "failed": 0}
    overall_idx = 0

    for entity in ENTITIES:
        log.info(">" * 70)
        log.info("PLANTS: %s", entity or "(all)")
        log.info(">" * 70)

        for date_from, date_to in periods:
            overall_idx += 1
            key   = _batch_key(entity, date_from, date_to)
            rec   = progress["batches"].setdefault(key, {
                "status":   "PENDING",
                "attempts": 0,
                "error":    None,
            })

            label = f"[{overall_idx:>3}/{total_batches}]  {entity or 'ALL'}  {date_from}->{date_to}"

            # Skip completed batches
            if rec["status"] == "COMPLETED":
                log.info("%s  SKIPPED (already completed)", label)
                stats["skipped"] += 1
                continue

            # Check retry limit
            if rec["attempts"] > ORCH_MAX_RETRIES and rec["status"] == "FAILED":
                log.warning(
                    "%s  SKIPPED — exceeded max retries (%d). "
                    "Fix the issue and reset to reprocess.",
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
                if run_extractor(date_from, date_to, entity, log):
                    extraction_ok = True
                    break
                if attempt <= ORCH_MAX_RETRIES:
                    log.warning(
                        "%s  Extraction failed — retrying in %.0fs...",
                        label, ORCH_RETRY_DELAY_S,
                    )
                    time.sleep(ORCH_RETRY_DELAY_S)

            rec["extraction_end_ts"] = datetime.now().isoformat()

            if not extraction_ok:
                rec["status"] = "FAILED"
                rec["error"]  = "Extractor subprocess returned non-zero exit code."
                _save_progress(progress)
                log.error(
                    "%s  FAILED — extraction unsuccessful after %d attempts.",
                    label, ORCH_MAX_RETRIES + 1,
                )
                stats["failed"] += 1
                continue

            log.info("%s  Extraction done — calling staging procedure...", label)

            # Step 2: Call the PostgreSQL procedure
            procedure_ok = call_staging_procedure(log)

            if not procedure_ok:
                rec["status"] = "FAILED"
                rec["error"]  = "Staging procedure call failed."
                _save_progress(progress)
                log.error(
                    "%s  FAILED — procedure call unsuccessful. "
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
            "then re-run — completed batches will be skipped automatically.",
            stats["failed"],
        )
        sys.exit(1)


if __name__ == "__main__":
    main()