"""
SAP Inventory Ageing Extraction Orchestrator
============================================
Drives sap_inventory_ageing.py one plant (valuation area, MBEW/MBEWH.BWKEY)
at a time, one fiscal period at a time.

Why this shape?
───────────────
The extractor pulls two tables:

  MBEW   current valuation snapshot  -> NOT period based (one row per material
                                        / valuation area right now)
  MBEWH  valuation history           -> one row per material / valuation area
                                        / fiscal year (LFGJA) / period (LFMON)

So MBEWH is split into fiscal-period batches, and MBEW is extracted once per
plant as a "snapshot" batch (or with every batch -- see
EXTRACT_MBEW_ONCE_PER_PLANT).

NOTE: LFGJA / LFMON are *fiscal* year / posting period, not calendar dates.
With an April-March fiscal year, period 01 = April. Set the loop range below
in fiscal year / period terms.

Loop structure
──────────────
  Outer loop : plant (BWKEY)                (PLANTS list below)
  Inner loop : [MBEW snapshot] + fiscal periods (START_FY/PER -> END_FY/PER)

  Batch keys : "{PLANT}_SNAPSHOT"   e.g. "1201_SNAPSHOT"
               "{PLANT}_{FY}{PP}"   e.g. "1201_202504"

After each batch -> PROCEDURE_CALL is executed (staging -> target load).

Extractor contract (env vars the extractor MUST read via os.getenv)
───────────────────────────────────────────────────────────────────
  PLANTS         comma-separated BWKEY list; "" = all plants (no BWKEY filter)
  TABLES         comma-separated tables to extract this run (e.g. "MBEW")
  FISCAL_YEAR    LFGJA  (applies to MBEWH only; "" = no filter)
  FISCAL_PERIOD  LFMON  (applies to MBEWH only; "" = no filter)

Start-up checks that the extractor source reads all four, so a
mis-wired extractor fails loudly instead of re-pulling BWKEY='1201' forever.

IMPORTANT: the extractor TRUNCATES each staging table at the start of a run.
A batch that doesn't include a table leaves that table's staging data as-is.
If your PROCEDURE_CALL needs MBEW and MBEWH in staging at the same time, the
snapshot-once mode is still safe (MBEW stays loaded from the snapshot batch)
but the procedure must be idempotent per period. If in doubt, set
EXTRACT_MBEW_ONCE_PER_PLANT = False.

Usage
─────
  python Inventory_Ageing_Orchestrator.py                  # normal (auto-resume)
  python Inventory_Ageing_Orchestrator.py --dry-run        # print plan only
  python Inventory_Ageing_Orchestrator.py --only 1201      # subset of plants
  python Inventory_Ageing_Orchestrator.py --retry-failed   # re-queue FAILED
  python Inventory_Ageing_Orchestrator.py --reset          # wipe progress

Required env vars: SAP_USER, SAP_PASSWORD, SAP_ASHOST, SAP_SYSNR, SAP_CLIENT,
                   PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD
Optional env vars: ORCH_MAX_RETRIES, ORCH_RETRY_DELAY_S,
                   ORCH_EXTRACTOR_TIMEOUT_S, LOG_DIR
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
from dataclasses import dataclass  # noqa: E402
from datetime import datetime  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Dict, List, Optional, Tuple  # noqa: E402

import psycopg2  # noqa: E402

# ─────────────────────────────────────────────────────────────
# ORCHESTRATOR CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Inclusive fiscal range for MBEWH (LFGJA / LFMON)
START_FY:     int = 2025
START_PERIOD: int = 1
END_FY:       int = 2026
END_PERIOD:   int = 12
PERIODS_PER_YEAR: int = 12      # use 16 to include special periods 13-16

# Plants (BWKEY) or comma-separated groups -- one batch set per entry.
# Empty list -> single run over ALL plants.
PLANTS: List[str] = ["1201"]
UNITS: List[str] = PLANTS if PLANTS else ["ALL"]

# Tables
SNAPSHOT_TABLES: List[str] = ["MBEW"]     # not period based
PERIOD_TABLES:   List[str] = ["MBEWH"]    # filtered by LFGJA / LFMON

# True : MBEW extracted once per plant (SNAPSHOT batch), MBEWH per period.
# False: every period batch extracts MBEW + MBEWH (MBEW re-pulled each time).
EXTRACT_MBEW_ONCE_PER_PLANT: bool = True

# Procedure called after every batch
# NOTE: placeholder name -- change to your real procedure.
PROCEDURE_CALL: str = "CALL sap_stg_ecc.prc_load_sap_inventory_ageing_data();"

# Paths -- extractor must sit in the same folder as this script
EXTRACTOR_SCRIPT: Path = Path(__file__).parent / "sap_inventory_ageing.py"
PROGRESS_FILE:    Path = Path(__file__).parent / "orchestrator_progress_ageing.json"
LOG_DIR:          Path = Path(os.getenv("LOG_DIR", "logs"))
LOG_PREFIX:       str  = "orchestrator_ageing"

# Retry / timing
ORCH_MAX_RETRIES:   int   = int(os.getenv("ORCH_MAX_RETRIES",   "2"))
ORCH_RETRY_DELAY_S: float = float(os.getenv("ORCH_RETRY_DELAY_S", "60"))
ORCH_EXTRACTOR_TIMEOUT_S: int = int(os.getenv("ORCH_EXTRACTOR_TIMEOUT_S", "0"))  # 0 = none

# Env vars the extractor MUST read via os.getenv(...)
REQUIRED_EXTRACTOR_ENV_VARS: List[str] = ["PLANTS", "TABLES", "FISCAL_YEAR", "FISCAL_PERIOD"]

UNIT_LABEL: str = "Plant"


# ─────────────────────────────────────────────────────────────
# Batch model
# ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Batch:
    key:    str            # unique id within a plant, e.g. SNAPSHOT or 202504
    tables: Tuple[str, ...]
    fy:     str            # "" when not period based
    period: str            # "" when not period based

    @property
    def label(self) -> str:
        if self.key == "SNAPSHOT":
            return "SNAPSHOT (" + ",".join(self.tables) + ")"
        return f"FY{self.fy}/P{self.period} (" + ",".join(self.tables) + ")"


def generate_fiscal_periods(
    start_fy: int, start_per: int, end_fy: int, end_per: int, per_year: int,
) -> List[Tuple[str, str]]:
    """Returns (LFGJA, LFMON) pairs, e.g. ('2025', '04'), inclusive."""
    if (start_fy, start_per) > (end_fy, end_per):
        raise ValueError("Loop start is after loop end.")
    out: List[Tuple[str, str]] = []
    fy, per = start_fy, start_per
    while (fy, per) <= (end_fy, end_per):
        out.append((str(fy), f"{per:02d}"))
        per += 1
        if per > per_year:
            fy, per = fy + 1, 1
    return out


def build_plan() -> List[Batch]:
    """Ordered batch list for ONE plant."""
    periods = generate_fiscal_periods(
        START_FY, START_PERIOD, END_FY, END_PERIOD, PERIODS_PER_YEAR,
    )
    plan: List[Batch] = []
    if EXTRACT_MBEW_ONCE_PER_PLANT:
        if SNAPSHOT_TABLES:
            plan.append(Batch("SNAPSHOT", tuple(SNAPSHOT_TABLES), "", ""))
        period_tables = tuple(PERIOD_TABLES)
    else:
        period_tables = tuple(SNAPSHOT_TABLES + PERIOD_TABLES)
    for fy, per in periods:
        plan.append(Batch(f"{fy}{per}", period_tables, fy, per))
    return plan


def _batch_key(unit: str, batch: Batch) -> str:
    return f"{unit}_{batch.key}"


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def _build_logger(run_id: str) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"{LOG_PREFIX}_{run_id}.log"

    log = logging.getLogger(f"{LOG_PREFIX}.{run_id}")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s"))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"))

    log.addHandler(fh)
    log.addHandler(ch)
    log.info("Orchestrator log: %s", log_file)
    return log


# ─────────────────────────────────────────────────────────────
# Progress file helpers
# ─────────────────────────────────────────────────────────────

def _load_progress() -> Dict:
    if PROGRESS_FILE.exists():
        try:
            with PROGRESS_FILE.open(encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            pass  # corrupt file -> start fresh
    return {"batches": {}}


def _save_progress(progress: Dict) -> None:
    """Atomic write via temp file + rename -- safe against mid-write crashes."""
    tmp = PROGRESS_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(progress, fh, indent=2, default=str)
    tmp.replace(PROGRESS_FILE)


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

def build_extractor_env(unit: str, batch: Batch) -> Dict[str, str]:
    """Environment for one extractor run (unit = plant, plant group or 'ALL')."""
    env = os.environ.copy()
    env["PLANTS"]        = "" if unit == "ALL" else unit
    env["TABLES"]        = ",".join(batch.tables)
    env["FISCAL_YEAR"]   = batch.fy
    env["FISCAL_PERIOD"] = batch.period
    return env


def verify_extractor_env_hooks() -> List[str]:
    """Names in REQUIRED_EXTRACTOR_ENV_VARS the extractor does NOT read via os.getenv."""
    if not EXTRACTOR_SCRIPT.exists():
        return []  # reported separately
    src = EXTRACTOR_SCRIPT.read_text(encoding="utf-8", errors="replace")
    return [
        v for v in REQUIRED_EXTRACTOR_ENV_VARS
        if not re.search(r"""getenv\(\s*["']%s["']""" % re.escape(v), src)
    ]


def run_extractor(unit: str, batch: Batch, log: logging.Logger, dry_run: bool = False) -> bool:
    """Launches the extractor as a subprocess. True if exit code 0."""
    env = build_extractor_env(unit, batch)
    cmd = [sys.executable, str(EXTRACTOR_SCRIPT)]

    log.info(
        "Launching extractor | %s=%s | TABLES=%s | FISCAL_YEAR=%s | FISCAL_PERIOD=%s",
        UNIT_LABEL.upper(), unit, env["TABLES"], env["FISCAL_YEAR"] or "-", env["FISCAL_PERIOD"] or "-",
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
        description="SAP inventory ageing orchestrator -- one plant at a time, fiscal-period batches."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the execution plan without making any SAP or DB calls.")
    parser.add_argument("--reset", action="store_true",
                        help="Wipe the progress file and start from the beginning.")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Give FAILED batches a fresh set of attempts; COMPLETED stay skipped.")
    parser.add_argument("--only", metavar="LIST",
                        help="Comma-separated subset of plants to run (useful for testing).")
    args = parser.parse_args()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    log    = _build_logger(run_id)

    # Resolve plant list
    units: List[str] = list(UNITS)
    if args.only:
        wanted = [u.strip() for u in args.only.split(",") if u.strip()]
        unknown = [u for u in wanted if u not in UNITS]
        if unknown:
            log.critical("--only contains plant(s) not in config: %s", unknown)
            sys.exit(1)
        units = wanted

    # Validate config / extractor wiring early
    try:
        plan = build_plan()
    except ValueError as exc:
        log.critical("Invalid loop range: %s", exc)
        sys.exit(1)
    if not plan:
        log.critical("Nothing to run -- check SNAPSHOT_TABLES / PERIOD_TABLES.")
        sys.exit(1)

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

    total_batches = len(units) * len(plan)
    n_periods = sum(1 for b in plan if b.key != "SNAPSHOT")

    log.info("=" * 70)
    log.info("SAP INVENTORY AGEING ORCHESTRATOR  --  run %s", run_id)
    log.info("Fiscal range    : FY%d/P%02d -> FY%d/P%02d", START_FY, START_PERIOD, END_FY, END_PERIOD)
    log.info("Period batches  : %d per plant  (+%d snapshot)", n_periods, len(plan) - n_periods)
    log.info("%-15s: %s  (%d)", UNIT_LABEL + "s", units, len(units))
    log.info("Total batches   : %d", total_batches)
    log.info("MBEW mode       : %s", "once per plant" if EXTRACT_MBEW_ONCE_PER_PLANT else "with every batch")
    log.info("Loop order      : plant-first -> all batches per plant -> next plant")
    log.info("Extractor       : %s", EXTRACTOR_SCRIPT.name)
    log.info("Procedure       : %s", PROCEDURE_CALL)
    log.info("Dry-run         : %s", args.dry_run)
    log.info("=" * 70)

    # Dry-run: print plan and exit
    if args.dry_run:
        idx = 0
        for unit in units:
            log.info("")
            log.info("  +-- %s: %s %s", UNIT_LABEL, unit, "-" * 50)
            for batch in plan:
                idx += 1
                status = progress["batches"].get(_batch_key(unit, batch), {}).get("status", "PENDING")
                log.info("  |  %4d. %s  [%s]", idx, batch.label, status)
            log.info("  +%s", "-" * 60)
        log.info("")
        log.info("[DRY-RUN] No data will be extracted or loaded.")
        return

    # Live run
    stats = {"completed": 0, "skipped": 0, "failed": 0}
    idx = 0

    for unit in units:
        log.info(">" * 70)
        log.info("%s: %s", UNIT_LABEL.upper(), unit)
        log.info(">" * 70)

        for batch in plan:
            idx += 1
            key = _batch_key(unit, batch)
            rec = progress["batches"].setdefault(key, {"status": "PENDING", "attempts": 0, "error": None})
            label = f"[{idx:>3}/{total_batches}]  {unit}  {batch.label}"

            if rec["status"] == "COMPLETED":
                log.info("%s  SKIPPED (already completed)", label)
                stats["skipped"] += 1
                continue

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

            rec["status"]              = "RUNNING"
            rec["attempts"]            = rec.get("attempts", 0) + 1
            rec["tables"]              = list(batch.tables)
            rec["extraction_start_ts"] = datetime.now().isoformat()
            rec["extraction_end_ts"]   = None
            rec["procedure_ts"]        = None
            rec["error"]               = None
            _save_progress(progress)

            # Step 1: extractor (with inner retries)
            extraction_ok = False
            for attempt in range(1, ORCH_MAX_RETRIES + 2):
                log.info("%s  Extraction attempt %d/%d...", label, attempt, ORCH_MAX_RETRIES + 1)
                if run_extractor(unit, batch, log):
                    extraction_ok = True
                    break
                if attempt <= ORCH_MAX_RETRIES:
                    log.warning("%s  Extraction failed -- retrying in %.0fs...", label, ORCH_RETRY_DELAY_S)
                    time.sleep(ORCH_RETRY_DELAY_S)

            rec["extraction_end_ts"] = datetime.now().isoformat()

            if not extraction_ok:
                rec["status"] = "FAILED"
                rec["error"]  = "Extractor subprocess failed (non-zero exit, timeout or launch error)."
                _save_progress(progress)
                log.error("%s  FAILED -- extraction unsuccessful after %d attempts.",
                          label, ORCH_MAX_RETRIES + 1)
                stats["failed"] += 1
                continue

            log.info("%s  Extraction done -- calling staging procedure...", label)

            # Step 2: staging procedure
            if not call_staging_procedure(log):
                rec["status"] = "FAILED"
                rec["error"]  = "Staging procedure call failed."
                _save_progress(progress)
                log.error("%s  FAILED -- procedure call unsuccessful. "
                          "Staging tables are intact; fix and re-run.", label)
                stats["failed"] += 1
                continue

            rec["status"]       = "COMPLETED"
            rec["procedure_ts"] = datetime.now().isoformat()
            rec["error"]        = None
            _save_progress(progress)

            log.info("%s  COMPLETED", label)
            stats["completed"] += 1

    # Summary
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