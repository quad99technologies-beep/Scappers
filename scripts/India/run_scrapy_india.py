#!/usr/bin/env python3
"""
India NPPA: Seed formulations queue and launch parallel Scrapy workers.

Flow:
    1. Initialize DB + seed formulations into formulation_status (pending)
    2. Launch N spider subprocesses, each with a unique worker_id
    3. Each spider claims work from the queue atomically (no double-scraping)

Usage:
    python run_scrapy_india.py                    # 1 worker, input from in_input_formulations
    python run_scrapy_india.py --workers 5        # 5 parallel workers
    python run_scrapy_india.py --limit 20         # Only 20 formulations (from input table)
"""

import argparse
import atexit
import concurrent.futures
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import traceback
from pathlib import Path
from typing import List, Optional

import psycopg2

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import load_env_file, get_output_dir, getenv_int

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_scrapy_india")


def parse_args():
    parser = argparse.ArgumentParser(description="Run India NPPA Scrapy spider")
    parser.add_argument("--fresh", action="store_true",
                        help="Start fresh run (new run_id, scrape ALL from input)")
    parser.add_argument("--limit", type=int, help="Limit number of formulations")
    parser.add_argument("--workers", type=int, default=getenv_int("INDIA_WORKERS", 1),
                        help="Number of parallel spider workers (default from INDIA_WORKERS)")
    parser.add_argument("--jobdir", action="store_true")
    parser.add_argument("--retry-zero-records", action="store_true",
                        help="On resume, move 'zero_records' back to pending (use after mapping fixes)")
    parser.add_argument("--retry-failed", action="store_true",
                        help="On resume, move 'failed' formulations back to pending for retry")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Max retry attempts for failed formulations (default: 3)")
    parser.add_argument("--step", type=int, choices=[1, 2],
                        help="Pipeline step: 1=Initial scrape, 2=Retry failed+zero_records")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DB seeding: load formulations and insert as 'pending' into formulation_status
# ---------------------------------------------------------------------------


def _load_formulations(db, limit: int = None) -> List[str]:
    """Load unique formulations from input table only (in_input_formulations). No CSV."""
    try:
        cur = db.execute("SELECT COUNT(*) FROM in_input_formulations")
        count = cur.fetchone()[0]
        if count == 0:
            raise SystemExit(
                "No formulations in input table (in_input_formulations). "
                "Upload via GUI or insert into in_input_formulations."
            )
        limit_clause = f" LIMIT {limit}" if limit else ""
        cur = db.execute(
            "SELECT DISTINCT generic_name FROM in_input_formulations "
            "WHERE generic_name IS NOT NULL AND generic_name != '' "
            "ORDER BY generic_name" + limit_clause
        )
        forms = [row[0].strip() for row in cur.fetchall() if row[0]]
        if not forms:
            raise SystemExit(
                "No non-empty formulations in input table (in_input_formulations). "
                "Upload via GUI or insert into in_input_formulations."
            )
        logger.info("Loaded %d unique formulations from input table (in_input_formulations)", len(forms))
        return forms
    except SystemExit:
        raise
    except Exception as exc:
        logger.debug("in_input_formulations not available: %s", exc)
        raise SystemExit(
            "Input table (in_input_formulations) not available. "
            "Ensure India schema is applied and upload formulations via GUI."
        ) from exc


def get_already_scraped_formulations(db, use_latest_completed_run: bool = True) -> set:
    """Return set of formulation names that exist in in_sku_main (latest completed run or any run)."""
    try:
        if use_latest_completed_run:
            cur = db.execute(
                "SELECT run_id FROM run_ledger "
                "WHERE scraper_name = 'India' AND status = 'completed' "
                "ORDER BY started_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if not row:
                return set()
            run_id = row[0]
            cur = db.execute(
                "SELECT DISTINCT formulation FROM in_sku_main WHERE run_id = %s",
                (run_id,),
            )
        else:
            cur = db.execute("SELECT DISTINCT formulation FROM in_sku_main")
        return {row[0] for row in cur.fetchall() if row[0]}
    except Exception as exc:
        logger.debug("Could not get already-scraped formulations: %s", exc)
        return set()


def seed_formulation_queue(db, formulations: List[str], run_id: str):
    """Insert all formulations as 'pending' into formulation_status (skip existing)."""
    # PK is (formulation, run_id) so each run inserts its own queue entries
    # Previous run data is preserved in the DB
    inserted = 0
    for f in formulations:
        try:
            db.execute(
                "INSERT INTO in_formulation_status (formulation, run_id, status) "
                "VALUES (%s, %s, 'pending') ON CONFLICT DO NOTHING",
                (f, run_id),
            )
            inserted += 1
        except psycopg2.IntegrityError:
            pass  # already exists for this run_id (resume case)
    db.commit()
    logger.info("Seeded queue: %d new, %d total formulations", inserted, len(formulations))


# ---------------------------------------------------------------------------
# Resume support
# ---------------------------------------------------------------------------

def find_resumable_run(db, scraper_name: str = "India") -> Optional[str]:
    """Find most recent run that can be resumed.

    Checks for:
      - 'resume'      : explicitly marked resumable (clean stop with pending work)
      - 'running'     : hard crash (process killed before cleanup)
      - 'partial'     : partially completed
      - 'interrupted' : fallback if cleanup set this (old runs / edge cases)
      - 'failed'      : worker failure with pending formulations remaining

    sync_queue_on_resume will correctly recompute remaining work regardless
    of which status triggered the resume.
    """
    try:
        cur = db.execute(
            "SELECT run_id FROM run_ledger "
            "WHERE scraper_name = %s AND status IN ('resume', 'running', 'partial', 'interrupted', 'failed') "
            "ORDER BY started_at DESC LIMIT 1",
            (scraper_name,),
        )
        row = cur.fetchone()
        return row[0] if row else None
    except Exception:
        return None


def recover_stale_claims(db, run_id: str, stale_minutes: int = 10) -> int:
    """Reset in_progress claims older than stale_minutes back to pending."""
    cur = db.execute("""
        UPDATE in_formulation_status
        SET status = 'pending', claimed_by = NULL, claimed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND status = 'in_progress'
          AND claimed_at < CURRENT_TIMESTAMP - (%s * INTERVAL '1 minute')
    """, (run_id, stale_minutes))
    recovered = cur.rowcount
    db.commit()
    if recovered > 0:
        logger.info("Recovered %d stale claims (older than %d min)", recovered, stale_minutes)
    return recovered


def reset_all_in_progress(db, run_id: str) -> int:
    """Reset ALL in_progress claims back to pending.

    Called on resume because all previous workers are dead — there is no
    running worker that could still be processing these items.  Without this,
    items claimed just before a crash/stop would stay 'in_progress' forever
    if the resume happens within the stale_minutes window.
    """
    cur = db.execute("""
        UPDATE in_formulation_status
        SET status = 'pending', claimed_by = NULL, claimed_at = NULL,
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND status = 'in_progress'
    """, (run_id,))
    reset_count = cur.rowcount
    db.commit()
    if reset_count > 0:
        logger.info("Reset %d in_progress claims to pending (all previous workers dead)", reset_count)
    return reset_count


def get_failed_count(db, run_id: str) -> int:
    """Get count of failed formulations for this run."""
    cur = db.execute(
        "SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s AND status = 'failed'",
        (run_id,),
    )
    return cur.fetchone()[0]


def get_zero_records_count(db, run_id: str) -> int:
    """Get count of zero_records formulations for this run."""
    cur = db.execute(
        "SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s AND status = 'zero_records'",
        (run_id,),
    )
    return cur.fetchone()[0]


def get_completion_stats(db, run_id: str) -> dict:
    """Get completion statistics for this run."""
    cur = db.execute(
        "SELECT status, COUNT(*) FROM in_formulation_status WHERE run_id = %s GROUP BY status",
        (run_id,),
    )
    counts = dict(cur.fetchall())
    total = sum(counts.values())
    completed = counts.get("completed", 0)
    zero_rec = counts.get("zero_records", 0)
    failed = counts.get("failed", 0)
    pending = counts.get("pending", 0)
    in_progress = counts.get("in_progress", 0)
    done = completed + zero_rec + failed
    pct = round((done / total) * 100, 2) if total > 0 else 0
    return {
        "total": total,
        "completed": completed,
        "zero_records": zero_rec,
        "failed": failed,
        "pending": pending,
        "in_progress": in_progress,
        "done": done,
        "completion_pct": pct,
    }


def reset_failed_to_pending(db, run_id: str, max_attempts: int = 5) -> int:
    """Reset failed formulations back to pending for retry (only if attempts < max)."""
    cur = db.execute("""
        UPDATE in_formulation_status
        SET status = 'pending', claimed_by = NULL, claimed_at = NULL,
            worker_id = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND status = 'failed' AND attempts < %s
    """, (run_id, max_attempts))
    reset_count = cur.rowcount
    db.commit()
    return reset_count


def reset_zero_records_to_pending(db, run_id: str) -> int:
    """Reset zero_records formulations back to pending for one final retry."""
    cur = db.execute("""
        UPDATE in_formulation_status
        SET status = 'pending', claimed_by = NULL, claimed_at = NULL,
            worker_id = NULL, attempts = 0, updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s AND status = 'zero_records'
    """, (run_id,))
    reset_count = cur.rowcount
    db.commit()
    return reset_count


def sync_queue_on_resume(db, run_id: str, limit: int = None) -> int:
    """
    RESUME ONLY: Compute the remaining list ONCE and store it in the queue.

    Logic (executed exactly once per resume click):
        1. Load ALL entries from input table (in_input_formulations)
        2. Get unique formulations already scraped (from in_sku_main, any run)
        3. Get formulations already processed in current run's queue
           (completed, zero_records — so we don't re-process them)
        4. Exclude both sets from input → remaining list
        5. Store remaining as 'pending' in in_formulation_status queue
        6. Workers claim from this pre-built queue — no per-batch recomputation

    Returns number of formulations left to scrape (pending).
    """
    logger.info("[RESUME] Computing remaining list ONCE (input - already_scraped)...")

    # Step 1: All entries from input
    formulations = _load_formulations(db, limit)
    logger.info("[RESUME] Input list: %d unique formulations", len(formulations))

    # Step 2: Unique formulations already scraped (have data in in_sku_main, any run)
    already_scraped = get_already_scraped_formulations(db, use_latest_completed_run=False)
    logger.info("[RESUME] Already scraped (in_sku_main): %d unique formulations", len(already_scraped))

    # Step 3: Formulations already processed in current run's queue (terminal statuses)
    cur = db.execute(
        "SELECT formulation, status FROM in_formulation_status WHERE run_id = %s",
        (run_id,),
    )
    queue_rows = cur.fetchall()
    in_queue = {row[0] for row in queue_rows if row[0]}
    already_processed = {row[0] for row in queue_rows
                         if row[0] and row[1] in ('completed', 'zero_records')}
    logger.info("[RESUME] Already processed in current run queue: %d", len(already_processed))

    # Step 4: Exclude already scraped + already processed → remaining
    exclude_set = already_scraped | already_processed
    remaining = [f for f in formulations if f not in exclude_set]
    logger.info("[RESUME] Remaining to scrape: %d (excluded %d)",
                len(remaining), len(formulations) - len(remaining))

    # Mark any queued items that have data in in_sku_main as 'completed'
    if already_scraped:
        scraped_in_queue = already_scraped & in_queue
        if scraped_in_queue:
            db.execute(
                "UPDATE in_formulation_status SET status = 'completed', updated_at = CURRENT_TIMESTAMP "
                "WHERE run_id = %s AND formulation = ANY(%s) AND status NOT IN ('completed', 'zero_records')",
                (run_id, list(scraped_in_queue)),
            )
    db.commit()

    # Step 5: Insert remaining as 'pending' (skip if already in queue)
    inserted = 0
    for f in remaining:
        if f not in in_queue:
            try:
                db.execute(
                    "INSERT INTO in_formulation_status (formulation, run_id, status) "
                    "VALUES (%s, %s, 'pending') ON CONFLICT (formulation, run_id) DO NOTHING",
                    (f, run_id),
                )
                inserted += 1
                in_queue.add(f)
            except psycopg2.IntegrityError:
                pass
    db.commit()

    # Step 6: Count what's actually pending in queue (ready for workers to claim)
    cur = db.execute(
        "SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s AND status IN ('pending', 'in_progress')",
        (run_id,),
    )
    pending_count = cur.fetchone()[0]

    logger.info(
        "[RESUME] Queue ready: input=%d, excluded=%d, remaining=%d, "
        "newly_added=%d, total_pending=%d (workers will claim from this pre-built queue)",
        len(formulations), len(exclude_set), len(remaining), inserted, pending_count,
    )
    return pending_count


# ---------------------------------------------------------------------------
# Progress reporter (background thread)
# ---------------------------------------------------------------------------

class ProgressReporter(threading.Thread):
    """Background thread that prints progress and writes snapshots to DB."""

    def __init__(self, db, run_id: str, interval: float = 5.0,
                 snapshot_interval: float = 30.0,
                 stale_minutes: int = 0,
                 stale_recover_interval: float = 60.0):
        super().__init__(daemon=True)
        self.db = db
        self.run_id = run_id
        self.interval = interval
        self.snapshot_interval = snapshot_interval
        self.stale_minutes = int(stale_minutes or 0)
        self.stale_recover_interval = float(stale_recover_interval)
        self._stop_event = threading.Event()
        self._start_time = time.monotonic()
        self._last_snapshot = 0.0
        self._last_completed = 0
        self._last_stale_recover = 0.0

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.wait(self.interval):
            try:
                # Recover stale in_progress rows so a crashed worker doesn't stall the run forever.
                now = time.monotonic()
                if self.stale_minutes > 0 and (now - self._last_stale_recover) >= self.stale_recover_interval:
                    recover_stale_claims(self.db, self.run_id, stale_minutes=self.stale_minutes)
                    self._last_stale_recover = now

                cur = self.db.execute(
                    "SELECT status, COUNT(*) FROM in_formulation_status "
                    "WHERE run_id = %s GROUP BY status", (self.run_id,)
                )
                counts = dict(cur.fetchall())

                pending = counts.get("pending", 0)
                in_prog = counts.get("in_progress", 0)
                completed = counts.get("completed", 0)
                failed = counts.get("failed", 0)
                blocked = counts.get("blocked", 0) + counts.get("blocked_captcha", 0)
                zero_rec = counts.get("zero_records", 0)
                done = completed + zero_rec + failed + blocked
                total = done + pending + in_prog

                # Rate calculation
                elapsed = time.monotonic() - self._start_time
                rate = (done / (elapsed / 60.0)) if elapsed > 10 else 0
                remaining = total - done
                eta_min = (remaining / rate) if rate > 0 else 0

                eta_str = f"ETA {int(eta_min)}m" if rate > 0 else "ETA --"

                # Per-worker breakdown
                worker_parts = []
                try:
                    cur_w = self.db.execute(
                        "SELECT claimed_by, status, COUNT(*) FROM in_formulation_status "
                        "WHERE run_id = %s AND claimed_by IS NOT NULL "
                        "GROUP BY claimed_by, status ORDER BY claimed_by",
                        (self.run_id,),
                    )
                    worker_data: dict = {}
                    for wid, wstatus, wcount in cur_w.fetchall():
                        worker_data.setdefault(wid, {})
                        worker_data[wid][wstatus] = wcount
                    for wid in sorted(worker_data):
                        wd = worker_data[wid]
                        w_done = sum(wd.get(s, 0) for s in ("completed", "zero_records", "failed", "blocked", "blocked_captcha"))
                        w_ip = wd.get("in_progress", 0)
                        worker_parts.append(f"W{wid}:{w_done}d/{w_ip}ip")
                except Exception:
                    pass

                pct = round((done / total) * 100, 1) if total > 0 else 0
                worker_str = " | ".join(worker_parts) if worker_parts else ""

                # Format: "[PROGRESS] Formulations: X/Y (Z%) - [W1:2d/3ip | W2:1d/5ip] | rate | ETA"
                # Matches GUI regex: \[PROGRESS\]\s+(.+?)\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*(?:-\s*(.+))?
                suffix = f"[{worker_str}] | {rate:.1f}/min | {eta_str}" if worker_str else f"{rate:.1f}/min | {eta_str}"
                print(
                    f"[PROGRESS] Formulations: {done}/{total} ({pct}%) - {suffix}",
                    flush=True,
                )

                # Snapshot to DB
                now = time.monotonic()
                if now - self._last_snapshot >= self.snapshot_interval:
                    # Get items_scraped count
                    try:
                        cur2 = self.db.execute(
                            "SELECT COUNT(*) FROM in_sku_main WHERE run_id = %s", (self.run_id,)
                        )
                        items = cur2.fetchone()[0]
                    except Exception:
                        items = 0

                    self.db.execute(
                        "INSERT INTO in_progress_snapshots "
                        "(run_id, pending, in_progress, completed, failed, blocked, zero_records, items_scraped, rate_per_min) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (self.run_id, pending, in_prog, completed, failed, blocked, zero_rec, items, round(rate, 2)),
                    )
                    # Also update run_ledger.items_scraped periodically
                    self.db.execute(
                        "UPDATE run_ledger SET items_scraped = %s WHERE run_id = %s",
                        (items, self.run_id),
                    )
                    self.db.commit()
                    self._last_snapshot = now
            except Exception as exc:
                logger.debug("Progress reporter error: %s", exc)


# ---------------------------------------------------------------------------
# Spider launcher
# ---------------------------------------------------------------------------

def run_spider_worker(run_id: str, worker_id: int,
                      limit: int = None, use_jobdir: bool = True) -> int:
    """Launch one Scrapy spider subprocess with a worker_id."""
    scrapy_project = _repo_root / "scrapy_project"
    output_dir = get_output_dir()
    platform_run_id = os.getenv("PLATFORM_RUN_ID", "").strip()

    cmd = [
        sys.executable, "-m", "scrapy", "crawl", "india_details",
        "-a", f"run_id={run_id}",
        "-a", f"worker_id={worker_id}",
    ]

    if platform_run_id:
        cmd.extend(["-a", f"platform_run_id={platform_run_id}"])

    if limit:
        cmd.extend(["-a", f"limit={limit}"])

    if use_jobdir:
        job_dir = output_dir / f".scrapy_job_w{worker_id}"
        cmd.extend(["-s", f"JOBDIR={job_dir}"])

    logger.info("Worker %d: %s", worker_id, " ".join(str(c) for c in cmd))

    result = subprocess.run(
        cmd,
        cwd=str(scrapy_project),
        env={**os.environ, "PYTHONPATH": str(_repo_root)},
    )
    return result.returncode


def main():
    load_env_file()
    
    # VPN Check (optional)
    from config_loader import getenv_bool, check_vpn_connection
    vpn_required = getenv_bool("VPN_REQUIRED", False)
    vpn_check_enabled = getenv_bool("VPN_CHECK_ENABLED", False)
    
    if vpn_check_enabled:
        print("[INIT] VPN check enabled, verifying connection...", flush=True)
        if not check_vpn_connection():
            print("[FATAL] VPN connection check failed. Please connect VPN or set VPN_CHECK_ENABLED=false", flush=True)
            sys.exit(1)
    elif vpn_required:
        print("[INIT] VPN required but check disabled. Ensure VPN is connected.", flush=True)
    else:
        print("[INIT] VPN not required, running without VPN check", flush=True)
    
    args = parse_args()
    output_dir = get_output_dir()
    workers = max(1, args.workers)

    use_jobdir = args.jobdir or (os.name != "nt")

    # Read config values from env
    from config_loader import getenv_int
    stale_minutes = getenv_int("STALE_CLAIM_MINUTES", 10)

    # --- Initialize DB schemas ---
    from core.db.postgres_connection import PostgresDB
    from core.db.models import (
        apply_common_schema, generate_run_id, run_ledger_start,
        run_ledger_finish, run_ledger_resume,
    )
    from core.db.schema_registry import SchemaRegistry

    db = PostgresDB("India")
    db.connect()
    apply_common_schema(db)

    registry = SchemaRegistry(db)

    # Apply inputs schema first (no migration dependencies)
    inputs_schema = _repo_root / "sql" / "schemas" / "postgres" / "inputs.sql"
    if inputs_schema.exists():
        registry.apply_schema(inputs_schema)

    # Apply india schema (creates tables with latest columns on fresh DB)
    india_schema = _repo_root / "sql" / "schemas" / "postgres" / "india.sql"
    if india_schema.exists():
        try:
            registry.apply_schema(india_schema)
        except Exception:
            # On existing DB, new indexes may fail if migration hasn't run yet — that's OK
            logger.debug("Schema apply had non-fatal errors (migration will fix)")

    # Apply pending migrations (adds new columns/indexes to existing tables)
    migrations_dir = _repo_root / "sql" / "migrations" / "postgres"
    if migrations_dir.exists():
        registry.apply_pending(migrations_dir)

    # Re-apply india schema after migrations to ensure all indexes exist
    if india_schema.exists():
        try:
            registry.apply_schema(india_schema)
        except Exception:
            pass

    # --- Resume or fresh run ---
    mode = "fresh"
    resumable_run = None if args.fresh else find_resumable_run(db)

    if resumable_run:
        run_id = resumable_run
        mode = "resume"
        logger.info("Resuming run: %s", run_id)

        # Resume run ledger
        sql, params = run_ledger_resume(run_id)
        db.execute(sql, params)

        # Mark other 'resume' runs as 'stopped' (only one can be active)
        db.execute(
            "UPDATE run_ledger SET status = 'stopped' "
            "WHERE scraper_name = 'India' AND status = 'resume' AND run_id != %s",
            (run_id,)
        )
        db.commit()

        # Reset ALL in_progress claims to pending — all previous workers are dead.
        # (recover_stale_claims only resets items older than N minutes, which misses
        #  items claimed just before crash/stop if resume happens quickly)
        reset_all_in_progress(db, run_id)

        if args.retry_zero_records:
            try:
                cur = db.execute(
                    "UPDATE in_formulation_status "
                    "SET status='pending', claimed_by=NULL, claimed_at=NULL, worker_id=NULL, "
                    "attempts=0, updated_at=CURRENT_TIMESTAMP "
                    "WHERE run_id=%s AND status='zero_records'",
                    (run_id,),
                )
                db.commit()
                logger.info("Reset %d zero_records rows back to pending for retry", cur.rowcount)
            except Exception as exc:
                logger.warning("Failed resetting zero_records rows: %s", exc)

        if args.retry_failed:
            try:
                # Only retry if attempts < max_retries
                cur = db.execute(
                    "UPDATE in_formulation_status "
                    "SET status='pending', claimed_by=NULL, claimed_at=NULL, worker_id=NULL, "
                    "updated_at=CURRENT_TIMESTAMP "
                    "WHERE run_id=%s AND status='failed' AND attempts < %s",
                    (run_id, args.max_retries),
                )
                db.commit()
                logger.info("Reset %d failed rows back to pending for retry (max attempts: %d)",
                           cur.rowcount, args.max_retries)
            except Exception as exc:
                logger.warning("Failed resetting failed rows: %s", exc)

        # RESUME: Compute remaining list ONCE and build the queue.
        # Workers will claim from this pre-built queue — no per-batch recomputation.
        # remaining = input - already_scraped - already_processed_in_this_run
        # BUT: Skip sync_queue_on_resume for Step 2, because Step 2 retries formulations that were already processed
        pipeline_step = getattr(args, 'step', None) or 1
        if pipeline_step != 2:
            pending_count = sync_queue_on_resume(db, run_id, limit=args.limit)
            print(f"[DB] RESUME | run_id={run_id} | remaining list computed once | {pending_count} pending", flush=True)
            if pending_count == 0:
                logger.info("No formulations left to scrape (all input already scraped or processed)")
                db.close()
                sys.exit(0)
        else:
            # Step 2: Don't sync queue - we're retrying formulations that are already in the queue
            # Just verify there are pending formulations to retry
            cur = db.execute("""
                SELECT COUNT(*) FROM in_formulation_status 
                WHERE run_id = %s AND status IN ('pending', 'failed', 'zero_records')
            """, (run_id,))
            pending_count = cur.fetchone()[0] or 0
            print(f"[DB] STEP 2 | run_id={run_id} | {pending_count} formulations available for retry (pending/failed/zero_records)", flush=True)
    else:
        run_id = generate_run_id()
        sql, params = run_ledger_start(run_id, "India", mode=mode, thread_count=workers)
        db.execute(sql, params)
        db.commit()

        # FRESH = ALL formulations from input (no exclusion)
        formulations = _load_formulations(db, args.limit)
        if not formulations:
            raise SystemExit("No formulations to process")

        seed_formulation_queue(db, formulations, run_id)
        print(f"[DB] FRESH | {len(formulations)} formulations queued (all from input) | run_id={run_id}", flush=True)

    try:
        (output_dir / "last_run_id.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    except Exception as exc:
        logger.debug("Failed writing last_run_id.json: %s", exc)

    # --- Cleanup function to update run_ledger on exit (crash-safe) ---
    _cleanup_done = False

    def cleanup_run_ledger(status: str = "interrupted"):
        nonlocal _cleanup_done
        if _cleanup_done:
            return
        _cleanup_done = True
        try:
            # Count items
            cur = db.execute("SELECT COUNT(*) FROM in_sku_main WHERE run_id = %s", (run_id,))
            total_items = cur.fetchone()[0]
            # Build totals_json
            cur = db.execute(
                "SELECT status, COUNT(*) FROM in_formulation_status WHERE run_id = %s GROUP BY status",
                (run_id,),
            )
            totals = dict(cur.fetchall())
            totals["items_scraped"] = total_items
            totals_str = json.dumps(totals)

            # If there are still pending/in_progress formulations, mark as 'resume'
            # so the run can be found and resumed later (even if the parent process exits cleanly).
            pending = totals.get("pending", 0) + totals.get("in_progress", 0)
            if pending > 0 and status in ("completed", "interrupted", "failed"):
                final_status = "resume"
            else:
                final_status = status

            sql, params = run_ledger_finish(run_id, final_status, items_scraped=total_items,
                                             totals_json=totals_str)
            db.execute(sql, params)
            db.commit()
            print(f"[DB] FINISH | run_ledger updated | status={final_status} items={total_items} pending={pending}", flush=True)
        except Exception as exc:
            logger.debug("Cleanup error: %s", exc)

    # Register cleanup on exit so crash/kill still updates run_ledger (resume or interrupted)
    atexit.register(lambda: cleanup_run_ledger("interrupted"))

    worker_failed = False
    interrupted = False
    reporter = None

    # --- Auto-retry settings ---
    MAX_AUTO_RETRIES = 5  # Max retry rounds for failed formulations
    auto_retry_round = 0

    def run_workers_once() -> bool:
        """Run all workers once. Returns True if any worker failed."""
        nonlocal worker_failed
        any_failed = False

        if workers == 1:
            rc = run_spider_worker(run_id, worker_id=1, limit=args.limit, use_jobdir=use_jobdir)
            if rc != 0:
                logger.error("Spider exited with code %d", rc)
                any_failed = True
        else:
            results = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {}
                for wid in range(1, workers + 1):
                    future = executor.submit(run_spider_worker, run_id, wid, args.limit, use_jobdir)
                    futures[future] = wid

                for future in concurrent.futures.as_completed(futures):
                    wid = futures[future]
                    try:
                        rc = future.result()
                        results[wid] = rc
                        status = "OK" if rc == 0 else f"FAILED (rc={rc})"
                        logger.info("Worker %d: %s", wid, status)
                    except Exception as exc:
                        results[wid] = 1
                        logger.error("Worker %d: EXCEPTION %s", wid, exc)

            failed_worker_count = sum(1 for rc in results.values() if rc != 0)
            if failed_worker_count:
                logger.error("%d/%d workers failed", failed_worker_count, len(results))
                any_failed = True

        return any_failed

    try:
        # --- Start progress reporter ---
        reporter = ProgressReporter(db, run_id, stale_minutes=stale_minutes)
        reporter.start()

        # Determine which step to run
        pipeline_step = getattr(args, 'step', None) or 1
        
        if pipeline_step == 1:
            # --- STEP 1: Initial scrape only ---
            print(f"\n{'='*60}", flush=True)
            print(f"STEP 1: INITIAL SCRAPE", flush=True)
            print(f"{'='*60}", flush=True)
            logger.info("=== STEP 1: Initial scrape (all formulations) ===")
            logger.info("Launching %d worker(s) (run_id=%s, mode=%s)", workers, run_id, mode)
            print(f"[STEP 1] Launching {workers} worker(s) to scrape all pending formulations...", flush=True)
            worker_failed = run_workers_once()
            
            # Show stats after Step 1
            stats = get_completion_stats(db, run_id)
            print(f"\n{'='*60}", flush=True)
            print(f"STEP 1 COMPLETE - INITIAL SCRAPE STATS", flush=True)
            print(f"{'='*60}", flush=True)
            print(f"  Total Formulations:  {stats['total']:,}", flush=True)
            print(f"  Completed:           {stats['completed']:,}", flush=True)
            print(f"  Zero Records:        {stats['zero_records']:,}", flush=True)
            print(f"  Failed:              {stats['failed']:,}", flush=True)
            print(f"  Pending:             {stats['pending']:,}", flush=True)
            print(f"  Completion:          {stats['completion_pct']}%", flush=True)
            print(f"{'='*60}", flush=True)
            print(f"\nNext: Run Step 2 to retry failed + zero_records", flush=True)
            
        elif pipeline_step == 2:
            # --- STEP 2: Retry failed + zero_records + process any remaining pending ---
            print(f"\n{'='*60}", flush=True)
            print(f"STEP 2: RETRY FAILED + ZERO RECORDS + REMAINING PENDING", flush=True)
            print(f"{'='*60}", flush=True)
            logger.info("=== STEP 2: Retry failed + zero_records + remaining pending ===")
            
            # Get counts before retry
            failed_count = get_failed_count(db, run_id)
            zero_count = get_zero_records_count(db, run_id)
            
            # Also check for remaining pending formulations from Step 1
            cur = db.execute("""
                SELECT COUNT(*) FROM in_formulation_status 
                WHERE run_id = %s AND status = 'pending'
            """, (run_id,))
            pending_count = cur.fetchone()[0] or 0
            
            total_to_process = failed_count + zero_count + pending_count
            
            if total_to_process == 0:
                print(f"\n[STEP 2] No formulations to process (failed={failed_count}, zero_records={zero_count}, pending={pending_count})", flush=True)
                logger.info("Step 2: No formulations to process, skipping")
                # Still need to show final stats
                stats = get_completion_stats(db, run_id)
                print(f"\n{'='*60}", flush=True)
                print(f"STEP 2 COMPLETE - FINAL STATS", flush=True)
                print(f"{'='*60}", flush=True)
                print(f"  Total Formulations:  {stats['total']:,}", flush=True)
                print(f"  Completed:           {stats['completed']:,}", flush=True)
                print(f"  Zero Records:        {stats['zero_records']:,}", flush=True)
                print(f"  Failed:              {stats['failed']:,}", flush=True)
                print(f"  Pending:             {stats['pending']:,}", flush=True)
                print(f"  Completion:          {stats['completion_pct']}%", flush=True)
                print(f"{'='*60}", flush=True)
                print(f"\nNext: Run Step 3 for QC and CSV export", flush=True)
            else:
                print(f"\n[STEP 2] Processing {total_to_process} formulations ({failed_count} failed + {zero_count} zero_records + {pending_count} pending)...", flush=True)
                
                # Reset failed to pending (up to MAX_AUTO_RETRIES attempts)
                failed_reset = 0
                if failed_count > 0:
                    failed_reset = reset_failed_to_pending(db, run_id, max_attempts=MAX_AUTO_RETRIES)
                    if failed_reset > 0:
                        logger.info("Reset %d failed formulations to pending", failed_reset)
                        print(f"[STEP 2] Reset {failed_reset} failed formulations to pending", flush=True)
                        # Verify they're claimable
                        cur = db.execute("""
                            SELECT COUNT(*) FROM in_formulation_status 
                            WHERE run_id = %s AND status = 'pending' 
                            AND (claimed_at IS NULL OR claimed_at <= CURRENT_TIMESTAMP)
                        """, (run_id,))
                        claimable = cur.fetchone()[0] or 0
                        print(f"[STEP 2]   -> {claimable} formulations are now claimable", flush=True)
                
                # Reset zero_records to pending (once only - check flag)
                zero_reset = 0
                if zero_count > 0:
                    # Check if zero_records retry was already done
                    zero_retry_done = False
                    try:
                        cur = db.execute(
                            "SELECT metadata_json FROM run_ledger WHERE run_id = %s",
                            (run_id,)
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            meta = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                            zero_retry_done = meta.get('zero_records_retried') == True
                    except Exception:
                        pass
                    
                    if zero_retry_done:
                        logger.info("Zero records retry already done, skipping")
                        print(f"[STEP 2] Zero records already retried once, skipping ({zero_count} zero_records)", flush=True)
                    else:
                        zero_reset = reset_zero_records_to_pending(db, run_id)
                        if zero_reset > 0:
                            logger.info("Reset %d zero_records formulations to pending", zero_reset)
                            print(f"[STEP 2] Reset {zero_reset} zero_records formulations to pending", flush=True)
                            # Verify they're claimable
                            cur = db.execute("""
                                SELECT COUNT(*) FROM in_formulation_status 
                                WHERE run_id = %s AND status = 'pending' 
                                AND (claimed_at IS NULL OR claimed_at <= CURRENT_TIMESTAMP)
                            """, (run_id,))
                            claimable = cur.fetchone()[0] or 0
                            print(f"[STEP 2]   -> {claimable} formulations are now claimable", flush=True)
                            
                            # Mark that we're doing zero_records retry
                            try:
                                cur = db.execute("SELECT metadata_json FROM run_ledger WHERE run_id = %s", (run_id,))
                                row = cur.fetchone()
                                meta = json.loads(row[0]) if row and row[0] else {}
                                meta['zero_records_retried'] = True
                                db.execute(
                                    "UPDATE run_ledger SET metadata_json = %s WHERE run_id = %s",
                                    (json.dumps(meta), run_id)
                                )
                                db.commit()
                            except Exception as exc:
                                logger.warning("Could not mark zero_records_retried: %s", exc)
                
                # Run workers if we have anything to process (retries + pending)
                # IMPORTANT: Always process pending formulations, even if no retries
                total_to_process = failed_reset + zero_reset + pending_count
                if total_to_process > 0 or pending_count > 0:
                    logger.info("Launching %d worker(s) for Step 2 (run_id=%s, %d formulations to process: %d retries + %d pending)", 
                               workers, run_id, total_to_process, failed_reset + zero_reset, pending_count)
                    print(f"\n[STEP 2] Launching {workers} worker(s) to process {total_to_process} formulations ({failed_reset + zero_reset} retries + {pending_count} pending)...", flush=True)
                    print(f"[STEP 2] Workers will perform same scraping process as Step 1:", flush=True)
                    print(f"  - Warm-up GET to establish session", flush=True)
                    print(f"  - Fetch formulation list from API", flush=True)
                    print(f"  - Claim pending formulations from queue", flush=True)
                    print(f"  - Search and scrape each formulation", flush=True)
                    
                    # Small delay to ensure DB commit is visible to workers
                    import time
                    time.sleep(0.5)
                    
                    # Verify formulations are actually pending and claimable
                    cur = db.execute("""
                        SELECT COUNT(*) FROM in_formulation_status 
                        WHERE run_id = %s AND status = 'pending' 
                        AND (claimed_at IS NULL OR claimed_at <= CURRENT_TIMESTAMP)
                    """, (run_id,))
                    claimable_count = cur.fetchone()[0] or 0
                    print(f"[STEP 2] Verified: {claimable_count} formulations are claimable (pending with no backoff)", flush=True)
                    
                    if claimable_count == 0 and total_to_process > 0:
                        print(f"[STEP 2] WARNING: No claimable formulations found! Checking status...", flush=True)
                        cur = db.execute("""
                            SELECT status, COUNT(*) FROM in_formulation_status 
                            WHERE run_id = %s GROUP BY status
                        """, (run_id,))
                        status_counts = dict(cur.fetchall())
                        for status, count in status_counts.items():
                            print(f"[STEP 2]   {status}: {count}", flush=True)
                        print(f"[STEP 2] ERROR: Expected {total_to_process} claimable formulations but found 0!", flush=True)
                        print(f"[STEP 2] This may indicate a database issue. Workers will still be launched...", flush=True)
                    
                    worker_failed = run_workers_once()
                    
                    # Show recovery stats
                    new_failed = get_failed_count(db, run_id)
                    new_zero = get_zero_records_count(db, run_id)
                    cur = db.execute("SELECT COUNT(*) FROM in_formulation_status WHERE run_id = %s AND status = 'pending'", (run_id,))
                    new_pending = cur.fetchone()[0] or 0
                    
                    failed_recovered = failed_count - new_failed if failed_count > 0 else 0
                    zero_recovered = zero_count - new_zero if zero_count > 0 else 0
                    pending_processed = pending_count - new_pending if pending_count > 0 else 0
                    
                    print(f"\n{'='*60}", flush=True)
                    print(f"STEP 2 COMPLETE - PROCESSING RESULTS", flush=True)
                    print(f"{'='*60}", flush=True)
                    if failed_count > 0:
                        print(f"  Failed Recovered:    {failed_recovered}/{failed_count}", flush=True)
                    if zero_count > 0:
                        print(f"  Zero Records Recovered: {zero_recovered}/{zero_count}", flush=True)
                    if pending_count > 0:
                        print(f"  Pending Processed:   {pending_processed}/{pending_count}", flush=True)
                    print(f"  Still Failed:        {new_failed}", flush=True)
                    print(f"  Still Zero Records:  {new_zero}", flush=True)
                    print(f"  Still Pending:       {new_pending}", flush=True)
                    print(f"{'='*60}", flush=True)
                else:
                    print(f"[STEP 2] No formulations to process", flush=True)
            
            # Final stats after Step 2
            stats = get_completion_stats(db, run_id)
            print(f"\n{'='*60}", flush=True)
            print(f"STEP 2 COMPLETE - FINAL STATS", flush=True)
            print(f"{'='*60}", flush=True)
            print(f"  Total Formulations:  {stats['total']:,}", flush=True)
            print(f"  Completed:           {stats['completed']:,}", flush=True)
            print(f"  Zero Records:        {stats['zero_records']:,}", flush=True)
            print(f"  Failed:              {stats['failed']:,}", flush=True)
            print(f"  Pending:             {stats['pending']:,}", flush=True)
            print(f"  Completion:          {stats['completion_pct']}%", flush=True)
            print(f"{'='*60}", flush=True)
            print(f"\nNext: Run Step 3 for QC and CSV export", flush=True)

        # Step-based logic is complete above
        # No additional retry loops needed - Step 2 handles all retries
        
        # Warn if not 100% complete (only for Step 1)
        if pipeline_step == 1:
            stats = get_completion_stats(db, run_id)
            if stats['pending'] > 0 or stats['in_progress'] > 0:
                logger.warning("Step 1 not 100%% complete: %d pending, %d in_progress",
                              stats['pending'], stats['in_progress'])
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        interrupted = True
    except Exception as exc:
        logger.exception("Unhandled exception; updating run_ledger and exiting")
        cleanup_run_ledger("failed")
        try:
            db.close()
        except Exception:
            pass
        raise
    finally:
        # --- Stop progress reporter ---
        try:
            if reporter is not None:
                reporter.stop()
                reporter.join(timeout=5)
        except Exception:
            pass

        # --- Finalize run ledger ---
        if not _cleanup_done:
            if interrupted:
                cleanup_run_ledger("interrupted")
            else:
                final_status = "completed" if not worker_failed else "failed"
                cleanup_run_ledger(final_status)
            logger.info("Scraping complete.")

        # --- Generate end-of-run statistics ---
        if not interrupted:
            try:
                logger.info("Generating run statistics...")
                from generate_stats import get_run_info, get_high_level_stats, get_per_formulation_stats, print_stats
                run_info = get_run_info(db, run_id)
                high_level = get_high_level_stats(db, run_id)
                per_formulation = get_per_formulation_stats(db, run_id, limit=100)
                print_stats(run_info, high_level, per_formulation, top_n=20)
            except Exception as exc:
                logger.warning("Failed to generate statistics: %s", exc)

        # Close DB connection
        try:
            db.close()
        except Exception:
            pass

    if worker_failed:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        logger.exception("India scraper crashed: %s", exc)

        # Write crash log to output directory for post-mortem debugging
        try:
            load_env_file()
            crash_log_path = get_output_dir() / "crash_log.json"
            crash_log_path.parent.mkdir(parents=True, exist_ok=True)
            import datetime
            crash_data = {
                "timestamp": datetime.datetime.now().isoformat(),
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
                "python_version": sys.version,
                "args": sys.argv,
            }
            # Append to existing crash log (keep last 20 entries)
            existing = []
            if crash_log_path.exists():
                try:
                    existing = json.loads(crash_log_path.read_text(encoding="utf-8"))
                    if not isinstance(existing, list):
                        existing = [existing]
                except Exception:
                    existing = []
            existing.append(crash_data)
            existing = existing[-20:]  # Keep last 20 crash entries
            crash_log_path.write_text(
                json.dumps(existing, indent=2, default=str),
                encoding="utf-8",
            )
            logger.info("Crash log saved to %s", crash_log_path)
        except Exception as log_exc:
            logger.warning("Failed to write crash log: %s", log_exc)

        sys.exit(1)
