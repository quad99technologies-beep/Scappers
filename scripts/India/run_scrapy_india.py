#!/usr/bin/env python3
"""
India NPPA: Seed formulations queue and launch parallel Scrapy workers.

Flow:
    1. Initialize DB + seed formulations into formulation_status (pending)
    2. Launch N spider subprocesses, each with a unique worker_id
    3. Each spider claims work from the queue atomically (no double-scraping)

Usage:
    python run_scrapy_india.py                    # 1 worker, input from DB
    python run_scrapy_india.py --workers 5        # 5 parallel workers
    python run_scrapy_india.py --limit 20         # Only 20 formulations
    python run_scrapy_india.py --formulations-file path/to/file.csv
"""

import argparse
import atexit
import concurrent.futures
import csv
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import List, Optional

import psycopg2

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import load_env_file, get_output_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("run_scrapy_india")


def parse_args():
    parser = argparse.ArgumentParser(description="Run India NPPA Scrapy spider")
    parser.add_argument("--formulations-file", type=str, help="Path to formulations CSV (overrides DB)")
    parser.add_argument("--fresh", action="store_true", help="Start fresh run (new run_id)")
    parser.add_argument("--limit", type=int, help="Limit number of formulations")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel spider workers")
    parser.add_argument("--jobdir", action="store_true")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# DB seeding: load formulations and insert as 'pending' into formulation_status
# ---------------------------------------------------------------------------

FORMULATION_COLUMN_CANDIDATES = ["formulation", "name", "generic_name", "generic name", "medicine", "drug"]


def _load_formulations_from_csv(path: Path, limit: int = None) -> List[str]:
    """Load formulation names from a CSV file."""
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError(f"CSV missing header: {path}")
        candidates = [col.strip() for col in reader.fieldnames if col]
        field = next(
            (col for col in candidates if col.lower() in FORMULATION_COLUMN_CANDIDATES),
            candidates[0],
        )
        formulations = []
        for row in reader:
            value = (row.get(field) or "").strip()
            if value:
                formulations.append(value)
            if limit and len(formulations) >= limit:
                break
    return formulations


def _load_formulations(db, csv_file: Optional[str], limit: int = None) -> List[str]:
    """Load formulations: explicit CSV > DB input_formulations > legacy CSV fallback."""
    # Priority 1: explicit CSV
    if csv_file:
        path = Path(csv_file)
        if not path.exists():
            raise SystemExit(f"Formulations file not found: {path}")
        forms = _load_formulations_from_csv(path, limit)
        logger.info("Loaded %d formulations from CSV: %s", len(forms), path)
        return forms

    # Priority 2: env var
    env_file = os.getenv("FORMULATIONS_FILE")
    if env_file:
        path = Path(env_file)
        forms = _load_formulations_from_csv(path, limit)
        logger.info("Loaded %d formulations from env CSV: %s", len(forms), path)
        return forms

    # Priority 3: DB input_formulations table
    try:
        cur = db.execute("SELECT COUNT(*) FROM in_input_formulations")
        count = cur.fetchone()[0]
        if count > 0:
            limit_clause = f" LIMIT {limit}" if limit else ""
            cur = db.execute(f"SELECT generic_name FROM in_input_formulations{limit_clause}")
            forms = [row[0] for row in cur.fetchall() if row[0]]
            logger.info("Loaded %d formulations from DB (in_input_formulations)", len(forms))
            return forms
    except Exception as exc:
        logger.debug("in_input_formulations not available: %s", exc)

    # Priority 4: legacy CSV fallback
    input_dir = _repo_root / "input" / "India"
    parts = sorted(input_dir.glob("formulations_part*.csv"))
    if parts:
        forms = []
        for p in parts:
            forms.extend(_load_formulations_from_csv(p, limit))
        logger.info("Loaded %d formulations from legacy CSV files", len(forms))
        return forms

    raise SystemExit("No formulations found. Upload via GUI or place CSV in input/India/")


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
    """Find a run that can be resumed (status='resume', 'running', or 'partial')."""
    try:
        # Find most recent run with 'resume' status first, then check for pending work
        cur = db.execute(
            "SELECT r.run_id FROM run_ledger r "
            "WHERE r.scraper_name = %s AND r.status IN ('resume', 'running', 'partial') "
            "AND EXISTS (SELECT 1 FROM in_formulation_status f "
            "            WHERE f.run_id = r.run_id AND f.status IN ('pending', 'in_progress')) "
            "ORDER BY r.started_at DESC LIMIT 1",
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
          AND claimed_at < CURRENT_TIMESTAMP - INTERVAL '%s minutes'
    """, (run_id, stale_minutes))
    recovered = cur.rowcount
    db.commit()
    if recovered > 0:
        logger.info("Recovered %d stale claims (older than %d min)", recovered, stale_minutes)
    return recovered


# ---------------------------------------------------------------------------
# Progress reporter (background thread)
# ---------------------------------------------------------------------------

class ProgressReporter(threading.Thread):
    """Background thread that prints progress and writes snapshots to DB."""

    def __init__(self, db, run_id: str, interval: float = 5.0,
                 snapshot_interval: float = 30.0):
        super().__init__(daemon=True)
        self.db = db
        self.run_id = run_id
        self.interval = interval
        self.snapshot_interval = snapshot_interval
        self._stop_event = threading.Event()
        self._start_time = time.monotonic()
        self._last_snapshot = 0.0
        self._last_completed = 0

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.wait(self.interval):
            try:
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

        # Recover stale claims
        recover_stale_claims(db, run_id, stale_minutes)
        print(f"[DB] RESUME | run_id={run_id} | recovered stale claims", flush=True)
    else:
        run_id = generate_run_id()
        sql, params = run_ledger_start(run_id, "India", mode=mode, thread_count=workers)
        db.execute(sql, params)
        db.commit()

        # --- Load and seed formulations ---
        formulations = _load_formulations(db, args.formulations_file, args.limit)
        if not formulations:
            raise SystemExit("No formulations to process")

        seed_formulation_queue(db, formulations, run_id)
        print(f"[DB] SEED | {len(formulations)} formulations queued | run_id={run_id}", flush=True)

    try:
        (output_dir / "last_run_id.json").write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
    except Exception as exc:
        logger.debug("Failed writing last_run_id.json: %s", exc)

    # --- Cleanup function to update run_ledger on exit ---
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
            # Update run_ledger
            sql, params = run_ledger_finish(run_id, status, items_scraped=total_items,
                                             totals_json=totals_str)
            db.execute(sql, params)
            db.commit()
            print(f"[DB] FINISH | run_ledger updated | status={status} items={total_items}", flush=True)
        except Exception as exc:
            logger.debug("Cleanup error: %s", exc)

    # Register cleanup on exit
    atexit.register(lambda: cleanup_run_ledger("interrupted"))

    # --- Start progress reporter ---
    reporter = ProgressReporter(db, run_id)
    reporter.start()

    # --- Launch workers ---
    logger.info("Launching %d worker(s) (run_id=%s, mode=%s)", workers, run_id, mode)

    worker_failed = False
    interrupted = False
    try:
        if workers == 1:
            rc = run_spider_worker(run_id, worker_id=1, limit=args.limit, use_jobdir=use_jobdir)
            if rc != 0:
                logger.error("Spider exited with code %d", rc)
                worker_failed = True
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

            failed_count = sum(1 for rc in results.values() if rc != 0)
            if failed_count:
                logger.error("%d/%d workers failed", failed_count, len(results))
                worker_failed = True
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        interrupted = True
    finally:
        # --- Stop progress reporter ---
        reporter.stop()
        reporter.join(timeout=5)

        # --- Finalize run ledger ---
        if interrupted:
            cleanup_run_ledger("interrupted")
        else:
            final_status = "completed" if not worker_failed else "failed"
            cleanup_run_ledger(final_status)
            logger.info("Scraping complete.")

        # Close DB connection
        db.close()

    if worker_failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
