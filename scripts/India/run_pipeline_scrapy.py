#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pipeline Runner -- Scrapy + PostgreSQL

All data is stored in PostgreSQL under a unique run_id.
No backup/clean step needed -- previous runs are preserved in the DB.

Pipeline:
    Step 1: Scrapy workers scrape to PostgreSQL (parallel, work-queue)
    Step 2: QC Gate + CSV Export from PostgreSQL (by run_id)

Usage:
    python run_pipeline_scrapy.py              # Resume from last step
    python run_pipeline_scrapy.py --fresh      # Start from step 0
    python run_pipeline_scrapy.py --step N     # Start from step N
    python run_pipeline_scrapy.py --workers 5  # Parallel Scrapy spiders
    python run_pipeline_scrapy.py --status     # Show DB run history
"""

import sys
import subprocess
import argparse
import time
import os
from pathlib import Path

os.environ.setdefault("PYTHONUNBUFFERED", "1")

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, load_env_file, getenv_int

# Platform DB (optional, for unified run tracking + fetch logs)
try:
    from scripts.common.db import (
        ensure_platform_schema,
        create_pipeline_run,
        update_run_status,
        update_run_step,
    )
    _PLATFORM_DB_AVAILABLE = True
except Exception:
    _PLATFORM_DB_AVAILABLE = False

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

load_env_file()

SCRAPER_NAME = "India"
MAX_STEPS = 2


def run_step(step_num: int, script_name: str, step_name: str,
             output_files: list = None, extra_args: list = None) -> bool:
    """Run a pipeline step and mark it complete."""
    display_step = step_num + 1
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{MAX_STEPS}: {step_name}")
    print(f"{'='*80}\n")

    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Scraping: Fetching drug pricing data from NPPA (Scrapy)",
        1: "Processing: QC validation and CSV export generation",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    pct = round((step_num / MAX_STEPS) * 100, 1)
    print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({pct}%) - {step_desc}", flush=True)

    # Update platform run step (best-effort)
    platform_run_id = os.getenv("PLATFORM_RUN_ID", "").strip() or None
    if _PLATFORM_DB_AVAILABLE and platform_run_id:
        try:
            update_run_step(platform_run_id, step_num, step_name)
        except Exception:
            pass

    # Update checkpoint metadata to show current running step
    cp = get_checkpoint_manager(SCRAPER_NAME)
    cp.update_metadata({"current_step": step_num, "current_step_name": step_name, "status": "running"})

    script_path = _script_dir / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    start_time = time.time()
    cmd = [sys.executable, "-u", str(script_path)]
    if extra_args:
        cmd.extend(extra_args)

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        duration = time.time() - start_time
        mins, secs = divmod(int(duration), 60)
        hours, mins = divmod(mins, 60)
        dur_str = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s" if mins else f"{secs}s"
        print(f"[TIMING] Step {step_num} completed in {dur_str}", flush=True)

        cp = get_checkpoint_manager(SCRAPER_NAME)
        output_dir = get_output_dir()
        if output_files:
            abs_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_files, duration_seconds=duration)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration)

        done_pct = round(((step_num + 1) / MAX_STEPS) * 100, 1)
        
        # Next step descriptions
        next_step_descriptions = {
            1: "Ready for QC validation and CSV export",
            2: "Pipeline completed successfully",
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({min(done_pct, 100)}%) - {next_desc}", flush=True)
        
        # Wait 10 seconds after step completion before proceeding
        if step_num < MAX_STEPS - 1:
            print(f"\n[PAUSE] Waiting 10 seconds before next step...", flush=True)
            time.sleep(10.0)
            print(f"[PAUSE] Resuming pipeline...\n", flush=True)
        
        return True

    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} ({duration:.1f}s)")
        return False
    except Exception as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e}")
        return False


def print_status():
    """Print DB run history and formulation status."""
    from core.db.postgres_connection import PostgresDB

    print(f"\n{'='*60}")
    print("INDIA PIPELINE STATUS")
    print(f"{'='*60}")

    try:
        db = PostgresDB("India")
        db.connect()
    except Exception as e:
        print(f"Database: Cannot connect to PostgreSQL: {e}")
        print(f"{'='*60}\n")
        return

    try:
        # Run history (last 5)
        cur = db.execute(
            "SELECT run_id, status, items_scraped, started_at, ended_at "
            "FROM run_ledger WHERE scraper_name = 'India' ORDER BY started_at DESC LIMIT 5"
        )
        runs = cur.fetchall()
        if runs:
            print("\nRun History (last 5):")
            print(f"  {'Run ID':<30} {'Status':<12} {'Items':<8} {'Started':<20} {'Ended':<20}")
            print(f"  {'-'*28} {'-'*10} {'-'*6} {'-'*18} {'-'*18}")
            for r in runs:
                print(f"  {r[0]:<30} {r[1]:<12} {r[2] or 0:<8} {str(r[3] or ''):<20} {str(r[4] or ''):<20}")
        else:
            print("\nNo runs found.")

        # Latest run formulation status
        if runs:
            latest_run = runs[0][0]
            cur = db.execute(
                "SELECT status, COUNT(*) FROM in_formulation_status "
                "WHERE run_id = %s GROUP BY status", (latest_run,)
            )
            statuses = dict(cur.fetchall())
            if statuses:
                print(f"\nLatest Run ({latest_run}) Formulation Status:")
                for s, c in sorted(statuses.items()):
                    print(f"  {s}: {c}")

            # Table row counts for latest run
            print(f"\nData Tables (run_id={latest_run}):")
            for table in ["in_sku_main", "in_sku_mrp", "in_brand_alternatives", "in_med_details"]:
                try:
                    cur = db.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (latest_run,))
                    count = cur.fetchone()[0]
                    display_name = table.replace("in_", "")
                    print(f"  {display_name}: {count} rows")
                except Exception:
                    pass
    finally:
        db.close()

    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="India NPPA Scrapy Pipeline")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0")
    parser.add_argument("--step", type=int, help=f"Start from step N (0-{MAX_STEPS - 1})")
    parser.add_argument("--workers", type=int, default=getenv_int("INDIA_WORKERS", 1),
                        help="Parallel Scrapy spiders")
    parser.add_argument("--status", action="store_true", help="Show run history and exit")
    parser.add_argument("--limit", type=int, help="Limit formulations to process")
    args = parser.parse_args()

    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines([SCRAPER_NAME])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")

    if args.status:
        print_status()
        return

    # Platform run tracking (best-effort)
    platform_run_id = os.getenv("PLATFORM_RUN_ID", "").strip() or os.getenv("WORKER_RUN_ID", "").strip()
    if _PLATFORM_DB_AVAILABLE:
        try:
            ensure_platform_schema()
            if not platform_run_id:
                platform_run_id = create_pipeline_run(
                    country=SCRAPER_NAME,
                    total_steps=MAX_STEPS,
                    metadata={"pipeline": "run_pipeline_scrapy"},
                )
            os.environ["PLATFORM_RUN_ID"] = platform_run_id
            update_run_status(platform_run_id, "running")
        except Exception:
            platform_run_id = None

    cp = get_checkpoint_manager(SCRAPER_NAME)
    output_dir = get_output_dir()
    worker_count = max(1, args.workers)

    # Determine start step
    force_fresh_scrape = False
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        force_fresh_scrape = True
        print("Starting fresh pipeline (new run_id, previous runs preserved in DB)")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if start_step >= MAX_STEPS:
            print("All steps already completed. Use --fresh to start a new run.")
            sys.exit(0)
        elif info["total_completed"] > 0:
            print(f"Resuming from step {start_step}")
        else:
            # No checkpoint, but check DB for a resumable run before going fresh
            has_resumable = False
            try:
                from core.db.postgres_connection import PostgresDB
                db = PostgresDB("India")
                db.connect()
                cur = db.execute(
                    "SELECT run_id, COUNT(*) FROM in_formulation_status "
                    "WHERE status IN ('completed','in_progress') "
                    "GROUP BY run_id ORDER BY run_id DESC LIMIT 1"
                )
                row = cur.fetchone()
                db.close()
                if row and row[1] > 0:
                    has_resumable = True
                    print(f"No checkpoint but found resumable run {row[0]} with {row[1]} formulations done — resuming")
            except Exception:
                pass
            if has_resumable:
                start_step = 0
                force_fresh_scrape = False
            else:
                print("Starting fresh (no checkpoint, no resumable run)")
                force_fresh_scrape = True

    # Build scraper args
    scrapy_args = ["--workers", str(worker_count)]
    if force_fresh_scrape:
        scrapy_args.append("--fresh")
    if args.limit:
        scrapy_args.extend(["--limit", str(args.limit)])

    # Pipeline steps (no backup/clean -- all data isolated by run_id in DB)
    steps = [
        (0, "run_scrapy_india.py", "Scrape to PostgreSQL", [], scrapy_args),
        (1, "05_qc_and_export.py", "QC Gate + CSV Export", ["details_combined.csv", "qc_report.json"], None),
    ]
    
    # Print execution plan
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script, name, outputs, extra in steps:
        display_step = step_num + 1
        if step_num < start_step:
            output_dir = get_output_dir()
            expected = None
            if outputs:
                expected = [str(output_dir / f) if not Path(f).is_absolute() else f for f in outputs]
            if cp.should_skip_step(step_num, name, verify_outputs=True, expected_output_files=expected):
                print(f"Step {display_step}/{MAX_STEPS}: {name} - SKIPPED (already completed)")
            else:
                print(f"Step {display_step}/{MAX_STEPS}: {name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/{MAX_STEPS}: {name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/{MAX_STEPS}: {name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")

    # Run
    for step_num, script, name, outputs, extra in steps:
        if step_num < start_step:
            expected = None
            if outputs:
                expected = [str(output_dir / f) if not Path(f).is_absolute() else f for f in outputs]
            if cp.should_skip_step(step_num, name, verify_outputs=True, expected_output_files=expected):
                display_step = step_num + 1
                print(f"\nStep {display_step}/{MAX_STEPS}: {name} - SKIPPED (already completed)")
                # Output progress for skipped step
                completion_percent = round(((step_num + 1) / MAX_STEPS) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                skip_descriptions = {
                    0: "Skipped: NPPA scraping already completed in database",
                    1: "Skipped: QC and export already completed",
                }
                skip_desc = skip_descriptions.get(step_num, f"Skipped: {name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {skip_desc}", flush=True)
                continue

        success = run_step(step_num, script, name, outputs, extra)
        if not success:
            print(f"\nPipeline failed at step {step_num}. Resume by re-running.")
            if _PLATFORM_DB_AVAILABLE and platform_run_id:
                try:
                    update_run_status(platform_run_id, "failed", error_message=f"step_{step_num}_failed")
                except Exception:
                    pass
            sys.exit(1)

    # Mark pipeline as completed
    cp.mark_as_completed()

    if _PLATFORM_DB_AVAILABLE and platform_run_id:
        try:
            update_run_status(platform_run_id, "completed")
        except Exception:
            pass

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}")
    print(f"[PROGRESS] Pipeline Step: {MAX_STEPS}/{MAX_STEPS} (100%)", flush=True)
    print(f"Database: PostgreSQL (India tables with in_ prefix)")


if __name__ == "__main__":
    main()
