#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0=Backup, 1=URLs, 2=Scrape, 3=Translate, 4=Stats, 5=Export)
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path

# Add repo root and script dir to path (script dir first for config_loader/db)
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Clear conflicting db when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]

from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, getenv
from core.browser.chrome_pid_tracker import terminate_scraper_pids

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

# Import step progress logger
try:
    from core.utils.step_progress_logger import log_step_progress, update_run_ledger_step_count, update_run_ledger_aggregation
    _STEP_PROGRESS_AVAILABLE = True
except ImportError:
    _STEP_PROGRESS_AVAILABLE = False
    def log_step_progress(*args, **kwargs):
        return False
    def update_run_ledger_step_count(*args, **kwargs):
        return False
    def update_run_ledger_aggregation(*args, **kwargs):
        return False

# Import foundation contracts
try:
    from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
    from core.pipeline.step_hooks import StepHookRegistry, StepMetrics
    from core.monitoring.alerting_integration import setup_alerting_hooks
    from core.data.data_quality_checks import DataQualityChecker
    from core.monitoring.audit_logger import audit_log
    from core.monitoring.benchmarking import record_step_benchmark
    _FOUNDATION_AVAILABLE = True
except ImportError:
    _FOUNDATION_AVAILABLE = False
    PreflightChecker = None
    StepHookRegistry = None
    setup_alerting_hooks = None
    DataQualityChecker = None
    audit_log = None
    record_step_benchmark = None

# Import Prometheus metrics
try:
    from core.monitoring.prometheus_exporter import (
        init_prometheus_metrics,
        record_scraper_run,
        record_scraper_duration,
        record_step_duration,
        record_error
    )
    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False
    def init_prometheus_metrics(*args, **kwargs):
        return False
    def record_scraper_run(*args, **kwargs):
        pass
    def record_scraper_duration(*args, **kwargs):
        pass
    def record_step_duration(*args, **kwargs):
        pass
    def record_error(*args, **kwargs):
        pass


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL for North Macedonia pipeline."""
    run_id = _read_run_id()
    print(f"[DB] Step progress: step={step_num}, status={status}, run_id={run_id}", flush=True)
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    result = log_step_progress("NorthMacedonia", run_id, step_num, step_name, status, error_message)
    if result:
        print(f"[DB] Step progress logged successfully", flush=True)
    else:
        print(f"[DB] Step progress logging failed", flush=True)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    print(f"[DB] Updating run_ledger step_count={step_num}, run_id={run_id}", flush=True)
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    result = update_run_ledger_step_count("NorthMacedonia", run_id, step_num)
    if result:
        print(f"[DB] run_ledger updated successfully", flush=True)
    else:
        print(f"[DB] run_ledger update failed", flush=True)


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID")
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            run_id = run_id_file.read_text(encoding="utf-8").strip()
            if run_id:
                return run_id
        except Exception:
            pass
    return ""


def _get_step_row_counts(step_num: int, run_id: str) -> int:
    """
    Get row counts from database for a given step.
    This provides fallback metrics when step scripts don't write metrics files.
    
    Args:
        step_num: Step number (0-based)
        run_id: Current run ID
        
    Returns:
        Number of rows processed/inserted for this step
    """
    if not run_id:
        return 0
    
    try:
        from core.db.connection import CountryDB
        
        with CountryDB("NorthMacedonia") as db:
            with db.cursor() as cur:
                # Step 1: URLs collected
                if step_num == 1:
                    cur.execute("SELECT COUNT(*) FROM nm_urls WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 2: Drug register entries
                elif step_num == 2:
                    cur.execute("SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 3: Translated (check final output)
                elif step_num == 3:
                    cur.execute("SELECT COUNT(*) FROM nm_final_output WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 4: Statistics computed (use final output count)
                elif step_num == 4:
                    cur.execute("SELECT COUNT(*) FROM nm_final_output WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 5: Export generated (use PCID mappings)
                elif step_num == 5:
                    cur.execute("SELECT COUNT(*) FROM nm_pcid_mappings WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 0: Backup - no rows to count
                else:
                    return 0
                    
    except Exception as e:
        print(f"[METRICS] Warning: Could not get DB row counts for step {step_num}: {e}")
        return 0


def _get_latest_run_id_from_db() -> str:
    """Return the best North Macedonia run_id to resume: prefer runs with data (items_scraped > 0), then latest by started_at."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("NorthMacedonia") as db:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC LIMIT 1",
                    ("NorthMacedonia",),
                )
                row = cur.fetchone()
                return (row[0] or "").strip() if row else ""
    except Exception:
        return ""


def _ensure_resume_run_id(start_step: int) -> None:
    """When resuming (start_step > 0), ensure we use the existing run_id."""
    if start_step <= 0:
        return
    cp = get_checkpoint_manager("NorthMacedonia")
    run_id = (cp.get_metadata() or {}).get("run_id") or ""
    if not run_id:
        run_id = _get_latest_run_id_from_db()
        if run_id:
            print(f"[RESUME] Using run from run_ledger (run with data): {run_id}", flush=True)
    if not run_id:
        run_id = _read_run_id()
        if run_id:
            print(f"[RESUME] Using run from .current_run_id: {run_id}", flush=True)
    if run_id:
        os.environ["NORTH_MACEDONIA_RUN_ID"] = run_id
        run_id_file = get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass
        cp.update_metadata({"run_id": run_id})


def run_step(step_num: int, script_name: str, step_name: str, total_steps: int, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")

    pipeline_percent = round((step_num / total_steps) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0

    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Collecting URLs from drug register",
        2: "Scraping: Extracting drug register details from Ministry of Health",
        3: "Translating: Applying dictionary translations to scraped data",
        4: "Validating: Computing statistics and checking data quality",
        5: "Exporting: Generating PCID-mapped CSV exports",
    }
    step_desc = step_descriptions.get(step_num, step_name)

    print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    start_time = time.time()
    duration_seconds = None
    run_id = _read_run_id()

    # Create log file for this step (persistent across resume)
    from datetime import datetime
    output_dir = get_output_dir()
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_name = f"step_{step_num:02d}_{step_name.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = logs_dir / log_file_name
    
    # Also create a symlink/latest log file for easy access
    latest_log_path = logs_dir / f"step_{step_num:02d}_latest.log"

    print(f"[LOG] Step output will be saved to: {log_file_path}")

    try:
        # Pre-clean any tracked Chrome/Firefox PIDs for this scraper
        terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)

        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        if run_id:
            env["NORTH_MACEDONIA_RUN_ID"] = run_id

        # Run subprocess with output tee to both console and log file
        process = subprocess.Popen(
            [sys.executable, "-u", str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Tee output to both log file and console
        with open(log_file_path, "w", encoding="utf-8") as log_f:
            # Write header to log file
            log_f.write(f"=== Step {display_step}/{total_steps}: {step_name} ===\n")
            log_f.write(f"=== Script: {script_name} ===\n")
            log_f.write(f"=== Started: {datetime.now().isoformat()} ===\n")
            log_f.write(f"=== Run ID: {run_id or 'pending'} ===\n")
            log_f.write("=" * 80 + "\n\n")
            log_f.flush()
            
            for line in process.stdout:
                # Write to log file
                log_f.write(line)
                log_f.flush()
                # Write to console
                print(line, end="", flush=True)
        
        process.wait()
        
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, script_path)
        
        # Update symlink to point to latest log
        try:
            if latest_log_path.exists() or latest_log_path.is_symlink():
                latest_log_path.unlink()
            latest_log_path.symlink_to(log_file_path.name)
        except Exception:
            pass  # Non-critical: symlink is just for convenience

        duration_seconds = time.time() - start_time

        # Format duration for display
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        seconds = int(duration_seconds % 60)
        if hours > 0:
            duration_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            duration_str = f"{minutes}m {seconds}s"
        else:
            duration_str = f"{seconds}s"
        print(f"[TIMING] Step {step_num} completed in {duration_str}", flush=True)

        cp = get_checkpoint_manager("NorthMacedonia")
        if output_files:
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        if step_num == 0:
            rid = _read_run_id()
            if rid:
                cp.update_metadata({"run_id": rid})
        
        # Log step progress to database
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(display_step)
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("NorthMacedonia", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass

        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0

        next_step_descriptions = {
            0: "Ready to collect URLs",
            1: "Ready to extract drug register details",
            2: "Ready to translate scraped data",
            3: "Ready to compute statistics",
            4: "Ready to generate exports",
            5: "Pipeline completed successfully",
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")

        print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
        # Wait 10 seconds after step completion before proceeding to next step
        print(f"\n[PAUSE] Waiting 10 seconds before next step...", flush=True)
        time.sleep(10.0)
        print(f"[PAUSE] Resuming pipeline...\n", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        duration_seconds = time.time() - start_time
        error_msg = f"exit_code={e.returncode}"
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False
    except Exception as e:
        duration_seconds = time.time() - start_time
        error_msg = str(e)
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False
    finally:
        # Post-clean any tracked browser PIDs for this scraper
        try:
            terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="North Macedonia Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0=Backup, 1=URLs, 2=Scrape, 3=Translate, 4=Stats, 5=Export)")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3, 4, 5],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")
    args = parser.parse_args()

    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["NorthMacedonia"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")

    cp = get_checkpoint_manager("NorthMacedonia")

    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set NORTH_MACEDONIA_RUN_ID.")

        from core.db.connection import CountryDB
        try:
            from db.repositories import NorthMacedoniaRepository
        except ImportError:
            from scripts.north_macedonia.db.repositories import NorthMacedoniaRepository

        run_id = _resolve_run_id()
        db = CountryDB("NorthMacedonia")
        repo = NorthMacedoniaRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")

    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("NORTH_MACEDONIA_RUN_ID")
        if external_run_id:
            print(f"[INIT] Using external run_id from environment: {external_run_id}")
            # Write to .current_run_id file so steps can find it
            run_id_file = get_output_dir() / ".current_run_id"
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(external_run_id, encoding="utf-8")
                cp.update_metadata({"run_id": external_run_id})
            except Exception as e:
                print(f"[WARN] Could not save external run_id: {e}")
        else:
            # Fresh runs must not reuse prior run_id
            os.environ.pop("NORTH_MACEDONIA_RUN_ID", None)
            try:
                run_id_file = get_output_dir() / ".current_run_id"
                if run_id_file.exists():
                    run_id_file.unlink()
                    print(f"[CLEAN] Removed previous run_id file: {run_id_file}")
            except Exception as e:
                print(f"[CLEAN] Warning: could not remove previous run_id file: {e}")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")

    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_collect_urls.py", "Collect URLs (Selenium)", None),
        (2, "02_fast_scrape_details.py", "Extract Drug Register Data", None),
        (3, "04_translate_using_dictionary.py", "Translate Using Dictionary", None),
        (4, "05_stats_and_validation.py", "Statistics & Data Validation", None),
        (5, "06_generate_export.py", "Generate PCID-Mapped Export", None),
    ]
    total_steps = len(steps)

    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / Path(f).name) if not Path(f).is_absolute() else f for f in output_files]
            should_skip = cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files)
            if not should_skip:
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) marked complete but outputs missing. Will re-run.")
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
            else:
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) verified - output files exist, will skip.")

    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step
    else:
        print(f"[CHECKPOINT] All steps before {start_step} verified successfully. Starting from step {start_step}.\n")

    # When resuming, lock to existing run_id
    _ensure_resume_run_id(start_step)
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    try:
        terminate_scraper_pids("NorthMacedonia", _repo_root, silent=True)
    except Exception:
        pass

    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num < start_step:
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {display_step}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {display_step}/{total_steps}: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/{total_steps}: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/{total_steps}: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")

    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / Path(f).name) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"\nStep {step_num + 1}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: URLs already collected",
                    2: "Skipped: Drug register data already extracted",
                    3: "Skipped: Translation already completed",
                    4: "Skipped: Statistics already computed",
                    5: "Skipped: Export already generated",
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(step_num + 1)
            else:
                display_step = step_num + 1
                print(f"\nStep {display_step}/{total_steps}: {step_name} - WILL RE-RUN (output files missing)")
            continue

        success = run_step(step_num, script_name, step_name, total_steps, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)

    # Calculate total pipeline duration
    cp = get_checkpoint_manager("NorthMacedonia")
    timing_info = cp.get_pipeline_timing()
    total_duration = timing_info.get("total_duration_seconds", 0.0)

    # Mark pipeline as completed
    cp.mark_as_completed()

    # Format total duration
    hours = int(total_duration // 3600)
    minutes = int((total_duration % 3600) // 60)
    seconds = int(total_duration % 60)
    if hours > 0:
        total_duration_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        total_duration_str = f"{minutes}m {seconds}s"
    else:
        total_duration_str = f"{seconds}s"

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {total_steps}/{total_steps} (100%)", flush=True)
    
    # Show log file location
    logs_dir = get_output_dir() / "logs"
    print(f"\n[LOGS] Step logs saved to: {logs_dir}")
    print(f"[LOGS] Use 'ls {logs_dir}/*.log' to view all step logs")

    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass


if __name__ == "__main__":
    main()
