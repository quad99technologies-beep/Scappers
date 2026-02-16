#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Pipeline Runner with Resume/Checkpoint Support (Simplified)

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-5)

Pipeline Steps:
    0: Backup and Clean - Backup previous output and clean for fresh run
    1: Extract VED Pricing - Scrape VED drug pricing from farmcom.info/site/reestr
    2: Extract Excluded List - Scrape excluded drugs from farmcom.info/site/reestr?vw=excl
    3: Retry Failed Pages - Retry pages with missing EAN or extraction failures (MANDATORY)
    4: Process and Translate - Fix dates, translate using ru_input_dictionary table + AI fallback
    5: Format for Export - Convert to standardized pricing and discontinued templates
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

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
from config_loader import get_central_output_dir, get_output_dir

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

# Import browser PID cleanup
try:
    from core.browser.chrome_pid_tracker import terminate_scraper_pids
    _BROWSER_CLEANUP_AVAILABLE = True
except ImportError:
    _BROWSER_CLEANUP_AVAILABLE = False
    def terminate_scraper_pids(scraper_name, repo_root, silent=False):
        return 0

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

# Total actual steps: steps 0-5 = 6 steps
TOTAL_STEPS = 6

# Add repo root for browser cleanup
_repo_root = Path(__file__).resolve().parents[2]


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("RUSSIA_RUN_ID")
    if run_id:
        return run_id.strip()
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            run_id = run_id_file.read_text(encoding="utf-8").strip()
            if run_id:
                return run_id
        except Exception:
            pass
    return ""


def _current_run_has_translated_data() -> bool:
    """True if the current run_id has any rows in ru_translated_products (so Step 4 can be skipped)."""
    run_id = _read_run_id()
    if not run_id:
        return False
    try:
        from core.db.connection import CountryDB
        from db.repositories import RussiaRepository
        db = CountryDB("Russia")
        repo = RussiaRepository(db, run_id)
        return len(repo.get_translated_products()) > 0
    except Exception:
        return False


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
        
        with CountryDB("Russia") as db:
            with db.cursor() as cur:
                # Step 1: VED Products
                if step_num == 1:
                    cur.execute("SELECT COUNT(*) FROM ru_ved_products WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 2: Excluded Products
                elif step_num == 2:
                    cur.execute("SELECT COUNT(*) FROM ru_excluded_products WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 3: Retry (count failed pages resolved)
                elif step_num == 3:
                    cur.execute("SELECT COUNT(*) FROM ru_step_progress WHERE run_id = %s AND step_number = 3 AND status = 'completed'", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 4: Translated Products
                elif step_num == 4:
                    cur.execute("SELECT COUNT(*) FROM ru_translated_products WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 5: Export Ready
                elif step_num == 5:
                    cur.execute("SELECT COUNT(*) FROM ru_export_ready WHERE run_id = %s", (run_id,))
                    return cur.fetchone()[0] or 0
                
                # Step 0: Backup - no rows to count
                else:
                    return 0
                    
    except Exception as e:
        print(f"[METRICS] Warning: Could not get DB row counts for step {step_num}: {e}")
        return 0


def _get_run_id_for_step(step_num: int) -> str:
    """Get run_id for passing to child; use DB latest if .current_run_id missing (e.g. when resuming 3–5)."""
    run_id = _read_run_id()
    if run_id:
        return run_id
    if step_num in (3, 4, 5):
        try:
            from core.db.connection import CountryDB
            from db.repositories import RussiaRepository
            db = CountryDB("Russia")
            repo = RussiaRepository(db, "")
            run_id = repo.get_latest_run_id()
            if run_id:
                return run_id
        except Exception:
            pass
    return ""


def _ensure_resume_run_id(start_step: int) -> None:
    """
    When resuming (start_step > 0), ensure we use the existing run_id.
    Priority: checkpoint metadata > .current_run_id > DB latest run with most data.
    Sets RUSSIA_RUN_ID and .current_run_id so child steps never create a new one.
    """
    if start_step <= 0:
        return
    
    cp = get_checkpoint_manager("Russia")
    run_id = (cp.get_metadata() or {}).get("run_id") or ""
    
    if not run_id:
        run_id = _read_run_id()
        if run_id:
            print(f"[RESUME] Using run from .current_run_id: {run_id}", flush=True)
    
    if not run_id:
        # Try to get the run with most data from DB
        try:
            from core.db.connection import CountryDB
            from db.repositories import RussiaRepository
            db = CountryDB("Russia")
            repo = RussiaRepository(db, "")
            run_id = repo.get_latest_run_id()
            if run_id:
                print(f"[RESUME] Using latest run from DB: {run_id}", flush=True)
        except Exception:
            pass
    
    if run_id:
        os.environ["RUSSIA_RUN_ID"] = run_id
        run_id_file = get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL for Russia pipeline."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress("Russia", run_id, step_num, step_name, status, error_message)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count("Russia", run_id, step_num)


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, extra_args: list = None):
    """Run a pipeline step and mark it complete if successful."""
    display_step = step_num + 1  # Display as 1-based for user friendliness

    print(f"\n{'='*80}")
    print(f"Step {display_step}/{TOTAL_STEPS}: {step_name}")
    print(f"{'='*80}\n")

    # Output overall pipeline progress with descriptive message
    pipeline_percent = round((step_num / TOTAL_STEPS) * 100, 1)

    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Extracting VED drug pricing data from farmcom.info",
        2: "Scraping: Extracting excluded drugs list from farmcom.info",
        3: "Retrying: Re-extracting pages with missing EAN or extraction failures",
        4: "Processing: Fixing dates and translating to English",
        5: "Formatting: Converting to standardized export templates",
    }
    step_desc = step_descriptions.get(step_num, step_name)

    print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)

    # Update checkpoint metadata to show current running step (for crash recovery and GUI)
    cp = get_checkpoint_manager("Russia")
    cp.update_metadata({"current_step": step_num, "current_step_name": step_name, "status": "running"})

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    run_id = _get_run_id_for_step(step_num)

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
        cmd = [sys.executable, "-u", str(script_path)]
        if extra_args:
            cmd.extend(extra_args)

        # Pass run_id to child so steps 3–5 get it even when 0–2 are skipped (uses DB latest if needed)
        env = os.environ.copy()
        if run_id:
            env["RUSSIA_RUN_ID"] = run_id

        # Run subprocess with output tee to both console and log file
        process = subprocess.Popen(
            cmd,
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
            log_f.write(f"=== Step {display_step}/{TOTAL_STEPS}: {step_name} ===\n")
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

        # Calculate duration
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

        # Mark step as complete
        cp = get_checkpoint_manager("Russia")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        # Store run_id in checkpoint metadata after step 1 or 2 (where run_id is generated)
        if step_num in (1, 2):
            current_run_id = _read_run_id()
            if current_run_id:
                cp.update_metadata({"run_id": current_run_id})
                print(f"[CHECKPOINT] Stored run_id in checkpoint metadata: {current_run_id}", flush=True)
        
        # Log step progress to database
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(display_step)
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("Russia", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass

        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / TOTAL_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0

        next_step_descriptions = {
            0: "Ready to extract VED drug pricing data",
            1: "Ready to extract excluded drugs list",
            2: "Ready to retry failed pages",
            3: "Ready to process and translate data",
            4: "Ready to format data for export",
            5: "Pipeline completed successfully",
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")

        print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({completion_percent}%) - {next_desc}", flush=True)

        # Wait 10 seconds after step completion before proceeding to next step
        print(f"\n[PAUSE] Waiting 10 seconds before next step...", flush=True)
        time.sleep(10.0)
        print(f"[PAUSE] Resuming pipeline...\n", flush=True)

        return True
    except subprocess.CalledProcessError as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        error_msg = f"exit_code={e.returncode}"
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False
    except Exception as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        error_msg = str(e)
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False

def main():
    parser = argparse.ArgumentParser(description="Russia Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint and scraper progress)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-5)")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3, 4, 5],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")

    args = parser.parse_args()
    
    # Check if another instance is already running (single instance enforcement)
    try:
        from core.config.config_manager import ConfigManager
        lock_file = ConfigManager.get_sessions_dir() / "Russia.lock"
        if lock_file.exists():
            # Check if process is actually running
            try:
                with open(lock_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip().split('\n')
                if content and content[0].isdigit():
                    pid = int(content[0])
                    # If lock PID is us, we're the one holding it (GUI spawned us and wrote our PID) - allow run
                    if pid == os.getpid():
                        pass  # Proceed - we own the lock
                    else:
                        # Check if another process exists
                        import subprocess
                        result = subprocess.run(
                            ['tasklist', '/FI', f'PID eq {pid}'],
                            capture_output=True, text=True, timeout=2
                        )
                        if str(pid) in result.stdout:
                            print(f"[ERROR] Another instance of Russia pipeline is already running (PID {pid}).")
                            print("[ERROR] If you're sure it's not running, clear the lock file and try again.")
                            sys.exit(1)
                        else:
                            # Stale lock - remove it
                            print(f"[WARNING] Removing stale lock file (PID {pid} not found)")
                            lock_file.unlink()
            except Exception:
                pass  # Continue if check fails
    except Exception:
        pass  # Continue if ConfigManager not available
    
    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["Russia"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")

    cp = get_checkpoint_manager("Russia")
    output_dir = get_output_dir()
    central_output_dir = get_central_output_dir()

    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        # Also clear scraper page-level progress files
        for progress_file in ["russia_scraper_progress.json", "russia_excluded_scraper_progress.json"]:
            pf = output_dir / progress_file
            if pf.exists():
                try:
                    pf.unlink()
                    print(f"Cleared {progress_file}")
                except Exception:
                    pass
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        
        # If RUSSIA_RUN_ID is set externally (GUI/Telegram/API), use it for fresh run
        external_run_id = os.environ.get("RUSSIA_RUN_ID")
        if external_run_id:
            print(f"[INIT] Using external run_id from environment: {external_run_id}")
            # Write to .current_run_id file so steps can find it
            run_id_file = output_dir / ".current_run_id"
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(external_run_id, encoding="utf-8")
                cp.update_metadata({"run_id": external_run_id})
            except Exception as e:
                print(f"[WARN] Could not save external run_id: {e}")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        # Resume from last completed step
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")

    # Define pipeline steps with their output files
    # Pipeline: 6 steps total (0-5)
    # Note: Steps 1-4 are now DB-based (no CSV output files to check)
    
    # Prepare extra_args for scrapers based on --fresh flag
    scraper_args = []
    if args.fresh:
        scraper_args.append("--fresh")
    
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None, None),
        (1, "01_russia_farmcom_scraper.py", "Extract VED Pricing Data", None, scraper_args if scraper_args else None),  # DB-based
        (2, "02_russia_farmcom_excluded_scraper.py", "Extract Excluded List", None, scraper_args if scraper_args else None),  # DB-based
        (3, "03_retry_failed_pages.py", "Retry Failed Pages", None, ["--skip-check"]),
        (4, "04_process_and_translate.py", "Process and Translate", None, None),  # DB-based
        (5, "05_format_for_export.py", "Format for Export",
         [str(central_output_dir / "Russia_Pricing_Data.csv"),
          str(central_output_dir / "Russia_Discontinued_List.csv")],
         None),
    ]

    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files, extra_args in steps:
        if step_num < start_step:
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
                # Debug: print file paths being checked
                print(f"[CHECKPOINT] Checking step {step_num} ({step_name}): expected files = {expected_files}")

            should_skip = cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files)
            if not should_skip:
                # Step marked complete but output files missing - needs re-run
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) marked complete but expected output files missing. Will re-run.")
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
            else:
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) verified - output files exist, will skip.")

    # Adjust start_step if any earlier step needs re-running
    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step
    else:
        print(f"[CHECKPOINT] All steps before {start_step} verified successfully. Starting from step {start_step}.\n")

    # When resuming, lock to existing run_id (checkpoint → .current_run_id → DB) so we never create a new one
    _ensure_resume_run_id(start_step)
    
    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("RUSSIA_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set RUSSIA_RUN_ID.")

        from core.db.connection import CountryDB
        from db.repositories import RussiaRepository

        run_id = _resolve_run_id()
        db = CountryDB("Russia")
        repo = RussiaRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Russia", _repo_root, silent=True)
        except Exception:
            pass

    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files, extra_args in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {display_step}/{TOTAL_STEPS}: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {display_step}/{TOTAL_STEPS}: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/{TOTAL_STEPS}: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/{TOTAL_STEPS}: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")

    # Now execute the steps
    for step_num, script_name, step_name, output_files, extra_args in steps:
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]

            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                display_step = step_num + 1  # Display as 1-based
                print(f"\nStep {display_step}/{TOTAL_STEPS}: {step_name} - SKIPPED (already completed in checkpoint)")
                # Output progress for skipped step
                completion_percent = round(((step_num + 1) / TOTAL_STEPS) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0

                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: VED pricing data already extracted",
                    2: "Skipped: Excluded list already extracted",
                    3: "Skipped: Failed pages already retried",
                    4: "Skipped: Processing and translation already completed",
                    5: "Skipped: Export formatting already completed",
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")

                print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(display_step)
            else:
                # Step marked complete but output files missing - will re-run
                display_step = step_num + 1
                print(f"\nStep {display_step}/{TOTAL_STEPS}: {step_name} - WILL RE-RUN (output files missing)")
            continue

        success = run_step(step_num, script_name, step_name, output_files, extra_args)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)

    # Calculate total pipeline duration
    cp = get_checkpoint_manager("Russia")
    timing_info = cp.get_pipeline_timing()
    total_duration = timing_info.get("total_duration_seconds", 0.0)

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

    # Mark pipeline as completed (enables resume detection and status in GUI)
    cp = get_checkpoint_manager("Russia")
    cp.mark_as_completed()

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {TOTAL_STEPS}/{TOTAL_STEPS} (100%)", flush=True)
    
    # Show log file location
    logs_dir = get_output_dir() / "logs"
    print(f"\n[LOGS] Step logs saved to: {logs_dir}")
    print(f"[LOGS] Use 'ls {logs_dir}/*.log' to view all step logs")

    # Post-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Russia", _repo_root, silent=True)
        except Exception:
            pass
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
