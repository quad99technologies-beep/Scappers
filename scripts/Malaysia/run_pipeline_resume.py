#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Malaysia Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-5)
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Malaysia to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

# Import foundation contracts
try:
    from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
    from core.step_hooks import StepHookRegistry, StepMetrics
    from core.alerting_integration import setup_alerting_hooks
    from core.data.data_quality_checks import DataQualityChecker
    from core.audit_logger import audit_log
    from core.monitoring.benchmarking import record_step_benchmark
    from core.utils.step_progress_logger import update_run_ledger_aggregation
    from datetime import datetime
    _FOUNDATION_AVAILABLE = True
except ImportError:
    _FOUNDATION_AVAILABLE = False
    PreflightChecker = None
    StepHookRegistry = None
    setup_alerting_hooks = None
    DataQualityChecker = None
    audit_log = None
    record_step_benchmark = None
    update_run_ledger_aggregation = None

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
    from core.utils.step_progress_logger import log_step_progress, update_run_ledger_step_count
    _STEP_PROGRESS_AVAILABLE = True
except ImportError:
    _STEP_PROGRESS_AVAILABLE = False
    def log_step_progress(*args, **kwargs):
        return False
    def update_run_ledger_step_count(*args, **kwargs):
        return False

# Import Prometheus metrics
try:
    from core.monitoring.prometheus_exporter import (
        init_prometheus_metrics,
        record_scraper_run,
        record_scraper_duration,
        record_items_scraped,
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
    def record_items_scraped(*args, **kwargs):
        pass
    def record_step_duration(*args, **kwargs):
        pass
    def record_error(*args, **kwargs):
        pass

# Import Frontier Queue
try:
    from scripts.common.frontier_integration import initialize_frontier_for_scraper
    _FRONTIER_AVAILABLE = True
except ImportError:
    _FRONTIER_AVAILABLE = False
    def initialize_frontier_for_scraper(*args, **kwargs):
        return None


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("MALAYSIA_RUN_ID")
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


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None) -> None:
    """Persist step progress in PostgreSQL for Malaysia pipeline."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress("Malaysia", run_id, step_num, step_name, status, error_message)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count("Malaysia", run_id, step_num)


def _get_latest_run_id_from_db() -> str:
    """Return the best Malaysia run_id to resume: prefer runs with data (items_scraped > 0), then latest by started_at."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Malaysia") as db:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC LIMIT 1",
                    ("Malaysia",),
                )
                row = cur.fetchone()
                return (row[0] or "").strip() if row else ""
    except Exception:
        return ""


def _ensure_resume_run_id(start_step: int) -> None:
    """When resuming (start_step > 0), ensure we use the existing run_id. Prefer checkpoint, then run_ledger
    (run with most data), then .current_run_id. Set MALAYSIA_RUN_ID and .current_run_id so child steps never create a new one."""
    if start_step <= 0:
        return
    cp = get_checkpoint_manager("Malaysia")
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
        os.environ["MALAYSIA_RUN_ID"] = run_id
        run_id_file = get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass
        cp.update_metadata({"run_id": run_id})


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    # Total actual steps: steps 0-5 = 6 steps
    total_steps = 6
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results, initializing DB, generating run_id",
        1: "Scraping: Fetching product registration numbers from MyPriMe (Playwright)",
        2: "Scraping: Extracting detailed product information from Quest3+ (this may take a while)",
        3: "Processing: Consolidating product details in database",
        4: "Scraping: Fetching fully reimbursable drugs list from FUKKM",
        5: "Generating: Creating PCID-mapped CSV exports from database"
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)

    # Update checkpoint metadata to show current running step
    cp = get_checkpoint_manager("Malaysia")
    cp.update_metadata({"current_step": step_num, "current_step_name": step_name, "status": "running"})

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    run_id = _read_run_id()

    # Create log file for this step (persistent across resume)
    output_dir = get_output_dir()
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file_name = f"step_{step_num:02d}_{step_name.replace(' ', '_').replace('(', '').replace(')', '').replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = logs_dir / log_file_name
    
    # Also create a symlink/latest log file for easy access
    latest_log_path = logs_dir / f"step_{step_num:02d}_latest.log"
    
    # Create metrics object for hooks
    metrics = None
    if _FOUNDATION_AVAILABLE and StepHookRegistry:
        try:
            metrics = StepMetrics(
                step_number=step_num,
                step_name=step_name,
                run_id=run_id or "pending",
                scraper_name="Malaysia",
                started_at=datetime.now(),
                log_file_path=str(log_file_path)
            )
            StepHookRegistry.emit_step_start(metrics)
        except Exception as e:
            print(f"[HOOKS] Warning: Could not emit step start hook: {e}")
    
    # Start the step and capture output to log file
    print(f"[LOG] Step output will be saved to: {log_file_path}")
    
    try:
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        if run_id:
            env["MALAYSIA_RUN_ID"] = run_id

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
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                record_step_duration("Malaysia", step_name, duration_seconds)
            except Exception as e:
                print(f"[METRICS] Warning: Could not record step duration: {e}")
        
        # Update metrics
        if metrics:
            metrics.duration_seconds = duration_seconds
            metrics.completed_at = datetime.now()
            # Note: Row metrics would be populated from step output if available
        
        # Mark step as complete
        cp = get_checkpoint_manager("Malaysia")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        if step_num == 0:
            rid = _read_run_id()
            if rid:
                cp.update_metadata({"run_id": rid})
        
        # Log step progress to database with enhanced metrics
        if _FOUNDATION_AVAILABLE:
            try:
                from core.utils.step_progress_logger import log_step_progress
                log_step_progress(
                    scraper_name="Malaysia",
                    run_id=run_id or "pending",
                    step_num=step_num,
                    step_name=step_name,
                    status="completed",
                    duration_seconds=duration_seconds,
                    log_file_path=log_file_path
                )
            except Exception:
                # Fallback to old method
                _log_step_progress(step_num, step_name, "completed")
        else:
            _log_step_progress(step_num, step_name, "completed")
        
        _update_run_ledger_step_count(display_step)
        
        # Record benchmark
        if _FOUNDATION_AVAILABLE and record_step_benchmark:
            try:
                record_step_benchmark(
                    scraper_name="Malaysia",
                    step_number=step_num,
                    step_name=step_name,
                    run_id=run_id or "pending",
                    duration_seconds=duration_seconds,
                    rows_processed=metrics.rows_processed if metrics else 0
                )
            except Exception:
                pass
        
        # Emit step end hook
        if metrics:
            try:
                StepHookRegistry.emit_step_end(metrics)
            except Exception as e:
                print(f"[HOOKS] Warning: Could not emit step end hook: {e}")
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("Malaysia", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass

        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        return True
        
    except subprocess.CalledProcessError as e:
        # Step failed
        duration_seconds = time.time() - start_time
        
        if metrics:
            metrics.duration_seconds = duration_seconds
            metrics.completed_at = datetime.now()
            metrics.error_message = str(e)
        
        # Log failure
        if _FOUNDATION_AVAILABLE:
            try:
                from core.utils.step_progress_logger import log_step_progress
                log_step_progress(
                    scraper_name="Malaysia",
                    run_id=run_id or "pending",
                    step_num=step_num,
                    step_name=step_name,
                    status="failed",
                    error_message=str(e),
                    duration_seconds=duration_seconds,
                    log_file_path=log_file_path
                )
            except Exception:
                _log_step_progress(step_num, step_name, "failed", str(e))
        else:
            _log_step_progress(step_num, step_name, "failed", str(e))
        
        # Emit error hook
        if metrics:
            try:
                StepHookRegistry.emit_step_error(metrics, e)
            except Exception:
                pass
        
        return False
    
    except Exception as e:
        # Unexpected error
        if metrics:
            metrics.error_message = str(e)
            metrics.completed_at = datetime.now()
            try:
                StepHookRegistry.emit_step_error(metrics, e)
            except Exception:
                pass
        raise
        
        # Keys = next step number (step_num + 1). Step 0=Backup, 1=Registration, 2=Product Details, 3=Consolidate, 4=Reimbursable, 5=PCID.
        next_step_descriptions = {
            1: "Ready to fetch registration numbers",
            2: "Ready to extract product details",
            3: "Ready to consolidate results",
            4: "Ready to fetch reimbursable drugs",
            5: "Ready to generate PCID mapped output",
            6: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
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
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                record_step_duration("Malaysia", step_name, duration_seconds)
                record_error("Malaysia", "step_failed")
            except Exception as e:
                print(f"[METRICS] Warning: Could not record error metrics: {e}")
        
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False

def main():
    # Initialize Prometheus metrics
    if _PROMETHEUS_AVAILABLE:
        try:
            init_prometheus_metrics(port=9090)
            print("[METRICS] Prometheus metrics initialized on port 9090")
        except Exception as e:
            print(f"[METRICS] Warning: Could not initialize Prometheus metrics: {e}")
    
    # Initialize Frontier Queue
    if _FRONTIER_AVAILABLE:
        try:
            frontier = initialize_frontier_for_scraper("Malaysia")
            if frontier:
                print("[FRONTIER] Frontier queue initialized for Malaysia")
        except Exception as e:
            print(f"[FRONTIER] Warning: Could not initialize frontier queue: {e}")
    
    parser = argparse.ArgumentParser(description="Malaysia Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-5)")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3, 4, 5],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")

    args = parser.parse_args()

    # Setup foundation contracts
    if _FOUNDATION_AVAILABLE:
        try:
            # Setup alerting hooks
            setup_alerting_hooks()
            print("[SETUP] Alerting hooks registered")
        except Exception as e:
            print(f"[SETUP] Warning: Could not setup alerting hooks: {e}")

    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["Malaysia"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")
    
    # Get run_id early for preflight checks
    run_id = _read_run_id()
    if not run_id and not args.fresh:
        # Try to get from checkpoint or generate new
        cp = get_checkpoint_manager("Malaysia")
        run_id = (cp.get_metadata() or {}).get("run_id") or ""
    
    # Run preflight health checks (MANDATORY GATE)
    if _FOUNDATION_AVAILABLE and PreflightChecker:
        try:
            checker = PreflightChecker("Malaysia", run_id or "pending")
            results = checker.run_all_checks()
            
            print("\n[PREFLIGHT] Health Checks:")
            for result in results:
                # Use ASCII-safe indicators for Windows console compatibility
                if result.severity == CheckSeverity.CRITICAL:
                    status = "[FAIL]" if not result.passed else "[OK]"
                elif result.severity == CheckSeverity.WARNING:
                    status = "[WARN]" if not result.passed else "[OK]"
                else:
                    status = "[INFO]" if result.passed else "[FAIL]"
                print(f"  {status} {result.name}: {result.message}")
            
            if checker.has_critical_failures():
                print("\n[PREFLIGHT] [FAIL] Pipeline blocked due to critical failures:")
                print(checker.get_failure_summary())
                sys.exit(1)
            
            # Run pre-flight data quality checks
            if run_id:
                dq_checker = DataQualityChecker("Malaysia", run_id)
                dq_checker.run_preflight_checks()
                dq_checker.save_results_to_db()
        except Exception as e:
            print(f"[PREFLIGHT] Warning: Could not run preflight checks: {e}")
    
    # Audit log: pipeline started
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_started",
                scraper_name="Malaysia",
                run_id=run_id or "pending",
                user="system"
            )
        except Exception:
            pass

    cp = get_checkpoint_manager("Malaysia")

    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("MALAYSIA_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set MALAYSIA_RUN_ID.")

        from core.db.connection import CountryDB
        from db.repositories import MalaysiaRepository

        run_id = _resolve_run_id()
        db = CountryDB("Malaysia")
        repo = MalaysiaRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("MALAYSIA_RUN_ID")
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
            os.environ.pop("MALAYSIA_RUN_ID", None)
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
        # Resume from last completed step
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")
    
    # Define pipeline steps with their output files
    # NEW: Use step scripts in steps/ directory (DB-backed architecture)
    output_dir = get_output_dir()
    steps = [
        (0, "steps/step_00_backup_clean.py", "Backup and Clean + DB Init", None),
        (1, "steps/step_01_registration.py", "Product Registration Number", None),
        (2, "steps/step_02_product_details.py", "Product Details", None),
        (3, "steps/step_03_consolidate.py", "Consolidate Results", None),
        (4, "steps/step_04_reimbursable.py", "Get Fully Reimbursable", None),
        (5, "steps/step_05_pcid_export.py", "Generate PCID Mapped", None),  # Output files vary
    ]
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
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

    # When resuming, lock to existing run_id (checkpoint → run_ledger → .current_run_id) so we never create a new one
    _ensure_resume_run_id(start_step)
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Malaysia", _repo_root, silent=True)
        except Exception:
            pass

    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {display_step}/6: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {display_step}/6: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/6: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/6: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")

    # Now execute the steps
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                # Total actual steps: steps 0-5 = 6 steps
                total_steps = 6
                display_step = step_num + 1  # Display as 1-based
                print(f"\nStep {display_step}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
                # Output progress for skipped step
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup and DB init already completed",
                    1: "Skipped: Registration numbers already in database",
                    2: "Skipped: Product details already in database",
                    3: "Skipped: Results already consolidated in database",
                    4: "Skipped: Reimbursable drugs already in database",
                    5: "Skipped: PCID mapping already generated"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(display_step)
            else:
                # Step marked complete but output files missing - will re-run
                display_step = step_num + 1
                print(f"\nStep {display_step}/6: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            
            # Record Prometheus metrics for failed pipeline
            if _PROMETHEUS_AVAILABLE:
                try:
                    record_scraper_run("Malaysia", "failed")
                    record_error("Malaysia", "pipeline_failed")
                except Exception as e:
                    print(f"[METRICS] Warning: Could not record failure metrics: {e}")
            
            # Update run-level aggregation on failure
            if _FOUNDATION_AVAILABLE and update_run_ledger_aggregation:
                try:
                    update_run_ledger_aggregation("Malaysia", run_id or "pending")
                except Exception:
                    pass
            
            # Audit log: pipeline failed
            if _FOUNDATION_AVAILABLE and audit_log:
                try:
                    audit_log(
                        action="run_failed",
                        scraper_name="Malaysia",
                        run_id=run_id or "pending",
                        user="system",
                        details={"failed_step": step_num, "step_name": step_name}
                    )
                except Exception:
                    pass
            
            sys.exit(1)
    
    # Pipeline completed successfully
    print("\n[SUCCESS] All steps completed successfully!")
    
    # Update run-level aggregation
    if _FOUNDATION_AVAILABLE and update_run_ledger_aggregation:
        try:
            update_run_ledger_aggregation("Malaysia", run_id or "pending")
        except Exception as e:
            print(f"[POST-RUN] Warning: Could not update aggregation: {e}")
    
    # Run post-run data quality checks
    if _FOUNDATION_AVAILABLE and DataQualityChecker and run_id:
        try:
            dq_checker = DataQualityChecker("Malaysia", run_id)
            dq_checker.run_postrun_checks()
            dq_checker.save_results_to_db()
            print("[POST-RUN] Data quality checks completed")
        except Exception as e:
            print(f"[POST-RUN] Warning: Could not run data quality checks: {e}")
    
    # Audit log: pipeline completed
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_completed",
                scraper_name="Malaysia",
                run_id=run_id or "pending",
                user="system"
            )
        except Exception:
            pass
    
    # Record Prometheus metrics for completed pipeline
    if _PROMETHEUS_AVAILABLE:
        try:
            pipeline_start_time = None
            # Try to get pipeline start time from checkpoint or run_ledger
            try:
                from core.db.connection import CountryDB
                with CountryDB("Malaysia") as db:
                    with db.cursor() as cur:
                        cur.execute(
                            "SELECT started_at FROM run_ledger WHERE scraper_name = %s AND run_id = %s",
                            ("Malaysia", run_id or "pending")
                        )
                        row = cur.fetchone()
                        if row and row[0]:
                            pipeline_start_time = row[0]
            except Exception:
                pass
            
            if pipeline_start_time:
                total_duration = (datetime.now() - pipeline_start_time).total_seconds()
                record_scraper_duration("Malaysia", total_duration)
            
            record_scraper_run("Malaysia", "success")
        except Exception as e:
            print(f"[METRICS] Warning: Could not record pipeline completion metrics: {e}")
    
    # Calculate total pipeline duration
    cp = get_checkpoint_manager("Malaysia")
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
    print(f"[PROGRESS] Pipeline Step: 6/6 (100%)", flush=True)
    
    # Show log file location
    logs_dir = get_output_dir() / "logs"
    print(f"\n[LOGS] Step logs saved to: {logs_dir}")
    print(f"[LOGS] Use 'ls {logs_dir}/*.log' to view all step logs")
    
    # Post-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids("Malaysia", _repo_root, silent=True)
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
