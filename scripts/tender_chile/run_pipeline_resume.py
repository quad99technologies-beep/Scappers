#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tender Chile Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-4)
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
print(f"[DEBUG] _repo_root: {_repo_root}", flush=True)
print(f"[DEBUG] _script_dir: {_script_dir}", flush=True)
print(f"[DEBUG] sys.path before: {sys.path[:3]}...", flush=True)
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
print(f"[DEBUG] sys.path after: {sys.path[:3]}...", flush=True)
print(f"[DEBUG] core exists: {(_repo_root / 'core').exists()}", flush=True)

# Clear conflicting db/config when run in same process as other scrapers (e.g. GUI)
for mod in list(sys.modules.keys()):
    if mod == "db" or mod.startswith("db."):
        del sys.modules[mod]
if "config_loader" in sys.modules:
    del sys.modules["config_loader"]

try:
    from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
except ImportError as e:
    print(f"[ERROR] Failed to import checkpoint manager: {e}", flush=True)
    print(f"[ERROR] sys.path: {sys.path}", flush=True)
    raise
from config_loader import get_output_dir

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
    from core.monitoring.prometheus_exporter import init_prometheus_metrics
    from services.frontier_integration import initialize_frontier_for_scraper
    _FOUNDATION_AVAILABLE = True
    _PROMETHEUS_AVAILABLE = True
    _FRONTIER_AVAILABLE = True
except ImportError:
    _FOUNDATION_AVAILABLE = False
    _PROMETHEUS_AVAILABLE = False
    _FRONTIER_AVAILABLE = False
    PreflightChecker = None
    StepHookRegistry = None
    StepMetrics = None
    DataQualityChecker = None
    audit_log = None
    record_step_benchmark = None
    init_prometheus_metrics = None
    initialize_frontier_for_scraper = None

SCRAPER_NAME = "Tender_Chile"
# Total actual steps: steps 0-4 = 5 steps
MAX_STEPS = 5

# Add repo root for browser cleanup
_repo_root = Path(__file__).resolve().parents[2]


def _log_step_progress(step_num: int, step_name: str, status: str, error_message: str = None, duration_seconds: float = None) -> None:
    """Persist step progress in PostgreSQL for Tender Chile pipeline."""
    run_id = os.environ.get("TENDER_CHILE_RUN_ID") or _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress(SCRAPER_NAME, run_id, step_num, step_name, status, error_message, duration_seconds=duration_seconds)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count(SCRAPER_NAME, run_id, step_num)


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("TENDER_CHILE_RUN_ID")
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


def _get_latest_run_id_from_db() -> str:
    """Return the best Chile run_id to resume: prefer runs with data (items_scraped > 0), then latest by started_at."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Tender_Chile") as db:
            with db.cursor() as cur:
                cur.execute(
                    "SELECT run_id FROM run_ledger WHERE scraper_name = %s "
                    "ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC LIMIT 1",
                    ("Tender_Chile",),
                )
                row = cur.fetchone()
                return (row[0] or "").strip() if row else ""
    except Exception:
        return ""


def get_db_resume_state(run_id: str) -> Dict[str, Any]:
    """Get resume state from database (PostgreSQL is source of truth).
    
    Returns dict with:
        - next_step: int (step to start from)
        - completed_steps: list of completed step numbers
        - stats: dict with counts per table
    """
    if not run_id:
        return {"next_step": 0, "completed_steps": [], "stats": {}}
    
    # Ensure script dir is in path for db module import
    _script_dir = Path(__file__).parent
    if str(_script_dir) not in sys.path:
        sys.path.insert(0, str(_script_dir))
    
    try:
        from core.db.connection import CountryDB
        try:
            from db.repositories import ChileRepository
        except ImportError:
            from scripts.tender_chile.db.repositories import ChileRepository
        
        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        
        # Get counts from each table to determine progress
        stats = repo.get_run_stats()
        
        # Determine completed steps based on data presence in PostgreSQL tables
        completed_steps = []
        
        # Step 0 (Backup) - always runs, check if run exists
        if stats.get("run_exists", False):
            completed_steps.append(0)
        
        # Step 1 (Get Redirect URLs) - check tc_tender_redirects table
        if stats.get("tender_redirects_count", 0) > 0:
            completed_steps.append(1)
        
        # Step 2 (Extract Tender Details) - check tc_tender_details table
        if stats.get("tender_details_count", 0) > 0:
            completed_steps.append(2)
        
        # Step 3 (Extract Tender Awards) - check tc_tender_awards table
        if stats.get("tender_awards_count", 0) > 0:
            completed_steps.append(3)
        
        # Step 4 (Merge Final CSV) - check tc_final_output table
        if stats.get("final_output_count", 0) > 0:
            completed_steps.append(4)
        
        # Calculate next step (find first incomplete step)
        all_steps = [0, 1, 2, 3, 4]
        next_step = 0
        for step in all_steps:
            if step not in completed_steps:
                next_step = step
                break
        else:
            # All steps completed
            next_step = 5  # Beyond last step
        
        db.close()
        return {
            "next_step": next_step,
            "completed_steps": completed_steps,
            "stats": stats
        }
    except Exception as e:
        print(f"[WARN] Could not get DB resume state: {e}")
        return {"next_step": 0, "completed_steps": [], "stats": {}}


def _ensure_resume_run_id(start_step: int) -> None:
    """When resuming (start_step > 0), ensure we use the existing run_id."""
    if start_step <= 0:
        return
    cp = get_checkpoint_manager(SCRAPER_NAME)
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
        os.environ["TENDER_CHILE_RUN_ID"] = run_id
        run_id_file = get_output_dir() / ".current_run_id"
        if not run_id_file.exists() or run_id_file.read_text(encoding="utf-8").strip() != run_id:
            try:
                run_id_file.parent.mkdir(parents=True, exist_ok=True)
                run_id_file.write_text(run_id, encoding="utf-8")
            except Exception:
                pass
        cp.update_metadata({"run_id": run_id})


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, allow_failure: bool = False):
    """Run a pipeline step and mark it complete if successful."""
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{MAX_STEPS}: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    pipeline_percent = round((step_num / MAX_STEPS) * 100, 1)
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Getting redirect URLs from tender list",
        2: "Scraping: Extracting tender details from MercadoPublico",
        3: "Scraping: Extracting tender award information",
        4: "Processing: Merging all data into final CSV output"
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    run_id = _read_run_id()
    log_file_path = None  # Could be set from step output
    
    # Create metrics object for hooks
    metrics = None
    if _FOUNDATION_AVAILABLE and StepHookRegistry:
        try:
            metrics = StepMetrics(
                step_number=step_num,
                step_name=step_name,
                run_id=run_id or "pending",
                scraper_name=SCRAPER_NAME,
                started_at=datetime.now(),
                log_file_path=log_file_path
            )
            StepHookRegistry.emit_step_start(metrics)
        except Exception as e:
            print(f"[HOOKS] Warning: Could not emit step start hook: {e}")
    
    try:
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        run_id = _read_run_id()
        if run_id:
            env["TENDER_CHILE_RUN_ID"] = run_id

        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=not allow_failure,
            capture_output=False,
            env=env
        )
        
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
        cp = get_checkpoint_manager(SCRAPER_NAME)
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
        _log_step_progress(step_num, step_name, "completed", duration_seconds=duration_seconds)
        _update_run_ledger_step_count(display_step)
        
        # Record Prometheus metrics
        if _PROMETHEUS_AVAILABLE:
            try:
                from core.monitoring.prometheus_exporter import record_step_duration
                record_step_duration(SCRAPER_NAME, step_name, duration_seconds)
            except Exception as e:
                print(f"[METRICS] Warning: Could not record Prometheus metrics: {e}")
        
        # Emit step end hook
        if _FOUNDATION_AVAILABLE and StepHookRegistry and metrics:
            try:
                metrics.duration_seconds = duration_seconds
                metrics.status = "completed"
                StepHookRegistry.emit_step_end(metrics)
            except Exception as e:
                print(f"[HOOKS] Warning: Could not emit step end hook: {e}")
        
        # Record benchmark
        if _FOUNDATION_AVAILABLE and record_step_benchmark:
            try:
                record_step_benchmark(SCRAPER_NAME, step_name, duration_seconds, rows_processed=0)
            except Exception:
                pass
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check(SCRAPER_NAME, force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass
        
        # Output completion progress
        completion_percent = round(((step_num + 1) / MAX_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to get redirect URLs",
            1: "Ready to extract tender details",
            2: "Ready to extract tender awards",
            3: "Ready to merge final CSV",
            4: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {next_desc}", flush=True)
        
        # Wait 10 seconds after step completion before proceeding to next step
        print(f"\n[PAUSE] Waiting 10 seconds before next step...", flush=True)
        time.sleep(10.0)
        print(f"[PAUSE] Resuming pipeline...\n", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        duration_seconds = time.time() - start_time
        error_msg = f"exit_code={e.returncode}"
        
        # Emit step error hook
        if _FOUNDATION_AVAILABLE and StepHookRegistry and metrics:
            try:
                metrics.duration_seconds = duration_seconds
                metrics.status = "failed"
                StepHookRegistry.emit_step_error(metrics, error_msg)
            except Exception:
                pass
        
        # Record Prometheus error
        if _PROMETHEUS_AVAILABLE:
            try:
                from core.monitoring.prometheus_exporter import record_error
                record_error(SCRAPER_NAME, "step_failed")
            except Exception:
                pass
        
        if allow_failure:
            print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True)")
            _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
            return True
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=error_msg)
        return False
    except Exception as e:
        duration_seconds = time.time() - start_time
        error_msg = str(e)
        
        # Emit step error hook
        if _FOUNDATION_AVAILABLE and StepHookRegistry and metrics:
            try:
                metrics.duration_seconds = duration_seconds
                metrics.status = "failed"
                StepHookRegistry.emit_step_error(metrics, error_msg)
            except Exception:
                pass
        
        # Record Prometheus error
        if _PROMETHEUS_AVAILABLE:
            try:
                from core.monitoring.prometheus_exporter import record_error
                record_error(SCRAPER_NAME, "step_exception")
            except Exception:
                pass
        
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
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
            frontier = initialize_frontier_for_scraper("Tender_Chile")
            if frontier:
                print("[FRONTIER] Frontier queue initialized for Tender_Chile")
        except Exception as e:
            print(f"[FRONTIER] Warning: Could not initialize frontier queue: {e}")
    
    parser = argparse.ArgumentParser(description="Tender Chile Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help=f"Start from specific step (0-{MAX_STEPS})")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3],
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
            recovery_result = recover_stale_pipelines(["Tender_Chile"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")
    
    # Get run_id early for preflight checks and resume state
    run_id = os.environ.get("TENDER_CHILE_RUN_ID") or _read_run_id()
    if not run_id and not args.fresh:
        # Try to get from checkpoint or generate new
        cp = get_checkpoint_manager(SCRAPER_NAME)
        run_id = (cp.get_metadata() or {}).get("run_id") or ""
        if not run_id:
            run_id = _get_latest_run_id_from_db()
    
    # Set run_id in environment for all scripts to use
    if run_id:
        os.environ["TENDER_CHILE_RUN_ID"] = run_id
    
    # Run preflight health checks (MANDATORY GATE)
    if _FOUNDATION_AVAILABLE and PreflightChecker:
        try:
            checker = PreflightChecker("Tender_Chile", run_id or "pending")
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
            
            # Run pre-flight data quality checks (only if run_id exists in run_ledger)
            if run_id:
                try:
                    # Verify run_id exists before running DQ checks
                    from core.db.connection import CountryDB
                    db = CountryDB("Tender_Chile")
                    with db.cursor() as cur:
                        cur.execute("SELECT 1 FROM run_ledger WHERE run_id = %s", (run_id,))
                        if cur.fetchone():
                            dq_checker = DataQualityChecker("Tender_Chile", run_id)
                            dq_checker.run_preflight_checks()
                            dq_checker.save_results_to_db()
                except Exception as e:
                    print(f"[DQ] Warning: Could not run pre-flight data quality checks: {e}")
        except Exception as e:
            print(f"[PREFLIGHT] Warning: Could not run preflight checks: {e}")
    
    # Audit log: pipeline started
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_started",
                scraper_name="Tender_Chile",
                run_id=run_id or "pending",
                user="system"
            )
        except Exception:
            pass
    
    cp = get_checkpoint_manager(SCRAPER_NAME)
    
    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("TENDER_CHILE_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set TENDER_CHILE_RUN_ID.")

        # Ensure script dir is in path for db module import
        _script_dir = Path(__file__).parent
        if str(_script_dir) not in sys.path:
            sys.path.insert(0, str(_script_dir))
        
        from core.db.connection import CountryDB
        try:
            from db.repositories import ChileRepository
        except ImportError:
            from scripts.tender_chile.db.repositories import ChileRepository

        run_id = _resolve_run_id()
        db = CountryDB("Tender_Chile")
        repo = ChileRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")
    
    # Determine start step - prefer DB-based resume, fallback to file checkpoint
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("TENDER_CHILE_RUN_ID")
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
            os.environ.pop("TENDER_CHILE_RUN_ID", None)
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
        # Try DB-based resume first (PostgreSQL is source of truth)
        db_state = get_db_resume_state(run_id)
        if db_state["completed_steps"]:
            start_step = db_state["next_step"]
            step_display_map = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}
            display_start = step_display_map.get(start_step, start_step + 1)
            print(f"[DB] Resuming from step {display_start} (step {start_step}) (completed: {db_state['completed_steps']})")
            if db_state["stats"]:
                stats = db_state["stats"]
                print(f"[DB] Stats: Redirects={stats.get('tender_redirects_count', 0)}, "
                      f"Details={stats.get('tender_details_count', 0)}, "
                      f"Awards={stats.get('tender_awards_count', 0)}, "
                      f"Final={stats.get('final_output_count', 0)}")
        else:
            # Fallback to file checkpoint
            info = cp.get_checkpoint_info()
            start_step = info["next_step"]
            if info["total_completed"] > 0:
                step_display_map = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}
                display_start = step_display_map.get(start_step, start_step + 1)
                display_last = step_display_map.get(info['last_completed_step'], info['last_completed_step'] + 1)
                print(f"Resuming from step {display_start} (step {start_step}) (last completed: step {display_last})")
            else:
                print("Starting fresh run (no checkpoint found)")
    
    # When resuming, lock to existing run_id
    _ensure_resume_run_id(start_step)
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
        except Exception:
            pass
    
    # Define pipeline steps with their output files
    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_fast_redirect_urls.py", "Get Redirect URLs", ["tender_redirect_urls.csv"]),
        (2, "02_extract_tender_details_parallel.py", "Extract Tender Details", ["tender_details.csv"]),
        (3, "03_fast_extract_awards.py", "Extract Tender Awards", ["mercadopublico_supplier_rows.csv", "mercadopublico_lot_summary.csv"]),
        (4, "04_merge_final_csv.py", "Merge Final CSV", ["final_tender_data.csv"]),
    ]
    
    # Check all steps before start_step using DB-based verification (PostgreSQL is source of truth)
    # CSV files are export-only, so we check PostgreSQL tables instead
    earliest_rerun_step = None
    db_state = get_db_resume_state(run_id)
    db_completed_steps = set(db_state.get("completed_steps", []))
    
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Check PostgreSQL tables instead of CSV files
            if step_num in db_completed_steps:
                print(f"[DB] Step {step_num} ({step_name}) verified - data exists in PostgreSQL, will skip.")
            else:
                print(f"[DB] Step {step_num} ({step_name}) not complete in database. Will re-run.")
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
    
    # Adjust start_step if any earlier step needs re-running
    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (data missing in PostgreSQL).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step
    else:
        print(f"[DB] All steps before {start_step} verified successfully in PostgreSQL. Starting from step {start_step}.\n")
    
    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num < start_step:
            # Skip completed steps (check PostgreSQL tables, not CSV files)
            if step_num in db_completed_steps:
                print(f"Step {display_step}/{MAX_STEPS}: {step_name} - SKIPPED (already completed in PostgreSQL)")
            else:
                print(f"Step {display_step}/{MAX_STEPS}: {step_name} - WILL RE-RUN (data missing in PostgreSQL)")
        elif step_num == start_step:
            print(f"Step {display_step}/{MAX_STEPS}: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/{MAX_STEPS}: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")
    
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps (check PostgreSQL tables, not CSV files)
            if step_num in db_completed_steps:
                display_step = step_num + 1  # Display as 1-based
                print(f"\nStep {display_step}/{MAX_STEPS}: {step_name} - SKIPPED (already completed in PostgreSQL)")
                completion_percent = round(((step_num + 1) / MAX_STEPS) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: Redirect URLs already fetched",
                    2: "Skipped: Tender details already extracted",
                    3: "Skipped: Tender awards already extracted",
                    4: "Skipped: Final CSV already merged"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed", duration_seconds=None)
                _update_run_ledger_step_count(display_step)
            else:
                display_step = step_num + 1
                print(f"\nStep {display_step}/{MAX_STEPS}: {step_name} - WILL RE-RUN (data missing in PostgreSQL)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    # Calculate total pipeline duration
    cp = get_checkpoint_manager(SCRAPER_NAME)
    timing_info = cp.get_pipeline_timing()
    total_duration = timing_info.get("total_duration_seconds", 0.0)

    # Mark pipeline as completed
    cp.mark_as_completed()
    
    # Update run-level aggregation
    if _STEP_PROGRESS_AVAILABLE:
        try:
            update_run_ledger_aggregation(SCRAPER_NAME, run_id or _read_run_id())
        except Exception as e:
            print(f"[AGGREGATION] Warning: Could not update run aggregation: {e}")
    
    # Run post-run data quality checks
    if _FOUNDATION_AVAILABLE and DataQualityChecker and run_id:
        try:
            dq_checker = DataQualityChecker("Tender_Chile", run_id)
            dq_checker.run_postrun_checks()
            dq_checker.save_results_to_db()
        except Exception as e:
            print(f"[DQ] Warning: Could not run post-run data quality checks: {e}")
    
    # Record Prometheus metrics for pipeline completion
    if _PROMETHEUS_AVAILABLE:
        try:
            from core.monitoring.prometheus_exporter import record_scraper_duration, record_scraper_run
            record_scraper_duration("Tender_Chile", total_duration)
            record_scraper_run("Tender_Chile", "completed")
        except Exception as e:
            print(f"[METRICS] Warning: Could not record Prometheus metrics: {e}")
    
    # Audit log: pipeline completed
    if _FOUNDATION_AVAILABLE and audit_log:
        try:
            audit_log(
                action="run_completed",
                scraper_name="Tender_Chile",
                run_id=run_id or _read_run_id(),
                user="system"
            )
        except Exception:
            pass

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
    print(f"[PROGRESS] Pipeline Step: {MAX_STEPS}/{MAX_STEPS} (100%)", flush=True)
    
    # Post-run cleanup of any leftover browser PIDs for this scraper
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
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
