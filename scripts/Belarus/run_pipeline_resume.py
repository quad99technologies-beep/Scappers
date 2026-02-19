#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Belarus Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-2)
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
from config_loader import get_output_dir

# Import startup recovery
try:
    from shared_workflow_runner import recover_stale_pipelines
    _RECOVERY_AVAILABLE = True
except ImportError:
    _RECOVERY_AVAILABLE = False

# Import browser PID cleanup (Chrome + Firefox for Belarus Tor Browser)
try:
    from core.browser.chrome_pid_tracker import terminate_scraper_pids
    _CHROME_CLEANUP_AVAILABLE = True
except ImportError:
    _CHROME_CLEANUP_AVAILABLE = False
    def terminate_scraper_pids(scraper_name, repo_root, silent=False):
        return 0
try:
    from core.browser.firefox_pid_tracker import terminate_firefox_pids
    _FIREFOX_CLEANUP_AVAILABLE = True
except ImportError:
    _FIREFOX_CLEANUP_AVAILABLE = False
    def terminate_firefox_pids(scraper_name, repo_root, silent=False):
        return 0
_BROWSER_CLEANUP_AVAILABLE = _CHROME_CLEANUP_AVAILABLE or _FIREFOX_CLEANUP_AVAILABLE

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


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("BELARUS_RUN_ID")
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
    """Persist step progress in PostgreSQL for Belarus pipeline."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    log_step_progress("Belarus", run_id, step_num, step_name, status, error_message)


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id or not _STEP_PROGRESS_AVAILABLE:
        return
    update_run_ledger_step_count("Belarus", run_id, step_num)

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    # Total actual steps: 0 (Backup) + 1 (Extract) + 2 (PCID Map) + 3 (Translate) + 4 (Format Export) = 5 steps (steps 0-4)
    total_steps = 5
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    # When starting a step, show progress based on completed steps
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Extracting drug registration and pricing data from rceth.by",
        2: "Mapping: Applying PCID mappings to extracted data",
        3: "Translating: Dictionary lookup + AI translation for missing words",
        4: "Export: Formatting English export slate (same template as Russia)",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
            capture_output=False
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
        cp = get_checkpoint_manager("Belarus")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        # Log step progress to database
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(display_step)
        
        # MEMORY FIX: Periodic resource monitoring
        try:
            from core.monitoring.resource_monitor import periodic_resource_check
            resource_status = periodic_resource_check("Belarus", force=False)
            if resource_status.get("warnings"):
                for warning in resource_status["warnings"]:
                    print(f"[RESOURCE WARNING] {warning}", flush=True)
        except Exception:
            pass
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to extract drug registration data",
            1: "Ready to apply PCID mappings",
            2: "Ready to translate Russian text to English",
            3: "Ready to format English export slate",
            4: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
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
    parser = argparse.ArgumentParser(description="Belarus Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-4)")
    parser.add_argument("--clear-step", type=int, choices=[1, 2, 3, 4],
                        help="Clear data for a step (and optionally downstream) before running")
    parser.add_argument("--clear-downstream", action="store_true",
                        help="When used with --clear-step, also clear downstream steps")
    
    args = parser.parse_args()
    
    # Recover stale pipelines on startup (handles crash recovery)
    if _RECOVERY_AVAILABLE:
        try:
            recovery_result = recover_stale_pipelines(["Belarus"])
            if recovery_result.get("total_recovered", 0) > 0:
                print(f"[RECOVERY] Recovered {recovery_result['total_recovered']} stale pipeline state(s)")
        except Exception as e:
            print(f"[RECOVERY] Warning: Could not run startup recovery: {e}")
    
    cp = get_checkpoint_manager("Belarus")
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        
        # Check if external run_id is provided (from GUI/Telegram/API sync)
        external_run_id = os.environ.get("BELARUS_RUN_ID")
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
    output_dir = get_output_dir()
    central_exports_dir = Path(__file__).resolve().parents[2] / "exports" / "Belarus"
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_belarus_rceth_extract.py", "Extract RCETH Data", ["belarus_rceth_raw.csv"]),
        (2, "02_belarus_pcid_mapping.py", "PCID Mapping", ["BELARUS_PCID_MAPPED_OUTPUT.csv"]),
        (3, "04_belarus_process_and_translate.py", "Process and Translate", None),  # DB-based, no CSV output
        (4, "03_belarus_format_for_export.py", "Format English Export Slate", [str(central_exports_dir / "Belarus_Pricing_Data.csv")]),
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
    
    # Optional pre-clear of data for a step/run_id
    if args.clear_step is not None:
        def _resolve_run_id():
            run_id = os.environ.get("BELARUS_RUN_ID")
            if run_id:
                return run_id
            run_id_file = get_output_dir() / ".current_run_id"
            if run_id_file.exists():
                return run_id_file.read_text(encoding="utf-8").strip()
            raise RuntimeError("No run_id found. Run Step 0 first or set BELARUS_RUN_ID.")

        from core.db.connection import CountryDB
        try:
            from db.repositories import BelarusRepository
        except ImportError:
            from scripts.Belarus.db.repositories import BelarusRepository

        run_id = _resolve_run_id()
        db = CountryDB("Belarus")
        repo = BelarusRepository(db, run_id)
        cleared = repo.clear_step_data(args.clear_step, include_downstream=args.clear_downstream)
        print(f"[CLEAR] run_id={run_id} step={args.clear_step} downstream={args.clear_downstream}")
        for tbl, cnt in cleared.items():
            print(f"  - {tbl}: deleted {cnt} rows")
    
    # Pre-run cleanup of any leftover browser PIDs for this scraper (Chrome + Firefox/Tor)
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            if _CHROME_CLEANUP_AVAILABLE:
                terminate_scraper_pids("Belarus", _repo_root, silent=True)
            if _FIREFOX_CLEANUP_AVAILABLE:
                terminate_firefox_pids("Belarus", _repo_root, silent=True)
        except Exception:
            pass
    
    # Run steps starting from start_step
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                total_steps = 5
                display_step = step_num + 1  # Display as 1-based
                print(f"\nStep {display_step}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
                # Output progress for skipped step
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: RCETH data already extracted",
                    2: "Skipped: PCID mapping already done",
                    3: "Skipped: Translation already completed",
                    4: "Skipped: English export slate already formatted"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(display_step)
            else:
                # Step marked complete but output files missing - will re-run
                display_step = step_num + 1
                print(f"\nStep {display_step}/5: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: 5/5 (100%)", flush=True)
    
    # Post-run cleanup of any leftover browser PIDs for this scraper (Chrome + Firefox/Tor)
    if _BROWSER_CLEANUP_AVAILABLE:
        try:
            if _CHROME_CLEANUP_AVAILABLE:
                terminate_scraper_pids("Belarus", _repo_root, silent=True)
            if _FIREFOX_CLEANUP_AVAILABLE:
                terminate_firefox_pids("Belarus", _repo_root, silent=True)
        except Exception:
            pass
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except Exception as e:
        print(f"[WARN] Lock file cleanup failed: {e}")

if __name__ == "__main__":
    main()
