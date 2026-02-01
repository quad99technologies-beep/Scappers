#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-6)
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

# Add scripts/Argentina to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, USE_API_STEPS


def _read_run_id() -> str:
    """Load run_id from env or .current_run_id if present."""
    run_id = os.environ.get("ARGENTINA_RUN_ID")
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
    """Persist step progress in PostgreSQL for Argentina pipeline."""
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ar_step_progress
                        (run_id, step_number, step_name, progress_key, status, error_message, started_at, completed_at)
                    VALUES
                        (%s, %s, %s, 'pipeline', %s, %s,
                         CASE WHEN %s = 'in_progress' THEN CURRENT_TIMESTAMP ELSE NULL END,
                         CASE WHEN %s IN ('completed','failed','skipped') THEN CURRENT_TIMESTAMP ELSE NULL END)
                    ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                        step_name = EXCLUDED.step_name,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message,
                        started_at = COALESCE(ar_step_progress.started_at, EXCLUDED.started_at),
                        completed_at = EXCLUDED.completed_at
                    """,
                    (run_id, step_num, step_name, status, error_message, status, status),
                )
    except Exception:
        # Non-blocking: progress logging should not break pipeline execution
        return


def _update_run_ledger_step_count(step_num: int) -> None:
    """Update run_ledger.step_count for the current run_id."""
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB

        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute(
                    "UPDATE run_ledger SET step_count = %s WHERE run_id = %s",
                    (step_num, run_id),
                )
    except Exception:
        return


def _mark_run_ledger_active_if_resume(start_step: int) -> None:
    """Ensure run_ledger status is set to running when resuming mid-pipeline."""
    if start_step <= 0:
        return
    run_id = _read_run_id()
    if not run_id:
        return
    try:
        from core.db.connection import CountryDB
        from db.repositories import ArgentinaRepository

        with CountryDB("Argentina") as db:
            repo = ArgentinaRepository(db, run_id)
            repo.resume_run()
    except Exception:
        # Non-blocking: don't fail pipeline if DB status update fails
        return


def cleanup_temp_files(output_dir: Path):
    """Remove stale temp files created during CSV rewrites (tmp* with no extension)."""
    try:
        removed = 0
        for item in output_dir.iterdir():
            if not item.is_file():
                continue
            if item.suffix:
                continue
            if not item.name.startswith("tmp"):
                continue
            try:
                item.unlink()
                removed += 1
            except Exception:
                continue
        if removed:
            print(f"[CLEANUP] Removed {removed} stale temp file(s) from {output_dir}", flush=True)
    except Exception:
        pass

def cleanup_legacy_progress(output_dir: Path):
    """Remove deprecated alfabeta_progress.csv if present."""
    try:
        legacy = output_dir / "alfabeta_progress.csv"
        if legacy.exists():
            legacy.unlink()
            print(f"[CLEANUP] Removed deprecated progress file: {legacy}", flush=True)
    except Exception:
        pass

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    # Total actual steps: steps 0-6 = 7 steps
    total_steps = 7
    display_step = step_num + 1  # Display as 1-based for user friendliness
    
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Fetching product list from AlfaBeta website",
        2: "Preparing: Building product URLs for scraping",
        3: "Scraping: Extracting product details using Selenium with 5-round retry (this may take a while)",
        4: "Scraping: Extracting remaining products using API",
        5: "Processing: Translating Spanish terms to English using dictionary",
        6: "Generating: Creating final output files with PCID mapping"
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None

    # Log step start (if run_id already available)
    _log_step_progress(step_num, step_name, "in_progress")
    
    try:
        env = os.environ.copy()
        env["PIPELINE_RUNNER"] = "1"
        env["PIPELINE_STEP_DISPLAY"] = str(display_step)
        env["PIPELINE_TOTAL_STEPS"] = str(total_steps)
        env["PIPELINE_STEP_NAME"] = step_name
        env["PIPELINE_SCRIPT"] = script_name
        run_id = _read_run_id()
        if run_id:
            env["ARGENTINA_RUN_ID"] = run_id
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
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
        cp = get_checkpoint_manager("Argentina")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)

        # Log DB progress now that run_id exists (step 0 gets logged here)
        _log_step_progress(step_num, step_name, "completed")
        _update_run_ledger_step_count(display_step)

        # Cleanup stale temp files (e.g., tmp* from CSV rewrites)
        cleanup_temp_files(get_output_dir())
        cleanup_legacy_progress(get_output_dir())
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to fetch product list",
            1: "Ready to prepare URLs",
            2: "Ready to scrape products with Selenium",
            3: "Ready to scrape products with API" if USE_API_STEPS else "Ready to translate terms",
            4: "Ready to translate terms",
            5: "Ready to generate final output",
            6: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=f"exit_code={e.returncode}")
        return False
    except Exception as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        _log_step_progress(step_num, step_name, "failed", error_message=str(e))
        return False

def main():
    parser = argparse.ArgumentParser(description="Argentina Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-6)")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager("Argentina")
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
        # Fresh run should not reuse previous run_id
        os.environ.pop("ARGENTINA_RUN_ID", None)
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
    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_getProdList.py", "Get Product List", None),  # DB-backed
        (2, "02_prepare_urls.py", "Prepare URLs", None),  # DB-backed
        (3, "03_alfabeta_selenium_scraper.py", "Scrape Products (Selenium - 5 Rounds)", None),  # DB-backed
        (4, "04_alfabeta_api_scraper.py", "Scrape Products (API)", None),  # DB-backed (to be refactored)
        (5, "05_TranslateUsingDictionary.py", "Translate Using Dictionary", None),
        (6, "06_GenerateOutput.py", "Generate Output", None),  # Output files vary by date
    ]
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
        if step_num == 4 and not USE_API_STEPS:
            continue
        if step_num < start_step:
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if not cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                # Step marked complete but output files missing - needs re-run
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
    
    # Adjust start_step if any earlier step needs re-running
    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step

    # If resuming mid-pipeline, mark run_ledger as running (status) and resume (mode)
    _mark_run_ledger_active_if_resume(start_step)
    
    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        display_step = step_num + 1  # Display as 1-based
        if step_num == 4 and not USE_API_STEPS:
            print(f"Step {display_step}/7: {step_name} - SKIPPED (disabled in config)")
            continue
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {display_step}/7: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {display_step}/7: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {display_step}/7: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {display_step}/7: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")
    
    # Now execute the steps
    for step_num, script_name, step_name, output_files in steps:
        if step_num == 4 and not USE_API_STEPS:
            display_step = step_num + 1
            total_steps = 7
            completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
            if completion_percent > 100.0:
                completion_percent = 100.0
            print(f"\nStep {display_step}/7: {step_name} - SKIPPED (disabled in config)")
            print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - Skipped: API step disabled", flush=True)
            cp.mark_step_complete(step_num, step_name, duration_seconds=0.0)
            _log_step_progress(step_num, step_name, "skipped")
            _update_run_ledger_step_count(display_step)
            continue
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                # Total actual steps: steps 0-6 = 7 steps
                total_steps = 7
                display_step = step_num + 1  # Display as 1-based
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: Product list already fetched",
                    2: "Skipped: URLs already prepared",
                    3: "Skipped: Selenium scraping already completed",
                    4: "Skipped: API scraping already completed",
                    5: "Skipped: Translation already completed",
                    6: "Skipped: Output already generated"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {display_step}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
                _log_step_progress(step_num, step_name, "completed")
                _update_run_ledger_step_count(display_step)
            else:
                # Step marked complete but output files missing - will re-run
                display_step = step_num + 1
                print(f"\nStep {display_step}/7: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    # Calculate total pipeline duration
    cp = get_checkpoint_manager("Argentina")
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
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: 7/7 (100%)", flush=True)
    cleanup_temp_files(get_output_dir())
    cleanup_legacy_progress(get_output_dir())
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
