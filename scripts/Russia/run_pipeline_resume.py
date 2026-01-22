#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Pipeline Runner with Resume/Checkpoint Support (Simplified)

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-3)

Pipeline Steps:
    0: Backup and Clean - Backup previous output and clean for fresh run
    1: Extract VED Pricing - Scrape VED drug pricing from farmcom.info/site/reestr
    2: Extract Excluded List - Scrape excluded drugs from farmcom.info/site/reestr?vw=excl
    3: Process and Translate - Fix dates, translate using Dictionary.csv + AI fallback
    4: Format for Export - Convert to standardized pricing and discontinued templates
"""

import sys
import subprocess
import argparse
import time
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Russia to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_central_output_dir, get_output_dir

# Total actual steps: steps 0-4 = 5 steps
TOTAL_STEPS = 5

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
        3: "Processing: Fixing dates and translating to English",
        4: "Formatting: Converting to standardized export templates",
    }
    step_desc = step_descriptions.get(step_num, step_name)

    print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    # Track step execution time
    start_time = time.time()
    duration_seconds = None

    try:
        cmd = [sys.executable, "-u", str(script_path)]
        if extra_args:
            cmd.extend(extra_args)

        result = subprocess.run(
            cmd,
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
        cp = get_checkpoint_manager("Russia")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)

        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / TOTAL_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0

        next_step_descriptions = {
            0: "Ready to extract VED drug pricing data",
            1: "Ready to extract excluded drugs list",
            2: "Ready to process and translate data",
            3: "Ready to format data for export",
            4: "Pipeline completed successfully",
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
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        return False
    except Exception as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        return False

def main():
    parser = argparse.ArgumentParser(description="Russia Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint and scraper progress)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-4)")

    args = parser.parse_args()

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
    # Pipeline: 5 steps total (0-4)
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None, None),
        (1, "01_russia_farmcom_scraper.py", "Extract VED Pricing Data", ["russia_farmcom_ved_moscow_region.csv"], None),
        (2, "02_russia_farmcom_excluded_scraper.py", "Extract Excluded List", ["russia_farmcom_excluded_list.csv"], None),
        (3, "03_process_and_translate.py", "Process and Translate",
         ["russia_farmcom_ved_moscow_region.csv",
          "en_russia_farmcom_ved_moscow_region.csv",
          "russia_farmcom_excluded_list.csv",
          "en_russia_farmcom_excluded_list.csv",
          str(central_output_dir / "Russia_VED_Moscow_Region.csv"),
          str(central_output_dir / "EN_Russia_VED_Moscow_Region.csv"),
          str(central_output_dir / "Russia_Excluded_List.csv"),
          str(central_output_dir / "EN_Russia_Excluded_List.csv")],
         None),
        (4, "04_format_for_export.py", "Format for Export",
         ["russia_pricing_data.csv",
          "russia_discontinued_list.csv",
          str(central_output_dir / "Russia_Pricing_Data.csv"),
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
                    3: "Skipped: Processing and translation already completed",
                    4: "Skipped: Export formatting already completed",
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")

                print(f"[PROGRESS] Pipeline Step: {display_step}/{TOTAL_STEPS} ({completion_percent}%) - {skip_desc}", flush=True)
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

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"[TIMING] Total pipeline duration: {total_duration_str}")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {TOTAL_STEPS}/{TOTAL_STEPS} (100%)", flush=True)

    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
