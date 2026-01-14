#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Ontario Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-3)
"""

import sys
import subprocess
import argparse
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Canada Ontario to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/3: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    total_steps = 4  # Steps 0-3 = 4 total steps
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing output directory",
        1: "Extracting product details",
        2: "Scraping EAP product prices from Ontario.ca",
        3: "Generating final output",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {step_num}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
            capture_output=False
        )
        
        # Mark step as complete
        cp = get_checkpoint_manager("CanadaOntario")
        if output_files:
            # Convert to absolute paths
            abs_output_files = []
            for f in output_files:
                if Path(f).is_absolute():
                    abs_output_files.append(f)
                else:
                    # Use platform output directory
                    output_path = get_output_dir() / f
                    if output_path.exists():
                        abs_output_files.append(str(output_path))
                    else:
                        # Fallback to script directory if file not found
                        script_dir = Path(__file__).parent
                        abs_output_files.append(str(script_dir / f))
            cp.mark_step_complete(step_num, step_name, abs_output_files)
        else:
            cp.mark_step_complete(step_num, step_name)
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to extract product details",
            1: "Ready to extract EAP prices",
            2: "Ready to generate final output",
            3: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Canada Ontario Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-3)")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager("CanadaOntario")
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
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
    script_dir = Path(__file__).parent
    output_dir = get_output_dir()
    from config_loader import (
        get_central_output_dir,
        FINAL_REPORT_NAME_PREFIX,
        FINAL_REPORT_DATE_FORMAT,
        EAP_PRICES_CSV_NAME
    )
    from datetime import datetime
    central_output_dir = get_central_output_dir()  # exports/CanadaOntario/
    date_str = datetime.now().strftime(FINAL_REPORT_DATE_FORMAT)
    final_report_name = f"{FINAL_REPORT_NAME_PREFIX}{date_str}.csv"
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_extract_product_details.py", "Extract Product Details", [
            str(output_dir / "products.csv"),
            str(output_dir / "manufacturer_master.csv"),
            str(output_dir / "completed_letters.json")
        ]),
        (2, "02_ontario_eap_prices.py", "Extract EAP Prices", [
            str(output_dir / EAP_PRICES_CSV_NAME)
        ]),
        (3, "03_GenerateOutput.py", "Generate Final Output", [
            str(central_output_dir / final_report_name)
        ]),
    ]
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        expected_files.append(f)
                    else:
                        # Use platform output directory
                        expected_files.append(str(get_output_dir() / Path(f).name))
            
            # Check if step output exists
            step_complete = cp.is_step_complete(step_num)
            if not step_complete:
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
            elif expected_files:
                # Verify output files exist
                all_exist = all(Path(f).exists() for f in expected_files)
                if not all_exist:
                    print(f"[WARNING] Step {step_num} marked complete but output files missing. Will re-run.")
                    if earliest_rerun_step is None or step_num < earliest_rerun_step:
                        earliest_rerun_step = step_num
    
    if earliest_rerun_step is not None and earliest_rerun_step < start_step:
        print(f"[WARNING] Step {earliest_rerun_step} needs to be re-run. Adjusting start step.")
        start_step = earliest_rerun_step
    
    # Run steps from start_step
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            print(f"[SKIP] Step {step_num}: {step_name} (already completed)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline stopped at step {step_num}")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
