#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Belarus Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-1)
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

# Add scripts/Belarus to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/1: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    total_steps = 1
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Scraping: Extracting drug registration and pricing data from rceth.by",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {step_num}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    
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
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to extract drug registration data",
            1: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
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
    parser = argparse.ArgumentParser(description="Belarus Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-1)")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager("Belarus")
    
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
    output_dir = get_output_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_belarus_rceth_extract.py", "Extract RCETH Data", ["belarus_rceth_raw.csv"]),
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
    
    # Run steps starting from start_step
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"\nStep {step_num}/1: {step_name} - SKIPPED (already completed in checkpoint)")
                # Output progress for skipped step
                total_steps = 1
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: RCETH data already extracted"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
            else:
                # Step marked complete but output files missing - will re-run
                print(f"\nStep {step_num}/1: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: 1/1 (100%)", flush=True)
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
