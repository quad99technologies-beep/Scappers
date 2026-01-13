#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-2)
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

# Add scripts/Netherlands to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/2: {step_name}")
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    total_steps = 2
    # When starting a step, show progress based on completed steps (not including current step)
    # This prevents showing 100% when starting the last step
    # Step 0 starting: 0%, Step 1 starting: 33%, Step 2 starting: 66%
    pipeline_percent = round((step_num / (total_steps + 1)) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Collecting: Gathering product URLs from search terms",
        2: "Extracting: Processing reimbursement data from collected URLs"
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {step_num}/{total_steps} ({pipeline_percent}%) - {step_desc}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print(f"[PIPELINE] This step will run until completion before moving to next step.\n")
    
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
        cp = get_checkpoint_manager("Netherlands")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        # Output completion progress with descriptive message
        # When completing step N, show progress as (N+1) completed steps out of total
        # Step 0 completed: 33%, Step 1 completed: 66%, Step 2 completed: 100%
        completion_percent = round(((step_num + 1) / (total_steps + 1)) * 100, 1)
        # Only show 100% when the last step actually completes
        if step_num == total_steps - 1:
            completion_percent = 100.0
        elif completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to collect URLs",
            1: "Ready to extract reimbursement data",
            2: "Pipeline completed successfully"
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
    parser = argparse.ArgumentParser(description="Netherlands Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-2)")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager("Netherlands")
    
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
        (1, "01_collect_urls.py", "Collect URLs", [
            str(output_dir / "collected_urls.csv"),
            str(output_dir / "packs.csv"),
            str(output_dir / "completed_prefixes.csv")
        ]),
        (2, "02_reimbursement_extraction.py", "Reimbursement Extraction", [
            str(output_dir / "details.csv"),
            str(output_dir / "costs.csv")
        ]),
    ]
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
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
    
    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"Step {step_num}/2: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {step_num}/2: {step_name} - WILL RE-RUN (output files missing)")
        elif step_num == start_step:
            print(f"Step {step_num}/2: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {step_num}/2: {step_name} - WILL RUN AFTER previous steps complete")
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
                total_steps = 2
                # For skipped steps, show progress based on completed steps
                # Step 0 skipped: 33%, Step 1 skipped: 66%, Step 2 skipped: 100%
                completion_percent = round(((step_num + 1) / (total_steps + 1)) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: URLs already collected",
                    2: "Skipped: Reimbursement extraction already completed"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
            else:
                # Step marked complete but output files missing - will re-run
                print(f"\nStep {step_num}/2: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    # Calculate total pipeline duration
    cp = get_checkpoint_manager("Netherlands")
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
    print(f"[PROGRESS] Pipeline Step: 2/2 (100%)", flush=True)
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()
