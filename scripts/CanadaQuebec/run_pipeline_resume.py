#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CanadaQuebec Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-6)
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

# Add scripts/CanadaQuebec to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_csv_output_dir, get_split_pdf_dir

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, allow_failure: bool = False):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/6: {step_name}")  # Display: last step is 6
    print(f"{'='*80}\n")
    
    # Output overall pipeline progress with descriptive message
    total_steps = 7  # Steps 0-6 = 7 total steps
    pipeline_percent = round((step_num / total_steps) * 100, 1)
    
    # Create meaningful progress description based on step
    step_descriptions = {
        0: "Preparing: Backing up previous results and cleaning output directory",
        1: "Processing: Splitting PDF into separate annexe files",
        2: "Validating: Checking PDF structure (optional step)",
        3: "Extracting: Processing Annexe IV.1 with AI (this may take a while)",
        4: "Extracting: Processing Annexe IV.2 with AI (this may take a while)",
        5: "Extracting: Processing Annexe V pages (this may take a while)",
        6: "Generating: Merging all annexes into final output"
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
            check=not allow_failure,
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
        
        # Mark step as complete (even if allow_failure and it failed)
        cp = get_checkpoint_manager("CanadaQuebec")
        if output_files:
            abs_output_files = []
            for f in output_files:
                if Path(f).is_absolute():
                    abs_output_files.append(f)
                else:
                    # Try to resolve relative to output directories
                    csv_dir = get_csv_output_dir()
                    pdf_dir = get_split_pdf_dir()
                    if f.endswith('.csv'):
                        abs_output_files.append(str(csv_dir / f))
                    elif f.endswith('.pdf'):
                        abs_output_files.append(str(pdf_dir / f))
                    else:
                        abs_output_files.append(f)
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        # Output completion progress with descriptive message
        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to split PDF",
            1: "Ready to validate PDF structure",
            2: "Ready to extract Annexe IV.1",
            3: "Ready to extract Annexe IV.2",
            4: "Ready to extract Annexe V",
            5: "Ready to merge annexes",
            6: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {next_desc}", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        if allow_failure:
            print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True) (duration: {duration_seconds:.2f}s)")
            # Still mark as complete and output progress
            completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
            if completion_percent > 100.0:
                completion_percent = 100.0
            cp = get_checkpoint_manager("CanadaQuebec")
            if output_files:
                abs_output_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        abs_output_files.append(f)
                    else:
                        csv_dir = get_csv_output_dir()
                        pdf_dir = get_split_pdf_dir()
                        if f.endswith('.csv'):
                            abs_output_files.append(str(csv_dir / f))
                        elif f.endswith('.pdf'):
                            abs_output_files.append(str(pdf_dir / f))
                        else:
                            abs_output_files.append(f)
                cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
            else:
                cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
            print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%)", flush=True)
            return True
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        return False
    except Exception as e:
        # Track duration even on failure
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        return False

def main():
    parser = argparse.ArgumentParser(description="CanadaQuebec Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-6)")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager("CanadaQuebec")
    
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
    csv_dir = get_csv_output_dir()
    pdf_dir = get_split_pdf_dir()
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_split_pdf_into_annexes.py", "Split PDF into Annexes", ["annexe_iv1.pdf", "annexe_iv2.pdf", "annexe_v.pdf"]),
        (2, "02_validate_pdf_structure.py", "Validate PDF Structure", None, True),  # Optional step
        (3, "03_extract_annexe_iv1.py", "Extract Annexe IV.1", ["annexe_iv1_extracted.csv"]),
        (4, "04_extract_annexe_iv2.py", "Extract Annexe IV.2", ["annexe_iv2_extracted.csv"]),
        (5, "05_extract_annexe_v.py", "Extract Annexe V", ["annexe_v_extracted.csv"]),
        (6, "06_merge_all_annexes.py", "Merge All Annexes", None),  # Output files vary by date
    ]
    
    # Check all steps before start_step to find the earliest step that needs re-running
    earliest_rerun_step = None
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
        else:
            step_num, script_name, step_name, output_files, allow_failure = step_info
        
        if step_num < start_step:
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                csv_dir = get_csv_output_dir()
                pdf_dir = get_split_pdf_dir()
                expected_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        expected_files.append(f)
                    elif f.endswith('.csv'):
                        expected_files.append(str(csv_dir / f))
                    elif f.endswith('.pdf'):
                        expected_files.append(str(pdf_dir / f))
                    else:
                        expected_files.append(str(csv_dir / f))  # Default to CSV dir
            
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
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
            allow_failure = False
        else:
            step_num, script_name, step_name, output_files, allow_failure = step_info
        
        if step_num < start_step:
            # Skip completed steps (verify output files exist)
            # Convert relative output files to absolute paths for verification
            expected_files = None
            if output_files:
                csv_dir = get_csv_output_dir()
                pdf_dir = get_split_pdf_dir()
                expected_files = []
                for f in output_files:
                    if Path(f).is_absolute():
                        expected_files.append(f)
                    elif f.endswith('.csv'):
                        expected_files.append(str(csv_dir / f))
                    elif f.endswith('.pdf'):
                        expected_files.append(str(pdf_dir / f))
                    else:
                        expected_files.append(str(csv_dir / f))  # Default to CSV dir
            
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                total_steps = 7  # Steps 0-6 = 7 total steps
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                
                step_descriptions = {
                    0: "Skipped: Backup already completed",
                    1: "Skipped: PDF already split into annexes",
                    2: "Skipped: PDF structure already validated",
                    3: "Skipped: Annexe IV.1 already extracted",
                    4: "Skipped: Annexe IV.2 already extracted",
                    5: "Skipped: Annexe V already extracted",
                    6: "Skipped: Annexes already merged"
                }
                skip_desc = step_descriptions.get(step_num, f"Skipped: {step_name} already completed")
                
                print(f"\nStep {step_num}/6: {step_name} - SKIPPED (already completed in checkpoint)")
                print(f"[PROGRESS] Pipeline Step: {step_num + 1}/7 ({completion_percent}%) - {skip_desc}", flush=True)
            else:
                # Step marked complete but output files missing - will re-run
                print(f"\nStep {step_num}/6: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files, allow_failure)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    # Calculate total pipeline duration
    cp = get_checkpoint_manager("CanadaQuebec")
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
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()

