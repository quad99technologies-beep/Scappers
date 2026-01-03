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
    print(f"Step {step_num}/6: {step_name}")
    print(f"{'='*80}\n")
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=not allow_failure,
            capture_output=False
        )
        
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
            cp.mark_step_complete(step_num, step_name, abs_output_files)
        else:
            cp.mark_step_complete(step_num, step_name)
        
        return True
    except subprocess.CalledProcessError as e:
        if allow_failure:
            print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True)")
            return True
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e}")
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
    
    # Run steps starting from start_step
    for step_info in steps:
        if len(step_info) == 4:
            step_num, script_name, step_name, output_files = step_info
            allow_failure = False
        else:
            step_num, script_name, step_name, output_files, allow_failure = step_info
        
        if step_num < start_step:
            # Skip completed steps
            if cp.is_step_complete(step_num):
                print(f"\nStep {step_num}/6: {step_name} - SKIPPED (already completed)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files, allow_failure)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    
    # Clean up lock file
    try:
        cleanup_script = Path(__file__).parent / "cleanup_lock.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], capture_output=True)
    except:
        pass

if __name__ == "__main__":
    main()

