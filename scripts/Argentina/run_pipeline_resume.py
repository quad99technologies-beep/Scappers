#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina Pipeline Runner with Resume/Checkpoint Support

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

# Add scripts/Argentina to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, PREPARED_URLS_FILE, OUTPUT_PRODUCTS_CSV, OUTPUT_TRANSLATED_CSV

def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num}/6: {step_name}")
    print(f"{'='*80}\n")
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
        cp = get_checkpoint_manager("Argentina")
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files)
        else:
            cp.mark_step_complete(step_num, step_name)
        
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e}")
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
        (1, "01_getProdList.py", "Get Product List", None),  # Output is in input dir
        (2, "02_prepare_urls.py", "Prepare URLs", [str(output_dir / PREPARED_URLS_FILE)]),
        (3, "03_alfabeta_api_scraper.py", "Scrape Products (API)", [str(output_dir / OUTPUT_PRODUCTS_CSV)]),
        (4, "04_alfabeta_selenium_scraper.py", "Scrape Products (Selenium)", [str(output_dir / OUTPUT_PRODUCTS_CSV)]),
        (5, "05_TranslateUsingDictionary.py", "Translate Using Dictionary", [str(output_dir / OUTPUT_TRANSLATED_CSV)]),
        (6, "06_GenerateOutput.py", "Generate Output", None),  # Output files vary by date
    ]
    
    # Run steps starting from start_step
    print(f"\n{'='*80}")
    print(f"PIPELINE EXECUTION PLAN")
    print(f"{'='*80}")
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps
            if cp.is_step_complete(step_num):
                print(f"Step {step_num}/6: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"Step {step_num}/6: {step_name} - SKIPPED (before start step {start_step})")
        elif step_num == start_step:
            print(f"Step {step_num}/6: {step_name} - WILL RUN NOW (starting from here)")
        else:
            print(f"Step {step_num}/6: {step_name} - WILL RUN AFTER previous steps complete")
    print(f"{'='*80}\n")
    
    # Now execute the steps
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            # Skip completed steps
            continue
        
        success = run_step(step_num, script_name, step_name, output_files)
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

