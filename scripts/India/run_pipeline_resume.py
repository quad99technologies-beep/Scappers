#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-1)
    python run_pipeline_resume.py --resume-details  # Resume only details extraction

Pipeline Steps:
    0: Backup and Clean - Backs up previous output and cleans directory
    1: Extract Medicine Details - Loads formulations from input/India/formulations.csv
       and scrapes medicine details from NPPA website

Features:
- Pipeline-level checkpoint (which step to run)
- Formulation-level checkpoint within details extraction (resume partial scrapes)
- Automatic cleanup of Chrome instances
- Final report generation
"""

import sys
import subprocess
import argparse
import time
import json
import atexit
import os
from pathlib import Path
from datetime import datetime

# Force unbuffered output
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/India to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import get_output_dir, load_env_file

# Import Chrome PID tracker for cleanup
try:
    from core.chrome_pid_tracker import terminate_chrome_pids, cleanup_pid_file
    _PID_TRACKER_AVAILABLE = True
except ImportError:
    _PID_TRACKER_AVAILABLE = False
    def terminate_chrome_pids(name, root, silent=False): return 0
    def cleanup_pid_file(name, root): pass

# Load environment configuration
load_env_file()

SCRAPER_NAME = "India"
# Total actual steps: steps 0-1 = 2 steps (backup/clean + get details)
MAX_STEPS = 2


def cleanup_chrome_on_exit():
    """Cleanup Chrome instances on exit."""
    if _PID_TRACKER_AVAILABLE:
        terminated = terminate_chrome_pids(SCRAPER_NAME, _repo_root, silent=True)
        cleanup_pid_file(SCRAPER_NAME, _repo_root)
        if terminated > 0:
            print(f"[CLEANUP] Terminated {terminated} Chrome process(es)")


# Register cleanup on exit
atexit.register(cleanup_chrome_on_exit)


def get_formulation_checkpoint_status(output_dir: Path) -> dict:
    """Get status of formulation-level checkpoint for details extraction step."""
    checkpoint_file = output_dir / ".checkpoints" / "formulation_progress.json"
    
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {
                    "completed": len(data.get("completed_formulations", [])),
                    "in_progress": data.get("in_progress"),
                    "last_updated": data.get("last_updated"),
                    "stats": data.get("stats", {})
                }
        except Exception as e:
            print(f"[WARN] Could not read formulation checkpoint: {e}")
    
    return {"completed": 0, "in_progress": None, "last_updated": None, "stats": {}}


def clear_formulation_checkpoint(output_dir: Path):
    """Clear formulation-level checkpoint."""
    checkpoint_file = output_dir / ".checkpoints" / "formulation_progress.json"
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        print("[INFO] Cleared formulation checkpoint")


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None, 
             allow_failure: bool = False, extra_args: list = None):
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
        1: "Scraping: Extracting medicine details and substitutes",
    }
    step_desc = step_descriptions.get(step_num, step_name)
    
    print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({pipeline_percent}%) - {step_desc}", flush=True)
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False
    
    # Track step execution time
    start_time = time.time()
    duration_seconds = None
    
    # Build command with extra args
    cmd = [sys.executable, "-u", str(script_path)]
    if extra_args:
        cmd.extend(extra_args)
    
    try:
        result = subprocess.run(
            cmd,
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
        
        # Mark step as complete
        cp = get_checkpoint_manager(SCRAPER_NAME)
        if output_files:
            # Convert to absolute paths
            output_dir = get_output_dir()
            abs_output_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            cp.mark_step_complete(step_num, step_name, abs_output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)
        
        # Output completion progress
        completion_percent = round(((step_num + 1) / MAX_STEPS) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0
        
        next_step_descriptions = {
            0: "Ready to extract medicine details",
            1: "Pipeline completed successfully"
        }
        next_desc = next_step_descriptions.get(step_num + 1, "Moving to next step")
        
        print(f"[PROGRESS] Pipeline Step: {display_step}/{MAX_STEPS} ({completion_percent}%) - {next_desc}", flush=True)
        
        return True
    except subprocess.CalledProcessError as e:
        duration_seconds = time.time() - start_time
        if allow_failure:
            print(f"\nWARNING: Step {step_num} ({step_name}) failed but continuing (allow_failure=True)")
            return True
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        return False
    except Exception as e:
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        return False


def print_checkpoint_status():
    """Print current checkpoint status."""
    cp = get_checkpoint_manager(SCRAPER_NAME)
    info = cp.get_checkpoint_info()
    output_dir = get_output_dir()
    formulation_status = get_formulation_checkpoint_status(output_dir)
    
    print("\n" + "=" * 60)
    print("CHECKPOINT STATUS")
    print("=" * 60)
    print(f"Pipeline Steps Completed: {info['total_completed']}")
    print(f"Last Completed Step: {info['last_completed_step']}")
    print(f"Next Step: {info['next_step']}")
    print(f"Last Run: {info['last_run']}")
    print("-" * 60)
    print("Formulation Progress (Details Extraction):")
    print(f"  Completed Formulations: {formulation_status['completed']}")
    print(f"  In Progress: {formulation_status['in_progress'] or 'None'}")
    print(f"  Last Updated: {formulation_status['last_updated'] or 'Never'}")
    if formulation_status['stats']:
        stats = formulation_status['stats']
        print(f"  Total Medicines: {stats.get('total_medicines', 0)}")
        print(f"  Total Substitutes: {stats.get('total_substitutes', 0)}")
        print(f"  Errors: {stats.get('errors', 0)}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="India NPPA Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear all checkpoints)")
    parser.add_argument("--step", type=int, help=f"Start from specific step (0-{MAX_STEPS})")
    parser.add_argument("--resume-details", action="store_true",
                       help="Resume only details extraction step from where it left off")
    parser.add_argument("--clear-formulation-checkpoint", action="store_true",
                       help="Clear formulation checkpoint (restart details extraction from beginning)")
    parser.add_argument("--status", action="store_true", help="Show checkpoint status and exit")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager(SCRAPER_NAME)
    output_dir = get_output_dir()
    
    # Show status and exit
    if args.status:
        print_checkpoint_status()
        return
    
    # Clear formulation checkpoint if requested
    if args.clear_formulation_checkpoint:
        clear_formulation_checkpoint(output_dir)
        print("Formulation checkpoint cleared. Details extraction will restart from beginning.")
        if not args.step and not args.resume_details:
            return
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        clear_formulation_checkpoint(output_dir)
        start_step = 0
        print("Starting fresh run (all checkpoints cleared)")
    elif args.resume_details:
        # Jump directly to step 1 for resume (details extraction)
        start_step = 1
        formulation_status = get_formulation_checkpoint_status(output_dir)
        print(f"Resuming Step 01 (details extraction)")
        print(f"  Already completed: {formulation_status['completed']} formulations")
        if formulation_status['in_progress']:
            print(f"  Was processing: {formulation_status['in_progress']}")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        # Resume from last completed step
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
            
            # Check formulation progress for Step 01 (details extraction)
            if start_step == 1:
                formulation_status = get_formulation_checkpoint_status(output_dir)
                if formulation_status['completed'] > 0:
                    print(f"  Formulation progress: {formulation_status['completed']} completed")
        else:
            print("Starting fresh run (no checkpoint found)")
    
    # Print current status
    print_checkpoint_status()
    
    # Define pipeline steps with their output files
    # Note: Ceiling prices step removed - formulations are now loaded from input/India/formulations.csv
    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None, False, None),
        (1, "02 get details.py", "Extract Medicine Details",
         ["details", "scraping_report.json"], False, None),
    ]
    
    # Run steps starting from start_step
    for step_num, script_name, step_name, output_files, allow_failure, extra_args in steps:
        if step_num < start_step:
            print(f"\nStep {step_num}/{MAX_STEPS}: {step_name} - SKIPPED (already completed)")
            continue
        
        success = run_step(step_num, script_name, step_name, output_files, 
                          allow_failure=allow_failure, extra_args=extra_args)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            print("You can resume from this step by running the script again.")
            sys.exit(1)
    
    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {MAX_STEPS}/{MAX_STEPS} (100%)", flush=True)
    
    # Print final report location
    report_file = output_dir / "scraping_report.json"
    if report_file.exists():
        print(f"\nFinal report: {report_file}")


if __name__ == "__main__":
    main()
