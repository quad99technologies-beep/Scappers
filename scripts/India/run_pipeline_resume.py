#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-1)
    python run_pipeline_resume.py --resume-details  # Resume only details extraction
    python run_pipeline_resume.py --workers 5  # Run details extraction with parallel workers

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
import re
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
from config_loader import get_output_dir, get_input_dir, load_env_file, getenv_int

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


def resolve_input_file() -> Path:
    override = (os.getenv("FORMULATIONS_FILE", "") or "").strip()
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = get_input_dir() / p
        return p
    return get_input_dir() / "formulations.csv"


def normalize_stem_for_parts(stem: str) -> str:
    return re.sub(r"_part\\d+$", "", stem, flags=re.IGNORECASE)


def find_existing_parts_for_input(input_file: Path) -> list:
    base_stem = normalize_stem_for_parts(input_file.stem)
    pattern = f"{base_stem}_part*{input_file.suffix}"
    candidates = list(input_file.parent.glob(pattern))

    def sort_key(p: Path) -> int:
        m = re.search(r"_part(\\d+)$", p.stem, flags=re.IGNORECASE)
        return int(m.group(1)) if m else 0

    return sorted(candidates, key=sort_key)


def get_formulation_checkpoint_status(output_dir: Path) -> dict:
    """Get status of formulation-level checkpoint for details extraction step."""
    checkpoint_file = output_dir / ".checkpoints" / "formulation_progress.json"
    
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

                completed = len(data.get("completed_formulations", []))
                zero_records = len(data.get("zero_record_formulations", []))
                failed = data.get("failed_formulations", {})
                failed_count = len(failed) if isinstance(failed, dict) else 0

                return {
                    "completed": completed,
                    "zero_records": zero_records,
                    "terminal": completed + zero_records,
                    "failed": failed_count,
                    "in_progress": data.get("in_progress"),
                    "last_updated": data.get("last_updated"),
                    "stats": data.get("stats", {})
                }
        except Exception as e:
            print(f"[WARN] Could not read formulation checkpoint: {e}")
    
    return {"completed": 0, "zero_records": 0, "terminal": 0, "failed": 0, "in_progress": None, "last_updated": None, "stats": {}}


def clear_formulation_checkpoint(output_dir: Path):
    """Clear formulation-level checkpoint."""
    checkpoint_file = output_dir / ".checkpoints" / "formulation_progress.json"
    if checkpoint_file.exists():
        checkpoint_file.unlink()
        print("[INFO] Cleared formulation checkpoint")


def get_worker_dirs(output_base: Path) -> list:
    if not output_base.exists():
        return []
    return sorted([p for p in output_base.glob("worker_*") if p.is_dir()], key=lambda p: p.name)


def get_parallel_workers_status(output_base: Path) -> dict:
    workers = get_worker_dirs(output_base)
    totals = {
        "workers": len(workers),
        "completed": 0,
        "zero_records": 0,
        "terminal": 0,
        "failed": 0,
        "in_progress": [],
        "stats": {"total_medicines": 0, "total_substitutes": 0, "errors": 0},
    }
    for worker_dir in workers:
        status = get_formulation_checkpoint_status(worker_dir)
        totals["completed"] += int(status.get("completed", 0) or 0)
        totals["zero_records"] += int(status.get("zero_records", 0) or 0)
        totals["terminal"] += int(status.get("terminal", 0) or 0)
        totals["failed"] += int(status.get("failed", 0) or 0)
        if status.get("in_progress"):
            totals["in_progress"].append(f"{worker_dir.name}: {status['in_progress']}")
        stats = status.get("stats", {}) or {}
        totals["stats"]["total_medicines"] += int(stats.get("total_medicines", 0) or 0)
        totals["stats"]["total_substitutes"] += int(stats.get("total_substitutes", 0) or 0)
        totals["stats"]["errors"] += int(stats.get("errors", 0) or 0)
    return totals


def clear_parallel_formulation_checkpoints(output_base: Path):
    for worker_dir in get_worker_dirs(output_base):
        clear_formulation_checkpoint(worker_dir)


def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None,
             allow_failure: bool = False, extra_args: list = None,
             parallel_workers: int = 1, parallel_output_base: Path = None):
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
        
        # Block completion when details extraction has remaining failures to retry
        if step_num == 1:
            if parallel_workers > 1 and parallel_output_base is not None:
                parallel_status = get_parallel_workers_status(parallel_output_base)
                failed = int(parallel_status.get("failed", 0) or 0)
                in_progress = len(parallel_status.get("in_progress", [])) > 0
                if failed > 0 or in_progress:
                    print(
                        f"\nERROR: Step {step_num} ({step_name}) completed with {failed} failed formulation(s)"
                        f"{' (in_progress set)' if in_progress else ''}; checkpoint not updated."
                    )
                    return False
            else:
                output_dir = get_output_dir()
                formulation_status = get_formulation_checkpoint_status(output_dir)
                failed = int(formulation_status.get("failed", 0) or 0)
                in_progress = formulation_status.get("in_progress")
                if failed > 0 or in_progress:
                    print(
                        f"\nERROR: Step {step_num} ({step_name}) completed with {failed} failed formulation(s)"
                        f"{' (in_progress set)' if in_progress else ''}; checkpoint not updated."
                    )
                    return False

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


def print_checkpoint_status(parallel_workers: int = 1):
    """Print current checkpoint status."""
    cp = get_checkpoint_manager(SCRAPER_NAME)
    info = cp.get_checkpoint_info()
    output_dir = get_output_dir()
    formulation_status = None
    parallel_status = None
    if parallel_workers > 1:
        parallel_status = get_parallel_workers_status(output_dir / "workers")
    else:
        formulation_status = get_formulation_checkpoint_status(output_dir)
    
    print("\n" + "=" * 60)
    print("CHECKPOINT STATUS")
    print("=" * 60)
    print(f"Pipeline Steps Completed: {info['total_completed']}")
    print(f"Last Completed Step: {info['last_completed_step']}")
    print(f"Next Step: {info['next_step']}")
    print(f"Last Run: {info['last_run']}")
    print("-" * 60)
    if parallel_workers > 1:
        print(f"Parallel Workers: {parallel_workers}")
        print("Formulation Progress (Parallel Details Extraction):")
        print(f"  Workers Detected: {parallel_status.get('workers', 0)}")
        print(f"  Terminal Formulations: {parallel_status.get('terminal', 0)}")
        print(f"    Success: {parallel_status.get('completed', 0)}")
        print(f"    Zero-record: {parallel_status.get('zero_records', 0)}")
        print(f"    Failed: {parallel_status.get('failed', 0)}")
        in_progress = parallel_status.get("in_progress", [])
        print(f"  In Progress: {', '.join(in_progress) if in_progress else 'None'}")
        stats = parallel_status.get("stats", {}) or {}
        print(f"  Total Medicines: {stats.get('total_medicines', 0)}")
        print(f"  Total Substitutes: {stats.get('total_substitutes', 0)}")
        print(f"  Failure Attempts: {stats.get('errors', 0)}")
    else:
        print("Formulation Progress (Details Extraction):")
        print(f"  Terminal Formulations: {formulation_status.get('terminal', 0)}")
        print(f"    Success: {formulation_status.get('completed', 0)}")
        print(f"    Zero-record: {formulation_status.get('zero_records', 0)}")
        print(f"    Failed: {formulation_status.get('failed', 0)}")
        print(f"  In Progress: {formulation_status['in_progress'] or 'None'}")
        print(f"  Last Updated: {formulation_status['last_updated'] or 'Never'}")
        if formulation_status['stats']:
            stats = formulation_status['stats']
            print(f"  Total Medicines: {stats.get('total_medicines', 0)}")
            print(f"  Total Substitutes: {stats.get('total_substitutes', 0)}")
            print(f"  Failure Attempts: {stats.get('errors', 0)}")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="India NPPA Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear all checkpoints)")
    parser.add_argument("--step", type=int, help=f"Start from specific step (0-{MAX_STEPS})")
    parser.add_argument("--resume-details", action="store_true",
                       help="Resume only details extraction step from where it left off")
    parser.add_argument("--clear-formulation-checkpoint", action="store_true",
                       help="Clear formulation checkpoint (restart details extraction from beginning)")
    parser.add_argument("--workers", type=int, default=getenv_int("INDIA_WORKERS", 1),
                       help="Number of parallel workers for details extraction (default: 1 or INDIA_WORKERS)")
    parser.add_argument("--status", action="store_true", help="Show checkpoint status and exit")
    
    args = parser.parse_args()
    
    cp = get_checkpoint_manager(SCRAPER_NAME)
    output_dir = get_output_dir()
    worker_count = max(1, int(args.workers or 1))
    # Platform runners sometimes inject `--workers` regardless of env config. Never exceed configured workers.
    configured_workers = max(1, int(getenv_int("INDIA_WORKERS", worker_count) or worker_count))
    if worker_count > configured_workers:
        print(f"[CONFIG] Overriding CLI workers={worker_count} with INDIA_WORKERS={configured_workers}", flush=True)
        worker_count = configured_workers
    parallel_mode = worker_count > 1
    
    # Show status and exit
    if args.status:
        print_checkpoint_status(parallel_workers=worker_count)
        return
    
    # Clear formulation checkpoint if requested
    if args.clear_formulation_checkpoint:
        clear_formulation_checkpoint(output_dir)
        if parallel_mode:
            clear_parallel_formulation_checkpoints(output_dir / "workers")
        print("Formulation checkpoint cleared. Details extraction will restart from beginning.")
        if not args.step and not args.resume_details:
            return
    
    # Determine start step
    if args.fresh:
        cp.clear_checkpoint()
        clear_formulation_checkpoint(output_dir)
        if parallel_mode:
            clear_parallel_formulation_checkpoints(output_dir / "workers")
        start_step = 0
        print("Starting fresh run (all checkpoints cleared)")
    elif args.resume_details:
        # Jump directly to step 1 for resume (details extraction)
        start_step = 1
        print(f"Resuming Step 01 (details extraction)")
        if parallel_mode:
            parallel_status = get_parallel_workers_status(output_dir / "workers")
            print(f"  Terminal: {parallel_status.get('terminal', 0)} formulations")
            print(f"    Success: {parallel_status.get('completed', 0)}")
            print(f"    Zero-record: {parallel_status.get('zero_records', 0)}")
            print(f"    Failed: {parallel_status.get('failed', 0)}")
            if parallel_status.get("in_progress"):
                print(f"  In Progress: {', '.join(parallel_status['in_progress'])}")
        else:
            formulation_status = get_formulation_checkpoint_status(output_dir)
            print(f"  Terminal: {formulation_status.get('terminal', 0)} formulations")
            print(f"    Success: {formulation_status.get('completed', 0)}")
            print(f"    Zero-record: {formulation_status.get('zero_records', 0)}")
            print(f"    Failed: {formulation_status.get('failed', 0)}")
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
                if formulation_status.get("terminal", 0) > 0:
                    print(
                        f"  Formulation progress: {formulation_status.get('terminal', 0)} terminal "
                        f"(success={formulation_status.get('completed', 0)}, "
                        f"zero={formulation_status.get('zero_records', 0)}, "
                        f"failed={formulation_status.get('failed', 0)})"
                    )
        else:
            print("Starting fresh run (no checkpoint found)")
    
    # Print current status
    print_checkpoint_status(parallel_workers=worker_count)
    
    # Define pipeline steps with their output files
    # Note: Ceiling prices step removed - formulations are now loaded from input/India/formulations.csv
    details_script = "02_get_details.py"
    details_outputs = ["details", "scraping_report.json"]
    details_args = None
    if parallel_mode:
        details_script = "run_parallel_workers.py"
        details_outputs = ["workers/.parallel_complete.json"]
        details_args = ["--workers", str(worker_count), "--output-base", str(output_dir / "workers")]
        formulations_override = os.getenv("FORMULATIONS_FILE")
        input_file = resolve_input_file()
        existing_parts = find_existing_parts_for_input(input_file)
        if existing_parts:
            if len(existing_parts) == worker_count:
                details_args.extend(["--use-existing-parts", "--parts-dir", str(input_file.parent)])
            else:
                print(
                    f"[WARN] Found {len(existing_parts)} part file(s) but workers={worker_count}; re-splitting input.",
                    flush=True,
                )
        if formulations_override:
            details_args.extend(["--input", formulations_override])

    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None, False, None),
        (1, details_script, "Extract Medicine Details", details_outputs, False, details_args),
    ]

    # Verify that steps before start_step are still valid (outputs exist).
    earliest_rerun_step = None
    for step_num, _, step_name, output_files, _, _ in steps:
        if step_num >= start_step:
            continue
        expected_files = None
        if output_files:
            expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
        if not cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
            earliest_rerun_step = step_num
            break

    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step}.\n")
        start_step = earliest_rerun_step
    
    # Run steps starting from start_step
    for step_num, script_name, step_name, output_files, allow_failure, extra_args in steps:
        if step_num < start_step:
            expected_files = None
            if output_files:
                expected_files = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files):
                print(f"\nStep {step_num}/{MAX_STEPS}: {step_name} - SKIPPED (already completed in checkpoint)")
            else:
                print(f"\nStep {step_num}/{MAX_STEPS}: {step_name} - WILL RE-RUN (output files missing)")
            continue
        
        success = run_step(
            step_num, script_name, step_name, output_files,
            allow_failure=allow_failure, extra_args=extra_args,
            parallel_workers=worker_count,
            parallel_output_base=(output_dir / "workers") if parallel_mode else None,
        )
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
