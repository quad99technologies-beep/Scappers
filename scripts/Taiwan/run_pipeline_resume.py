#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taiwan Pipeline Runner with Resume/Checkpoint Support

Usage:
    python run_pipeline_resume.py          # Resume from last step or start fresh
    python run_pipeline_resume.py --fresh  # Start from step 0 (clear checkpoint)
    python run_pipeline_resume.py --step N # Start from step N (0-2)
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Taiwan to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.pipeline_checkpoint import get_checkpoint_manager
from config_loader import load_env_file, getenv, get_output_dir, get_central_output_dir
from core.chrome_pid_tracker import terminate_scraper_pids

SCRAPER_NAME = "Taiwan"


def configure_realtime_output() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass


def run_step(step_num: int, script_name: str, step_name: str, total_steps: int, output_files: list = None) -> bool:
    """Run a pipeline step and mark it complete if successful."""
    print(f"\n{'='*80}")
    print(f"Step {step_num + 1}/{total_steps}: {step_name}")
    print(f"{'='*80}\n")

    pipeline_percent = round(((step_num + 1) / total_steps) * 100, 1)
    if pipeline_percent > 100.0:
        pipeline_percent = 100.0

    print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({pipeline_percent}%) - {step_name}", flush=True)
    print(f"[PIPELINE] Executing: {script_name}")
    print("[PIPELINE] This step will run until completion before moving to next step.\n")

    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        print(f"ERROR: Script not found: {script_path}")
        return False

    start_time = time.time()
    duration_seconds = None

    try:
        terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)

        subprocess.run(
            [sys.executable, "-u", str(script_path)],
            check=True,
            capture_output=False
        )
        duration_seconds = time.time() - start_time

        cp = get_checkpoint_manager(SCRAPER_NAME)
        if output_files:
            cp.mark_step_complete(step_num, step_name, output_files, duration_seconds=duration_seconds)
        else:
            cp.mark_step_complete(step_num, step_name, duration_seconds=duration_seconds)

        completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
        if completion_percent > 100.0:
            completion_percent = 100.0

        print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - Step completed", flush=True)
        return True
    except subprocess.CalledProcessError as e:
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed with exit code {e.returncode} (duration: {duration_seconds:.2f}s)")
        return False
    except Exception as e:
        duration_seconds = time.time() - start_time
        print(f"\nERROR: Step {step_num} ({step_name}) failed: {e} (duration: {duration_seconds:.2f}s)")
        return False
    finally:
        try:
            terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
        except Exception:
            pass


def main() -> None:
    configure_realtime_output()
    load_env_file()

    parser = argparse.ArgumentParser(description="Taiwan Pipeline Runner with Resume Support")
    parser.add_argument("--fresh", action="store_true", help="Start from step 0 (clear checkpoint)")
    parser.add_argument("--step", type=int, help="Start from specific step (0-2, where 0=Backup, 1=Collect URLs, 2=Extract Details)")
    args = parser.parse_args()

    cp = get_checkpoint_manager(SCRAPER_NAME)

    if args.fresh:
        cp.clear_checkpoint()
        start_step = 0
        print("Starting fresh run (checkpoint cleared)")
    elif args.step is not None:
        start_step = args.step
        print(f"Starting from step {start_step}")
    else:
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        if info["total_completed"] > 0:
            print(f"Resuming from step {start_step} (last completed: step {info['last_completed_step']})")
        else:
            print("Starting fresh run (no checkpoint found)")

    output_dir = get_output_dir()
    exports_dir = get_central_output_dir()
    codes_file = getenv("SCRIPT_01_OUT_CODES", "taiwan_drug_code_urls.csv")
    details_file = getenv("SCRIPT_02_OUT_DETAILS", "taiwan_drug_code_details.csv")

    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean", None),
        (1, "01_taiwan_collect_drug_code_urls.py.py", "Collect Drug Code URLs", [str(output_dir / codes_file)]),
        (2, "02_taiwan_extract_drug_code_details.py", "Extract Drug Code Details", [str(exports_dir / details_file)]),
    ]
    total_steps = len(steps)

    earliest_rerun_step = None
    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            expected_files = None
            if output_files:
                expected_files = [str(Path(f)) for f in output_files]
            should_skip = cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=expected_files)
            if not should_skip:
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) marked complete but outputs missing. Will re-run.")
                if earliest_rerun_step is None or step_num < earliest_rerun_step:
                    earliest_rerun_step = step_num
            else:
                print(f"[CHECKPOINT] Step {step_num} ({step_name}) verified - output files exist, will skip.")

    if earliest_rerun_step is not None:
        print(f"\nWARNING: Step {earliest_rerun_step} needs re-run (output files missing).")
        print(f"Adjusting start step from {start_step} to {earliest_rerun_step} to maintain pipeline integrity.\n")
        start_step = earliest_rerun_step
    else:
        print(f"[CHECKPOINT] All steps before {start_step} verified successfully. Starting from step {start_step}.\n")

    try:
        terminate_scraper_pids(SCRAPER_NAME, _repo_root, silent=True)
    except Exception:
        pass

    for step_num, script_name, step_name, output_files in steps:
        if step_num < start_step:
            if cp.should_skip_step(step_num, step_name, verify_outputs=True, expected_output_files=output_files):
                print(f"\nStep {step_num + 1}/{total_steps}: {step_name} - SKIPPED (already completed in checkpoint)")
                completion_percent = round(((step_num + 1) / total_steps) * 100, 1)
                if completion_percent > 100.0:
                    completion_percent = 100.0
                skip_desc = "Skipped: Backup already completed" if step_num == 0 else "Skipped: Output already generated"
                print(f"[PROGRESS] Pipeline Step: {step_num + 1}/{total_steps} ({completion_percent}%) - {skip_desc}", flush=True)
            continue

        success = run_step(step_num, script_name, step_name, total_steps, output_files)
        if not success:
            print(f"\nPipeline failed at step {step_num}")
            sys.exit(1)

    print(f"\n{'='*80}")
    print("Pipeline completed successfully!")
    print(f"{'='*80}\n")
    print(f"[PROGRESS] Pipeline Step: {total_steps}/{total_steps} (100%)", flush=True)


if __name__ == "__main__":
    main()
