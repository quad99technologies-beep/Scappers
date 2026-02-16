#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Pipeline Runner

Provides common pipeline orchestration: lock acquisition, step execution,
checkpoint handling, cleanup. Scrapers define their step list and call
run_pipeline() for the shared logic.

Step format: (step_num, script_name, step_label, output_files)
  - step_num: int (0-based)
  - script_name: str (e.g. "00_backup_and_clean.py")
  - step_label: str (e.g. "Backup and Clean")
  - output_files: list of str or None (paths for checkpoint verification)
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

# Step: (step_num, script_name, step_label, output_files)
StepDef = Tuple[int, str, str, Optional[List[str]]]


def run_pipeline(
    scraper_id: str,
    steps: List[StepDef],
    script_dir: Path,
    repo_root: Optional[Path] = None,
    run_id_env_var: str = "",
    run_id_file_name: str = ".current_run_id",
    on_step_complete: Optional[Callable[[int, str, str], None]] = None,
    on_cleanup: Optional[Callable[[], None]] = None,
) -> bool:
    """
    Run a pipeline with checkpoint support.

    Args:
        scraper_id: Scraper identifier
        steps: List of (step_num, script_name, step_label, output_files)
        script_dir: Directory containing step scripts
        repo_root: Repository root (default: parent of script_dir)
        run_id_env_var: Env var for run_id (e.g. "BELARUS_RUN_ID")
        run_id_file_name: Name of run_id file in output dir
        on_step_complete: Optional callback(step_num, step_name, status)
        on_cleanup: Optional callback before exit (e.g. cleanup lock)

    Returns:
        True if all steps completed, False otherwise
    """
    repo_root = repo_root or script_dir.parent.parent

    try:
        from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
        from core.pipeline.pipeline_start_lock import claim_pipeline_start_lock, release_pipeline_lock, update_pipeline_lock
        from core.config.config_manager import ConfigManager
        cp = get_checkpoint_manager(scraper_id)
        output_dir = ConfigManager.get_output_dir(scraper_id)
    except Exception as e:
        print(f"[ERROR] Pipeline setup failed: {e}", file=sys.stderr)
        return False

    # Acquire lock
    acquired, lock_file, reason = claim_pipeline_start_lock(scraper_id, repo_root=repo_root)
    if not acquired:
        print(f"[ERROR] Could not acquire pipeline lock: {reason}", file=sys.stderr)
        return False

    try:
        update_pipeline_lock(lock_file, os.getpid(), None)
        info = cp.get_checkpoint_info()
        start_step = info["next_step"]
        total_steps = len(steps)

        for step_num, script_name, step_label, output_files in steps:
            if step_num < start_step:
                # Verify checkpoint
                expected = None
                if output_files:
                    expected = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
                if cp.should_skip_step(step_num, step_label, verify_outputs=True, expected_output_files=expected):
                    continue
                start_step = step_num
                break

        for step_num, script_name, step_label, output_files in steps:
            if step_num < start_step:
                continue

            script_path = script_dir / script_name
            if not script_path.exists():
                print(f"[ERROR] Script not found: {script_path}", file=sys.stderr)
                return False

            print(f"\n{'='*80}")
            print(f"Step {step_num + 1}/{total_steps}: {step_label}")
            print(f"{'='*80}\n")

            try:
                subprocess.run(
                    [sys.executable, "-u", str(script_path)],
                    check=True,
                    capture_output=False,
                )
                resolved_outputs = None
                if output_files:
                    resolved_outputs = [str(output_dir / f) if not Path(f).is_absolute() else f for f in output_files]
                cp.mark_step_complete(step_num, step_label, output_files=resolved_outputs)
                if on_step_complete:
                    on_step_complete(step_num, step_label, "completed")
            except subprocess.CalledProcessError as e:
                print(f"[ERROR] Step {step_num} failed: {e}", file=sys.stderr)
                if on_step_complete:
                    on_step_complete(step_num, step_label, "failed")
                return False
            except Exception as e:
                print(f"[ERROR] Step {step_num} error: {e}", file=sys.stderr)
                if on_step_complete:
                    on_step_complete(step_num, step_label, "failed")
                return False

        print(f"\n{'='*80}")
        print("Pipeline completed successfully!")
        print(f"{'='*80}\n")
        return True

    finally:
        try:
            update_pipeline_lock(lock_file, os.getpid(), None)
        except Exception:
            pass
        release_pipeline_lock(lock_file)
        if on_cleanup:
            try:
                on_cleanup()
            except Exception:
                pass
