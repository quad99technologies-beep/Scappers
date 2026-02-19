#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Replay Tool

Replay a previous run's steps with same inputs.

Usage:
    python services/run_replay.py Malaysia run_20260201_abc --step 2
"""

import sys
import argparse
import subprocess
from pathlib import Path

# Add repo root to path
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from core.db.postgres_connection import get_db
from core.progress.run_comparison import get_run_metrics


def replay_step(scraper_name: str, original_run_id: str, step_number: int):
    """Replay a specific step from a previous run."""
    print(f"Replaying {scraper_name} Step {step_number} from run {original_run_id}")
    
    # Get original step metrics
    metrics = get_run_metrics(scraper_name, original_run_id)
    if not metrics or step_number not in metrics.get("steps", {}):
        print(f"Error: Step {step_number} not found in run {original_run_id}")
        return False
    
    step_info = metrics["steps"][step_number]
    print(f"Original step: {step_info['step_name']}")
    print(f"Original duration: {step_info['duration_seconds']:.1f}s")
    
    # Load input data from original run
    # This would load the same input data that was used in the original run
    # Implementation depends on how inputs are stored
    
    # Run the step script
    script_path = REPO_ROOT / "scripts" / scraper_name / f"steps/step_{step_number:02d}_*.py"
    scripts = list(script_path.parent.glob(f"step_{step_number:02d}_*.py"))
    
    if not scripts:
        print(f"Error: Step script not found for step {step_number}")
        return False
    
    script = scripts[0]
    print(f"Running: {script}")
    
    # Set environment to use original run's data
    env = os.environ.copy()
    env["REPLAY_RUN_ID"] = original_run_id
    env["REPLAY_MODE"] = "1"
    
    result = subprocess.run(
        [sys.executable, str(script)],
        env=env,
        cwd=str(script.parent)
    )
    
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Replay a previous pipeline run")
    parser.add_argument("scraper_name", help="Scraper name")
    parser.add_argument("run_id", help="Original run ID to replay")
    parser.add_argument("--step", type=int, help="Specific step to replay (default: all)")
    
    args = parser.parse_args()
    
    if args.step is not None:
        replay_step(args.scraper_name, args.run_id, args.step)
    else:
        print("Replaying all steps...")
        # Get all steps from original run
        metrics = get_run_metrics(args.scraper_name, args.run_id)
        if metrics:
            for step_num in sorted(metrics.get("steps", {}).keys()):
                replay_step(args.scraper_name, args.run_id, step_num)


if __name__ == "__main__":
    import os
    main()
