
#!/usr/bin/env python3
"""
Italy Pipeline Runner with Resume Support
"""

import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# Setup paths
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))


from config_loader import get_output_dir
from core.db.connection import CountryDB
from db.repositories import ItalyRepository

def _read_run_id() -> str:
    run_id = os.environ.get("ITALY_RUN_ID")

    if run_id:
        return run_id
    # Generate new one if fresh
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def run_step(step_num: int, script_name: str, step_name: str):
    print(f"\n{'='*80}")
    print(f"Step {step_num}: {step_name}")
    print(f"{'='*80}")
    
    script_path = Path(__file__).parent / script_name
    
    env = os.environ.copy()
    if "ITALY_RUN_ID" not in env:
        env["ITALY_RUN_ID"] = _read_run_id()
        
    process = subprocess.run(
        [sys.executable, "-u", str(script_path)],
        env=env,
        check=False
    )
    
    if process.returncode != 0:
        print(f"Step {step_num} failed with code {process.returncode}")
        return False
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true", help="Start fresh run")
    parser.add_argument("--step", type=int, default=0, help="Start from step N")
    args = parser.parse_args()
    

    if args.fresh:
        os.environ["ITALY_RUN_ID"] = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Register Run
    run_id = _read_run_id()
    try:
        with CountryDB("Italy") as db:
            repo = ItalyRepository(db, run_id)
            if args.fresh:
                repo.start_run()
            else:
                repo.ensure_run_in_ledger()
    except Exception as e:
        print(f"Warning: Could not register run: {e}")

    start_step = args.step

    
    steps = [
        (0, "steps/step_00_backup_clean.py", "Init / Schema"),
        (1, "steps/step_01_list_determinas.py", "List Determinas"),
        (2, "steps/step_02_download_pdfs.py", "Download PDFs"),
        (3, "steps/step_03_extract_data.py", "Extract Data"),
        (4, "steps/step_04_export.py", "Export Excel"),
    ]
    
    for num, script, name in steps:
        if num < start_step:
            continue
        if not run_step(num, script, name):
            break

if __name__ == "__main__":
    main()
