
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

try:
    from db.repositories import ItalyRepository
except ImportError:
    from scripts.Italy.db.repositories import ItalyRepository

RUN_ID_FILE = Path(__file__).parent / ".last_run"
CURRENT_RUN_ID_FILE = get_output_dir() / ".current_run_id"

def _read_run_id(fresh: bool = False) -> str:
    # 1. Check Env
    env_id = os.environ.get("ITALY_RUN_ID", "").strip()
    if env_id:
        # Update file if env provided
        RUN_ID_FILE.write_text(env_id)
        try:
            CURRENT_RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
            CURRENT_RUN_ID_FILE.write_text(env_id, encoding="utf-8")
        except Exception:
            pass
        return env_id

    # 2. Fresh requested?
    if fresh:
        new_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        RUN_ID_FILE.write_text(new_id)
        try:
            CURRENT_RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
            CURRENT_RUN_ID_FILE.write_text(new_id, encoding="utf-8")
        except Exception:
            pass
        return new_id

    # 3. Check File
    if RUN_ID_FILE.exists():
        last_id = RUN_ID_FILE.read_text().strip()
        if last_id:
            try:
                CURRENT_RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
                CURRENT_RUN_ID_FILE.write_text(last_id, encoding="utf-8")
            except Exception:
                pass
            return last_id

    # 4. Fallback new
    new_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    RUN_ID_FILE.write_text(new_id)
    try:
        CURRENT_RUN_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
        CURRENT_RUN_ID_FILE.write_text(new_id, encoding="utf-8")
    except Exception:
        pass
    return new_id


def run_step(step_num: int, script_name: str, step_name: str, run_id: str):
    print(f"\n{'='*80}")
    print(f"Step {step_num}: {step_name}")
    print(f"{'='*80}")

    script_path = Path(__file__).parent / script_name

    env = os.environ.copy()
    env["ITALY_RUN_ID"] = run_id

    # Use check=False so we can handle return code manually
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
    parser.add_argument("--fresh", action="store_true", help="Start fresh run (generates new ID)")
    parser.add_argument("--step", type=int, default=0, help="Start from step N")
    args = parser.parse_args()

    # Get or create Run ID
    run_id = _read_run_id(fresh=args.fresh)
    print(f"Target Run ID: {run_id}")

    try:
        with CountryDB("Italy") as db:
            repo = ItalyRepository(db, run_id)
            if args.fresh:
                # Cleanup old data for this run_id if it existed (rare collision)
                repo.ensure_run_in_ledger(mode="fresh")
            else:
                repo.ensure_run_in_ledger(mode="resume")
    except Exception as e:
        print(f"Warning: Could not register run in ledger: {e}")

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
        if not run_step(num, script, name, run_id):
            break

if __name__ == "__main__":
    main()
