#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CanadaQuebec Pipeline Runner (9-Step Version)
"""
import os
import sys
import subprocess
import argparse
import time
from pathlib import Path
from datetime import datetime

# Path wiring
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_csv_output_dir, get_split_pdf_dir, DB_ENABLED,
    OPENAI_API_KEY, OPENAI_MODEL
)
from db_handler import DBHandler
from core.pipeline.pipeline_checkpoint import get_checkpoint_manager

SCRAPER_NAME = "CanadaQuebec"
MAX_STEPS = 9

def _get_output_dir() -> Path:
    return get_csv_output_dir().parent

def _read_run_id() -> str:
    run_id = os.environ.get("PIPELINE_RUN_ID")
    if run_id: return run_id
    run_id_file = _get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        return run_id_file.read_text(encoding="utf-8").strip()
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def run_step(step_num, script_name, step_name, output_files=None, allow_failure=False):
    display_step = step_num + 1
    print(f"\n{'='*80}")
    print(f"Step {display_step}/{MAX_STEPS}: {step_name}")
    print(f"{'='*80}\n")

    run_id = _read_run_id()
    script_path = _script_dir / script_name
    
    env = os.environ.copy()
    env["PIPELINE_RUNNER"] = "1"
    env["PIPELINE_RUN_ID"] = run_id
    env["DB_ENABLED"] = "true" if DB_ENABLED else "false"
    if OPENAI_API_KEY: env["OPENAI_API_KEY"] = OPENAI_API_KEY
    if OPENAI_MODEL: env["OPENAI_MODEL"] = OPENAI_MODEL

    start_time = time.time()
    process = subprocess.Popen(
        [sys.executable, "-u", str(script_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    for line in process.stdout:
        print(line, end="", flush=True)

    process.wait()
    duration = time.time() - start_time

    if process.returncode != 0:
        if not allow_failure:
            print(f"\nERROR: Step {step_num} failed with code {process.returncode}.")
            return False
        else:
            print(f"\nWARNING: Step {step_num} failed but we are continuing.")

    cp = get_checkpoint_manager(SCRAPER_NAME)
    cp.mark_step_complete(step_num, step_name, duration_seconds=duration)
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fresh", action="store_true")
    parser.add_argument("--step", type=int)
    args = parser.parse_args()

    run_id = _read_run_id()
    if args.fresh:
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.environ["PIPELINE_RUN_ID"] = run_id

    # DB handler & Schema Init
    db = DBHandler()
    if DB_ENABLED:
        db.init_schema()
        db.start_run(run_id)
        print(f"[DB] Pipeline Run ID: {run_id}")

    # Ensure output dir exists
    _get_output_dir().mkdir(parents=True, exist_ok=True)
    (_get_output_dir() / ".current_run_id").write_text(run_id)

    cp = get_checkpoint_manager(SCRAPER_NAME)
    if args.fresh: cp.clear_checkpoint()

    info = cp.get_checkpoint_info()
    start_step = args.step if args.step is not None else info["next_step"]

    steps = [
        (0, "00_backup_and_clean.py", "Backup and Clean"),
        (1, "01_split_pdf_into_annexes.py", "Split PDF into Annexes"),
        (2, "02_validate_pdf_structure.py", "Validate PDF Structure", None, True),
        (3, "03_extract_annexe_iii.py", "Extract Annexe III"),
        (4, "04_extract_annexe_iv.py", "Extract Annexe IV"),
        (5, "05_extract_annexe_iv1.py", "Extract Annexe IV.1"),
        (6, "06_extract_annexe_iv2.py", "Extract Annexe IV.2"),
        (7, "07_extract_annexe_v.py", "Extract Annexe V"),
        (8, "08_merge_all_annexes.py", "Merge All Annexes"),
    ]

    for step_info in steps:
        step_num = step_info[0]
        script = step_info[1]
        name = step_info[2]
        out_files = step_info[3] if len(step_info) > 3 else None
        allow_fail = step_info[4] if len(step_info) > 4 else False

        if step_num < start_step:
            print(f"Step {step_num+1}/{MAX_STEPS}: {name} - SKIPPED")
            continue
        
        if not run_step(step_num, script, name, output_files=out_files, allow_failure=allow_fail):
            sys.exit(1)

    if DB_ENABLED:
        db.finish_run(run_id, status="COMPLETED")

    print("\nPipeline completed successfully!")

if __name__ == "__main__":
    main()
