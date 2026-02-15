#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Step 9: Refresh Export (Re-run translation + final export)

Goal
----
Re-run translation and final export to include any newly scraped data
(e.g., from Step 7 no-data retry or manual scraping).

This is a lightweight step that just regenerates the output reports
without doing any new scraping.

Usage
-----
This script is designed to be called by `run_pipeline_resume.py`.
It will respect ARGENTINA_RUN_ID from env / output/.current_run_id.
"""

from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from config_loader import get_output_dir


def _get_run_id(output_dir: Path) -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID", "").strip()
    if rid:
        return rid
    run_id_file = output_dir / ".current_run_id"
    if run_id_file.exists():
        try:
            txt = run_id_file.read_text(encoding="utf-8").strip()
            if txt:
                os.environ["ARGENTINA_RUN_ID"] = txt
                return txt
        except Exception:
            pass
    raise RuntimeError("ARGENTINA_RUN_ID not set and .current_run_id missing. Run Step 0 first.")


def _run_script(script_name: str, extra_env: dict | None = None) -> None:
    script_path = _SCRIPT_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")
    env = os.environ.copy()
    env["PIPELINE_RUNNER"] = "1"
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    subprocess.run([sys.executable, "-u", str(script_path)], check=True, env=env)


def main() -> None:
    output_dir = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = _get_run_id(output_dir)

    print("\n" + "=" * 80)
    print(f"STEP 9 - REFRESH EXPORT (run_id={run_id})")
    print("=" * 80 + "\n")

    # Re-run translation + output so any new rows are included in reports
    print("[REFRESH] Re-running translation (Step 5)...", flush=True)
    _run_script("05_TranslateUsingDictionary.py")

    print("\n[REFRESH] Re-running final export (Step 6)...", flush=True)
    _run_script("06_GenerateOutput.py")

    print(f"\n[REFRESH] Done at {datetime.now().isoformat(timespec='seconds')}", flush=True)
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
