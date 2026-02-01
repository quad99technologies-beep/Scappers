#!/usr/bin/env python3
"""
Step 3: Consolidate product details via SQL

Deduplicates product_details → consolidated_products table.
No CSV or pandas needed — pure SQL.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import load_env_file, get_output_dir
load_env_file()


def _get_run_id() -> str:
    run_id = os.environ.get("MALAYSIA_RUN_ID")
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            run_id = run_id_file.read_text(encoding="utf-8").strip()
    if not run_id:
        raise RuntimeError("No MALAYSIA_RUN_ID found. Run Step 0 first.")
    return run_id


def main() -> None:
    from core.db.connection import CountryDB
    from db.repositories import MalaysiaRepository

    db = CountryDB("Malaysia")
    run_id = _get_run_id()
    repo = MalaysiaRepository(db, run_id)

    print("[STEP 3] Consolidating product details...", flush=True)

    detail_count = repo.get_detail_count()
    print(f"  -> Product details in DB: {detail_count:,}", flush=True)

    if detail_count == 0:
        print("[WARNING] No product details found. Run Step 2 first.", flush=True)
        return

    count = repo.consolidate()
    print(f"[PROGRESS] Consolidating: {count}/{count} (100%)", flush=True)
    print(f"\n[DONE] Step 3 complete — {count:,} consolidated products", flush=True)


if __name__ == "__main__":
    from core.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(
        main, "Malaysia", 3, "Consolidate Results",
    )
