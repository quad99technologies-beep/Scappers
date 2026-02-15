#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 0 - Backup, Clean, DB Init & run_id registration (Argentina).

What it does:
- Backs up the output folder (same behaviour as legacy script)
- Cleans output (keeps runs/backups)
- Applies PostgreSQL schema for Argentina (ar_ tables)
- Generates/persists a run_id and registers it in run_ledger

Reference data (dictionary, PCID mapping, ignore list) is not loaded from CSV here;
use only manually uploaded data (e.g. via GUI).
"""

import os
from pathlib import Path
import sys

# Add repo root to path for shared imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir,
    get_backup_dir,
    get_central_output_dir,
    load_env_file,
)
from core.utils.shared_utils import backup_output_folder, clean_output_folder
from core.db.models import generate_run_id
from core.db.connection import CountryDB
from core.db.schema_registry import SchemaRegistry
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository

load_env_file()

OUTPUT_DIR = get_output_dir()
BACKUP_DIR = get_backup_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    print("\n" + "=" * 80)
    print("STEP 0 - BACKUP, CLEAN, DB INIT (ARGENTINA)")
    print("=" * 80 + "\n")

    # 1) Backup
    print("[1/3] Creating backup of output folder...")
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)],
    )
    status = backup_result.get("status")
    if status == "ok":
        print(f"[OK] Backup: {backup_result['backup_folder']}")
    elif status == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        # Backup failed or incomplete: warn but continue so DB init and run_ledger always run
        # (otherwise step 1 would use an unregistered run_id and hit FK violation)
        print(f"[WARN] Backup issue: {backup_result['message']}")
        print("[WARN] Continuing with clean and DB init so pipeline can proceed...")

    # 2) Clean
    print("\n[2/3] Cleaning output folder...")
    clean_result = clean_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        keep_files=[],
        keep_dirs=["runs", "backups"],
    )
    if clean_result["status"] != "ok":
        print(f"[ERROR] {clean_result.get('message')}")
        return
    print(f"[OK] Cleaned ({clean_result['files_deleted']} files removed)")

    # 3) DB init + run_id
    print("\n[3/3] Applying PostgreSQL schema and generating run_id...")
    db = CountryDB("Argentina")
    apply_argentina_schema(db)
    # Ensure shared pcid_mapping table exists (single source for GUI + pipeline)
    inputs_sql = _repo_root / "sql" / "schemas" / "postgres" / "inputs.sql"
    if inputs_sql.exists():
        try:
            SchemaRegistry(db).apply_schema(inputs_sql)
        except Exception as e:
            print(f"[WARN] Could not apply inputs schema: {e}")
    run_id = os.environ.get("ARGENTINA_RUN_ID") or generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = run_id
    run_id_file = OUTPUT_DIR / ".current_run_id"
    run_id_file.write_text(run_id, encoding="utf-8")
    print(f"[OK] run_id = {run_id} (saved to {run_id_file})")

    repo = ArgentinaRepository(db, run_id)
    repo.start_run(mode="fresh")
    print("[OK] run_ledger entry created")

    # Reference data (dictionary, PCID, ignore list) is not loaded from CSV here.
    # Use only manually uploaded data (e.g. via GUI); pipeline does not seed from CSV.

    print("\n" + "=" * 80)
    print("Backup, cleanup, DB init complete. Ready for pipeline.")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    from core.standalone_checkpoint import run_with_checkpoint

    run_with_checkpoint(main, "Argentina", 0, "Backup and Clean + DB Init", output_files=None)
