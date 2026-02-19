#!/usr/bin/env python3
"""
Step 0: Backup & Clean + DB Init + Run ID Generation

- Backs up previous output folder
- Cleans output for fresh run
- Initialises Malaysia DB schema
- Generates and registers a new run_id (or resumes existing one)
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_malaysia_dir = Path(__file__).resolve().parents[2]  # scripts/Malaysia
if str(_malaysia_dir) not in sys.path:
    sys.path.insert(0, str(_malaysia_dir))

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import (
    load_env_file, getenv_list, get_output_dir, get_backup_dir,
    get_central_output_dir,
)
from core.utils.shared_utils import backup_output_folder, clean_output_folder
from core.db.models import generate_run_id

load_env_file()


def main() -> None:
    OUTPUT_DIR = get_output_dir()
    BACKUP_DIR = get_backup_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()

    print()
    print("=" * 80)
    print("STEP 0 — BACKUP, CLEAN & DB INIT")
    print("=" * 80)
    print()

    # ── 1. Backup ───────────────────────────────────────────────────────
    print("[1/3] Creating backup of output folder...", flush=True)
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)],
    )
    if backup_result["status"] == "ok":
        print(f"[OK] Backup created: {backup_result['backup_folder']}", flush=True)
        print(f"     Files backed up: {backup_result['files_backed_up']}", flush=True)
    elif backup_result["status"] == "skipped":
        print(f"[SKIP] {backup_result['message']}", flush=True)
    else:
        print(f"[ERROR] {backup_result['message']}", flush=True)
        return

    # ── 2. Clean ────────────────────────────────────────────────────────
    print()
    print("[2/3] Cleaning output folder...", flush=True)
    keep_files = getenv_list("SCRIPT_00_KEEP_FILES", ["execution_log.txt"])
    keep_dirs = getenv_list("SCRIPT_00_KEEP_DIRS", ["runs", "backups"])
    clean_result = clean_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        keep_files=keep_files,
        keep_dirs=keep_dirs,
    )
    if clean_result["status"] == "ok":
        print(f"[OK] Cleaned: {clean_result['files_deleted']} files, "
              f"{clean_result['directories_deleted']} dirs removed", flush=True)
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result['message']}", flush=True)
    else:
        print(f"[ERROR] {clean_result['message']}", flush=True)
        return

    # ── 3. Init DB + run_id ─────────────────────────────────────────────
    print()
    print("[3/3] Initialising database & run_id...", flush=True)

    # Ensure Malaysia directory is at the front of sys.path to prioritize local 'db' package
    # This fixes conflict with core/db which might be in sys.path
    malaysia_dir = str(Path(__file__).resolve().parents[2]) # scripts/Malaysia (actually ends up being registered as scripts/Malaysia in sys.path by logic)
    script_dir = str(Path(__file__).resolve().parents[1]) # scripts/Malaysia directory ref
    
    # We found that D:\quad99\Scrappers\core was incorrectly in sys.path, causing 'import db' to pick core/db
    # We remove any path ending in 'core' from sys.path to prevent this shadowing
    sys.path = [p for p in sys.path if not Path(p).name == 'core']
    
    # Re-insert Malaysia directory at the front
    if script_dir in sys.path:
        sys.path.remove(script_dir)
    sys.path.insert(0, script_dir)

    # Force re-import of db module if it was incorrectly loaded from core/db
    if 'db' in sys.modules:
        del sys.modules['db']
    
    from core.db.connection import CountryDB
    from db.schema import apply_malaysia_schema

    db = CountryDB("Malaysia")
    apply_malaysia_schema(db)
    print("[OK] Database ready (PostgreSQL)", flush=True)

    # Generate or reuse run_id
    run_id = os.environ.get("MALAYSIA_RUN_ID")
    is_resumed = False
    if run_id:
        print(f"[OK] Resuming run_id from env: {run_id}", flush=True)
        is_resumed = True
    else:
        run_id = generate_run_id()
        os.environ["MALAYSIA_RUN_ID"] = run_id
        print(f"[OK] Generated new run_id: {run_id}", flush=True)

    # Persist run_id to a file so child processes can read it
    run_id_file = OUTPUT_DIR / ".current_run_id"
    run_id_file.write_text(run_id, encoding="utf-8")
    print(f"[OK] Saved run_id to {run_id_file}", flush=True)

    # Register run in DB
    from db.repositories import MalaysiaRepository
    repo = MalaysiaRepository(db, run_id)
    
    if is_resumed:
        # If we are resuming, ensure the run exists in the ledger (or update its status)
        # using mode="resume" or preserving the original mode if complex logic were added.
        # Here we just ensure it exists and set status to running.
        repo.ensure_run_in_ledger(mode="resume")
        print(f"[OK] Run resumed/ensured in run_ledger", flush=True)
    else:
        # New run, so we start it as fresh
        repo.start_run(mode="fresh")
        print(f"[OK] Run registered in run_ledger", flush=True)

    print()
    print("=" * 80)
    print("Backup, cleanup and DB init complete. Ready for pipeline.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    from core.pipeline.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(main, "Malaysia", 0, "Backup and Clean", output_files=None)
