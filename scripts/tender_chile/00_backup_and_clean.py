#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder and Initialize Database

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
Also initializes the database schema for Tender Chile.
"""

from pathlib import Path
import sys
import os

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
os.environ.setdefault("PYTHONUNBUFFERED", "1")

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import load_env_file, getenv_list, get_output_dir, get_backup_dir, get_central_output_dir
from core.utils.shared_utils import backup_output_folder, clean_output_folder


def init_database():
    """Initialize Tender Chile database schema."""
    print("[DB] Initializing Tender Chile database schema...")
    try:
        from core.db.connection import CountryDB
        from db.schema import apply_chile_schema
        from core.db.models import generate_run_id
        
        db = CountryDB("Tender_Chile")
        apply_chile_schema(db)
        
        # Generate and store run_id
        run_id = generate_run_id()
        run_id_file = get_output_dir() / ".current_run_id"
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding="utf-8")
        
        # Set environment variable for child processes
        os.environ["TENDER_CHILE_RUN_ID"] = run_id
        
        print(f"[DB] Schema applied successfully. Run ID: {run_id}")
        return True
    except Exception as e:
        print(f"[DB] Warning: Could not initialize database: {e}")
        return False


def main() -> None:
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    load_env_file()
    output_dir = get_output_dir()
    backup_dir = get_backup_dir()
    central_output = get_central_output_dir()

    print("[1/3] Creating backup of output folder...")
    backup_result = backup_output_folder(
        output_dir=output_dir,
        backup_dir=backup_dir,
        central_output_dir=central_output,
        exclude_dirs=[str(backup_dir)]
    )

    if backup_result["status"] == "ok":
        print("[OK] Backup created successfully!")
        print(f"     Location: {backup_result['backup_folder']}")
        print(f"     Timestamp: {backup_result['timestamp']}")
        print(f"     Latest file modification: {backup_result['latest_modification']}")
        print(f"     Files backed up: {backup_result['files_backed_up']}")
    elif backup_result["status"] == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        print(f"[ERROR] {backup_result['message']}")
        return

    print()

    print("[2/3] Cleaning output folder...")
    keep_files = getenv_list("SCRIPT_00_KEEP_FILES", ["execution_log.txt"])
    keep_dirs = getenv_list("SCRIPT_00_KEEP_DIRS", ["runs", "backups"])
    clean_result = clean_output_folder(
        output_dir=output_dir,
        backup_dir=backup_dir,
        central_output_dir=central_output,
        keep_files=keep_files,
        keep_dirs=keep_dirs
    )

    if clean_result["status"] == "ok":
        print("[OK] Output folder cleaned successfully!")
        print(f"     Files deleted: {clean_result['files_deleted']}")
        print(f"     Directories deleted: {clean_result['directories_deleted']}")
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result['message']}")
    else:
        print(f"[ERROR] {clean_result['message']}")
        return

    print()

    # Step 3: Initialize Database
    print("[3/3] Initializing database schema...")
    init_database()

    print()
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
