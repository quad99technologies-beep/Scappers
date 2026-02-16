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
_script_dir = Path(__file__).parent
print(f"[DEBUG] _repo_root: {_repo_root}")
print(f"[DEBUG] _script_dir: {_script_dir}")
print(f"[DEBUG] sys.path before: {sys.path[:3]}...")
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
print(f"[DEBUG] sys.path after: {sys.path[:3]}...")

from config_loader import load_env_file, get_output_dir
from core.utils.shared_utils import run_backup_and_clean

SCRAPER_ID = "Tender_Chile"


def init_database():
    """Initialize Tender Chile database schema."""
    print("[DB] Initializing Tender Chile database schema...")
    
    # Force local script directory to front of path to prefer local 'db' package
    if str(_script_dir) in sys.path:
        sys.path.remove(str(_script_dir))
    sys.path.insert(0, str(_script_dir))
    
    # If 'db' is already in modules and pointing effectively to core.db or elsewhere,
    # we should clear it so we can import the local one.
    # Note: 'core.db' is usually imported as 'core.db', but if 'core' was in path, it could be 'db'.
    if "db" in sys.modules:
        del sys.modules["db"]
        
    print(f"[DB] sys.path: {sys.path}")
    try:
        from core.db.connection import CountryDB
        print(f"[DB] Importing db.schema from {Path(__file__).parent / 'db'}")
        from db.schema import apply_chile_schema
        from core.db.models import generate_run_id
        
        db = CountryDB("Tender_Chile")
        apply_chile_schema(db)
        
        # Generate and store run_id
        run_id = generate_run_id()
        output_dir = get_output_dir()
        run_id_file = output_dir / ".current_run_id"
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

    print("[1/3] Creating backup of output folder...")
    result = run_backup_and_clean(SCRAPER_ID)
    backup_result = result["backup"]
    clean_result = result["clean"]

    if backup_result["status"] == "ok":
        print(f"[OK] Backup: {backup_result['backup_folder']}")
    elif backup_result["status"] == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        print(f"[ERROR] {backup_result.get('message', 'Backup failed')}")
        return

    print()
    print("[2/3] Cleaning output folder...")
    if clean_result["status"] == "ok":
        print(f"[OK] Cleaned ({clean_result.get('files_deleted', 0)} files)")
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result.get('message', '')}")
    else:
        print(f"[ERROR] {clean_result.get('message', 'Clean failed')}")
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
