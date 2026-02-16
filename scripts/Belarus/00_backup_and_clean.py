#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder and Initialize Database

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
Also initializes the database schema for Belarus.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import sys
import os

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.utils.shared_utils import run_backup_and_clean
from core.config.config_manager import ConfigManager

SCRAPER_ID = "Belarus"
try:
    ConfigManager.ensure_dirs()
    OUTPUT_DIR = ConfigManager.get_output_dir(SCRAPER_ID)
    BACKUP_DIR = ConfigManager.get_backups_dir(SCRAPER_ID)
    CENTRAL_OUTPUT_DIR = ConfigManager.get_exports_dir(SCRAPER_ID)
except Exception:
    OUTPUT_DIR = _repo_root / "output" / SCRAPER_ID
    BACKUP_DIR = _repo_root / "backups" / SCRAPER_ID
    CENTRAL_OUTPUT_DIR = _repo_root / "exports" / SCRAPER_ID


def init_database():
    """Initialize Belarus database schema."""
    print("[DB] Initializing Belarus database schema...")
    try:
        # Fix for module shadowing: Ensure local directory is first in path
        _current_dir = str(Path(__file__).resolve().parent)
        if _current_dir not in sys.path:
            sys.path.insert(0, _current_dir)
        
        # Remove any path ending in 'core' to prevent 'import db' from resolving to 'core/db'
        sys.path = [p for p in sys.path if Path(p).name != 'core' and Path(p).name != 'db']
        
        # Force re-import of db module if it was incorrectly loaded
        if 'db' in sys.modules:
             del sys.modules['db']

        from core.db.connection import CountryDB
        from db.schema import apply_belarus_schema
        from core.db.models import generate_run_id
        
        db = CountryDB("Belarus")
        apply_belarus_schema(db)
        
        # Generate and store run_id
        run_id = generate_run_id()
        run_id_file = OUTPUT_DIR / ".current_run_id"
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding="utf-8")
        
        # Set environment variable for child processes
        os.environ["BELARUS_RUN_ID"] = run_id
        
        print(f"[DB] Schema applied successfully. Run ID: {run_id}")
        return True
    except Exception as e:
        print(f"[DB] Warning: Could not initialize database: {e}")
        return False


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    # Step 1 & 2: Backup and Clean (shared)
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
        print(f"[OK] Cleaned ({clean_result.get('files_deleted', 0)} files, {clean_result.get('directories_deleted', 0)} dirs)")
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result.get('message', '')}")
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
