#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder and Initialize Database

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
Also initializes the database schema for North Macedonia.
"""

from pathlib import Path
import sys
import os

# Force unbuffered output for real-time console updates
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
os.environ.setdefault("PYTHONUNBUFFERED", "1")

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add script directory to path FIRST to prioritize local db module over core/db
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from core.utils.shared_utils import run_backup_and_clean
from core.config.config_manager import ConfigManager

SCRAPER_ID = "NorthMacedonia"
try:
    ConfigManager.ensure_dirs()
    OUTPUT_DIR = ConfigManager.get_output_dir(SCRAPER_ID)
    BACKUP_DIR = ConfigManager.get_backups_dir(SCRAPER_ID)
    CENTRAL_OUTPUT_DIR = ConfigManager.get_exports_dir(SCRAPER_ID)
except Exception:
    OUTPUT_DIR = _repo_root / "output" / SCRAPER_ID
    BACKUP_DIR = _repo_root / "backups" / SCRAPER_ID
    CENTRAL_OUTPUT_DIR = _repo_root / "exports" / SCRAPER_ID


def drop_backup_tables():
    """Drop nm_drug_register_backup_* tables (not needed)."""
    print("[DB] Dropping nm_drug_register_backup_* tables...")
    try:
        from core.db.connection import CountryDB
        with CountryDB("NorthMacedonia") as db:
            with db.cursor() as cur:
                # Find all backup tables
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE 'nm_drug_register_backup_%'
                """)
                backup_tables = [row[0] for row in cur.fetchall()]
                
                if not backup_tables:
                    print("[DB] No backup tables found.")
                    return
                
                print(f"[DB] Found {len(backup_tables)} backup table(s) to drop")
                for table in backup_tables:
                    try:
                        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                        print(f"[DB] Dropped: {table}")
                    except Exception as e:
                        print(f"[DB] Warning: Could not drop {table}: {e}")
                db.commit()
    except Exception as e:
        print(f"[DB] Warning: Could not drop backup tables: {e}")


def init_database():
    """Initialize North Macedonia database schema."""
    print("[DB] Initializing North Macedonia database schema...")
    
    # First, drop any backup tables (not needed)
    drop_backup_tables()
    
    try:
        # Fix for module shadowing: Ensure local directory is first in path
        _current_dir = str(Path(__file__).resolve().parent)
        if _current_dir not in sys.path:
            sys.path.insert(0, _current_dir)
        
        # Force re-import of db module if it was incorrectly loaded
        if "db" in sys.modules:
            del sys.modules["db"]
        
        # Import from local db module explicitly
        try:
            from db.schema import apply_north_macedonia_schema
        except ImportError:
            # Fallback: try importing from scripts.north_macedonia.db
            from scripts.north_macedonia.db.schema import apply_north_macedonia_schema
            
        # Try new database layer first
        try:
            from core.db import get_db
            
            db = get_db("NorthMacedonia")
            apply_north_macedonia_schema(db)
            print("[DB] Schema applied successfully using new database layer")
        except (ImportError, AttributeError) as e:
            # Fallback to old CountryDB if available
            try:
                from core.db.connection import CountryDB
                
                db = CountryDB("NorthMacedonia")
                apply_north_macedonia_schema(db)
                print("[DB] Schema applied successfully using CountryDB")
            except (ImportError, AttributeError) as e2:
                print(f"[DB] Could not import database modules: {e} | {e2}")
                return False
        
        # Generate and store run_id
        try:
            from core.db.models import generate_run_id
            run_id = generate_run_id()
        except ImportError:
            # Fallback to timestamp-based run_id
            from datetime import datetime
            run_id = f"NorthMacedonia_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        run_id_file = OUTPUT_DIR / ".current_run_id"
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding="utf-8")
        
        # Set environment variable for child processes
        os.environ["NORTH_MACEDONIA_RUN_ID"] = run_id
        
        print(f"[DB] Run ID: {run_id}")
        return True
    except Exception as e:
        import traceback
        print(f"[DB] Warning: Could not initialize database: {e}")
        print(f"[DB] Traceback: {traceback.format_exc()}")
        return False


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

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
