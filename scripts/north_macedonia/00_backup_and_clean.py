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

try:
    from config_loader import load_env_file, getenv_list, get_output_dir, get_backup_dir, get_central_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
    BACKUP_DIR = get_backup_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()
except ImportError as e:
    # Fallback if config_loader doesn't have all functions
    BASE_DIR = Path(__file__).resolve().parents[1]
    OUTPUT_DIR = BASE_DIR / "output"
    BACKUP_DIR = BASE_DIR / "backup"
    CENTRAL_OUTPUT_DIR = _repo_root / "output"
    
    # Define getenv_list fallback
    def getenv_list(key: str, default: list = None) -> list:
        """Get environment variable as list."""
        import json
        val = os.getenv(key)
        if not val:
            return default if default is not None else []
        try:
            return json.loads(val)
        except:
            return [v.strip() for v in val.split(',') if v.strip()]

from core.utils.shared_utils import backup_output_folder, clean_output_folder


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
        # Try new database layer first
        try:
            from core.db import get_db
            from db import apply_north_macedonia_schema
            
            db = get_db("NorthMacedonia")
            apply_north_macedonia_schema(db)
            print("[DB] Schema applied successfully using new database layer")
        except ImportError as e:
            # Fallback to old CountryDB if available
            try:
                from core.db.connection import CountryDB
                from db import apply_north_macedonia_schema
                
                db = CountryDB("NorthMacedonia")
                apply_north_macedonia_schema(db)
                print("[DB] Schema applied successfully using CountryDB")
            except ImportError:
                print(f"[DB] Could not import database modules: {e}")
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
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)]
    )

    if backup_result["status"] == "ok":
        print(f"[OK] Backup created successfully!")
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
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=CENTRAL_OUTPUT_DIR,
        keep_files=keep_files,
        keep_dirs=keep_dirs
    )

    if clean_result["status"] == "ok":
        print(f"[OK] Output folder cleaned successfully!")
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
