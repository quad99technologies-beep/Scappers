    #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder and Initialize Database - Canada Ontario

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
Also initializes the database schema for Canada Ontario.
"""

from pathlib import Path
import sys
import os

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.utils.shared_utils import backup_output_folder, clean_output_folder
from core.utils.logger import setup_standard_logger
from core.progress.progress_tracker import StandardProgress
from core.db.models import generate_run_id
from config_loader import get_run_id, get_run_dir

# Get script directory
SCRIPT_DIR = Path(__file__).resolve().parent

from core.config.config_manager import ConfigManager
# Migrated: get_path_manager() -> ConfigManager
OUTPUT_DIR = ConfigManager.get_output_dir("CanadaOntario")
BACKUP_DIR = ConfigManager.get_backups_dir("CanadaOntario")
LOCAL_OUTPUT_DIR = SCRIPT_DIR / "output"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
if LOCAL_OUTPUT_DIR != OUTPUT_DIR:
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def init_database():
    """Initialize Canada Ontario database schema and clear data for fresh run."""
    print("[DB] Initializing Canada Ontario database schema...")
    try:
        from core.db.postgres_connection import PostgresDB
        from core.db.schema_registry import SchemaRegistry
        from pathlib import Path

        db = PostgresDB("CanadaOntario")
        db.connect()
        
        # Apply schema
        repo_root = Path(__file__).resolve().parents[2]
        schema_path = repo_root / "sql" / "schemas" / "postgres" / "canada_ontario.sql"
        if schema_path.exists():
            registry = SchemaRegistry(db)
            registry.apply_schema(schema_path)
        
        # Truncate tables for fresh run
        # Use CASCADE to handle foreign keys if any
        tables = [
            "co_products", 
            "co_manufacturers", 
            "co_eap_prices", 
            "co_final_output", 
            "co_pcid_mappings", 
            "co_step_progress", 
            "co_export_reports", 
            "co_errors"
        ]
        
        print(f"[DB] Cleaning tables: {', '.join(tables)}")
        for table in tables:
            try:
                # Check if table exists first
                res = db.fetchone("SELECT to_regclass(%s)", (table,))
                if res and res[0]:
                    db.execute(f"TRUNCATE TABLE {table} CASCADE")
            except Exception as e:
                print(f"[DB] Warning: Failed to truncate {table}: {e}")

        # Generate and store run_id
        run_id = generate_run_id()
        run_id_file = OUTPUT_DIR / ".current_run_id"
        run_id_file.parent.mkdir(parents=True, exist_ok=True)
        run_id_file.write_text(run_id, encoding="utf-8")
        
        # Set environment variable for child processes
        os.environ["CANADA_ONTARIO_RUN_ID"] = run_id
        
        print(f"[DB] Schema applied and tables cleaned. Run ID: {run_id}")
        return True
    except Exception as e:
        print(f"[DB] Warning: Could not initialize database: {e}")
        return False


def main() -> None:
    """Main entry point."""
    run_id = get_run_id()
    run_dir = get_run_dir(run_id)
    logger = setup_standard_logger(
        "canada_ontario_backup",
        scraper_name="CanadaOntario",
        log_file=run_dir / "logs" / "backup.log",
    )
    progress = StandardProgress("canada_ontario_backup", total=3, unit="steps", logger=logger, state_path=BACKUP_DIR / "backup_progress.json")
    logger.info("Backup and clean output folder - Canada Ontario")

    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER - CANADA ONTARIO")
    print("=" * 80)
    print()

    # Step 1: Backup
    print("[1/3] Creating backup of output folder...")
    progress.update(0, message="creating backup", force=True)
    backup_result = backup_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=OUTPUT_DIR,
        exclude_dirs=[str(BACKUP_DIR)]
    )

    if backup_result["status"] == "ok":
        logger.info(f"Backup created: {backup_result['backup_folder']}")
        print(f"[OK] Backup created successfully!")
        print(f"     Location: {backup_result['backup_folder']}")
        print(f"     Timestamp: {backup_result['timestamp']}")
    elif backup_result["status"] == "skipped":
        logger.info(f"Backup skipped: {backup_result['message']}")
        print(f"[SKIP] {backup_result['message']}")
    else:
        logger.error(f"Backup failed: {backup_result['message']}")
        print(f"[ERROR] {backup_result['message']}")
        return

    print()

    # Step 2: Clean
    print("[2/3] Cleaning output folder...")
    progress.update(1, message="cleaning output", force=True)
    clean_result = clean_output_folder(
        output_dir=OUTPUT_DIR,
        backup_dir=BACKUP_DIR,
        central_output_dir=OUTPUT_DIR,
        keep_files=["execution_log.txt"],
        keep_dirs=["runs", "backups"]
    )

    if clean_result["status"] == "ok":
        logger.info(f"Output cleaned: {clean_result['files_deleted']} files deleted")
        print(f"[OK] Output folder cleaned successfully!")
        print(f"     Files deleted: {clean_result['files_deleted']}")
        print(f"     Directories deleted: {clean_result['directories_deleted']}")
    elif clean_result["status"] == "skipped":
        logger.info(f"Clean skipped: {clean_result['message']}")
        print(f"[SKIP] {clean_result['message']}")
    else:
        logger.error(f"Clean failed: {clean_result['message']}")
        print(f"[ERROR] {clean_result['message']}")
        return
        
    # Explicitly ensure specific files are gone (in case clean skipped them or paths differ)
    from config_loader import PRODUCTS_CSV_NAME, MANUFACTURER_MASTER_CSV_NAME
    for fname in [PRODUCTS_CSV_NAME, MANUFACTURER_MASTER_CSV_NAME, "completed_letters.json"]:
        fpath = OUTPUT_DIR / fname
        if fpath.exists():
            try:
                fpath.unlink()
                print(f"[CLEAN] Explicitly deleted stale file: {fpath}")
            except Exception as e:
                print(f"[CLEAN] Warning: Failed to delete {fpath}: {e}")

    print()

    # Step 3: Initialize Database
    print("[3/3] Initializing database schema...")
    progress.update(2, message="initializing database", force=True)
    init_database()
    progress.update(3, message="backup complete", force=True)

    print()
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
