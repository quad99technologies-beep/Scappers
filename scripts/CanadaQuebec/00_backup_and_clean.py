#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import sys
import os

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

try:
    from config_loader import (
        get_base_dir, get_output_dir, get_backup_dir, get_central_output_dir,
        getenv_list
    )
    BASE_DIR = get_base_dir()
    OUTPUT_DIR = get_output_dir()
    BACKUP_DIR = get_backup_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()
except ImportError:
    # Check strict mode
    CONF_STRICT_MODE = os.getenv("CONF_STRICT_MODE", "false").lower() in ("true", "1", "yes")
    
    if CONF_STRICT_MODE:
        raise ImportError(
            "config_loader is required but not available. "
            "Set CONF_STRICT_MODE=false to allow fallback paths."
        )
    
    # Fallback: use env config or defaults
    BASE_DIR = Path(__file__).resolve().parents[1]
    legacy_output = os.getenv("LEGACY_OUTPUT_DIR", "output")
    legacy_backup = os.getenv("LEGACY_BACKUP_DIR", "backups")
    legacy_central = os.getenv("LEGACY_CENTRAL_OUTPUT_DIR", "output")
    
    if Path(legacy_output).is_absolute():
        OUTPUT_DIR = Path(legacy_output)
    else:
        OUTPUT_DIR = BASE_DIR / legacy_output
    
    if Path(legacy_backup).is_absolute():
        BACKUP_DIR = Path(legacy_backup)
    else:
        BACKUP_DIR = BASE_DIR / legacy_backup
    
    if Path(legacy_central).is_absolute():
        CENTRAL_OUTPUT_DIR = Path(legacy_central)
    else:
        CENTRAL_OUTPUT_DIR = _repo_root / legacy_central
    
    # Define getenv_list for fallback mode
    def getenv_list(key: str, default: list = None):
        if default is None:
            default = []
        value = os.getenv(key)
        if value is None:
            return default
        # Try to parse as JSON array or comma-separated
        try:
            import json
            return json.loads(value)
        except:
            return [item.strip() for item in value.split(",") if item.strip()] if value else default

from core.shared_utils import backup_output_folder, clean_output_folder


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    # Step 1: Backup
    print("[1/2] Creating backup of output folder...")
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

    # Step 2: Clean
    print("[2/2] Cleaning output folder...")
    # Get keep_files and keep_dirs from config (with fallback)
    try:
        keep_files = getenv_list("SCRIPT_00_KEEP_FILES", [])
    except (NameError, ImportError):
        keep_files = []
    
    try:
        keep_dirs = getenv_list("SCRIPT_00_KEEP_DIRS", ["runs", "backups"])
    except (NameError, ImportError):
        keep_dirs = ["runs", "backups"]
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
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
