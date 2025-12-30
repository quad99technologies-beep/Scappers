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
import shutil
import os
from datetime import datetime

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "Output"
BACKUP_DIR = BASE_DIR / "backups"
# Central output at repo root level
_repo_root = Path(__file__).resolve().parents[2]
CENTRAL_OUTPUT_DIR = _repo_root / "output"


def get_latest_modification_time(directory: Path) -> datetime:
    """
    Get the latest modification time of any file in the directory tree.

    Args:
        directory: Directory to scan

    Returns:
        datetime of the most recent file modification
    """
    if not directory.exists():
        return datetime.now()

    latest_time = None

    # Walk through all files in the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            try:
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if latest_time is None or mtime > latest_time:
                    latest_time = mtime
            except Exception:
                continue

    return latest_time if latest_time else datetime.now()


def backup_output_folder() -> dict:
    """
    Backup the output folder to backups directory with timestamp.

    Returns:
        dict with status and details
    """
    # Check if output folder exists
    if not OUTPUT_DIR.exists():
        return {
            "status": "skipped",
            "message": "Output folder does not exist, nothing to backup"
        }

    # Check if output folder is empty
    if not any(OUTPUT_DIR.iterdir()):
        return {
            "status": "skipped",
            "message": "Output folder is empty, nothing to backup"
        }

    # Get latest modification time from output folder
    latest_time = get_latest_modification_time(OUTPUT_DIR)

    # Create backup folder name with timestamp
    timestamp = latest_time.strftime("%Y%m%d_%H%M%S")
    backup_folder = BACKUP_DIR / f"output_{timestamp}"

    # Create backups directory if it doesn't exist
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Copy entire output folder to backup location
        shutil.copytree(OUTPUT_DIR, backup_folder)
        
        # Note: Final output files (in CENTRAL_OUTPUT_DIR) are NOT backed up or removed
        # They are preserved across runs as they represent final reports

        # Count files backed up
        file_count = sum(1 for _ in backup_folder.rglob('*') if _.is_file())

        return {
            "status": "ok",
            "backup_folder": str(backup_folder),
            "timestamp": timestamp,
            "latest_modification": latest_time.strftime("%Y-%m-%d %H:%M:%S"),
            "files_backed_up": file_count
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to backup output folder: {str(e)}"
        }


def clean_output_folder() -> dict:
    """
    Delete all files and subdirectories from the output folder.

    Returns:
        dict with status and details
    """
    if not OUTPUT_DIR.exists():
        return {
            "status": "skipped",
            "message": "Output folder does not exist"
        }

    try:
        files_deleted = 0
        dirs_deleted = 0

        # Get central output directory path for comparison
        central_output_dir_resolved = None
        if CENTRAL_OUTPUT_DIR.exists():
            central_output_dir_resolved = CENTRAL_OUTPUT_DIR.resolve()
        
        # Remove all contents except final output files
        for item in OUTPUT_DIR.iterdir():
            # Skip final output files (CSV/XLSX) if OUTPUT_DIR == CENTRAL_OUTPUT_DIR
            if central_output_dir_resolved and OUTPUT_DIR.resolve() == central_output_dir_resolved:
                if item.is_file() and item.suffix.lower() in ('.csv', '.xlsx'):
                    continue  # Do not delete final output files
            
            # Skip runs and backups directories
            if item.is_dir() and item.name.lower() in ("runs", "backups"):
                continue
            
            if item.is_file():
                item.unlink()
                files_deleted += 1
            elif item.is_dir():
                shutil.rmtree(item)
                dirs_deleted += 1

        return {
            "status": "ok",
            "files_deleted": files_deleted,
            "directories_deleted": dirs_deleted
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to clean output folder: {str(e)}"
        }


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    # Step 1: Backup
    print("[1/2] Creating backup of output folder...")
    backup_result = backup_output_folder()

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
    clean_result = clean_output_folder()

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

