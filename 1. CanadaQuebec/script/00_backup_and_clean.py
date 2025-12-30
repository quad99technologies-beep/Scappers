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
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
try:
    from config_loader import get_base_dir, get_output_dir, get_backup_dir, get_central_output_dir
    BASE_DIR = get_base_dir()
    OUTPUT_DIR = get_output_dir()
    BACKUP_DIR = get_backup_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()
except ImportError:
    # Fallback to original values if config_loader not available
    BASE_DIR = Path(__file__).resolve().parents[1]
    OUTPUT_DIR = BASE_DIR / "output"
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
    # Get resolved paths to avoid recursion
    directory_resolved = directory.resolve()
    backup_dir_resolved = BACKUP_DIR.resolve()

    # Walk through all files in the directory
    for root, dirs, files in os.walk(directory):
        # Skip backup directory if it's inside the output directory to prevent recursion
        root_path = Path(root).resolve()
        if root_path == backup_dir_resolved or (backup_dir_resolved.is_relative_to(directory_resolved) and backup_dir_resolved.is_relative_to(root_path)):
            dirs[:] = []  # Don't descend into backup directory
            continue
        
        # Also skip if any parent directory is the backup directory
        try:
            if backup_dir_resolved.is_relative_to(root_path):
                dirs[:] = []
                continue
        except (AttributeError, ValueError):
            # Python < 3.9 fallback
            if str(backup_dir_resolved).startswith(str(root_path)):
                dirs[:] = []
                continue
        
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

    # Note: BACKUP_DIR can be inside OUTPUT_DIR - recursion is prevented by the ignore function in copytree

    # Get latest modification time from output folder
    latest_time = get_latest_modification_time(OUTPUT_DIR)

    # Create backup folder name with timestamp
    timestamp = latest_time.strftime("%Y%m%d_%H%M%S")
    backup_folder = BACKUP_DIR / f"output_{timestamp}"

    # Create backups directory if it doesn't exist
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    try:
        # Copy entire output folder to backup location
        # Use ignore function to prevent copying backup directories (recursion prevention)
        backup_dir_str = str(BACKUP_DIR.resolve())
        def ignore_backup_dirs(dirname, names):
            """Ignore backup directories to prevent recursion"""
            ignored = []
            for name in names:
                item_path = Path(dirname) / name
                try:
                    item_resolved = str(item_path.resolve())
                    # Skip if this path is the backup directory or inside it
                    if item_resolved == backup_dir_str or item_resolved.startswith(backup_dir_str + os.sep):
                        ignored.append(name)
                except:
                    pass
            return ignored
        
        shutil.copytree(OUTPUT_DIR, backup_folder, ignore=ignore_backup_dirs, dirs_exist_ok=True)
        
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
    Excludes backups and runs directories to prevent deleting active run data.

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
        
        # Get resolved paths for comparison
        output_dir_resolved = OUTPUT_DIR.resolve()
        backup_dir_resolved = BACKUP_DIR.resolve()
        
        # Determine runs directory path (usually output/runs)
        # Check if runs is inside output directory
        runs_dir = None
        try:
            # Try to get runs directory from platform config if available
            from config_loader import get_base_dir
            base = get_base_dir()
            # Check common locations
            possible_runs = [
                output_dir_resolved / "runs",
                base.parent / "output" / "runs" if base.parent.name == "ScraperPlatform" else None,
            ]
            for runs_path in possible_runs:
                if runs_path and runs_path.exists():
                    runs_dir = runs_path.resolve()
                    break
        except:
            pass
        
        # Get central output directory path for comparison
        central_output_dir_resolved = None
        if CENTRAL_OUTPUT_DIR.exists():
            central_output_dir_resolved = CENTRAL_OUTPUT_DIR.resolve()
        
        # Remove all contents, excluding backups, runs, and final output directories
        for item in OUTPUT_DIR.iterdir():
            item_resolved = item.resolve()
            
            # Skip backups directory
            if item_resolved == backup_dir_resolved or (backup_dir_resolved.exists() and str(item_resolved).startswith(str(backup_dir_resolved) + os.sep)):
                continue
            
            # Skip runs directory
            if runs_dir and (item_resolved == runs_dir or str(item_resolved).startswith(str(runs_dir) + os.sep)):
                continue
            
            # Skip central output directory (final output files - do not remove)
            if central_output_dir_resolved and OUTPUT_DIR.resolve() == central_output_dir_resolved:
                # If OUTPUT_DIR is the same as CENTRAL_OUTPUT_DIR, skip final output files (CSV/XLSX)
                if item.is_file() and item.suffix.lower() in ('.csv', '.xlsx'):
                    continue
            
            # Skip if item name is "runs" or "backups"
            if item.name.lower() in ("runs", "backups"):
                continue
            
            try:
                if item.is_file():
                    item.unlink()
                    files_deleted += 1
                elif item.is_dir():
                    shutil.rmtree(item)
                    dirs_deleted += 1
            except (PermissionError, OSError) as e:
                # Skip files/dirs that are in use (like open log files)
                # Log but continue with other items
                continue

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

