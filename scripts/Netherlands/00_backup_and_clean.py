#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder - Netherlands

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
"""

from pathlib import Path
import sys
import os

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.shared_utils import backup_output_folder, clean_output_folder

# Get script directory
SCRIPT_DIR = Path(__file__).resolve().parent

# Use platform paths if available, otherwise use script-relative paths
try:
    from platform_config import get_path_manager
    pm = get_path_manager()
    OUTPUT_DIR = pm.get_output_dir("Netherlands")
    BACKUP_DIR = pm.get_backups_dir("Netherlands")
    # Also backup script's local output if it exists
    LOCAL_OUTPUT_DIR = SCRIPT_DIR / "output"
except Exception:
    # Fallback: use script-relative paths
    OUTPUT_DIR = SCRIPT_DIR / "output"
    BACKUP_DIR = _repo_root / "backups" / "Netherlands"
    LOCAL_OUTPUT_DIR = OUTPUT_DIR

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BACKUP_DIR.mkdir(parents=True, exist_ok=True)
if LOCAL_OUTPUT_DIR != OUTPUT_DIR:
    LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER - Netherlands")
    print("=" * 80)
    print()

    # Backup both platform output and local output (if different)
    output_dirs_to_backup = []
    if OUTPUT_DIR.exists():
        output_dirs_to_backup.append(OUTPUT_DIR)
    if LOCAL_OUTPUT_DIR != OUTPUT_DIR and LOCAL_OUTPUT_DIR.exists():
        output_dirs_to_backup.append(LOCAL_OUTPUT_DIR)

    if not output_dirs_to_backup:
        print("[SKIP] No output directories found to backup")
    else:
        # Step 1: Backup
        print("[1/2] Creating backup of output folder(s)...")
        for output_dir in output_dirs_to_backup:
            backup_result = backup_output_folder(
                output_dir=output_dir,
                backup_dir=BACKUP_DIR,
                central_output_dir=None,
                exclude_dirs=[str(BACKUP_DIR)]
            )

            if backup_result["status"] == "ok":
                print(f"[OK] Backup created successfully for {output_dir.name}!")
                print(f"     Location: {backup_result['backup_folder']}")
                print(f"     Files backed up: {backup_result['files_backed_up']}")
            elif backup_result["status"] == "skipped":
                print(f"[SKIP] {backup_result['message']}")
            else:
                print(f"[ERROR] {backup_result['message']}")

        print()

        # Step 2: Clean
        print("[2/2] Cleaning output folder(s)...")
        for output_dir in output_dirs_to_backup:
            clean_result = clean_output_folder(
                output_dir=output_dir,
                backup_dir=BACKUP_DIR,
                central_output_dir=None,
                keep_files=[],
                keep_dirs=["runs", "backups", ".checkpoints"]
            )

            if clean_result["status"] == "ok":
                print(f"[OK] Output folder cleaned successfully: {output_dir.name}")
                print(f"     Files deleted: {clean_result['files_deleted']}")
                print(f"     Directories deleted: {clean_result['directories_deleted']}")
            elif clean_result["status"] == "skipped":
                print(f"[SKIP] {clean_result['message']}")
            else:
                print(f"[ERROR] {clean_result['message']}")

    print()
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
