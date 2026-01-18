#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder

Creates a backup of the output folder with a timestamp based on the latest
file modification date, then cleans the output folder for a fresh run.
"""

from pathlib import Path
import sys
import os

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, "reconfigure") else None
os.environ.setdefault("PYTHONUNBUFFERED", "1")

# Add repo root to path for shared utilities
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Taiwan to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

try:
    from config_loader import load_env_file, getenv_list, get_output_dir, get_backup_dir, get_central_output_dir
    load_env_file()
    OUTPUT_DIR = get_output_dir()
    BACKUP_DIR = get_backup_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()
except ImportError:
    # Fallback to repo-relative paths
    OUTPUT_DIR = _script_dir
    BACKUP_DIR = _repo_root / "backups" / "Taiwan"
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    CENTRAL_OUTPUT_DIR = _repo_root / "output" / "Taiwan"
    CENTRAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

    # Step 2: Clean
    print("[2/2] Cleaning output folder...")
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
        print("[OK] Output folder cleaned successfully!")
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
