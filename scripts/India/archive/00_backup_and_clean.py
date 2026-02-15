#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
India NPPA Scraper - Step 00: Backup and Clean

Backs up the current output folder and cleans it for a fresh run.
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/India to path for imports
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import get_output_dir, get_backup_dir, getenv_list, SCRAPER_ID

# Files and directories to keep during clean
KEEP_FILES = ["execution_log.txt"]
KEEP_DIRS = ["runs", "backups", ".checkpoints"]


def backup_output():
    """Backup the current output folder with timestamp."""
    output_dir = get_output_dir()
    backup_base = get_backup_dir()
    
    if not output_dir.exists():
        print(f"[INFO] Output directory does not exist: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        return
    
    # Check if there are files to backup
    files_to_backup = [f for f in output_dir.iterdir() 
                       if f.name not in KEEP_FILES and f.name not in KEEP_DIRS]
    
    if not files_to_backup:
        print("[INFO] No files to backup in output directory")
        return
    
    # Create timestamped backup folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = backup_base / f"output_{timestamp}"
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Backing up {len(files_to_backup)} items to: {backup_dir}")
    
    for item in files_to_backup:
        dest = backup_dir / item.name
        try:
            if item.is_dir():
                shutil.copytree(item, dest)
            else:
                shutil.copy2(item, dest)
            print(f"  Backed up: {item.name}")
        except Exception as e:
            print(f"  [WARN] Failed to backup {item.name}: {e}")
    
    print(f"[OK] Backup complete: {backup_dir}")


def clean_output():
    """Clean the output folder, keeping specified files/dirs."""
    output_dir = get_output_dir()
    
    if not output_dir.exists():
        print(f"[INFO] Output directory does not exist, creating: {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
        return
    
    # Get configurable keep lists
    keep_files = set(KEEP_FILES + getenv_list("SCRIPT_00_KEEP_FILES", []))
    keep_dirs = set(KEEP_DIRS + getenv_list("SCRIPT_00_KEEP_DIRS", []))
    
    items_removed = 0
    for item in output_dir.iterdir():
        if item.name in keep_files or item.name in keep_dirs:
            print(f"  Keeping: {item.name}")
            continue
        
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
            items_removed += 1
            print(f"  Removed: {item.name}")
        except Exception as e:
            print(f"  [WARN] Failed to remove {item.name}: {e}")
    
    print(f"[OK] Cleaned output directory ({items_removed} items removed)")


def main():
    print("=" * 60)
    print(f"India NPPA Scraper - Step 00: Backup and Clean")
    print("=" * 60)
    
    # Step 1: Backup
    print("\n[STEP 1] Backing up output folder...")
    backup_output()
    
    # Step 2: Clean
    print("\n[STEP 2] Cleaning output folder...")
    clean_output()
    
    print("\n" + "=" * 60)
    print("Backup and clean complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
