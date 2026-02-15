#!/usr/bin/env python3
"""
Netherlands Backup and Clean Script
Backs up previous results and cleans output directory before running new pipeline.
"""

import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add repo root to path for config_loader import
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import get_output_dir, get_backup_dir
from core.standalone_checkpoint import run_with_checkpoint

SCRIPT_ID = "Netherlands"

def backup_previous_results():
    """Backup previous results to timestamped folder."""
    output_dir = get_output_dir()
    backup_dir = get_backup_dir()
    
    if not output_dir.exists():
        print("[BACKUP] Output directory doesn't exist, skipping backup")
        return
    
    # Create backup directory if it doesn't exist
    backup_dir.mkdir(exist_ok=True)
    
    # Create timestamped backup folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_folder = backup_dir / f"backup_{timestamp}"
    backup_folder.mkdir(exist_ok=True)
    
    # Copy files from output to backup
    files_backed_up = 0
    for file_path in output_dir.glob("*"):
        if file_path.is_file():
            try:
                shutil.copy2(file_path, backup_folder / file_path.name)
                files_backed_up += 1
                print(f"[BACKUP] Backed up: {file_path.name}")
            except Exception as e:
                print(f"[BACKUP] Error backing up {file_path.name}: {e}")
    
    print(f"[BACKUP] Backup complete: {files_backed_up} files backed up to {backup_folder}")

def clean_output_directory():
    """Clean the output directory for new run."""
    output_dir = get_output_dir()
    
    if not output_dir.exists():
        output_dir.mkdir(exist_ok=True)
        print("[CLEAN] Created output directory")
        return
    
    # Remove old files (keep recent ones from last hour)
    files_removed = 0
    current_time = datetime.now()
    
    for file_path in output_dir.glob("*"):
        if file_path.is_file():
            try:
                # Check file modification time
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                time_diff = (current_time - mod_time).total_seconds()
                
                # Remove files older than 1 hour
                if time_diff > 3600:
                    file_path.unlink()
                    files_removed += 1
                    print(f"[CLEAN] Removed old file: {file_path.name}")
            except Exception as e:
                print(f"[CLEAN] Error removing {file_path.name}: {e}")
    
    print(f"[CLEAN] Clean complete: {files_removed} old files removed")

def main():
    """Main backup and clean function."""
    print("=" * 60)
    print("NETHERLANDS BACKUP AND CLEAN")
    print("=" * 60)
    
    try:
        # Backup previous results
        print("\n[STEP 1] Backing up previous results...")
        backup_previous_results()
        
        # Clean output directory
        print("\n[STEP 2] Cleaning output directory...")
        clean_output_directory()
        
        print("\n[SUCCESS] Backup and clean completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n[ERROR] Backup and clean failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    run_with_checkpoint(
        main,
        SCRIPT_ID,
        0,
        "Backup and Clean"
    )
