#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backup Output Folder

Creates a backup of the output folder, then cleans the output folder for a fresh run.
"""

import sys
import os

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
os.environ.setdefault("PYTHONUNBUFFERED", "1")

from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.utils.shared_utils import run_backup_and_clean

SCRAPER_ID = "Taiwan"


def main() -> None:
    print()
    print("=" * 80)
    print("BACKUP AND CLEAN OUTPUT FOLDER")
    print("=" * 80)
    print()

    result = run_backup_and_clean(SCRAPER_ID)
    backup_result = result["backup"]
    clean_result = result["clean"]

    print("[1/2] Creating backup of output folder...")
    if backup_result["status"] == "ok":
        print(f"[OK] Backup: {backup_result['backup_folder']}")
    elif backup_result["status"] == "skipped":
        print(f"[SKIP] {backup_result['message']}")
    else:
        print(f"[ERROR] {backup_result.get('message', 'Backup failed')}")
        return

    print()
    print("[2/2] Cleaning output folder...")
    if clean_result["status"] == "ok":
        print(f"[OK] Cleaned ({clean_result.get('files_deleted', 0)} files removed)")
    elif clean_result["status"] == "skipped":
        print(f"[SKIP] {clean_result.get('message', '')}")
    else:
        print(f"[ERROR] {clean_result.get('message', 'Clean failed')}")
        return

    print()
    print("=" * 80)
    print("Backup and cleanup complete! Ready for fresh pipeline run.")
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
