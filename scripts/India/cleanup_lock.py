#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleanup lock files for India scraper.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

SCRAPER_ID = "India"


def cleanup_locks():
    """Remove stale lock files."""
    # Check for Chrome PID tracker files
    lock_files = [
        _repo_root / f".{SCRAPER_ID}_chrome_pids.json",
        _repo_root / "sessions" / "app.lock",
    ]
    
    for lock_file in lock_files:
        if lock_file.exists():
            try:
                lock_file.unlink()
                print(f"Removed: {lock_file}")
            except Exception as e:
                print(f"Could not remove {lock_file}: {e}")


if __name__ == "__main__":
    cleanup_locks()
