#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleanup Lock File

Removes the scraper lock file after pipeline completion.
"""

import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

try:
    from platform_config import PathManager
    
    lock_file = PathManager.get_lock_file("Russia")
    if lock_file.exists():
        lock_file.unlink()
        print(f"Removed lock file: {lock_file}")
except Exception as e:
    print(f"Warning: Could not remove lock file: {e}")
