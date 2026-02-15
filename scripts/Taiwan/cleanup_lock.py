#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cleanup lock file for Taiwan scraper.
"""

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.config_manager import ConfigManager


def main():
    """Remove lock file if it exists."""
    # Migrated: get_path_manager() -> ConfigManager
    lock_file = pm.get_lock_file("Taiwan")
    if lock_file.exists():
        try:
            lock_file.unlink()
            print(f"[CLEANUP] Removed lock file: {lock_file}")
        except Exception as e:
            print(f"[CLEANUP] Failed to remove lock file: {e}")
            return 1
    else:
        print(f"[CLEANUP] No lock file found: {lock_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
