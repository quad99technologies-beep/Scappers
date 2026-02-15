#!/usr/bin/env python3
"""
Cleanup Lock File
Removes lock files after pipeline completion.
"""

import sys
import time
import argparse
from pathlib import Path

# Add repo root to path
script_dir = Path(__file__).resolve().parent
repo_root = script_dir.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from config_loader import getenv_int, getenv_float, getenv_bool
    max_retries = getenv_int("MAX_RETRIES_CLEANUP", 5)
    retry_delay = getenv_float("CLEANUP_RETRY_DELAY_BASE", 0.3)
    lock_force = getenv_bool("LOCK_FORCE", False)
except Exception:
    max_retries = 5
    retry_delay = 0.3
    lock_force = False

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Remove lock even if in use")
    args = parser.parse_args()

    try:
        from core.config.config_manager import ConfigManager
        # Migrated: get_path_manager() -> ConfigManager
        lock_file = pm.get_lock_file("CanadaOntario")
    except Exception:
        lock_file = repo_root / ".locks" / "CanadaOntario.lock"

    force = args.force or lock_force
    for attempt in range(max_retries):
        try:
            if lock_file.exists():
                lock_file.unlink()
                if not lock_file.exists():
                    return 0
            else:
                return 0
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            elif force:
                try:
                    lock_file.unlink()
                    return 0
                except Exception:
                    return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
