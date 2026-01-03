#!/usr/bin/env python3
"""
Cleanup Lock File
Removes lock files after pipeline completion
"""
import sys
import time
from pathlib import Path

# Add repo root to path
script_dir = Path(__file__).resolve().parent
repo_root = script_dir.parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

try:
    from config_loader import get_env_int, get_env_float
    MAX_RETRIES_CLEANUP = get_env_int("MAX_RETRIES_CLEANUP", 5)
    CLEANUP_RETRY_DELAY_BASE = get_env_float("CLEANUP_RETRY_DELAY_BASE", 0.3)
except ImportError:
    # Fallback if config_loader not available
    MAX_RETRIES_CLEANUP = 5
    CLEANUP_RETRY_DELAY_BASE = 0.3

try:
    from platform_config import get_path_manager
    pm = get_path_manager()
    lock_file = pm.get_lock_file("Malaysia")
    
    # Try to delete lock file with retries
    for attempt in range(MAX_RETRIES_CLEANUP):
        try:
            if lock_file.exists():
                lock_file.unlink()
                if not lock_file.exists():
                    break
            else:
                break
        except Exception:
            if attempt < MAX_RETRIES_CLEANUP - 1:
                time.sleep(CLEANUP_RETRY_DELAY_BASE * (attempt + 1))
    
    # Also clean up old lock location
    old_lock = repo_root / ".Malaysia_run.lock"
    for attempt in range(MAX_RETRIES_CLEANUP):
        try:
            if old_lock.exists():
                old_lock.unlink()
                if not old_lock.exists():
                    break
            else:
                break
        except Exception:
            if attempt < MAX_RETRIES_CLEANUP - 1:
                time.sleep(CLEANUP_RETRY_DELAY_BASE * (attempt + 1))
except Exception:
    pass  # Ignore errors

