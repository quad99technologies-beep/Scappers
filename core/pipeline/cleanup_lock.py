#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared cleanup lock for pipeline completion.

Removes lock files after pipeline completion. Use run_cleanup(scraper_id) from
scraper-specific cleanup_lock.py or call directly.
"""

import os
import sys
import time
from pathlib import Path
from typing import Optional


def _get_repo_root() -> Path:
    """Detect repo root (parent of core/)."""
    return Path(__file__).resolve().parents[1]


def _get_retry_params() -> tuple:
    """Get MAX_RETRIES_CLEANUP and CLEANUP_RETRY_DELAY_BASE from env or ConfigManager."""
    try:
        from core.config.config_manager import ConfigManager
        val = os.getenv("MAX_RETRIES_CLEANUP")
        if val is not None:
            max_retries = int(val)
        else:
            max_retries = 5
        val = os.getenv("CLEANUP_RETRY_DELAY_BASE")
        if val is not None:
            delay_base = float(val)
        else:
            delay_base = 0.3
        return max_retries, delay_base
    except Exception:
        return 5, 0.3


def run_cleanup(
    scraper_id: str,
    repo_root: Optional[Path] = None,
    force: bool = False,
    verbose: bool = True,
) -> int:
    """
    Remove lock files for the given scraper. Cleans both new (sessions/) and old (.{scraper}_run.lock) locations.

    Args:
        scraper_id: Scraper identifier (e.g. "Argentina", "Belarus", "Tender_Chile")
        repo_root: Repository root; defaults to auto-detect
        force: If True, attempt removal even after retries fail
        verbose: If True, print status messages

    Returns:
        0 on success, 1 on failure
    """
    repo_root = repo_root or _get_repo_root()
    max_retries, delay_base = _get_retry_params()

    try:
        from core.pipeline.pipeline_start_lock import get_lock_paths
        lock_file, old_lock = get_lock_paths(scraper_id, repo_root)
    except Exception:
        try:
            from core.config.config_manager import ConfigManager
            lock_file = ConfigManager.get_sessions_dir() / f"{scraper_id}.lock"
            old_lock = repo_root / f".{scraper_id}_run.lock"
        except Exception:
            if verbose:
                print("[CLEANUP] Could not resolve lock paths", file=sys.stderr)
            return 1

    def _remove(p: Path) -> bool:
        if not p.exists():
            return True
        try:
            p.unlink()
            return not p.exists()
        except Exception:
            return False

    def _retry_remove(p: Path) -> bool:
        for attempt in range(max_retries):
            if _remove(p):
                return True
            if attempt < max_retries - 1:
                time.sleep(delay_base * (attempt + 1))
        if force:
            return _remove(p)
        return False

    ok_new = _retry_remove(lock_file)
    ok_old = _retry_remove(old_lock)

    if verbose:
        if ok_new and ok_old:
            print(f"[CLEANUP] Lock files removed for {scraper_id}")
        elif lock_file.exists() or old_lock.exists():
            print(f"[CLEANUP] Failed to remove lock file(s) for {scraper_id}", file=sys.stderr)

    return 0 if (ok_new or not lock_file.exists()) and (ok_old or not old_lock.exists()) else 1
