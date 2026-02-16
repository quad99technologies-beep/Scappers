#!/usr/bin/env python3
"""Cleanup lock file for Canada Quebec scraper."""
import sys
import argparse
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.pipeline.cleanup_lock import run_cleanup

SCRAPER_ID = "CanadaQuebec"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Remove lock even if in use")
    args = parser.parse_args()
    force = args.force
    try:
        from config_loader import getenv_bool
        force = force or getenv_bool("LOCK_FORCE", False)
    except Exception:
        pass
    sys.exit(run_cleanup(SCRAPER_ID, _repo_root, force=force, verbose=True))
