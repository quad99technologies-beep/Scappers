#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Cleanup lock file for Belarus scraper."""
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.pipeline.cleanup_lock import run_cleanup

SCRAPER_ID = "Belarus"

if __name__ == "__main__":
    sys.exit(run_cleanup(SCRAPER_ID, _repo_root, verbose=True))
