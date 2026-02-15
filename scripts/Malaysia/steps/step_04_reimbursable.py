#!/usr/bin/env python3
"""
Step 4: Fully Reimbursable Drugs from FUKKM

Thin wrapper that calls FUKKMScraper → reimbursable_drugs table.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_malaysia_dir = Path(__file__).resolve().parents[2]
if str(_malaysia_dir) not in sys.path:
    sys.path.insert(0, str(_malaysia_dir))

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import load_env_file, get_output_dir
load_env_file()


def _get_run_id() -> str:
    run_id = os.environ.get("MALAYSIA_RUN_ID")
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            run_id = run_id_file.read_text(encoding="utf-8").strip()
    if not run_id:
        raise RuntimeError("No MALAYSIA_RUN_ID found. Run Step 0 first.")
    return run_id


def main() -> None:
    from core.db.connection import CountryDB
    from scrapers.fukkm_scraper import FUKKMScraper

    db = CountryDB("Malaysia")
    run_id = _get_run_id()

    from config_loader import getenv, getenv_int, getenv_float

    # Load ALL config values from environment (matching Malaysia.env.json)
    config = {
        "SCRIPT_04_BASE_URL": getenv("SCRIPT_04_BASE_URL", "https://pharmacy.moh.gov.my/ms/apps/fukkm"),
        "SCRIPT_04_REQUEST_TIMEOUT": getenv_int("SCRIPT_04_REQUEST_TIMEOUT", 30),
        "SCRIPT_04_PAGE_DELAY": getenv_float("SCRIPT_04_PAGE_DELAY", 0.04),
        "SCRIPT_04_FAIL_FAST": getenv("SCRIPT_04_FAIL_FAST", "false"),
        "SCRIPT_04_PAGE_MAX_RETRIES": getenv_int("SCRIPT_04_PAGE_MAX_RETRIES", 0),
        "SCRIPT_04_RETRY_BASE_DELAY": getenv_float("SCRIPT_04_RETRY_BASE_DELAY", 2.0),
        "SCRIPT_04_RETRY_MAX_DELAY": getenv_float("SCRIPT_04_RETRY_MAX_DELAY", 60.0),

        # Selectors
        "SCRIPT_04_TABLE_SELECTOR": getenv("SCRIPT_04_TABLE_SELECTOR", "table.views-table.cols-7"),
        "SCRIPT_04_HEADER_SELECTOR": getenv("SCRIPT_04_HEADER_SELECTOR", "thead th"),
        "SCRIPT_04_FIRST_ROW_TH_SELECTOR": getenv("SCRIPT_04_FIRST_ROW_TH_SELECTOR", "tr th"),
        "SCRIPT_04_TBODY_ROW_SELECTOR": getenv("SCRIPT_04_TBODY_ROW_SELECTOR", "tbody tr"),
        "SCRIPT_04_TR_SELECTOR": getenv("SCRIPT_04_TR_SELECTOR", "tr"),

        # HTTP headers
        "SCRIPT_04_USER_AGENT": getenv("SCRIPT_04_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        "SCRIPT_04_ACCEPT_HEADER": getenv("SCRIPT_04_ACCEPT_HEADER",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        "SCRIPT_04_ACCEPT_LANGUAGE": getenv("SCRIPT_04_ACCEPT_LANGUAGE", "en-US,en;q=0.9"),
    }

    scraper = FUKKMScraper(run_id=run_id, db=db, config=config)
    count = scraper.run()
    print(f"\n[DONE] Step 4 complete — {count:,} reimbursable drugs in DB", flush=True)


if __name__ == "__main__":
    from core.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(
        main, "Malaysia", 4, "Get Fully Reimbursable",
    )
