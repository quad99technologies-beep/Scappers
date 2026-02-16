#!/usr/bin/env python3
"""
Step 2: Product Details from Quest3Plus

Thin wrapper that calls Quest3Scraper → product_details table.
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

# Ensure Malaysia directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']

# Re-insert Malaysia directory at the front
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

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
    # Fix import conflict: ensure local 'db' package is prioritized over core/db
    import sys
    from pathlib import Path
    
    # 1. Remove 'core' directory from path to avoid shadowing
    # 2. Force re-import of Malaysia directory (scripts/Malaysia) at the front
    _step_parent = Path(__file__).resolve().parent # steps/
    malaysia_dir = str(_step_parent.parent) # scripts/Malaysia
    
    sys.path = [p for p in sys.path if not Path(p).name == 'core']
    
    if malaysia_dir in sys.path:
        sys.path.remove(malaysia_dir)
    sys.path.insert(0, malaysia_dir)

    # 3. Clear cached wrong import
    if 'db' in sys.modules:
        del sys.modules['db']

    from core.db.connection import CountryDB
    from scrapers.quest3_scraper import Quest3Scraper

    output_dir = get_output_dir()
    db = CountryDB("Malaysia")
    run_id = _get_run_id()

    from config_loader import getenv, getenv_int, getenv_float, get_input_dir

    # Load ALL required config values from environment
    config = {
        # URLs
        "SCRIPT_02_SEARCH_URL": getenv("SCRIPT_02_SEARCH_URL", "https://quest3plus.bpfk.gov.my/pmo2/index.php"),
        "SCRIPT_02_DETAIL_URL": getenv("SCRIPT_02_DETAIL_URL", "https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={}"),
        "SCRIPT_02_HEADLESS": getenv("SCRIPT_02_HEADLESS", "false"),

        # Timeouts
        "SCRIPT_02_PAGE_TIMEOUT": getenv_int("SCRIPT_02_PAGE_TIMEOUT", 60000),
        "SCRIPT_02_SELECTOR_TIMEOUT": getenv_int("SCRIPT_02_SELECTOR_TIMEOUT", 30000),
        "SCRIPT_02_DATA_LOAD_WAIT": getenv_float("SCRIPT_02_DATA_LOAD_WAIT", 3),
        "SCRIPT_02_CSV_WAIT_SECONDS": getenv_float("SCRIPT_02_CSV_WAIT_SECONDS", 60),
        "SCRIPT_02_CSV_WAIT_MAX_SECONDS": getenv_float("SCRIPT_02_CSV_WAIT_MAX_SECONDS", 300),

        # Delays
        "SCRIPT_02_SEARCH_DELAY": getenv_float("SCRIPT_02_SEARCH_DELAY", 5),
        "SCRIPT_02_INDIVIDUAL_DELAY": getenv_float("SCRIPT_02_INDIVIDUAL_DELAY", 3),

        # Selectors
        "SCRIPT_02_SEARCH_BY_SELECTOR": getenv("SCRIPT_02_SEARCH_BY_SELECTOR", "#searchBy"),
        "SCRIPT_02_SEARCH_TXT_SELECTOR": getenv("SCRIPT_02_SEARCH_TXT_SELECTOR", "#searchTxt"),
        "SCRIPT_02_SEARCH_BUTTON_SELECTOR": getenv("SCRIPT_02_SEARCH_BUTTON_SELECTOR", "button.btn-primary"),
        "SCRIPT_02_RESULT_TABLE_SELECTOR": getenv("SCRIPT_02_RESULT_TABLE_SELECTOR", "table.table"),
        "SCRIPT_02_CSV_BUTTON_SELECTOR": getenv("SCRIPT_02_CSV_BUTTON_SELECTOR", "button.buttons-csv"),
        "SCRIPT_02_DETAIL_TABLE_SELECTOR": getenv("SCRIPT_02_DETAIL_TABLE_SELECTOR", "table.table tr"),

        # Labels for parsing
        "SCRIPT_02_PRODUCT_NAME_LABEL": getenv("SCRIPT_02_PRODUCT_NAME_LABEL", "product name :"),
        "SCRIPT_02_HOLDER_LABEL": getenv("SCRIPT_02_HOLDER_LABEL", "holder :"),
        "SCRIPT_02_HOLDER_ADDRESS_LABEL": getenv("SCRIPT_02_HOLDER_ADDRESS_LABEL", "holder address"),
        "SCRIPT_02_REGISTRATION_COLUMN": getenv("SCRIPT_02_REGISTRATION_COLUMN", "Registration No / Notification No"),
    }

    # DB-FIRST: No CSV input - Quest3Scraper will query my_products table
    scraper = Quest3Scraper(
        run_id=run_id,
        db=db,
        config=config,
        input_products_path=None,  # Not used - queries DB instead
        output_dir=output_dir,
    )
    count = scraper.run()
    print(f"\n[DONE] Step 2 complete — {count:,} product details in DB", flush=True)


if __name__ == "__main__":
    from core.pipeline.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(
        main, "Malaysia", 2, "Product Details",
    )
