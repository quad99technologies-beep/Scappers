#!/usr/bin/env python3
"""
Step 1: Product Registration Numbers from MyPriMe

Thin wrapper that calls MyPriMeScraper → products table.
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_malaysia_dir = Path(__file__).resolve().parents[1]  # scripts/Malaysia (Fixed: was parents[2])
if str(_malaysia_dir) not in sys.path:
    sys.path.insert(0, str(_malaysia_dir))

_script_dir = Path(__file__).resolve().parents[0] # scripts/Malaysia/steps (Fixed: was parents[1])
if str(_script_dir) not in sys.path:
    # Insert at 0 again to be sure
    sys.path.insert(0, str(_script_dir))

# Ensure no other 'db' package conflicts (e.g. from core)
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if 'db' in sys.modules:
    del sys.modules['db']

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import load_env_file, get_output_dir
load_env_file()



def _get_run_id() -> str:
    import sys
    from pathlib import Path
    
    # Remove any path ending in 'core' to avoid conflicts
    sys.path = [p for p in sys.path if not Path(p).name == 'core']
    
    # Ensure local db is prioritized
    # This logic mirrors what is used in Step 2 for consistency
    _step_parent = Path(__file__).resolve().parent # steps/
    malaysia_dir = str(_step_parent.parent) # scripts/Malaysia
    
    if malaysia_dir in sys.path:
        sys.path.remove(malaysia_dir)
    sys.path.insert(0, malaysia_dir)
    
    # Clear potentially wrong cached modules
    if 'db' in sys.modules and 'db.repositories' not in sys.modules:
        del sys.modules['db']

    run_id = os.environ.get("MALAYSIA_RUN_ID")
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            run_id = run_id_file.read_text(encoding="utf-8").strip()
    if not run_id:
        raise RuntimeError("No MALAYSIA_RUN_ID found. Run Step 0 first.")
    return run_id


def main() -> None:
    # Path setup handled by bootstrap

    from core.db.connection import CountryDB
    from scrapers.myprime_scraper import MyPriMeScraper

    db = CountryDB("Malaysia")
    run_id = _get_run_id()

    # Load ALL config values from environment (matching Malaysia.env.json)
    from config_loader import getenv, getenv_int, getenv_float

    config = {
        # URL
        "SCRIPT_01_URL": getenv("SCRIPT_01_URL", "https://pharmacy.moh.gov.my/ms/apps/drug-price"),
        "SCRIPT_01_HEADLESS": getenv("SCRIPT_01_HEADLESS", "false"),

        # Timeouts and delays
        "SCRIPT_01_WAIT_TIMEOUT": getenv_int("SCRIPT_01_WAIT_TIMEOUT", 20),
        "SCRIPT_01_CLICK_DELAY": getenv_int("SCRIPT_01_CLICK_DELAY", 2),

        # Selectors
        "SCRIPT_01_TABLE_SELECTOR": getenv("SCRIPT_01_TABLE_SELECTOR", "table.tinytable"),
        "SCRIPT_01_HEADER_SELECTOR": getenv("SCRIPT_01_HEADER_SELECTOR", "thead th"),
        "SCRIPT_01_ROW_SELECTOR": getenv("SCRIPT_01_ROW_SELECTOR", "tbody tr"),
        "SCRIPT_01_CELL_SELECTOR": getenv("SCRIPT_01_CELL_SELECTOR", "td"),
        "SCRIPT_01_VIEW_ALL_XPATH": getenv("SCRIPT_01_VIEW_ALL_XPATH",
            "//a[@href='javascript:sorter.showall()' or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view all') or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'lihat semua')]"),
    }

    scraper = MyPriMeScraper(run_id=run_id, db=db, config=config)
    count = scraper.run()
    print(f"\n[DONE] Step 1 complete — {count:,} products in DB", flush=True)


if __name__ == "__main__":
    from core.pipeline.standalone_checkpoint import run_with_checkpoint
    run_with_checkpoint(
        main, "Malaysia", 1, "Product Registration Number",
    )
