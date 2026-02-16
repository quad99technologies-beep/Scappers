#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium Runner (DB-only, LOOP vs RETRY; see doc/Argentina/LOOP_VS_RETRY.md)

Behavior:
- LOOP = full pass over queue. After each loop only products with total_records=0 are re-checked.
- Runs Selenium worker for SELENIUM_MAX_LOOPS loops (full passes) until either:
  * no eligible rows remain (all have total_records > 0 or loop_count >= max), or
  * all loops completed.
- API input is DB-only: rows with total_records=0 and loop_count >= SELENIUM_MAX_LOOPS go to Step 4.
- RETRY = per-attempt retries (e.g. on timeout); see MAX_RETRIES_TIMEOUT in config.
"""

import csv
import logging
import os
import subprocess
import sys
import time
import socket
from pathlib import Path

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent

# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from core.config.config_manager import ConfigManager, get_env_bool, get_env_int
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id
from core.utils.text_utils import nk
from core.network.tor_manager import ensure_tor_proxy_running

# Load Config
PREPARED_URLS_FILE = ConfigManager.get_env_value("Argentina", "PREPARED_URLS_FILE", "prepared_urls.csv")
SELENIUM_MAX_LOOPS = get_env_int("Argentina", "SELENIUM_MAX_LOOPS", 3)
SELENIUM_MAX_RUNS = get_env_int("Argentina", "SELENIUM_MAX_RUNS", 1)
SELENIUM_ROUNDS = get_env_int("Argentina", "SELENIUM_ROUNDS", 1)
ROUND_PAUSE_SECONDS = get_env_int("Argentina", "ROUND_PAUSE_SECONDS", 0)
TOR_CONTROL_HOST = ConfigManager.get_env_value("Argentina", "TOR_CONTROL_HOST", "127.0.0.1")
TOR_CONTROL_PORT = get_env_int("Argentina", "TOR_CONTROL_PORT", 9051)
TOR_CONTROL_COOKIE_FILE = ConfigManager.get_env_value("Argentina", "TOR_CONTROL_COOKIE_FILE", "")
AUTO_START_TOR_PROXY = get_env_bool("Argentina", "AUTO_START_TOR_PROXY", True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_runner")

SELENIUM_SCRIPT = "03_alfabeta_selenium_worker.py"

# DB setup
_OUTPUT_DIR = ConfigManager.get_output_dir("Argentina")
_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = _OUTPUT_DIR / ".current_run_id"

def _get_run_id() -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)


def _pipeline_prefix() -> str:
    step = os.environ.get("PIPELINE_STEP_DISPLAY", "").strip()
    total = os.environ.get("PIPELINE_TOTAL_STEPS", "").strip()
    if step and total:
        return f"PIPELINE STEP {step}/{total} - "
    return ""


def _count_eligible(prepared_urls_path: Path, debug: bool = False) -> tuple[int, dict]:
    """DB-backed eligible count matching worker eligibility logic.
    
    Returns:
        (eligible_count, debug_info) where debug_info contains breakdown of why products were filtered
    """
    debug_info = {
        "pending_from_db": 0,
        "missing_fields": 0,
        "duplicate_keys": 0,
        "in_skip_set": 0,
        "already_scraped": 0,
        "eligible": 0
    }
    try:
        # Match the exact query used by get_pending_products() in repositories.py
        # Then apply the same filters used in the worker (skip_set, is_product_already_scraped)
        # Load skip_set (products already in output/progress/ignore)
        # Uses enhanced repository method
        skip_set = _REPO.combine_skip_sets()
        
        # Get pending products using same query as worker
        pending_rows = _REPO.get_pending_products(max_loop=int(SELENIUM_MAX_LOOPS), limit=200000)
        debug_info["pending_from_db"] = len(pending_rows)
        
        # Apply same filters as worker
        eligible_count = 0
        seen_keys = set()
        for row in pending_rows:
            prod = (row.get("product") or "").strip()
            comp = (row.get("company") or "").strip()
            url = (row.get("url") or "").strip()
            if not (prod and comp and url):
                debug_info["missing_fields"] += 1
                continue
            
            # Use core nk (imported globally in previous step)
            key = (nk(comp), nk(prod))
            if key in seen_keys:
                debug_info["duplicate_keys"] += 1
                continue
            if key in skip_set:
                debug_info["in_skip_set"] += 1
                continue
            # double-check DB for already scraped
            if _REPO.is_product_already_scraped(comp, prod):
                debug_info["already_scraped"] += 1
                continue
            seen_keys.add(key)
            eligible_count += 1
            debug_info["eligible"] = eligible_count
        
        if debug:
            log.info(f"[COUNT_ELIGIBLE] Breakdown: pending={debug_info['pending_from_db']}, "
                    f"missing_fields={debug_info['missing_fields']}, "
                    f"duplicates={debug_info['duplicate_keys']}, "
                    f"in_skip_set={debug_info['in_skip_set']}, "
                    f"already_scraped={debug_info['already_scraped']}, "
                    f"eligible={debug_info['eligible']}")
        
        return eligible_count, debug_info
    except Exception as e:
        log.warning(f"[COUNT_ELIGIBLE] Error counting eligible products: {e}")
        return 0, debug_info


def run_selenium_pass(max_rows: int = 0) -> bool:
    script_path = _script_dir / SELENIUM_SCRIPT
    if not script_path.exists():
        log.error(f"Selenium worker not found: {script_path}")
        return False

    cmd = [sys.executable, "-u", str(script_path)]
    if max_rows and max_rows > 0:
        cmd += ["--max-rows", str(max_rows)]

    env = os.environ.copy()
    return subprocess.run(cmd, check=False, env=env).returncode == 0


def write_api_input():
    """DB-only: log count of API-eligible rows (no CSV written)."""
    try:
        with _DB.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                  FROM ar_product_index
                 WHERE run_id = %s
                   AND COALESCE(total_records,0) = 0
                   AND COALESCE(loop_count,0) >= %s
                """,
                (_RUN_ID, int(SELENIUM_MAX_LOOPS)),
            )
            row = cur.fetchone()
            count = row[0] if isinstance(row, tuple) else row["count"]
        log.info(f"[API_INPUT] DB-only mode: {count} rows eligible for API fallback")
    except Exception as e:
        log.warning(f"[API_INPUT] Failed to count API-eligible rows: {e}")


def log_db_counts():
    """Log DB count summary for cross-checking (single round-trip)."""
    try:
        with _DB.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s),
                    (SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND COALESCE(total_records,0) > 0),
                    (SELECT COUNT(*) FROM ar_products WHERE run_id = %s)
                """,
                (_RUN_ID, _RUN_ID, _RUN_ID),
            )
            total, scraped, products = cur.fetchone()
        log.info("[COUNT] product_index=%s scraped_with_records=%s products_rows=%s", total, scraped, products)
        print(f"[COUNT] product_index={total} scraped_with_records={scraped} products_rows={products}", flush=True)
    except Exception as e:
        log.warning(f"[COUNT] Failed to load DB counts: {e}")


def reset_failed_products_for_retry():
    """Reset failed products to 'pending' status so they can be retried in step 3."""
    try:
        with _DB.cursor() as cur:
            cur.execute("""
                UPDATE ar_product_index
                SET status = 'pending',
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s
                  AND total_records = 0
                  AND status = 'failed'
                  AND loop_count < %s
            """, (_RUN_ID, int(SELENIUM_MAX_LOOPS)))
            reset_count = cur.rowcount
        # Always commit the transaction (even if 0 rows updated)
        _DB.commit()
        if reset_count > 0:
            log.info(f"[RESET] Reset {reset_count} failed products to 'pending' status for retry")
        return reset_count
    except Exception as e:
        log.warning(f"[RESET] Failed to reset failed products: {e}")
        try:
            _DB.rollback()  # Rollback on error
        except Exception:
            pass
        return 0


def main() -> int:
    ensure_tor_proxy_running()

    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE

    max_loops = int(SELENIUM_MAX_LOOPS)
    print("ARGENTINA SELENIUM SCRAPER - LOOP vs RETRY (see doc/Argentina/LOOP_VS_RETRY.md)")
    print(f"[CONFIG]   Max loops (full passes; after each pass only total_records=0 re-checked): {max_loops}")
    # Diagnostic: show config source
    log.info(f"[CONFIG] SELENIUM_MAX_LOOPS={max_loops}, SELENIUM_ROUNDS={SELENIUM_ROUNDS}, SELENIUM_MAX_RUNS={SELENIUM_MAX_RUNS}")
    if max_loops != 8 and SELENIUM_ROUNDS == 8:
        log.warning(f"[CONFIG] SELENIUM_ROUNDS is 8 but SELENIUM_MAX_LOOPS is {max_loops}. Check config loading.")
    
    # Reset failed products to 'pending' so they can be retried
    reset_failed_products_for_retry()

    for pass_num in range(1, max_loops + 1):
        eligible, debug_info = _count_eligible(prepared_urls_path, debug=(pass_num == 1))
        print(f"\n{'='*80}")
        print(f"{_pipeline_prefix()}SELENIUM SCRAPING - LOOP {pass_num} OF {max_loops} (only total_records=0 re-checked)")
        print(f"{'='*80}")
        print(f"[PASS {pass_num}] Eligible products: {eligible:,}")
        
        if eligible == 0:
            # Log detailed breakdown on early exit
            log.info(f"[PASS {pass_num}] No eligible products")
            log.info(f"[COUNT_ELIGIBLE] Breakdown: pending={debug_info['pending_from_db']}, "
                    f"missing_fields={debug_info['missing_fields']}, "
                    f"duplicates={debug_info['duplicate_keys']}, "
                    f"in_skip_set={debug_info['in_skip_set']}, "
                    f"already_scraped={debug_info['already_scraped']}, "
                    f"eligible={debug_info['eligible']}")
            # Only exit early if we've run at least 1 loop (to ensure we try at least once)
            if pass_num > 1:
                log.info(f"[PASS {pass_num}] Early exit: No eligible products after {pass_num-1} completed loop(s)")
                break
            else:
                log.warning(f"[PASS {pass_num}] No eligible products on first loop, but continuing to check config...")
                # Still run the worker once to ensure it also sees 0 eligible and logs appropriately

        ok = run_selenium_pass()
        if not ok:
            log.error(f"[PASS {pass_num}] Selenium worker failed")
            return 1

        if pass_num < max_loops and ROUND_PAUSE_SECONDS and int(ROUND_PAUSE_SECONDS) > 0:
            pause = int(ROUND_PAUSE_SECONDS)
            log.info(f"[PAUSE] Waiting {pause}s before next pass")
            time.sleep(pause)

    write_api_input()
    log_db_counts()
    print(f"\n{'='*80}")
    print("[SUCCESS] Selenium loop-count scraping completed")
    print(f"[SUCCESS] API input: DB-only (no CSV written)")
    print(f"{'='*80}\n")
    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        # Only mark step complete if there are no more eligible products to scrape
        # Check if there are still products that need selenium scraping
        try:
            eligible, debug_info = _count_eligible(_OUTPUT_DIR / PREPARED_URLS_FILE)
            if eligible > 0:
                log.warning(f"[CHECKPOINT] Step 3 completed but {eligible} products still eligible for Selenium scraping. "
                           f"Not marking step complete. Breakdown: pending={debug_info['pending_from_db']}, "
                           f"in_skip_set={debug_info['in_skip_set']}, already_scraped={debug_info['already_scraped']}, "
                           f"eligible={debug_info['eligible']}")
                # Don't mark complete - there's still work to do
            else:
                # All eligible products processed - safe to mark complete
                try:
                    from core.pipeline.pipeline_checkpoint import get_checkpoint_manager
                    cp = get_checkpoint_manager("Argentina")
                    cp.mark_step_complete(
                        3,
                        "Scrape Products (Selenium)",
                        output_files=None,
                    )
                    log.info(f"[CHECKPOINT] Step 3 marked complete - no eligible products remaining")
                except Exception as exc:
                    log.warning(f"[CHECKPOINT] Failed to mark selenium step: {exc}")
        except Exception as e:
            log.warning(f"[CHECKPOINT] Failed to validate step 3 completion: {e}")
            # On validation error, don't mark complete to be safe
    raise SystemExit(exit_code)
