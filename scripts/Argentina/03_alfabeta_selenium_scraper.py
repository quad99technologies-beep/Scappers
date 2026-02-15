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
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir,
    PREPARED_URLS_FILE,
    SELENIUM_MAX_LOOPS,
    SELENIUM_MAX_RUNS,
    SELENIUM_ROUNDS,
    ROUND_PAUSE_SECONDS,
    TOR_CONTROL_HOST, TOR_CONTROL_PORT, TOR_CONTROL_COOKIE_FILE,
    AUTO_START_TOR_PROXY,
)
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_runner")

SELENIUM_SCRIPT = "03_alfabeta_selenium_worker.py"

# DB setup
_OUTPUT_DIR = get_output_dir()
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


def nk(s: str) -> str:
    import re
    import unicodedata
    if not s:
        return ""
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s.strip()) if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).lower()


def _is_port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _tor_authenticate(sock) -> bool:
    try:
        cookie = Path(TOR_CONTROL_COOKIE_FILE).read_bytes()
        cmd = f"AUTHENTICATE {cookie.hex()}\r\n"
    except Exception:
        cmd = "AUTHENTICATE\r\n"
    try:
        sock.sendall(cmd.encode("utf-8"))
        resp = sock.recv(4096).decode("utf-8", "ignore")
        return resp.startswith("250")
    except Exception:
        return False


def _tor_get_bootstrap_percent() -> int:
    host = TOR_CONTROL_HOST or "127.0.0.1"
    port = int(TOR_CONTROL_PORT or 0)
    if port <= 0:
        return -1
    try:
        with socket.create_connection((host, port), timeout=2) as s:
            s.settimeout(2)
            if not _tor_authenticate(s):
                return -1
            s.sendall(b"GETINFO status/bootstrap-phase\r\n")
            data = s.recv(4096).decode("utf-8", "ignore")
            for part in data.split():
                if part.startswith("PROGRESS="):
                    try:
                        return int(part.split("=", 1)[1])
                    except Exception:
                        return -1
            return -1
    except Exception:
        return -1


def _auto_start_tor_proxy() -> bool:
    """
    Best-effort auto-start for a standalone Tor daemon on 127.0.0.1:9050 (control 9051).
    Reuses Tor Browser's tor.exe if present.
    """
    if not AUTO_START_TOR_PROXY:
        return False

    # Check if already running
    host = TOR_CONTROL_HOST or "127.0.0.1"
    port = int(TOR_CONTROL_PORT or 0)
    if port > 0 and _is_port_open(host, port):
        return True

    home = Path.home()
    tor_exe_candidates = [
        home / "OneDrive" / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
        home / "Desktop" / "Tor Browser" / "Browser" / "TorBrowser" / "Tor" / "tor.exe",
    ]
    tor_exe = next((p for p in tor_exe_candidates if p.exists()), None)
    if not tor_exe:
        log.warning("[TOR_AUTO] tor.exe not found; cannot auto-start Tor proxy")
        return False

    torrc = Path("C:/TorProxy/torrc")
    data_dir = Path("C:/TorProxy/data")
    data_dir.mkdir(parents=True, exist_ok=True)
    torrc.parent.mkdir(parents=True, exist_ok=True)

    desired_torrc = (
        "DataDirectory C:\\TorProxy\\data\n"
        "SocksPort 9050\n"
        "ControlPort 9051\n"
        "CookieAuthentication 1\n"
    )
    try:
        torrc.write_text(desired_torrc, encoding="ascii")
    except Exception as e:
        log.warning(f"[TOR_AUTO] Failed to write torrc: {e}")
        return False

    try:
        log.info(f"[TOR_AUTO] Starting Tor proxy: {tor_exe} -f {torrc}")
        subprocess.Popen(
            [str(tor_exe), "-f", str(torrc)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception as e:
        log.warning(f"[TOR_AUTO] Failed to start Tor: {e}")
        return False

    # Wait for SOCKS port to open (best-effort)
    deadline = time.time() + 90
    while time.time() < deadline:
        if _is_port_open(host, port):
            log.info(f"[TOR_AUTO] Tor proxy is now running on {host}:{port}")
            return True
        time.sleep(1)
    log.warning("[TOR_AUTO] Tor proxy did not come up within 90s")
    return False


def ensure_tor_proxy_running():
    host = TOR_CONTROL_HOST or "127.0.0.1"
    port = int(TOR_CONTROL_PORT or 0)
    if port <= 0:
        return
    if _is_port_open(host, port):
        log.info(f"[TOR] Control port {host}:{port} is already running")
        return

    # Try to auto-start Tor
    if AUTO_START_TOR_PROXY:
        log.warning(f"[TOR] Control port {host}:{port} not reachable; attempting auto-start...")
        if _auto_start_tor_proxy():
            return
    
    log.warning(f"[TOR] Control port {host}:{port} not reachable; start Tor Browser/tor.exe if required")


def _read_state_rows(path: Path) -> tuple[list[dict], dict]:
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
    for encoding in encoding_attempts:
        try:
            with open(path, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                headers = {nk(h): h for h in fieldnames}
                return (list(reader), headers)
        except UnicodeDecodeError:
            continue
    return ([], {})


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
        from scraper_utils import combine_skip_sets, is_product_already_scraped
        
        # Load skip_set (products already in output/progress/ignore)
        skip_set = combine_skip_sets()
        
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
            key = (nk(comp), nk(prod))
            if key in seen_keys:
                debug_info["duplicate_keys"] += 1
                continue
            if key in skip_set:
                debug_info["in_skip_set"] += 1
                continue
            # double-check DB for already scraped
            if is_product_already_scraped(comp, prod):
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
