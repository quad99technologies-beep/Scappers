#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium Runner (DB-only, loop-count mode)

Behavior:
- Runs Selenium worker repeatedly until either:
  * no eligible rows remain, or
  * Loop Count reaches SELENIUM_MAX_RUNS for all remaining 0-record rows
- API input is now DB-only (no CSV written); rows are selected by:
  Total Records == 0 and Loop Count >= SELENIUM_MAX_RUNS
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
    SELENIUM_MAX_RUNS,
    API_INPUT_CSV,
    ROUND_PAUSE_SECONDS,
    TOR_CONTROL_HOST, TOR_CONTROL_PORT, TOR_CONTROL_COOKIE_FILE,
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


def ensure_tor_proxy_running():
    host = TOR_CONTROL_HOST or "127.0.0.1"
    port = int(TOR_CONTROL_PORT or 0)
    if port <= 0:
        return
    if _is_port_open(host, port):
        return

    # We don't auto-start Tor here; this runner just logs a warning and continues.
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


def _count_eligible(prepared_urls_path: Path) -> int:
    """DB-backed eligible count (ignores CSV path)."""
    try:
        with _DB.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                  FROM ar_product_index
                 WHERE run_id = %s
                   AND COALESCE(total_records,0) = 0
                   AND COALESCE(loop_count,0) < %s
                   AND url IS NOT NULL AND url <> ''
                """,
                (_RUN_ID, int(SELENIUM_MAX_RUNS)),
            )
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]
    except Exception:
        return 0


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


def write_api_input(prepared_urls_path: Path, api_input_path: Path):
    """DB-only: log count of API-eligible rows instead of writing CSV."""
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
                (_RUN_ID, int(SELENIUM_MAX_RUNS)),
            )
            row = cur.fetchone()
            count = row[0] if isinstance(row, tuple) else row["count"]
        log.info(f"[API_INPUT] DB-only mode: {count} rows eligible for API fallback")
    except Exception as e:
        log.warning(f"[API_INPUT] Failed to count API-eligible rows: {e}")


def log_db_counts():
    """Log DB count summary for cross-checking."""
    try:
        with _DB.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (_RUN_ID,))
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND COALESCE(total_records,0) > 0",
                (_RUN_ID,),
            )
            scraped = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (_RUN_ID,))
            products = cur.fetchone()[0]
        log.info("[COUNT] product_index=%s scraped_with_records=%s products_rows=%s", total, scraped, products)
        print(f"[COUNT] product_index={total} scraped_with_records={scraped} products_rows={products}", flush=True)
    except Exception as e:
        log.warning(f"[COUNT] Failed to load DB counts: {e}")


def main() -> int:
    ensure_tor_proxy_running()

    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE
    api_input_path = output_dir / API_INPUT_CSV

    max_runs = int(SELENIUM_MAX_RUNS)
    print("ARGENTINA SELENIUM SCRAPER - SIMPLE LOOP-COUNT MODE")
    print(f"[CONFIG]   Max runs per product: {max_runs}")

    for pass_num in range(1, max_runs + 1):
        eligible = _count_eligible(prepared_urls_path)
        print(f"\n{'='*80}")
        print(f"{_pipeline_prefix()}SELENIUM SCRAPING - PASS {pass_num} OF {max_runs}")
        print(f"{'='*80}")
        print(f"[PASS {pass_num}] Eligible products: {eligible:,}")

        if eligible == 0:
            log.info(f"[PASS {pass_num}] No eligible products; stopping early")
            break

        ok = run_selenium_pass()
        if not ok:
            log.error(f"[PASS {pass_num}] Selenium worker failed")
            return 1

        if pass_num < max_runs and ROUND_PAUSE_SECONDS and int(ROUND_PAUSE_SECONDS) > 0:
            pause = int(ROUND_PAUSE_SECONDS)
            log.info(f"[PAUSE] Waiting {pause}s before next pass")
            time.sleep(pause)

    write_api_input(prepared_urls_path, api_input_path)
    log_db_counts()
    print(f"\n{'='*80}")
    print("[SUCCESS] Selenium loop-count scraping completed")
    print(f"[SUCCESS] API input: DB-only (no CSV written)")
    print(f"{'='*80}\n")
    return 0


if __name__ == "__main__":
    exit_code = main()
    if exit_code == 0:
        try:
            from core.pipeline_checkpoint import get_checkpoint_manager
            cp = get_checkpoint_manager("Argentina")
            cp.mark_step_complete(
                3,
                "Scrape Products (Selenium)",
                output_files=None,
            )
        except Exception as exc:
            log.warning(f"[CHECKPOINT] Failed to mark selenium step: {exc}")
    raise SystemExit(exit_code)
