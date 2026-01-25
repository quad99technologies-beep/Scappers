#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium Runner (Simple Loop-Count Mode)

Uses Productlist_with_urls.csv as the only state file with columns:
  Product, Company, URL, Loop Count, Total Records

Behavior:
- Runs Selenium worker repeatedly until either:
  * no eligible rows remain, or
  * Loop Count reaches SELENIUM_MAX_RUNS for all remaining 0-record rows
- After Selenium runs complete, generates an API input CSV containing rows where:
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("selenium_runner")

SELENIUM_SCRIPT = "03_alfabeta_selenium_worker.py"


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
    if not prepared_urls_path.exists():
        return 0
    rows, headers = _read_state_rows(prepared_urls_path)
    if not rows or not headers:
        return 0

    pcol = headers.get(nk("Product")) or "Product"
    ccol = headers.get(nk("Company")) or "Company"
    ucol = headers.get(nk("URL")) or "URL"
    loop_col = headers.get(nk("Loop Count")) or headers.get(nk("Loop_Count")) or "Loop Count"
    total_col = headers.get(nk("Total Records")) or headers.get(nk("Total_Records")) or "Total Records"

    eligible = 0
    for r in rows:
        prod = (r.get(pcol) or "").strip()
        comp = (r.get(ccol) or "").strip()
        url = (r.get(ucol) or "").strip()
        try:
            loop_count = int(float((r.get(loop_col) or "0").strip() or "0"))
        except Exception:
            loop_count = 0
        try:
            total_records = int(float((r.get(total_col) or "0").strip() or "0"))
        except Exception:
            total_records = 0

        if prod and comp and url and total_records == 0 and loop_count < int(SELENIUM_MAX_RUNS):
            eligible += 1
    return eligible


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
    rows, headers = _read_state_rows(prepared_urls_path)
    pcol = headers.get(nk("Product")) or "Product"
    ccol = headers.get(nk("Company")) or "Company"
    ucol = headers.get(nk("URL")) or "URL"
    loop_col = headers.get(nk("Loop Count")) or headers.get(nk("Loop_Count")) or "Loop Count"
    total_col = headers.get(nk("Total Records")) or headers.get(nk("Total_Records")) or "Total Records"

    api_rows = []
    seen = set()
    for r in rows:
        prod = (r.get(pcol) or "").strip()
        comp = (r.get(ccol) or "").strip()
        url = (r.get(ucol) or "").strip()
        try:
            loop_count = int(float((r.get(loop_col) or "0").strip() or "0"))
        except Exception:
            loop_count = 0
        try:
            total_records = int(float((r.get(total_col) or "0").strip() or "0"))
        except Exception:
            total_records = 0
        if prod and comp and url and total_records == 0 and loop_count >= int(SELENIUM_MAX_RUNS):
            key = (nk(comp), nk(prod))
            if key in seen:
                continue
            seen.add(key)
            api_rows.append({"Product": prod, "Company": comp, "URL": url})

    api_input_path.parent.mkdir(parents=True, exist_ok=True)
    with open(api_input_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["Product", "Company", "URL"])
        w.writeheader()
        w.writerows(api_rows)

    log.info(f"[API_INPUT] Wrote {len(api_rows)} rows to: {api_input_path}")


def main() -> int:
    ensure_tor_proxy_running()

    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE
    api_input_path = output_dir / API_INPUT_CSV

    if not prepared_urls_path.exists():
        log.error(f"Prepared URLs file not found: {prepared_urls_path}")
        return 1

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
    print(f"\n{'='*80}")
    print("[SUCCESS] Selenium loop-count scraping completed")
    print(f"[SUCCESS] API input file: {api_input_path}")
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
                output_files=[
                    str(get_output_dir() / PREPARED_URLS_FILE),
                    str(get_output_dir() / API_INPUT_CSV),
                ],
            )
        except Exception as exc:
            log.warning(f"[CHECKPOINT] Failed to mark selenium step: {exc}")
    raise SystemExit(exit_code)
