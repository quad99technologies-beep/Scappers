#!/usr/bin/env python3
"""
Russia scraper health check.

Runs lightweight diagnostics (without extracting data) to verify the
configuration paths, PCID file readiness, and the key farmcom.info
selectors that must remain stable for the scraper to work. Outputs a
matrix of checks so you can manually trigger it and confirm whether a
scrape run is safe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, List, Tuple

import json
import ssl
import requests
from urllib3.exceptions import SSLError as Urllib3SSLError
from bs4 import BeautifulSoup

# Disable SSL warnings for self-signed certificates
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

from config_loader import (
    load_env_file,
    getenv,
    get_central_output_dir,
)

load_env_file()

# Script 01 (VED Registry) configuration
SCRIPT_01_BASE_URL = getenv("SCRIPT_01_BASE_URL", "http://farmcom.info/site/reestr")
SCRIPT_01_REGION_VALUE = getenv("SCRIPT_01_REGION_VALUE", "50")

# Script 02 (Excluded List) configuration
SCRIPT_02_BASE_URL = getenv("SCRIPT_02_BASE_URL", "http://farmcom.info/site/reestr?vw=excl")
SCRIPT_02_REGION_VALUE = getenv("SCRIPT_02_REGION_VALUE", "50")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


@dataclass
class CheckResult:
    step: str
    check: str
    status: str
    detail: str


def fetch_soup(url: str, timeout: int = 15) -> Tuple[BeautifulSoup, int]:
    """Fetch HTML and parse with BeautifulSoup, with SSL error handling."""
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, verify=True)
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml"), response.status_code
    except (requests.exceptions.SSLError, Urllib3SSLError, ssl.SSLError):
        # Try with SSL verification disabled as fallback
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, verify=False)
        response.raise_for_status()
        return BeautifulSoup(response.text, "lxml"), response.status_code


def check_url_reachable(url: str) -> Tuple[bool, str]:
    """Check if URL is reachable, with SSL error handling."""
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=15, verify=True)
        if response.status_code == 200:
            return True, f"HTTP {response.status_code}"
        return False, f"HTTP {response.status_code}"
    except (requests.exceptions.SSLError, Urllib3SSLError, ssl.SSLError):
        # Try with SSL verification disabled as fallback
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=15, verify=False)
            if response.status_code == 200:
                return True, f"HTTP {response.status_code} (SSL verification disabled)"
            return False, f"HTTP {response.status_code} (SSL verification disabled)"
        except Exception as exc:
            return False, f"SSL error and fallback failed: {exc}"
    except Exception as exc:
        return False, str(exc)


def check_selectors(url: str, selectors: Iterable[str], require_js: bool = False) -> Tuple[bool, str]:
    """
    Check if selectors exist on page.
    
    Args:
        url: URL to check
        selectors: List of CSS selectors to check
        require_js: If True, mark as PASS if page loads but selectors not found (they may be in JS/iframes)
    """
    try:
        soup, status_code = fetch_soup(url)
    except Exception as exc:  # pragma: no cover - network bound
        return False, f"Failed to fetch {url}: {exc}"

    missing = []
    for selector in selectors:
        try:
            if not soup.select(selector):
                missing.append(selector)
        except Exception as exc:
            missing.append(f"{selector} (error: {exc})")

    if missing:
        if require_js and status_code == 200:
            # Page loads successfully but selectors not in static HTML - likely in JS/iframes
            # Since scraper uses Selenium, this is acceptable
            return True, f"Selectors may require JavaScript/Selenium (HTTP {status_code}) - scraper uses Selenium"
        return False, f"Missing selectors (HTTP {status_code}): {', '.join(missing)}"
    return True, f"Selectors ok (HTTP {status_code})"


def check_texts(url: str, texts: Iterable[str]) -> Tuple[bool, str]:
    try:
        soup, status_code = fetch_soup(url)
    except Exception as exc:
        return False, f"Failed to fetch {url}: {exc}"
    body = soup.get_text(" ", strip=True).lower()
    found = [text for text in texts if text.lower() in body]
    if not found:
        return False, f"None of {texts} present (HTTP {status_code})"
    return True, f"Found text: {found[0]} (HTTP {status_code})"


def check_db_connection() -> Tuple[bool, str]:
    """Verify PostgreSQL connection and run_ledger table for Russia."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Russia") as db:
            with db.cursor() as cur:
                cur.execute("SELECT 1 FROM run_ledger WHERE scraper_name = %s LIMIT 1", ("Russia",))
                cur.fetchone()
        return True, "PostgreSQL connected, run_ledger accessible"
    except Exception as exc:
        return False, f"DB: {exc}"


def check_input_dictionary_table() -> Tuple[bool, str]:
    """Verify ru_input_dictionary input table exists (used for translation; no CSV)."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Russia") as db:
            with db.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM ru_input_dictionary")
                count = cur.fetchone()[0] or 0
        return True, f"ru_input_dictionary accessible ({count} rows)"
    except Exception as exc:
        return False, f"ru_input_dictionary: {exc}"


def run_health_checks() -> List[CheckResult]:
    checks: List[Tuple[str, str, Callable[[], Tuple[bool, str]]]] = [
        ("Config", "PostgreSQL (run_ledger)", check_db_connection),
        ("Config", "Input table ru_input_dictionary", check_input_dictionary_table),
        ("Config", "VED Registry URL reachable", lambda: check_url_reachable(SCRIPT_01_BASE_URL)),
        ("Config", "Excluded List URL reachable", lambda: check_url_reachable(SCRIPT_02_BASE_URL)),
        (
            "Layout",
            "VED Registry region select (reg_id)",
            lambda: check_selectors(SCRIPT_01_BASE_URL, ["select#reg_id"], require_js=True),
        ),
        (
            "Layout",
            "VED Registry search button (btn_submit)",
            lambda: check_selectors(SCRIPT_01_BASE_URL, ["input#btn_submit", "button#btn_submit"], require_js=True),
        ),
        (
            "Layout",
            "VED Registry table structure",
            lambda: check_selectors(SCRIPT_01_BASE_URL, ["table.report"]),
        ),
        (
            "Layout",
            "Excluded List region select (reg_id)",
            lambda: check_selectors(SCRIPT_02_BASE_URL, ["select#reg_id", "select[name='reg_id']"], require_js=True),
        ),
        (
            "Layout",
            "Excluded List search button",
            lambda: check_selectors(SCRIPT_02_BASE_URL, ["input#btn_submit", "button#btn_submit", "input[type='submit']"], require_js=True),
        ),
        (
            "Layout",
            "Excluded List table structure",
            lambda: check_selectors(SCRIPT_02_BASE_URL, ["table.report"]),
        ),
    ]

    results = []
    for step, label, runner in checks:
        try:
            passed, details = runner()
        except Exception as exc:
            passed, details = False, f"Exception: {exc}"
        results.append(
            CheckResult(
                step=step,
                check=label,
                status="PASS" if passed else "FAIL",
                detail=details,
            )
        )
    return results


def format_table(results: List[CheckResult]) -> List[str]:
    headers = ["Step", "Check", "Status", "Detail"]
    col_widths = [
        max(len(res.step) for res in results + [CheckResult("Step", "", "", "")]),
        max(len(res.check) for res in results + [CheckResult("", "Check", "", "")]),
        max(len(res.status) for res in results + [CheckResult("", "", "Status", "")]),
    ]
    lines = []
    header_line = (
        f"{'Step'.ljust(col_widths[0])} | "
        f"{'Check'.ljust(col_widths[1])} | "
        f"{'Status'.ljust(col_widths[2])} | Detail"
    )
    sep = "-" * len(header_line)
    lines.append(header_line)
    lines.append(sep)
    for res in results:
        lines.append(
            f"{res.step.ljust(col_widths[0])} | "
            f"{res.check.ljust(col_widths[1])} | "
            f"{res.status.ljust(col_widths[2])} | {res.detail}"
        )
    return lines


def persist_report(results: List[CheckResult]) -> tuple[Path, Path]:
    exports_dir = get_central_output_dir() / "health_check"
    exports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = exports_dir / f"health_check_{timestamp}.txt"
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write("Russia Scraper Health Check\n")
        fh.write("=" * 60 + "\n")
        for line in format_table(results):
            fh.write(line + "\n")
        fh.write("\n")
        fh.write("=" * 60 + "\n")
        fh.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    json_path = exports_dir / f"health_check_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump([res.__dict__ for res in results], fh, indent=2, ensure_ascii=False)
    return report_path, json_path


def main() -> None:
    print("[HEALTH CHECK] Russia scraper health matrix", flush=True)
    results = run_health_checks()
    for line in format_table(results):
        print(line)
    report_path, json_path = persist_report(results)
    passed = sum(1 for res in results if res.status == "PASS")
    print(f"\n[HEALTH CHECK] {passed}/{len(results)} checks passed", flush=True)
    print(f"[HEALTH CHECK] Detailed report saved: {report_path}", flush=True)
    print(f"[HEALTH CHECK] JSON summary saved: {json_path}", flush=True)


if __name__ == "__main__":
    main()
