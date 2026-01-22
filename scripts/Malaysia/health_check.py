#!/usr/bin/env python3
"""
Malaysia scraper health check.

Runs lightweight diagnostics (without extracting data) to verify the
configuration paths, PCID file readiness, and the key MyPriMe/QUEST3+
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
    require_env,
    getenv,
    get_central_output_dir,
    get_input_dir,
)

load_env_file()

MYPRIME_URL = require_env("SCRIPT_01_URL")
VIEW_ALL_TEXTS = ["view all", "lihat semua"]
# Use getenv() for optional selectors - skip checks if not configured
MYPRIME_TABLE_SELECTOR = getenv("SCRIPT_01_TABLE_SELECTOR")
MYPRIME_HEADER_SELECTOR = getenv("SCRIPT_01_HEADER_SELECTOR")

QUEST_URL = require_env("SCRIPT_02_SEARCH_URL")
QUEST_SEARCH_BY = getenv("SCRIPT_02_SEARCH_BY_SELECTOR")
QUEST_SEARCH_TEXT = getenv("SCRIPT_02_SEARCH_TXT_SELECTOR")
QUEST_SEARCH_BTN = getenv("SCRIPT_02_SEARCH_BUTTON_SELECTOR")
QUEST_TABLE_SELECTOR = getenv("SCRIPT_02_RESULT_TABLE_SELECTOR")

# Script 04 (FUKKM) configuration
FUKKM_URL = getenv("SCRIPT_04_BASE_URL")
FUKKM_TABLE_SELECTOR = getenv("SCRIPT_04_TABLE_SELECTOR")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
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
        # Provide more context for HTTP 415 (Unsupported Media Type)
        if response.status_code == 415:
            return False, f"HTTP {response.status_code} (Unsupported Media Type - server may require specific headers)"
        return False, f"HTTP {response.status_code}"
    except (requests.exceptions.SSLError, Urllib3SSLError, ssl.SSLError):
        # Try with SSL verification disabled as fallback
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=15, verify=False)
            if response.status_code == 200:
                return True, f"HTTP {response.status_code} (SSL verification disabled)"
            if response.status_code == 415:
                return False, f"HTTP {response.status_code} (Unsupported Media Type - server may require specific headers)"
            return False, f"HTTP {response.status_code} (SSL verification disabled)"
        except Exception as exc:
            return False, f"SSL error and fallback failed: {exc}"
    except Exception as exc:
        return False, str(exc)


def check_selectors(url: str, selectors: Iterable[str]) -> Tuple[bool, str]:
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


def run_health_checks() -> List[CheckResult]:
    pcid_mapping_path = get_input_dir() / "PCID Mapping - Malaysia.csv"

    checks: List[Tuple[str, str, Callable[[], Tuple[bool, str]]]] = [
        ("Config", "MyPriMe URL reachable", lambda: check_url_reachable(MYPRIME_URL)),
        ("Config", "Quest3+ search URL reachable", lambda: check_url_reachable(QUEST_URL)),
        ("Config", "Malaysia PCID file present", lambda: (pcid_mapping_path.exists(), str(pcid_mapping_path.resolve()) if pcid_mapping_path.exists() else "missing file")),
        (
            "Layout",
            "'View All' text / button",
            lambda: check_texts(MYPRIME_URL, VIEW_ALL_TEXTS),
        ),
    ]
    
    # Add MyPriMe table selectors check if configured
    if MYPRIME_TABLE_SELECTOR and MYPRIME_HEADER_SELECTOR:
        checks.append(
            (
                "Layout",
                "MyPriMe price table",
                lambda: check_selectors(MYPRIME_URL, [MYPRIME_TABLE_SELECTOR, MYPRIME_HEADER_SELECTOR]),
            )
        )
    else:
        checks.append(
            (
                "Layout",
                "MyPriMe price table",
                lambda: (False, "Selectors not configured (SCRIPT_01_TABLE_SELECTOR, SCRIPT_01_HEADER_SELECTOR)"),
            )
        )
    
    # Add Quest3+ search form check if search form selectors are configured
    # Note: Result table selector is NOT checked here because it only appears after a search is performed
    if QUEST_SEARCH_BY and QUEST_SEARCH_TEXT and QUEST_SEARCH_BTN:
        checks.append(
            (
                "Layout",
                "Quest3+ search form",
                lambda: check_selectors(
                    QUEST_URL,
                    [QUEST_SEARCH_BY, QUEST_SEARCH_TEXT, QUEST_SEARCH_BTN],
                ),
            )
        )
    else:
        checks.append(
            (
                "Layout",
                "Quest3+ search form",
                lambda: (False, "Selectors not configured (SCRIPT_02_SEARCH_BY_SELECTOR, SCRIPT_02_SEARCH_TXT_SELECTOR, SCRIPT_02_SEARCH_BUTTON_SELECTOR)"),
            )
        )
    
    # Add separate check for result table selector (informational only, since it requires a search to appear)
    if QUEST_TABLE_SELECTOR:
        checks.append(
            (
                "Layout",
                "Quest3+ result table selector",
                lambda: (True, f"Selector configured: {QUEST_TABLE_SELECTOR} (Note: Table appears after search, cannot verify on initial page)"),
            )
        )
    
    # Add FUKKM (Script 04) check if configured
    if FUKKM_URL:
        checks.append(
            ("Config", "FUKKM URL reachable", lambda: check_url_reachable(FUKKM_URL)),
        )
        if FUKKM_TABLE_SELECTOR:
            checks.append(
                (
                    "Layout",
                    "FUKKM table selector",
                    lambda: check_selectors(FUKKM_URL, [FUKKM_TABLE_SELECTOR]),
                )
            )

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
        fh.write("Malaysia Scraper Health Check\n")
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
    print("[HEALTH CHECK] Malaysia scraper health matrix", flush=True)
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
