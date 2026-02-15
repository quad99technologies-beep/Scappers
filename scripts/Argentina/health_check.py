#!/usr/bin/env python3
"""
Argentina scraper health check.

This script runs manual, non-destructive checks to verify the platform configuration,
PCID mapping availability, and the key AlfaBeta layout selectors that the scraper
depends on. The output is a human-readable matrix summarizing each pass/fail so
operators can confirm whether the workflow still justifies running the full pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, List, Tuple

import json
import requests
from bs4 import BeautifulSoup

from config_loader import (
    PRODUCTS_URL,
    PCID_MAPPING_FILE,
    get_input_dir,
    get_central_output_dir,
)

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
    """Fetch the URL and return a BeautifulSoup object plus HTTP status."""
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml"), response.status_code


def check_url_reachable(url: str) -> Tuple[bool, str]:
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=15)
        if response.status_code == 200:
            return True, f"HTTP {response.status_code}"
        return False, f"HTTP {response.status_code}"
    except Exception as exc:
        return False, f"{exc}"


def check_path_exists(path: Path) -> Tuple[bool, str]:
    if path.exists():
        return True, f"{path.resolve()}"
    return False, "missing file"


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
        return (
            False,
            f"Missing selectors (HTTP {status_code}): {', '.join(missing)}",
        )
    return True, f"Selectors ok (HTTP {status_code})"


def check_db_connection() -> Tuple[bool, str]:
    """Verify PostgreSQL connection and run_ledger table for Argentina."""
    try:
        from core.db.connection import CountryDB
        with CountryDB("Argentina") as db:
            with db.cursor() as cur:
                cur.execute("SELECT 1 FROM run_ledger WHERE scraper_name = %s LIMIT 1", ("Argentina",))
                cur.fetchone()
        return True, "PostgreSQL connected, run_ledger accessible"
    except Exception as exc:
        return False, f"DB: {exc}"


def run_health_checks() -> List[CheckResult]:
    pcid_path = get_input_dir() / PCID_MAPPING_FILE

    checks: List[Tuple[str, str, Callable[[], Tuple[bool, str]]]] = [
        ("Config", "PostgreSQL (run_ledger)", check_db_connection),
        ("Config", "PRODUCTS_URL reachable", lambda: check_url_reachable(PRODUCTS_URL)),
        ("Config", "PCID mapping file present", lambda: check_path_exists(pcid_path)),
        (
            "Layout",
            "Search form selectors",
            lambda: check_selectors(
                PRODUCTS_URL,
                [
                    "form#srvPr",
                    "input[name='patron']",
                    "input.mfsubmit",
                ],
            ),
        ),
        (
            "Layout",
            "Product listing hints",
            lambda: check_selectors(
                PRODUCTS_URL,
                [
                    "table.estandar",
                    "table.estandar td a.rprod",
                ],
            ),
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
        0,
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
    summary_lines = format_table(results)
    exports_dir = get_central_output_dir() / "health_check"
    exports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = exports_dir / f"health_check_{timestamp}.txt"
    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write("Argentina Scraper Health Check\n")
        fh.write("=" * 60 + "\n")
        fh.write("\n".join(summary_lines))
        fh.write("\n")
        fh.write("=" * 60 + "\n")
        fh.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    json_path = exports_dir / f"health_check_{timestamp}.json"
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump([res.__dict__ for res in results], fh, indent=2, ensure_ascii=False)
    return report_path, json_path


def main() -> None:
    print("[HEALTH CHECK] Argentina scraper health matrix", flush=True)
    results = run_health_checks()
    for line in format_table(results):
        print(line)
    report_file, json_file = persist_report(results)
    passes = sum(1 for res in results if res.status == "PASS")
    print(f"\n[HEALTH CHECK] {passes}/{len(results)} checks passed", flush=True)
    print(f"[HEALTH CHECK] Detailed report saved: {report_file}", flush=True)
    print(f"[HEALTH CHECK] JSON summary saved: {json_file}", flush=True)


if __name__ == "__main__":
    main()
