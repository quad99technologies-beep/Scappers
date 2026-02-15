#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Ontario health check.

Checks:
- Formulary search results page reachable and has rows or explicit "no results"
- Detail page reachable for a sample drugId
- EAP prices page reachable and has tables
"""

import sys
import time
import re
from pathlib import Path
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.utils.logger import setup_standard_logger
from core.config.retry_config import RetryConfig
from config_loader import EAP_PRICES_URL, getenv_int, get_proxy_config, get_run_id, get_run_dir

BASE = "https://www.formulary.health.gov.on.ca/formulary/"
RESULTS_URL = BASE + "results.xhtml"
DETAIL_URL = BASE + "detail.xhtml"

RETRIES = getenv_int("HEALTH_RETRIES", RetryConfig.MAX_RETRIES)
TIMEOUT = getenv_int("HEALTH_TIMEOUT", RetryConfig.CONNECTION_CHECK_TIMEOUT)
PROXIES = get_proxy_config()


def fetch_with_retry(url: str, params: dict = None, timeout: int = 20) -> Tuple[str, int]:
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout, proxies=PROXIES or None)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2))
                time.sleep(min(retry_after + 1, RetryConfig.RETRY_DELAY_MAX_SECONDS))
                continue
            resp.raise_for_status()
            return resp.text, resp.status_code
        except Exception as exc:
            last = exc
            time.sleep(RetryConfig.calculate_backoff_delay(attempt - 1))
    raise RuntimeError(f"Health check failed for {url}: {last}")


def has_no_results(html: str) -> bool:
    return bool(re.search(r"No results found|No results were found|No matches", html, re.I))


def find_first_drug_id(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    link = soup.select_one("a[href*='detail.xhtml?drugId=']")
    if not link:
        return None
    m = re.search(r"drugId=([0-9A-Za-z]+)", link.get("href", ""))
    return m.group(1) if m else None


def check_formulary() -> None:
    html, _ = fetch_with_retry(RESULTS_URL, params={"q": "a", "s": "true", "type": "4"}, timeout=TIMEOUT)
    soup = BeautifulSoup(html, "lxml")
    tbody = soup.select_one("tbody#j_id_l\\:searchResultFull_data") or soup.find("tbody", id=re.compile(r"searchResultFull_data$"))
    if not tbody and not has_no_results(html):
        raise RuntimeError("Formulary results table missing and no 'no results' marker found.")

    drug_id = find_first_drug_id(html)
    if not drug_id:
        return

    detail_html, _ = fetch_with_retry(DETAIL_URL, params={"drugId": drug_id}, timeout=TIMEOUT)
    if "Manufacturer" not in detail_html:
        raise RuntimeError("Detail page missing manufacturer label.")


def check_eap_prices() -> None:
    html, _ = fetch_with_retry(EAP_PRICES_URL, timeout=TIMEOUT)
    soup = BeautifulSoup(html, "lxml")
    tables = soup.select("table.table.full-width.numeric")
    if not tables:
        raise RuntimeError("EAP prices page missing expected tables.")


def main() -> int:
    run_id = get_run_id()
    run_dir = get_run_dir(run_id)
    logger = setup_standard_logger(
        "canada_ontario_health",
        scraper_name="CanadaOntario",
        log_file=run_dir / "logs" / "health_check.log",
    )
    errors = []
    if PROXIES:
        logger.info("Proxy enabled for health checks")
    try:
        check_formulary()
        logger.info("Formulary search/detail reachable")
    except Exception as exc:
        errors.append(str(exc))
        logger.error("Formulary check failed: %s", exc)

    try:
        check_eap_prices()
        logger.info("EAP prices reachable")
    except Exception as exc:
        errors.append(str(exc))
        logger.error("EAP check failed: %s", exc)

    if errors:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
