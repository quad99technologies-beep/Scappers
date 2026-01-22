#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FUKKM scraper — saves ALL rows from the table:
  <table class="views-table cols-7"> ... </table>

Site: https://pharmacy.moh.gov.my/ms/apps/fukkm

How it works
- Downloads the first page
- Finds the maximum pager index from any link containing ?page=
- Iterates page=0..max_page
- Extracts the table headers + rows from table.views-table.cols-7
- Writes CSV output

Dependencies:
  pip install requests beautifulsoup4 lxml
"""

from __future__ import annotations

import csv
import re
import sys
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3.exceptions import SSLError as Urllib3SSLError
import ssl
from bs4 import BeautifulSoup
from config_loader import load_env_file, require_env, getenv, get_output_dir

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.standalone_checkpoint import run_with_checkpoint
from core.standalone_checkpoint import run_with_checkpoint

# Load environment variables from .env file
load_env_file()

BASE_URL = require_env("SCRIPT_04_BASE_URL")

# Use ConfigManager output directory instead of local output folder
output_dir_path = getenv("SCRIPT_04_OUTPUT_DIR", "")
if output_dir_path and Path(output_dir_path).is_absolute():
    OUTPUT_DIR = Path(output_dir_path)
else:
    OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

out_csv_name = require_env("SCRIPT_04_OUT_CSV")
OUT_CSV = str(OUTPUT_DIR / out_csv_name)

TABLE_SELECTOR = require_env("SCRIPT_04_TABLE_SELECTOR")


@dataclass
class PageResult:
    page_index: int
    headers: List[str]
    rows: List[List[str]]
    page_url: str


def _session() -> requests.Session:
    s = requests.Session()
    
    # Get headers from environment with defaults
    user_agent = getenv("SCRIPT_04_USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    accept_header = getenv("SCRIPT_04_ACCEPT_HEADER", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
    accept_language = getenv("SCRIPT_04_ACCEPT_LANGUAGE", "en-US,en;q=0.9")
    connection = getenv("SCRIPT_04_CONNECTION", "keep-alive")
    
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": accept_header,
            "Accept-Language": accept_language,
            "Connection": connection,
        }
    )
    
    # Configure retry strategy for SSL errors
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    
    # Create adapter with retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    
    # Configure SSL to handle connection issues
    # Disable SSL warnings for self-signed certificates (if fallback is needed)
    try:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception:
        pass
    
    s.verify = True  # Try with verification first, fallback in fetch_html if needed
    
    return s


def fetch_html(sess: requests.Session, url: str, timeout: int = None, retries: int = 3) -> str:
    if timeout is None:
        timeout = int(require_env("SCRIPT_04_REQUEST_TIMEOUT"))
    
    last_exception = None
    for attempt in range(retries):
        try:
            # Try with SSL verification enabled first
            r = sess.get(url, timeout=timeout, verify=True)
            r.raise_for_status()
            # requests will guess encoding; keep as text
            return r.text
        except (requests.exceptions.SSLError, Urllib3SSLError, ssl.SSLError) as e:
            last_exception = e
            error_msg = str(e)
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 2  # Exponential backoff: 2s, 4s, 6s
                print(f"  SSL error on attempt {attempt + 1}/{retries}: {error_msg[:100]}...", flush=True)
                print(f"  Retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                # Last attempt failed, try with verify=False as fallback
                print(f"  All SSL verification attempts failed, trying with SSL verification disabled...", flush=True)
                try:
                    # Create a new session for the fallback attempt to avoid connection pool issues
                    fallback_sess = requests.Session()
                    fallback_sess.headers.update(sess.headers)
                    r = fallback_sess.get(url, timeout=timeout, verify=False)
                    r.raise_for_status()
                    print(f"  Successfully fetched with SSL verification disabled", flush=True)
                    return r.text
                except Exception as fallback_error:
                    raise RuntimeError(f"SSL error after {retries} attempts and fallback failed: {fallback_error}") from e
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"  Request error on attempt {attempt + 1}/{retries}, retrying in {wait_time}s...", flush=True)
                time.sleep(wait_time)
            else:
                raise
    
    # Should not reach here, but just in case
    if last_exception:
        raise last_exception
    raise RuntimeError("Failed to fetch HTML after retries")


def soupify(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def find_max_page_index(soup: BeautifulSoup) -> int:
    """
    Drupal-style pagers typically use ?page=N (0-indexed).
    We scan all <a href="...page=N"> and take max N.
    If none found, returns 0.
    """
    max_page = 0
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        # quick filter first
        if "page=" not in href:
            continue

        try:
            abs_url = urljoin(BASE_URL, href)
            qs = parse_qs(urlparse(abs_url).query)
            if "page" in qs and qs["page"]:
                n = int(qs["page"][0])
                if n > max_page:
                    max_page = n
        except Exception:
            continue

    # Some sites have pager links without explicit page=, e.g. /apps/fukkm?page=12
    # Already covered. If "last" exists but no page=, we can't infer; 0 is safe.
    return max_page


def extract_table(soup: BeautifulSoup) -> Tuple[List[str], List[List[str]]]:
    table = soup.select_one(TABLE_SELECTOR)
    if not table:
        raise RuntimeError(f"Could not find table with selector: {TABLE_SELECTOR}")

    # Headers: prefer <thead><th>, fallback to first row's <th>
    headers: List[str] = []
    header_selector = require_env("SCRIPT_04_HEADER_SELECTOR")
    thead_th = table.select(header_selector)
    if thead_th:
        headers = [th.get_text(" ", strip=True) for th in thead_th]
    else:
        first_row_th_selector = require_env("SCRIPT_04_FIRST_ROW_TH_SELECTOR")
        first_th = table.select(first_row_th_selector)
        if first_th:
            headers = [th.get_text(" ", strip=True) for th in first_th]

    # Body rows: usually tbody tr
    rows: List[List[str]] = []
    tbody_row_selector = require_env("SCRIPT_04_TBODY_ROW_SELECTOR")
    tr_selector = require_env("SCRIPT_04_TR_SELECTOR")
    tr_list = table.select(tbody_row_selector) or table.select(tr_selector)

    for tr in tr_list:
        # skip header-like rows
        if tr.find("th"):
            continue
        tds = tr.find_all("td")
        if not tds:
            continue
        row = [td.get_text(" ", strip=True) for td in tds]
        rows.append(row)

    return headers, rows


def pad_or_trim(row: List[str], n: int) -> List[str]:
    if n <= 0:
        return row
    if len(row) < n:
        return row + [""] * (n - len(row))
    if len(row) > n:
        return row[:n]
    return row


def scrape_all_pages(
    delay_s: float = None,
    fail_fast: bool = None,
) -> List[PageResult]:
    if delay_s is None:
        delay_s = float(require_env("SCRIPT_04_PAGE_DELAY"))
    if fail_fast is None:
        fail_fast_str = getenv("SCRIPT_04_FAIL_FAST")
        fail_fast = fail_fast_str.lower() == "true" if fail_fast_str else False
    sess = _session()

    # First page
    print(f"Fetching first page: {BASE_URL}", flush=True)
    html0 = fetch_html(sess, BASE_URL)
    soup0 = soupify(html0)

    max_page = find_max_page_index(soup0)
    print(f"Detected max page index (0-based): {max_page}", flush=True)
    print(f"Will scrape pages 0 through {max_page} (total: {max_page + 1} pages)", flush=True)

    results: List[PageResult] = []
    all_headers: Optional[List[str]] = None

    for page in range(0, max_page + 1):
        page_url = BASE_URL if page == 0 else f"{BASE_URL}?page={page}"
        try:
            print(f"  -> Fetching page {page}/{max_page}: {page_url}", flush=True)
            html = fetch_html(sess, page_url)
            soup = soupify(html)
            print(f"  -> Extracting table data...", flush=True)
            headers, rows = extract_table(soup)
            print(f"  -> Found {len(rows)} rows, {len(headers)} columns", flush=True)
            
            # Output progress for page scraping
            total_pages = max_page + 1
            percent = round(((page + 1) / total_pages) * 100, 1) if total_pages > 0 else 0
            print(f"[PROGRESS] Scraping pages: {page + 1}/{total_pages} ({percent}%)", flush=True)

            # stabilize headers across pages
            if all_headers is None:
                all_headers = headers
            else:
                # If headers vary, keep the first and just align row lengths.
                pass

            # Align each row to header length if we have headers
            if all_headers and len(all_headers) > 0:
                rows = [pad_or_trim(r, len(all_headers)) for r in rows]

            results.append(PageResult(page_index=page, headers=all_headers or headers, rows=rows, page_url=page_url))
            print(f"Page {page}/{max_page}: {len(rows)} rows", flush=True)
        except Exception as e:
            msg = f"ERROR on page={page} ({page_url}): {e}"
            if fail_fast:
                raise RuntimeError(msg) from e
            print(msg, file=sys.stderr, flush=True)

        if delay_s:
            time.sleep(delay_s)

    return results


def write_csv(results: List[PageResult], out_csv: str) -> None:
    # Pick headers from first non-empty
    headers: List[str] = []
    for pr in results:
        if pr.headers:
            headers = pr.headers
            break

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if headers:
            w.writerow(headers + ["_source_page"])
        else:
            # If no headers, just write raw rows with page marker
            pass

        for pr in results:
            for row in pr.rows:
                if headers:
                    row = pad_or_trim(row, len(headers))
                w.writerow(row + [pr.page_url])


def main():
    """Main function to scrape all FUKKM pages and save results."""
    print("Starting FUKKM scraper...", flush=True)
    print(f"Target URL: {BASE_URL}", flush=True)
    
    delay_s = float(require_env("SCRIPT_04_PAGE_DELAY"))
    fail_fast_str = getenv("SCRIPT_04_FAIL_FAST")
    fail_fast = fail_fast_str.lower() == "true" if fail_fast_str else False
    results = scrape_all_pages(delay_s=delay_s, fail_fast=fail_fast)

    total_rows = sum(len(r.rows) for r in results)
    print(f"\nTotal rows scraped: {total_rows:,}", flush=True)

    if total_rows == 0:
        print("[WARNING] WARNING: No rows scraped. The table selector may have changed or content is blocked.", file=sys.stderr, flush=True)
        # Create empty CSV file with Generic Name header to allow pipeline to continue
        print(f"Creating empty CSV file with headers to allow pipeline to continue...", flush=True)
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            # Use a minimal header set - at minimum include "Generic Name" which is required by script 05
            # Try to get headers from results if available, otherwise use default
            headers = []
            for pr in results:
                if pr.headers:
                    headers = pr.headers
                    break
            # If no headers found, use a default set that includes Generic Name
            if not headers:
                headers = ["Generic Name"]
            w.writerow(headers + ["_source_page"])
        print(f"[OK] Created empty CSV file: {OUT_CSV}", flush=True)
        return

    print(f"Saving results to CSV...", flush=True)
    write_csv(results, OUT_CSV)

    print(f"[OK] Wrote {total_rows:,} rows to: {OUT_CSV}", flush=True)


if __name__ == "__main__":
    run_with_checkpoint(
        main,
        "Malaysia",
        4,
        "Get Fully Reimbursable",
        output_files=[OUT_CSV]
    )
