#!/usr/bin/env python3
"""
FUKKM Fully Reimbursable Drugs Scraper (Step 4)

Scrapes all pages from https://pharmacy.moh.gov.my/ms/apps/fukkm
Uses requests + BeautifulSoup (no browser needed).
DB-backed page-level resume.
"""

import logging
import re
import ssl
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class FUKKMScraper:
    """Scrapes fully reimbursable drugs from FUKKM website."""

    def __init__(self, run_id: str, db, config: dict = None):
        self.run_id = run_id
        self.db = db
        self.config = config or {}
        self.base_url = config.get("SCRIPT_04_BASE_URL",
                                   "https://pharmacy.moh.gov.my/ms/apps/fukkm")
        self.timeout = int(config.get("SCRIPT_04_REQUEST_TIMEOUT", 30))
        self.page_delay = float(config.get("SCRIPT_04_PAGE_DELAY", 0.04))
        self.table_selector = config.get("SCRIPT_04_TABLE_SELECTOR",
                                         "table.views-table.cols-7")
        self.user_agent = config.get("SCRIPT_04_USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.fail_fast = str(config.get("SCRIPT_04_FAIL_FAST", False)).lower() == "true"
        self.page_max_retries = int(config.get("SCRIPT_04_PAGE_MAX_RETRIES", 3))  # Changed from 0 to 3 (safety limit)
        self.retry_base_delay = float(config.get("SCRIPT_04_RETRY_BASE_DELAY", 2.0))
        self.retry_max_delay = float(config.get("SCRIPT_04_RETRY_MAX_DELAY", 60.0))

    def run(self) -> int:
        """Execute the full FUKKM scrape. Returns number of drugs scraped."""
        from db.repositories import MalaysiaRepository
        repo = MalaysiaRepository(self.db, self.run_id)

        session = self._create_session()

        # Fetch first page to detect total pages
        print(f"[FUKKM] Fetching first page to detect pagination...", flush=True)
        html = self._fetch_html(session, self.base_url)
        max_page = self._detect_max_page(html)
        total_pages = max_page + 1
        print(f"[FUKKM] Detected {total_pages} pages (0..{max_page})", flush=True)

        # Get already-completed pages
        completed_keys = repo.get_completed_keys(step_number=4)

        # Scrape all pages
        total_drugs = 0
        for page_idx in range(total_pages):
            progress_key = f"fukkm_page:{page_idx}"
            if progress_key in completed_keys:
                print(f"[FUKKM] [{page_idx+1}/{total_pages}] SKIP (already in DB)", flush=True)
                continue

            pct = round((page_idx / total_pages) * 100, 1)
            print(f"[PROGRESS] Scraping pages: {page_idx}/{total_pages} ({pct}%)", flush=True)

            repo.mark_progress(4, "Get Fully Reimbursable", progress_key, "in_progress")

            attempt = 0
            while True:
                try:
                    if page_idx == 0:
                        page_html = html  # Already fetched
                    else:
                        url = f"{self.base_url}?page={page_idx}"
                        page_html = self._fetch_html(session, url)

                    drugs = self._parse_page(page_html, page_idx)
                    if drugs:
                        repo.insert_reimbursable_drugs(drugs, source_page=page_idx)
                        total_drugs += len(drugs)

                    repo.mark_progress(4, "Get Fully Reimbursable", progress_key, "completed")
                    print(f"[FUKKM] [{page_idx+1}/{total_pages}] {len(drugs)} drugs", flush=True)
                    break

                except Exception as e:
                    attempt += 1
                    repo.mark_progress(4, "Get Fully Reimbursable", progress_key, "failed", str(e))
                    print(f"[ERROR] Page {page_idx}: {e}", flush=True)

                    if self.fail_fast:
                        raise

                    if self.page_max_retries > 0 and attempt > self.page_max_retries:
                        print(f"[ERROR] Page {page_idx}: exceeded max retries ({self.page_max_retries}), skipping", flush=True)
                        break

                    delay = min(self.retry_base_delay * (2 ** (attempt - 1)), self.retry_max_delay)
                    print(f"[RETRY] Page {page_idx}: attempt {attempt} retrying in {delay:.1f}s", flush=True)
                    time.sleep(delay)

            if self.page_delay > 0:
                time.sleep(self.page_delay)

        print(f"[PROGRESS] Scraping pages: {total_pages}/{total_pages} (100%)", flush=True)
        print(f"[OK] Scraped {total_drugs:,} reimbursable drugs from {total_pages} pages", flush=True)
        return total_drugs

    def _create_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        })
        # PERFORMANCE FIX: Tune connection pool for better throughput
        retry = Retry(total=3, backoff_factor=1,
                       status_forcelist=[429, 500, 502, 503, 504],
                       allowed_methods=["GET"])
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,    # Number of connection pools to cache
            pool_maxsize=20,        # Max connections per pool
            pool_block=False        # Don't block when pool is full
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
        return s

    def _fetch_html(self, session: requests.Session, url: str, retries: int = 3) -> str:
        """Fetch HTML with SSL retry + fallback to verify=False."""
        last_err = None
        for attempt in range(retries):
            try:
                r = session.get(url, timeout=self.timeout, verify=True)
                r.raise_for_status()
                return r.text
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                last_err = e
                if attempt < retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"  SSL error (attempt {attempt+1}): retrying in {wait}s", flush=True)
                    time.sleep(wait)
                else:
                    # Fallback: disable verification
                    try:
                        r = session.get(url, timeout=self.timeout, verify=False)
                        r.raise_for_status()
                        return r.text
                    except Exception as e2:
                        raise RuntimeError(f"SSL fallback failed: {e2}") from last_err
            except Exception as e:
                raise
        raise RuntimeError(f"fetch_html failed after {retries} retries") from last_err

    def _detect_max_page(self, html: str) -> int:
        """Find highest ?page=N value in the HTML."""
        soup = BeautifulSoup(html, "lxml")
        max_page = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "page=" in href:
                parsed = urlparse(href)
                qs = parse_qs(parsed.query)
                try:
                    p = int(qs.get("page", [0])[0])
                    if p > max_page:
                        max_page = p
                except (ValueError, IndexError):
                    pass
        return max_page

    def _parse_page(self, html: str, page_idx: int) -> List[Dict]:
        """Parse a single FUKKM page and return list of drug dicts."""
        soup = BeautifulSoup(html, "lxml")
        table = soup.select_one(self.table_selector)
        if not table:
            return []
        # Get rows (prefer tbody)
        drugs = []
        tbody = table.select_one("tbody")
        rows = tbody.find_all("tr") if tbody else table.find_all("tr")

        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            # Extract text with line breaks normalized to spaces.
            values = [c.get_text(" ", strip=True) for c in cells]
            classes = [c.get("class", []) for c in cells]

            drug = {"source_url": f"{self.base_url}?page={page_idx}"}

            # Prefer class-based mapping (matches site markup).
            for idx, cls_list in enumerate(classes):
                cls = " ".join(cls_list).lower()
                val = values[idx]
                if "views-field-counter" in cls:
                    continue
                if "views-field-text-1" in cls:
                    drug["registration_no"] = val
                elif "views-field-text-2" in cls:
                    drug["dosage_form"] = val
                elif "views-field-text-3" in cls:
                    drug["strength"] = val
                elif "views-field-text-4" in cls:
                    drug["manufacturer"] = val
                elif "views-field-text-5" in cls:
                    drug["pack_size"] = val
                elif "views-field-text" in cls:
                    drug["drug_name"] = val

            # Positional fallback: skip counter, then map in order.
            if not drug.get("drug_name"):
                cleaned = [
                    v for i, v in enumerate(values)
                    if "views-field-counter" not in " ".join(classes[i]).lower()
                ]
                if cleaned:
                    drug.setdefault("drug_name", cleaned[0])
                if len(cleaned) > 1:
                    drug.setdefault("registration_no", cleaned[1])
                if len(cleaned) > 2:
                    drug.setdefault("dosage_form", cleaned[2])
                if len(cleaned) > 3:
                    drug.setdefault("strength", cleaned[3])
                if len(cleaned) > 4:
                    drug.setdefault("pack_size", cleaned[4])
                if len(cleaned) > 5:
                    drug.setdefault("manufacturer", cleaned[5])

            # Only add if we have at least a name or code.
            if drug.get("drug_name") or drug.get("registration_no"):
                drugs.append(drug)

        return drugs
