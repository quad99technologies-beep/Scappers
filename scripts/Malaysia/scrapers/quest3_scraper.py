#!/usr/bin/env python3
"""
Quest3Plus Product Details Scraper (Step 2)

Two-phase extraction:
1. Bulk search by product keyword → CSV download → parse → DB
2. Individual detail pages for missing registration numbers → DB

DB-backed resume at keyword and regno granularity.
"""

import csv
import io
import logging
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from playwright.sync_api import Page

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Loading indicator selectors for Quest3Plus
LOADING_SELECTORS = [
    ".loading", ".spinner", "[class*='loading']", "[class*='spinner']",
    "[data-loading='true']", ".dataTables_processing",
    "#searchContent img[src*='spin.gif']",
]


def _clean(x):
    return re.sub(r"\s+", " ", str(x or "")).strip()


def _norm_regno(x):
    return _clean(x).upper().replace(" ", "")


def _sanitize(x):
    return re.sub(r"[^a-zA-Z0-9]+", "_", _clean(x)).strip("_")


def _first_words(x, n=3):
    return " ".join(_clean(x).split()[:n])


class Quest3Scraper(BaseScraper):
    """Scrapes product details from Quest3Plus website."""

    def __init__(self, run_id: str, db, config: dict = None,
                 input_products_path: Path = None,
                 output_dir: Path = None):
        super().__init__(run_id, db, config)
        self.search_url = config.get("SCRIPT_02_SEARCH_URL",
                                     "https://quest3plus.bpfk.gov.my/pmo2/index.php")
        self.detail_url_tpl = config.get("SCRIPT_02_DETAIL_URL",
                                         "https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={}")
        self.page_timeout = int(config.get("SCRIPT_02_PAGE_TIMEOUT", 60000))
        self.selector_timeout = int(config.get("SCRIPT_02_SELECTOR_TIMEOUT", 30000))
        self.search_delay = float(config.get("SCRIPT_02_SEARCH_DELAY", 5))
        self.individual_delay = float(config.get("SCRIPT_02_INDIVIDUAL_DELAY", 3))
        self.csv_wait_min = float(config.get("SCRIPT_02_CSV_WAIT_SECONDS", 60))
        self.csv_wait_max = float(config.get("SCRIPT_02_CSV_WAIT_MAX_SECONDS", 300))
        self.data_load_wait = float(config.get("SCRIPT_02_DATA_LOAD_WAIT", 3))

        # Selectors
        self.search_by_sel = config.get("SCRIPT_02_SEARCH_BY_SELECTOR", "#searchBy")
        self.search_txt_sel = config.get("SCRIPT_02_SEARCH_TXT_SELECTOR", "#searchTxt")
        self.search_btn_sel = config.get("SCRIPT_02_SEARCH_BUTTON_SELECTOR", "button.btn-primary")
        self.result_table_sel = config.get("SCRIPT_02_RESULT_TABLE_SELECTOR", "table.table")
        row_override = config.get("SCRIPT_02_RESULT_ROW_SELECTOR")
        self.result_row_sel = row_override or f"{self.result_table_sel} tbody tr"
        self.info_sel = config.get("SCRIPT_02_INFO_SELECTOR", "#searchTable_info")
        self.csv_btn_sel = config.get("SCRIPT_02_CSV_BUTTON_SELECTOR", "button.buttons-csv")
        self.detail_table_sel = config.get("SCRIPT_02_DETAIL_TABLE_SELECTOR", "table.table tr")
        self.product_name_label = config.get("SCRIPT_02_PRODUCT_NAME_LABEL", "product name :")
        self.holder_label = config.get("SCRIPT_02_HOLDER_LABEL", "holder :")
        self.holder_address_label = config.get("SCRIPT_02_HOLDER_ADDRESS_LABEL", "holder address")
        self.registration_column = config.get("SCRIPT_02_REGISTRATION_COLUMN",
                                              "Registration No / Notification No")

        self.input_products_path = input_products_path
        self.output_dir = output_dir or Path(".")

        # Bulk count reporting / CSV preservation
        self.bulk_counts_filename = config.get("SCRIPT_02_OUT_COUNT_REPORT") or "bulk_search_counts.csv"
        self.bulk_counts_path = self.output_dir / self.bulk_counts_filename
        self.bulk_csv_dirname = config.get("SCRIPT_02_BULK_DIR_NAME") or "bulk_search_csvs"
        self.bulk_csv_pattern = config.get("SCRIPT_02_BULK_CSV_PATTERN") or "bulk_search_{index:03d}_{keyword}.csv"
        self.bulk_retry_missing = int(config.get("SCRIPT_02_BULK_RETRY_MISSING", 1))
        self.individual_retry_missing = int(config.get("SCRIPT_02_INDIVIDUAL_RETRY_MISSING", 1))

    def run(self) -> int:
        """Execute full Quest3Plus scrape (bulk + individual)."""
        from db.repositories import MalaysiaRepository
        repo = MalaysiaRepository(self.db, self.run_id)

        headless = str(self.config.get("SCRIPT_02_HEADLESS", "false")).lower() == "true"

        with self.browser_session(headless=headless) as page:
            # Enable downloads
            self._context.set_default_timeout(self.page_timeout)

            # Stage 1: Bulk search
            print("\n" + "=" * 70, flush=True)
            print("STAGE 1: Bulk Search by Product Type", flush=True)
            print("=" * 70, flush=True)
            self._bulk_search(page, repo)

            # PERFORMANCE FIX: Clear page state between stages to prevent memory bloat
            print("[PERFORMANCE] Clearing page state between stages...", flush=True)
            try:
                page.evaluate("""
                    (function() {
                        window.localStorage && window.localStorage.clear();
                        window.sessionStorage && window.sessionStorage.clear();
                        var cookies = document.cookie.split(';');
                        for (var i = 0; i < cookies.length; i++) {
                            var cookie = cookies[i];
                            var eqPos = cookie.indexOf('=');
                            var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
                            document.cookie = name + '=;expires=Thu, 01 Jan 1970 00:00:00 GMT;path=/';
                        }
                    })()
                """)
                # Force garbage collection
                import gc
                gc.collect()
                print("[PERFORMANCE] Page state cleared", flush=True)
            except Exception as e:
                logger.debug(f"Page cleanup warning (non-critical): {e}")

            # Stage 2: Find missing + individual detail pages
            missing = repo.get_missing_registration_nos()
            if missing:
                print(f"\n" + "=" * 70, flush=True)
                print(f"STAGE 2: Individual Detail Pages ({len(missing):,} products)", flush=True)
                print("=" * 70, flush=True)
                self._individual_phase(page, repo, missing)
                missing = repo.get_missing_registration_nos()
                if missing and self.individual_retry_missing > 0:
                    for attempt in range(1, self.individual_retry_missing + 1):
                        print(f"\n" + "-" * 70, flush=True)
                        print(f"STAGE 2 RETRY {attempt}: Re-check missing details ({len(missing):,})", flush=True)
                        print("-" * 70, flush=True)
                        self._individual_phase(page, repo, missing, force=True)
                        missing = repo.get_missing_registration_nos()
                        if not missing:
                            break
            else:
                print("\n[STAGE 2] All products found in bulk. Skipping individual phase.", flush=True)

        total = repo.get_detail_count()
        print(f"\n[OK] Quest3Plus scrape complete. {total:,} product details in DB.", flush=True)
        return total

    # ------------------------------------------------------------------
    # Stage 1: Bulk search
    # ------------------------------------------------------------------

    def _bulk_search(self, page: Page, repo) -> None:
        """Search by product keyword, download CSV, parse into DB."""
        if self.input_products_path is None or not self.input_products_path.exists():
            logger.warning("products.csv not found: %s (skipping bulk; will proceed to individual details)", self.input_products_path)
            print(f"[BULK] products.csv not found at {self.input_products_path} -> skipping bulk search, continuing to individual detail pages.", flush=True)
            return

        df = pd.read_csv(self.input_products_path)
        total = len(df)
        print(f"[BULK] Found {total:,} product keywords to search", flush=True)

        completed_keys = repo.get_completed_keys(step_number=2)
        completed = 0
        skipped = 0
        failed = 0
        retry_items: List[Dict[str, Any]] = []
        retry_statuses = {"no_results", "no_csv_button", "download_failed", "csv_empty", "mismatch", "error", "unknown"}

        for i, row in df.iterrows():
            keyword = _first_words(row.iloc[0])
            progress_key = f"bulk_keyword:{_sanitize(keyword)}"

            if progress_key in completed_keys:
                skipped += 1
                print(f"[BULK] [{i+1}/{total}] SKIP {keyword} (already in DB)", flush=True)
                continue

            completed += 1
            pct = round((i / total) * 100, 1) if total else 0
            print(f"[BULK] [{i+1}/{total}] SEARCH {keyword} (Done: {completed}, Skip: {skipped}, Fail: {failed})", flush=True)
            print(f"[PROGRESS] Bulk search: {i}/{total} ({pct}%)", flush=True)

            repo.mark_progress(2, "Product Details", progress_key, "in_progress")

            status = self._process_bulk_keyword(
                page=page,
                repo=repo,
                keyword=keyword,
                index=i + 1,
                total=total,
                progress_key=progress_key,
            )
            if status in retry_statuses:
                retry_items.append({"index": i + 1, "keyword": keyword})
                failed += 1

            pct = round(((i + 1) / total) * 100, 1) if total else 0
            print(f"[PROGRESS] Bulk search: {i+1}/{total} ({pct}%)", flush=True)

            if self.search_delay > 0:
                time.sleep(self.search_delay)

        if retry_items and self.bulk_retry_missing > 0:
            for attempt in range(1, self.bulk_retry_missing + 1):
                print("\n" + "-" * 70, flush=True)
                print(f"STAGE 1 RETRY {attempt}: Re-check missing bulk records ({len(retry_items):,})", flush=True)
                print("-" * 70, flush=True)
                retry_items = self._bulk_retry_pass(page, repo, retry_items, retry_statuses)
                if not retry_items:
                    break

    def _bulk_retry_pass(self, page: Page, repo, retry_items: List[Dict[str, Any]],
                         retry_statuses: Set[str]) -> List[Dict[str, Any]]:
        """Retry bulk keywords that had missing or failed results."""
        remaining: List[Dict[str, Any]] = []
        total = len(retry_items)
        for idx, item in enumerate(retry_items, 1):
            keyword = item["keyword"]
            index = item["index"]
            progress_key = f"bulk_keyword:{_sanitize(keyword)}"
            pct = round((idx / total) * 100, 1) if total else 0
            print(f"[BULK-RETRY] [{idx}/{total}] SEARCH {keyword}", flush=True)
            print(f"[PROGRESS] Bulk retry: {idx}/{total} ({pct}%)", flush=True)
            status = self._process_bulk_keyword(
                page=page,
                repo=repo,
                keyword=keyword,
                index=index,
                total=total,
                progress_key=progress_key,
                display_index=idx,
            )
            if status in retry_statuses:
                remaining.append(item)
            if self.search_delay > 0:
                time.sleep(self.search_delay)
        return remaining

    def _process_bulk_keyword(self, page: Page, repo, keyword: str, index: int,
                              total: int, progress_key: str,
                              display_index: Optional[int] = None) -> str:
        """Run a single bulk keyword search and return its status."""
        if display_index is None:
            display_index = index
        repo.mark_progress(2, "Product Details", progress_key, "in_progress")

        try:
            result = self._search_keyword(page, keyword, index)
            details = result.get("details", [])
            if details:
                repo.insert_product_details_bulk(details, search_method="bulk")

            self._record_bulk_count(
                repo=repo,
                keyword=keyword,
                page_rows=result.get("page_rows"),
                csv_rows=result.get("csv_rows"),
                status=result.get("status"),
                reason=result.get("reason"),
                csv_file=result.get("csv_file"),
            )

            repo.mark_progress(2, "Product Details", progress_key, "completed")
            print(f"[BULK] [{display_index}/{total}] Completed: {keyword} ({len(details)} rows)\n", flush=True)
            return result.get("status") or "unknown"
        except Exception as e:
            self._record_bulk_count(
                repo=repo,
                keyword=keyword,
                page_rows=None,
                csv_rows=None,
                status="error",
                reason=str(e),
                csv_file=None,
            )
            repo.mark_progress(2, "Product Details", progress_key, "failed", str(e))
            print(f"[ERROR] Bulk search failed for {keyword}: {e}", flush=True)
            time.sleep(1.0)
            return "error"

    def _search_keyword(self, page: Page, keyword: str, index: int) -> Dict[str, Any]:
        """Search for a keyword and return parsed CSV rows plus row counts."""
        result: Dict[str, Any] = {
            "details": [],
            "page_rows": 0,
            "csv_rows": 0,
            "status": "unknown",
            "reason": "",
            "csv_file": "",
        }

        try:
            page.goto(self.search_url, timeout=self.page_timeout)
            page.wait_for_load_state("networkidle", timeout=30000)
            self.pause(0.5, 1.0)

            # Fill search form
            page.select_option(self.search_by_sel, "1")
            self.pause(0.2, 0.5)
            self.human_type(page, self.search_txt_sel, keyword)

            # Click search
            search_start = time.time()
            page.click(self.search_btn_sel)
            self.pause(0.5, 1.0)

            # Wait for search to settle
            self._wait_for_search_settle(page, search_start)

            # Wait for table data
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass

            # Check for "no results"
            body_text = page.inner_text("body").lower()
            if "no result" in body_text or "no data" in body_text or "tidak dijumpai" in body_text:
                print(f"  -> No results for '{keyword}'", flush=True)
                result["status"] = "no_results"
                result["reason"] = "No results"
                return result

            # Wait for table stability
            row_sel = self.result_row_sel
            self._wait_for_table_data_loaded(page, row_sel)
            self._wait_for_info_ready(page, self.info_sel)
            page_rows = self._extract_total_entries(page, self.info_sel)
            if page_rows is None:
                page_rows = self._count_visible_rows(page, row_sel)
            result["page_rows"] = page_rows

            # Try to download CSV
            csv_btn = page.query_selector(self.csv_btn_sel)
            if not csv_btn or not csv_btn.is_visible():
                print(f"  -> No CSV button for '{keyword}'", flush=True)
                result["status"] = "no_csv_button"
                result["reason"] = "CSV button not available"
                return result

            download_timeout = 300000  # 5 minutes
            with page.expect_download(timeout=download_timeout) as dl_info:
                csv_btn.click()
                self.pause(0.3, 0.8)

            # Read downloaded CSV into memory
            dl = dl_info.value
            tmp_path = dl.path()
            if tmp_path is None:
                print(f"  -> Download failed for '{keyword}'", flush=True)
                result["status"] = "download_failed"
                result["reason"] = "Download path missing"
                return result

            saved_path = self._persist_bulk_csv(tmp_path, index, keyword)
            result["csv_file"] = str(saved_path) if saved_path else str(tmp_path)

            # Parse CSV
            try:
                csv_df = pd.read_csv(tmp_path, on_bad_lines="skip", encoding="utf-8")
            except UnicodeDecodeError:
                csv_df = pd.read_csv(tmp_path, on_bad_lines="skip", encoding="latin-1")

            if csv_df.empty:
                result["status"] = "csv_empty"
                result["reason"] = "CSV downloaded but empty"
                return result

            # Convert to list of dicts
            results = []
            reg_col = self.registration_column
            for _, r in csv_df.iterrows():
                detail = {
                    "registration_no": _norm_regno(r.get(reg_col, "")),
                    "product_name": _clean(r.get("Product Name", "")),
                    "holder": _clean(r.get("Holder", "")),
                    "holder_address": _clean(r.get("Holder Address", "")),
                }
                if detail["registration_no"]:
                    results.append(detail)

            result["details"] = results
            result["csv_rows"] = len(results)

            print(f"  -> Parsed {len(results)} rows from CSV for '{keyword}'", flush=True)

            # If DataTables info was missing/low (e.g., 10) but CSV has full rows,
            # trust the CSV row count as the true total for comparison.
            if result.get("page_rows") is None or result.get("page_rows", 0) < len(results):
                result["page_rows"] = len(results)
                page_rows = result["page_rows"]

            if page_rows == 0 and len(results) == 0:
                result["status"] = "no_results"
                result["reason"] = "No results"
            elif page_rows and page_rows != len(results):
                result["status"] = "mismatch"
                result["reason"] = f"page_rows={page_rows} csv_rows={len(results)}"
            elif page_rows == 0 and len(results) > 0:
                result["status"] = "mismatch"
                result["reason"] = f"page_rows={page_rows} csv_rows={len(results)}"
            else:
                result["status"] = "ok"
            return result

        except Exception as e:
            result["status"] = "error"
            result["reason"] = str(e)
            return result

    def _wait_for_search_settle(self, page: Page, search_started_at: float):
        """Wait minimum time after search and for loading indicators to clear."""
        target = search_started_at + self.csv_wait_min
        deadline = search_started_at + self.csv_wait_max

        while True:
            now = time.time()
            loading = self._is_loading(page)
            if now >= target and not loading:
                break
            if now >= deadline:
                break
            time.sleep(0.5)

    def _wait_for_table_data_loaded(self, page: Page, row_selector: str,
                                    timeout_s: float = 60.0):
        """Wait for table row count to stabilize."""
        start = time.time()
        stable = 0
        last = -1
        while time.time() - start < timeout_s:
            try:
                rows = page.query_selector_all(row_selector)
                count = sum(1 for r in rows if r.is_visible())
            except Exception:
                count = 0
            if count > 0 and count == last:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            last = count
            time.sleep(1.0)

        # Additional data-load wait
        if self.data_load_wait > 0:
            time.sleep(self.data_load_wait)

    def _count_visible_rows(self, page: Page, row_selector: str) -> int:
        """Count visible data rows in the result table."""
        try:
            rows = page.query_selector_all(row_selector)
        except Exception:
            return 0
        count = 0
        for r in rows:
            try:
                if not r.is_visible():
                    continue
                if r.query_selector("th"):
                    continue
                text = _clean(r.inner_text()).lower()
                if "no result" in text or "no data" in text or "tidak dijumpai" in text:
                    continue
                count += 1
            except Exception:
                continue
        return count

    def _extract_total_entries(self, page: Page, info_selector: str) -> Optional[int]:
        """Parse DataTables info text like 'Showing 1 to 10 of 6,121 entries'."""
        try:
            el = page.query_selector(info_selector)
            if not el:
                return None
            text = _clean(el.inner_text())
            # Prefer total count when filtered: "filtered from 6,121 total entries"
            m = re.search(r"filtered from\\s+([0-9,]+)\\s+total\\s+entries", text, flags=re.IGNORECASE)
            if m:
                return int(m.group(1).replace(",", ""))
            # Otherwise: "Showing 1 to 10 of 6,121 entries"
            m = re.search(r"of\\s+([0-9,]+)\\s+entries", text, flags=re.IGNORECASE)
            if m:
                return int(m.group(1).replace(",", ""))
            return None
        except Exception:
            return None

    def _wait_for_info_ready(self, page: Page, info_selector: str,
                             timeout_s: float = 30.0) -> None:
        """Wait for DataTables info text to populate after search."""
        start = time.time()
        while time.time() - start < timeout_s:
            try:
                el = page.query_selector(info_selector)
                if el:
                    text = _clean(el.inner_text()).lower()
                    if "entries" in text and "of" in text:
                        return
            except Exception:
                pass
            time.sleep(0.5)

    def _persist_bulk_csv(self, tmp_path: str, index: int, keyword: str) -> Optional[Path]:
        """Copy the downloaded CSV into the output folder for audit."""
        try:
            bulk_dir = self.output_dir / self.bulk_csv_dirname
            bulk_dir.mkdir(parents=True, exist_ok=True)
            safe_keyword = _sanitize(keyword) or "keyword"
            try:
                filename = self.bulk_csv_pattern.format(index=index, keyword=safe_keyword)
            except Exception:
                filename = f"bulk_search_{index:03d}_{safe_keyword}.csv"
            filename = Path(filename).name
            dest_path = bulk_dir / filename
            shutil.copy2(tmp_path, dest_path)
            return dest_path
        except Exception as exc:
            logger.warning("Failed to persist bulk CSV for %s: %s", keyword, exc)
            return None

    def _append_bulk_count_row(self, row: Dict[str, Any]) -> None:
        """Append a single bulk count record to the CSV report."""
        try:
            self.bulk_counts_path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = self.bulk_counts_path.exists()
            with self.bulk_counts_path.open("a", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(
                    fh,
                    fieldnames=[
                        "timestamp",
                        "keyword",
                        "page_rows",
                        "csv_rows",
                        "difference",
                        "status",
                        "reason",
                        "csv_file",
                    ],
                )
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
        except Exception as exc:
            logger.warning("Failed to write bulk count row for %s: %s", row.get("keyword"), exc)

    def _record_bulk_count(
        self,
        repo,
        keyword: str,
        page_rows: Optional[int],
        csv_rows: Optional[int],
        status: str,
        reason: str = None,
        csv_file: str = None,
    ) -> None:
        """Persist bulk search row counts to CSV + DB."""
        status = status or "unknown"
        diff = None
        if page_rows is not None and csv_rows is not None:
            diff = page_rows - csv_rows
        row = {
            "timestamp": datetime.now().isoformat(),
            "keyword": keyword,
            "page_rows": page_rows if page_rows is not None else "",
            "csv_rows": csv_rows if csv_rows is not None else "",
            "difference": diff if diff is not None else "",
            "status": status,
            "reason": reason or "",
            "csv_file": csv_file or "",
        }
        self._append_bulk_count_row(row)
        try:
            if repo is not None:
                repo.log_bulk_search_count(
                    keyword=keyword,
                    page_rows=page_rows,
                    csv_rows=csv_rows,
                    status=status,
                    reason=reason,
                    csv_file=csv_file,
                )
        except Exception as exc:
            logger.warning("Failed to log bulk count to DB for %s: %s", keyword, exc)

    @staticmethod
    def _is_loading(page: Page) -> bool:
        for sel in LOADING_SELECTORS:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    return True
            except Exception:
                continue
        return False

    # ------------------------------------------------------------------
    # Stage 2: Individual detail pages
    # ------------------------------------------------------------------

    def _individual_phase(self, page: Page, repo, missing: Set[str], force: bool = False) -> None:
        """Scrape detail pages for each missing registration number."""
        completed_keys = repo.get_completed_keys(step_number=2)
        if force:
            remaining = [r for r in sorted(missing)]
        else:
            remaining = [r for r in sorted(missing)
                         if f"individual_regno:{r}" not in completed_keys]
        total = len(remaining)
        print(f"[INDIV] Processing {total:,} remaining products", flush=True)

        for idx, regno in enumerate(remaining, 1):
            progress_key = f"individual_regno:{regno}"
            pct = round((idx / total) * 100, 1) if total else 0
            print(f"[INDIV] [{idx}/{total}] DETAIL {regno}", flush=True)
            print(f"[PROGRESS] Individual search: {idx}/{total} ({pct}%)", flush=True)

            repo.mark_progress(2, "Product Details", progress_key, "in_progress")

            try:
                product_name, holder = self._extract_detail(page, regno)
                repo.insert_product_detail(
                    registration_no=regno,
                    product_name=product_name,
                    holder=holder,
                    search_method="individual",
                    source_url=self.detail_url_tpl.format(regno),
                )
                repo.mark_progress(2, "Product Details", progress_key, "completed")
                print(f"[SAVED] {idx}/{total} - {regno}", flush=True)
            except Exception as e:
                error_msg = str(e)
                repo.mark_progress(2, "Product Details", progress_key, "failed", error_msg)
                print(f"[ERROR] Failed {regno}: {error_msg}", flush=True)

                # For timeout errors, record with error marker
                if "timeout" in error_msg.lower():
                    repo.insert_product_detail(
                        registration_no=regno,
                        product_name="[TIMEOUT ERROR]",
                        holder="",
                        search_method="individual",
                    )

            time.sleep(1.0)
            if self.individual_delay > 0:
                time.sleep(self.individual_delay)

    def _extract_detail(self, page: Page, regno: str):
        """Navigate to detail page and extract Product Name + Holder."""
        url = self.detail_url_tpl.format(regno)
        page.goto(url, timeout=self.page_timeout)
        page.wait_for_load_state("networkidle", timeout=30000)
        self.pause(0.5, 1.0)

        product_name = ""
        holder = ""

        rows = page.query_selector_all(self.detail_table_sel)
        for r in rows:
            tds = r.query_selector_all("td")
            for td in tds:
                raw = _clean(td.inner_text())
                low = raw.lower()

                if low.startswith(self.product_name_label):
                    b = td.query_selector("b")
                    if b:
                        product_name = _clean(b.inner_text())
                    else:
                        parts = raw.split(":", 1)
                        product_name = _clean(parts[1]) if len(parts) == 2 else ""

                if low.startswith(self.holder_label) and not low.startswith(self.holder_address_label):
                    b = td.query_selector("b")
                    if b:
                        holder = _clean(b.inner_text())
                    else:
                        parts = raw.split(":", 1)
                        holder = _clean(parts[1]) if len(parts) == 2 else ""

        return product_name, holder
