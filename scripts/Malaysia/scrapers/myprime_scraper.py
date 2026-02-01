#!/usr/bin/env python3
"""
MyPriMe Drug Price Scraper (Step 1)

Scrapes all drug registration numbers and prices from:
  https://pharmacy.moh.gov.my/ms/apps/drug-price

Replaces Selenium + undetected-chromedriver with Playwright stealth context.
Handles Cloudflare verification, "View All" click, table stability, extraction.
Saves to DB (products table).
"""

import logging
import time
import sys
from typing import Dict, List, Tuple

from playwright.sync_api import Page

from scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class MyPriMeScraper(BaseScraper):
    """Scrapes drug prices from MyPriMe website using Playwright."""

    def __init__(self, run_id: str, db, config: dict = None):
        super().__init__(run_id, db, config)
        self.url = config.get("SCRIPT_01_URL", "https://pharmacy.moh.gov.my/ms/apps/drug-price")
        self.wait_timeout = float(config.get("SCRIPT_01_WAIT_TIMEOUT", 20))
        self.table_selector = config.get("SCRIPT_01_TABLE_SELECTOR", "table.tinytable")
        self.header_selector = config.get("SCRIPT_01_HEADER_SELECTOR", "thead th")
        self.row_selector = config.get("SCRIPT_01_ROW_SELECTOR", "tbody tr")
        self.cell_selector = config.get("SCRIPT_01_CELL_SELECTOR", "td")
        self.view_all_xpath = config.get("SCRIPT_01_VIEW_ALL_XPATH", "")

    def run(self) -> int:
        """
        Execute the full MyPriMe scrape.
        Returns number of products scraped.
        """
        from db.repositories import MalaysiaRepository
        repo = MalaysiaRepository(self.db, self.run_id)

        headless_str = self.config.get("SCRIPT_01_HEADLESS", "false")
        headless = str(headless_str).lower() == "true"
        if headless:
            print("NOTE: Running VISIBLE browser (Cloudflare requirement).", flush=True)
            headless = False

        with self.browser_session(headless=headless) as page:
            # Navigate
            print("Opening MyPriMe website...", flush=True)
            page.goto(self.url, wait_until="domcontentloaded", timeout=60000)
            self.pause(1.0, 2.0)

            # Cloudflare bypass
            print("Checking for Cloudflare verification...", flush=True)
            self.wait_for_cloudflare(page, timeout_s=90)
            self.pause(1.0, 2.0)

            # Verify target content
            self._verify_target_page(page)

            # Wait for page stability
            print("Waiting for page content to stabilize...", flush=True)
            page.wait_for_load_state("networkidle", timeout=30000)
            self.pause(1.5, 3.0)

            # Click "View All"
            print("Clicking 'View All' to load all products...", flush=True)
            self._click_view_all(page)
            self.pause(1.0, 2.0)

            # Wait for table to stabilize
            print("Waiting for table to load all rows...", flush=True)
            row_count = self.wait_for_table_stable(
                page, f"{self.table_selector} {self.row_selector}",
                checks=3, interval=2.0, timeout=self.wait_timeout * 3
            )
            if row_count == 0:
                raise RuntimeError("Table has no rows after View All click")

            # Extract data
            print(f"Extracting {row_count:,} rows...", flush=True)
            products, stats = self._extract_table(page)
            raw_count = len(products)
            # No dedup: keep all extracted rows
            dup_count = 0

            # Save to DB
            print(f"Saving {len(products):,} products to database...", flush=True)
            count = repo.insert_products(products)

            db_count = repo.get_product_count()
            source_rows = stats.get("source_rows", 0)
            extracted_rows = stats.get("extracted_rows", raw_count)
            if extracted_rows != raw_count:
                print(
                    f"  -> NOTE: extracted_rows={extracted_rows} differs from raw_count={raw_count}",
                    flush=True,
                )
            if db_count > len(products):
                raise RuntimeError(
                    "Row count mismatch: "
                    f"source_rows={source_rows}, extracted_rows={extracted_rows}, "
                    f"deduped_rows={len(products)}, inserted_rows={count}, db_rows={db_count}."
                )
            if db_count < len(products):
                print(
                    f"  -> NOTE: {len(products) - db_count} row(s) were not inserted (DB constraints)",
                    flush=True,
                )

            print(f"[OK] Scraped {count:,} products from MyPriMe", flush=True)
            print(f"[PROGRESS] Extracting rows: {count}/{count} (100%)", flush=True)

            # Log HTTP request
            repo.log_request(self.url, status_code=200,
                             elapsed_ms=(time.time() * 1000))

            return count

    def _verify_target_page(self, page: Page):
        """Verify we're on the actual MyPriMe page, not still on Cloudflare."""
        try:
            content = page.content().lower()
            indicators = ["tablefooter", "tinytable", "sorter.showall", "drug-price"]
            if any(ind in content for ind in indicators):
                print("  -> Page content verified", flush=True)
                return

            # Wait additional time for content
            print("  -> Target content not yet detected, waiting...", flush=True)
            for i in range(5):
                time.sleep(2)
                content = page.content().lower()
                if any(ind in content for ind in indicators):
                    print("  -> Target page content now detected", flush=True)
                    return
            print("  -> WARNING: Target page indicators not found. Proceeding anyway.", flush=True)
        except Exception as e:
            logger.debug("Error verifying page content: %s", e)

    def _click_view_all(self, page: Page):
        """Find and click the 'View All' link using multiple strategies."""
        # Strategy 1: Text-based (Playwright native)
        for text in ["View All", "view all", "Lihat Semua", "lihat semua"]:
            try:
                link = page.get_by_text(text, exact=False)
                if link.count() > 0:
                    link.first.click()
                    print(f"  -> Clicked 'View All' via text: '{text}'", flush=True)
                    return
            except Exception:
                continue

        # Strategy 2: JavaScript href click
        try:
            clicked = page.evaluate("""() => {
                const links = document.querySelectorAll('a');
                for (const a of links) {
                    const href = (a.href || '').toLowerCase();
                    const txt = (a.textContent || '').toLowerCase();
                    if (href.includes('sorter.showall') ||
                        txt.includes('view all') ||
                        txt.includes('lihat semua')) {
                        a.click();
                        return true;
                    }
                }
                return false;
            }""")
            if clicked:
                print("  -> Clicked 'View All' via JavaScript", flush=True)
                return
        except Exception:
            pass

        # Strategy 3: XPath from config
        if self.view_all_xpath:
            try:
                el = page.locator(f"xpath={self.view_all_xpath}")
                if el.count() > 0:
                    el.first.click()
                    print("  -> Clicked 'View All' via XPath", flush=True)
                    return
            except Exception:
                pass

        raise RuntimeError("Could not find 'View All' link with any strategy")

    def _extract_table(self, page: Page) -> Tuple[List[Dict], Dict[str, int]]:
        """Extract all rows from the product table. Expands headers by colspan so column count matches."""
        # Get headers and expand by colspan so we have one entry per data column
        headers = page.evaluate(f"""() => {{
            const table = document.querySelector('{self.table_selector}');
            if (!table) return [];
            const ths = table.querySelectorAll('thead th');
            const expanded = [];
            for (const th of ths) {{
                const text = (th.textContent || '').trim();
                const colspan = parseInt(th.getAttribute('colspan') || '1', 10) || 1;
                for (let k = 0; k < colspan; k++) {{
                    expanded.push(text);
                }}
            }}
            return expanded;
        }}""")
        if not headers:
            raise RuntimeError("No table headers found")
        print(f"  -> Found {len(headers)} columns: {', '.join(headers[:5])}{'...' if len(headers) > 5 else ''}", flush=True)

        # Get all rows in one JS call (much faster than iterating)
        rows_data = page.evaluate(f"""() => {{
            const table = document.querySelector('{self.table_selector}');
            if (!table) return [];
            const rows = table.querySelectorAll('{self.row_selector}');
            const result = [];
            for (const row of rows) {{
                const cells = row.querySelectorAll('{self.cell_selector}');
                if (cells.length > 0) {{
                    result.push(Array.from(cells).map(c => c.textContent.trim()));
                }}
            }}
            return result;
        }}""")

        # MyPriMe table fixed column order (from official HTML):
        # 0=Nombor Pendaftaran, 1=Nama Generik, 2=Nama Dagangan, 3=Deskripsi Pembungkusan,
        # 4=Unit (SKU), 5=Kuantiti, 6=Harga/unit, 7=Harga/pack, 8=Tahun Kemaskini
        MYPRIME_COLUMN_MAP = [
            "registration_no", "generic_name", "product_name", "dosage_form",
            "pack_unit", "pack_size", "unit_price", "retail_price", None
        ]

        products = []
        total = len(rows_data)
        num_cols = len(headers)
        mismatch_rows = 0
        padded_rows = 0
        truncated_rows = 0
        for i, row_cells in enumerate(rows_data):
            if len(row_cells) != num_cols:
                mismatch_rows += 1
                if len(row_cells) < num_cols:
                    row_cells = row_cells + [""] * (num_cols - len(row_cells))
                    padded_rows += 1
                else:
                    row_cells = row_cells[:num_cols]
                    truncated_rows += 1

            product = {"source_url": self.url}
            if num_cols == 9:
                # Use fixed positional mapping for MyPriMe 9-column table (no header text dependency)
                for j in range(min(9, len(row_cells))):
                    key = MYPRIME_COLUMN_MAP[j] if j < len(MYPRIME_COLUMN_MAP) else None
                    value = (row_cells[j] or "").strip()
                    if key is None:
                        continue  # skip year column
                    if key in ("unit_price", "retail_price"):
                        product[key] = _parse_float(value)
                    else:
                        product[key] = value
            else:
                # Fallback: header-based mapping for other table shapes
                price_column_seen = False
                for j, header in enumerate(headers):
                    h_lower = (header or "").lower().strip()
                    value = (row_cells[j] if j < len(row_cells) else "").strip()
                    if "registration" in h_lower or "notification" in h_lower or "pendaftaran" in h_lower:
                        product["registration_no"] = value
                    elif "nama generik" in h_lower or ("generic" in h_lower and "name" in h_lower):
                        product["generic_name"] = value
                    elif "nama dagangan" in h_lower or "dagangan" in h_lower or "brand" in h_lower:
                        product["product_name"] = value
                    elif "product" in h_lower and "name" in h_lower and "registration" not in h_lower:
                        product["product_name"] = value
                    elif "deskripsi" in h_lower or "pembungkusan" in h_lower or "packaging" in h_lower:
                        if not product.get("dosage_form"):
                            product["dosage_form"] = value
                    elif "dosage" in h_lower or "form" in h_lower:
                        product["dosage_form"] = value
                    elif "strength" in h_lower:
                        product["strength"] = value
                    elif "pack" in h_lower and "size" in h_lower:
                        product["pack_size"] = value
                    elif "pack" in h_lower and "unit" in h_lower:
                        product["pack_unit"] = value
                    elif "unit" in h_lower and "sku" in h_lower:
                        product["pack_unit"] = value
                    elif "kuantiti" in h_lower or "quantity" in h_lower:
                        if not product.get("pack_size"):
                            product["pack_size"] = value
                    elif "manufacturer" in h_lower or "company" in h_lower:
                        product["manufacturer"] = value
                    elif "unit" in h_lower and "price" in h_lower:
                        product["unit_price"] = _parse_float(value)
                    elif ("retail" in h_lower or "harga" in h_lower) and "tahun" not in h_lower and "kemaskini" not in h_lower and "year" not in h_lower:
                        if not price_column_seen:
                            product["unit_price"] = _parse_float(value)
                            price_column_seen = True
                        else:
                            product["retail_price"] = _parse_float(value)
                if not product.get("registration_no"):
                    product["registration_no"] = row_cells[0] if row_cells else ""

            if not product.get("registration_no"):
                product["registration_no"] = row_cells[0] if row_cells else ""
            products.append(product)

            if (i + 1) % 500 == 0:
                pct = round((i + 1) / total * 100, 1)
                print(f"  -> Processed {i + 1:,}/{total:,} rows ({pct}%)", flush=True)
                print(f"[PROGRESS] Extracting rows: {i + 1}/{total} ({pct}%)", flush=True)

        if mismatch_rows:
            print(
                f"  -> NOTE: {mismatch_rows} row(s) had column count mismatch "
                f"(padded={padded_rows}, truncated={truncated_rows})",
                flush=True,
            )

        stats = {
            "source_rows": total,
            "extracted_rows": len(products),
            "mismatch_rows": mismatch_rows,
            "padded_rows": padded_rows,
            "truncated_rows": truncated_rows,
        }

        return products, stats


def _parse_float(value: str):
    """Parse a string to float, returning None on failure."""
    if not value:
        return None
    try:
        # Remove currency symbols, commas
        cleaned = (value.replace(",", "").replace("RM", "").replace("$", "")
                   .replace("€", "").replace("\u20ac", "").strip())
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


def _is_empty(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _dedupe_products(products: List[Dict]) -> Tuple[List[Dict], int]:
    """
    Deduplicate products by all-column combination (exact row match).
    """
    deduped: List[Dict] = []
    seen: Dict[Tuple, Dict] = {}
    duplicates = 0

    # Build a stable column set so equality is "all columns match"
    all_keys = sorted({k for p in products for k in p.keys()})

    for product in products:
        key_values = []
        for k in all_keys:
            val = product.get(k, "")
            if isinstance(val, str):
                val = val.strip()
            key_values.append(val)
        key = tuple(key_values)

        if key in seen:
            duplicates += 1
            continue

        seen[key] = product
        deduped.append(product)

    return deduped, duplicates
