#!/usr/bin/env python3
"""
Netherlands Scraper - Hybrid Playwright + HTTPX Edition
Uses Playwright for cookie/session setup, then httpx for fast URL collection.
Based on the proven approach from archive/01_collect_urls.py
"""

import sys
import os
from pathlib import Path

# ---- Path wiring ----
# Ensure project root is in sys.path BEFORE importing core modules
# Depending on execution context, we might be 2 or 3 levels deep
try:
    # Try 3 levels up first (e.g. Scrappers/scripts/Netherlands)
    project_root = Path(__file__).resolve().parents[2]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
except IndexError:
    # Fallback
    pass

from core.bootstrap.environment import setup_scraper_environment
setup_scraper_environment(__file__)

import db

import asyncio
import re
import time
import random
from datetime import date
from typing import List, Dict, Optional, Set
from urllib.parse import quote, urlencode

import httpx
from lxml import html
from playwright.async_api import async_playwright

from core.pipeline.base_scraper import BaseScraper
from db.repositories import NetherlandsRepository

# Text helpers
from core.utils.text_utils import normalize_ws

def clean_single_line(text: str) -> str:
    """Wrapper for core normalization."""
    return normalize_ws(text)

def first_euro_amount(text: str) -> str:
    if not text:
        return ""
    t = clean_single_line(text)
    m = re.search(r"[â‚¬]\s*\d[\d\.\,]*", t)
    if m:
        return "â‚¬" + m.group(0).strip()[1:].strip()
    return ""

def parse_total(page_html: str) -> Optional[int]:
    """Parse total results count from page HTML"""
    try:
        doc = html.fromstring(page_html)
        txt = doc.xpath('string(//*[@id="summary"]//strong)').strip()
        return int(txt) if txt.isdigit() else None
    except Exception:
        return None

def extract_links(fragment_html: str, base_url: str) -> List[Dict]:
    """Extract product links and details from HTML fragment"""
    try:
        doc = html.fromstring(fragment_html)
        results = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]')
        out = []
        for a in results:
            href = a.get("href", "")
            if not href or not href.startswith("/medicijn?"):
                continue
            if "pagenotfound" in href.lower() or href == "/medicijn?":
                continue
            
            url = base_url + href
            out.append({
                'url': url,
                'url_with_id': url,
                'title': clean_single_line(a.xpath('string(.//h3[contains(@class,"result-title")])')),
                'active_substance': clean_single_line(a.xpath('string(.//span[contains(@class,"active-substance")])')),
                'manufacturer': clean_single_line(a.xpath('string(.//span[contains(@class,"manufacturer")])')),
                'document_type': clean_single_line(a.xpath('string(.//span[contains(@class,"document-type")])')),
                'price_text': clean_single_line(a.xpath('string(.//span[contains(@class,"price")])')),
                'reimbursement': clean_single_line(a.xpath('string(.//span[contains(@class,"reimbursement")])')),
                'prefix': 'all_products',
            })
        return out
    except Exception:
        return []

def parse_euro_amount(text: str) -> Optional[float]:
    """Extract first euro amount from text as float."""
    if not text:
        return None
    t = clean_single_line(text)
    t = t.replace("â‚¬", "").replace("Ã¢â€šÂ¬", "").strip()
    m = re.search(r"\d[\d\.,]*", t)
    if not m:
        return None
    s = m.group(0)
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def get_depends_amount(node, mode: str) -> Optional[float]:
    """Get amount from span[data-pat-depends='inline-days=<mode>'] within node."""
    spans = node.xpath(f'.//span[contains(@data-pat-depends,"inline-days={mode}")]')
    if not spans:
        return None
    return parse_euro_amount(spans[0].text_content())

class NetherlandsScraper(BaseScraper):
    def __init__(self, run_id: Optional[str] = None):
        if not run_id:
            run_id = os.environ.get("NL_RUN_ID")
        super().__init__("Netherlands", run_id)
        
        # Config
        self.base_url = self.config.get("BASE_URL", "https://www.medicijnkosten.nl")
        self.search_keyword = self.config.get("SEARCH_TERM", "632 Medicijnkosten Drugs4")
        self.margin_rule = self.config.get("MARGIN_RULE", "632 Medicijnkosten Drugs4")
        self.max_workers = int(self.config.get("MAX_WORKERS", self.config.get("CONCURRENT_WORKERS", "5")))
        self.page_delay = float(self.config.get("PAGE_DELAY", "1.0"))
        
        # Repo
        self.repo = NetherlandsRepository(self.db, self.run_id)
        
        # Ensure new columns exist
        try:
            with self.db.cursor() as cur:
                cur.execute("ALTER TABLE nl_search_combinations ADD COLUMN IF NOT EXISTS urls_inserted INTEGER DEFAULT 0;")
                cur.execute("ALTER TABLE nl_search_combinations ADD COLUMN IF NOT EXISTS products_found INTEGER DEFAULT 0;")
                cur.execute("ALTER TABLE nl_search_combinations ADD COLUMN IF NOT EXISTS urls_discovered INTEGER DEFAULT 0;")
                cur.execute("ALTER TABLE nl_search_combinations ADD COLUMN IF NOT EXISTS urls_fetched INTEGER DEFAULT 0;")
                cur.execute("ALTER TABLE nl_search_combinations ADD COLUMN IF NOT EXISTS urls_duplicate INTEGER DEFAULT 0;")
            self.db.commit()
        except:
            self.db.rollback()

    def run(self):
        """Main entry point."""
        self.logger.info("Starting Netherlands Scraper (Hybrid Playwright + HTTPX)")
        
        # Ensure run exists in ledger (prevents FK errors)
        try:
            self.repo.ensure_run_in_ledger()
        except Exception as e:
            self.logger.warning(f"Could not ensure run in ledger (might be harmless if not using strict FKs): {e}")

        try:
            asyncio.run(self._async_run())
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        except Exception as e:
            self.logger.error(f"Fatal error in run loop: {e}", exc_info=True)
            self.record_error("fatal_crash")
            raise

    async def _async_run(self):
        """Async execution flow."""
        # 1. Collect URLs
        self.logger.info("Phase 1: URL Collection")
        
        existing_urls = self.repo.get_collected_url_keys()
        self.logger.info(f"Loaded {len(existing_urls)} existing URLs to skip")

        # Resume behavior: if URLs already exist for this run, skip recollection by default.
        # This avoids replaying all pagination on resume and continues directly with pending URLs.
        force_recollect = str(self.config.get("FORCE_URL_RECOLLECTION", "false")).strip().lower() in ("1", "true", "yes", "on")
        resume_skip_collect = str(self.config.get("RESUME_SKIP_URL_COLLECTION", "true")).strip().lower() in ("1", "true", "yes", "on")

        if existing_urls and resume_skip_collect and not force_recollect:
            self.logger.info(
                f"Phase 1 skipped (resume): reusing {len(existing_urls)} collected URLs. "
                "Set FORCE_URL_RECOLLECTION=true to force recrawl."
            )
            collected_urls = []
            self.last_expected_total = len(existing_urls)
        else:
            collected_urls = await self.collect_urls_hybrid(existing_urls)
        
        # VERIFICATION
        total_collected = len(collected_urls) + len(existing_urls)
        expected_total = getattr(self, "last_expected_total", None)
        EXPECTED_MIN = 20000

        if expected_total:
            if total_collected < int(expected_total * 0.95):
                self.logger.error(
                    f"Collected only {total_collected} URLs, below 95% of expected total {expected_total}. "
                    "Aborting product scraping."
                )
                return
        else:
            if total_collected < EXPECTED_MIN:
                self.logger.error(f"Collected only {total_collected} URLs, which is below expected {EXPECTED_MIN}. Aborting product scraping.")
                return

        self.logger.info(f"Total URL collection count: {total_collected} (New: {len(collected_urls)})")

        # 2. Scrape Products
        self.logger.info("Phase 2: Product Scraping")
        
        pending_items = self.repo.get_pending_scrape_urls(limit=30000)
        self.logger.info(f"Found {len(pending_items)} pending URLs to scrape")
        
        if pending_items:
            url_pairs = [(item['url'], item['id']) for item in pending_items]
            await self.scrape_products_concurrent(url_pairs)
            

        
        self.logger.info("Scraping finished")

    async def collect_urls_hybrid(self, existing_urls: set) -> List[Dict]:
        """
        Collect URLs using hybrid approach:
        1. Playwright - Get cookies and accept cookie banner
        2. HTTPX - Collect ALL URLs at once using "Alle sterktes" (all strengths)
        """
        self.logger.info("Starting Hybrid URL Collection (Playwright + HTTPX)...")
        
        # Use the base URL with all strengths to collect everything at once
        filter_url = "https://www.medicijnkosten.nl/zoeken?searchTerm=632%20Medicijnkosten%20Drugs4&type=medicine&searchTermHandover=632%20Medicijnkosten%20Drugs4&vorm=Alle%20vormen&sterkte=Alle%20sterktes"
        
        self.logger.info(f"Initial URL: {filter_url}")
        
        # Step 1: Get cookies using Playwright
        self.logger.info("[Playwright] Getting session cookies...")
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            
            # URL collection does not need static assets.
            async def _block_static_assets(route):
                if route.request.resource_type in ("image", "stylesheet", "font", "media"):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", _block_static_assets)
            page = await context.new_page()
            
            # Navigate to search page
            await page.goto(filter_url, wait_until="networkidle")
            
            # Accept cookie banner if present
            try:
                await page.get_by_role("button", name=re.compile("Akkoord|Accept", re.I)).click(timeout=2000)
                self.logger.info("[Playwright] Accepted cookie banner")
            except Exception:
                pass
            
            # Get initial page HTML for total count
            html0 = await page.content()
            
            # Get cookies for HTTPX
            cookies = await context.cookies()
            await browser.close()
        
        self.logger.info(f"[Playwright] Got {len(cookies)} cookies")
        
        # Parse total from first page
        total = parse_total(html0)
        if total:
            self.logger.info(f"[INFO] Total expected products: {total}")
            self.last_expected_total = total
        
        # Convert cookies to httpx format
        jar = httpx.Cookies()
        for c in cookies:
            jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        
        # Step 2: Collect ALL URLs at once using "Alle sterktes"
        all_records = []
        seen_urls = set(existing_urls)
        
        # Insert a single combination record for "All Strengths"
        combo_id = self.repo.insert_combination("Alle vormen", "Alle sterktes", filter_url)
        
        # Collect all URLs
        total_urls, count = await self.collect_all_urls(
            jar, seen_urls, all_records, filter_url
        )
        
        # Save any remaining URLs
        if all_records:
            self.logger.info(f"Saving final batch of {len(all_records)} URLs...")
            inserted = self.repo.insert_collected_urls(all_records)
            self.logger.info(f"Saved {inserted} URLs")
        
        # Update combination record
        if combo_id:
            self.repo.mark_combination_completed(
                combo_id,
                products_found=count,
                urls_discovered=len(total_urls),
                urls_fetched=len(total_urls),
                urls_inserted=len(total_urls),
                urls_collected=len(total_urls)
            )
        
        # Return all collected records for verification
        return [{'url': url, 'url_with_id': url, 'prefix': 'all_products'} for url in total_urls]

    async def collect_all_urls(self, cookies: httpx.Cookies, 
                               seen_urls: set, all_records: List[Dict],
                               referer_url: str) -> tuple[List[str], int]:
        """Collect ALL URLs at once using 'Alle sterktes' (all strengths)."""
        all_urls = []
        
        # Replace direct httpx with HTTP Client would go here
        # But extensive refactor required to match exact API.
        # For now, ensure we use standard headers/timeouts.
        
        async with httpx.AsyncClient(
            cookies=cookies,
            headers={
                "Accept": "text/html",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": referer_url,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
            },
            timeout=45.0,
            follow_redirects=True, # Standard
        ) as client:
            
            # Build initial URL - use the exact URL that works
            params = {
                "page": "0",
                "searchTerm": self.search_keyword,
                "type": "medicine",
                "searchTermHandover": self.search_keyword,
                "vorm": "Alle vormen",
                "sterkte": "Alle sterktes",
                "sorting": "",
                "debugMode": ""
            }
            url = f"{self.base_url}/zoeken?" + urlencode(params)
            
            # Get first page
            self.logger.debug(f"  Requesting: {url}")
            try:
                response = await client.get(url)
                html_content = response.text
                self.logger.debug(f"  Response status: {response.status_code}")
            except Exception as e:
                self.logger.error(f"Failed to fetch page 1: {e}")
                return all_urls, 0
            
            # Parse total
            total = parse_total(html_content)
            if total:
                self.logger.info(f"  Website says: {total} total results")
            
            # Extract links from first page
            page_items = extract_links(html_content, self.base_url)
            new_count = 0
            for item in page_items:
                url = item['url']
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(url)
                    all_records.append(item)
                    new_count += 1
            
            self.logger.info(f"  Page 1: {len(page_items)} links, {new_count} new")
            
            # Paginate through remaining pages
            empty_count = 0
            page_num = 1
            
            while True:
                # Check if we've collected enough
                if total and len(all_urls) >= total:
                    break
                
                # Build pagination URL
                params["page"] = str(page_num)
                url = f"{self.base_url}/zoeken?" + urlencode(params)
                
                try:
                    response = await client.get(url)
                    page_items = extract_links(response.text, self.base_url)
                    
                    new_on_page = 0
                    for item in page_items:
                        url = item['url']
                        if url not in seen_urls:
                            seen_urls.add(url)
                            all_urls.append(url)
                            all_records.append(item)
                            new_on_page += 1
                    
                    self.logger.info(f"  Page {page_num + 1}: {len(page_items)} links, {new_on_page} new, total: {len(all_urls)}")
                    
                    # Stop conditions
                    if not page_items:
                        empty_count += 1
                        if empty_count >= 3:
                            break
                    else:
                        empty_count = 0
                    
                    if total and len(all_urls) >= total:
                        break
                    
                    page_num += 1
                    
                    # Batch save every 500 URLs
                    if len(all_records) >= 500:
                        self.logger.info("  Saving batch to database...")
                        inserted = self.repo.insert_collected_urls(all_records)
                        self.logger.info(f"  Saved {inserted} URLs")
                        all_records.clear()
                    
                except Exception as e:
                    self.logger.error(f"  Error on page {page_num + 1}: {e}")
                    break
        
        return all_urls, total or 0

    async def scrape_products_concurrent(self, url_pairs: List[tuple]):
        """Scrape product details using httpx."""
        queue = asyncio.Queue()
        for u in url_pairs:
            queue.put_nowait(u)
        
        total_urls = len(url_pairs)
        progress_every = max(1, int(self.config.get("SCRAPE_PROGRESS_EVERY", "100")))
        heartbeat_seconds = max(1.0, float(self.config.get("SCRAPE_HEARTBEAT_SECONDS", "20")))
        stats = {"processed": 0, "success": 0, "failed": 0, "packs": 0}
        stats_lock = asyncio.Lock()
        last_progress_log_at = time.monotonic()

        self.logger.info(
            f"[Phase2] Starting concurrent scraping: workers={self.max_workers}, "
            f"pending={total_urls}, progress_every={progress_every}, heartbeat={heartbeat_seconds}s"
        )

        def _safe_update_status(url_id: int, status: str, error_message: str = None) -> None:
            try:
                self.repo.update_url_status(url_id, status, error_message)
            except Exception as e:
                self.logger.error(f"[Phase2] Failed to update URL status id={url_id} to {status}: {e}")

        async def _maybe_log_progress(force: bool = False):
            nonlocal last_progress_log_at
            now = time.monotonic()
            async with stats_lock:
                processed = stats["processed"]
                success = stats["success"]
                failed = stats["failed"]
                packs = stats["packs"]
                should_log = force or (processed > 0 and processed % progress_every == 0) or (now - last_progress_log_at >= heartbeat_seconds)
                if not should_log:
                    return
                last_progress_log_at = now
            remaining = max(total_urls - processed, 0)
            self.logger.info(
                f"[Phase2] Progress: processed={processed}/{total_urls}, success={success}, "
                f"failed={failed}, packs={packs}, pending~={remaining}"
            )
        
        async def worker(w_id):
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                while not queue.empty():
                    if self._shutdown_requested:
                        break
                    
                    try:
                        url, c_id = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    
                    try:
                        success_delta = 0
                        failed_delta = 0
                        packs_delta = 0
                        resp = await client.get(url)
                        
                        if resp.status_code == 200:
                            packs = self.extract_product_from_html(resp.text, url, c_id)
                            inserted = 0
                            if packs:
                                try:
                                    inserted = self.repo.insert_packs(packs, log_db=False)
                                except Exception as e:
                                    self.logger.error(f"[Phase2] DB insert failed for {url}: {e}")
                                    self.record_error("db_insert_exception")
                            
                            if packs and inserted > 0:
                                _safe_update_status(c_id, "success")
                                self.record_scraped_item(inserted, "products")
                                success_delta = 1
                                packs_delta = inserted
                            else:
                                _safe_update_status(c_id, "failed", "no_pack_data_or_db_insert_failed")
                                self.record_error("db_insert_failed")
                                failed_delta = 1
                        else:
                            self.logger.warning(f"Failed to scrape {url} (Status: {resp.status_code})")
                            _safe_update_status(c_id, "failed", f"http_{resp.status_code}")
                            self.record_error(f"http_{resp.status_code}")
                            failed_delta = 1
                    
                    except Exception as e:
                        self.logger.error(f"Worker {w_id} failed on {url}: {e}")
                        _safe_update_status(c_id, "failed", str(e))
                        self.record_error("worker_exception")
                        failed_delta = 1
                    finally:
                        async with stats_lock:
                            stats["processed"] += 1
                            stats["success"] += success_delta
                            stats["failed"] += failed_delta
                            stats["packs"] += packs_delta

                        await _maybe_log_progress()
                        queue.task_done()
                        await asyncio.sleep(self.page_delay + random.uniform(0.1, 1.0))
        
        workers = [asyncio.create_task(worker(i)) for i in range(self.max_workers)]
        worker_results = await asyncio.gather(*workers, return_exceptions=True)
        for idx, result in enumerate(worker_results):
            if isinstance(result, Exception):
                self.logger.error(f"[Phase2] Worker task {idx} crashed: {result}")
                self.record_error("worker_task_crash")

        await _maybe_log_progress(force=True)

    def extract_product_from_html(self, html_text: str, url: str, collected_url_id: int) -> List[dict]:
        """
        Extract pack-level rows from HTML.

        Rules:
        - unit_price is piece-level
        - ppp_vat/copay/deductible/reimbursement are package-level
        - local_pack_code uses available RVG/EU number from each pack block
        """
        doc = html.fromstring(html_text)
        page_title = clean_single_line(doc.xpath('string(//h1)') or doc.xpath('string(//title)'))
        pack_blocks = doc.xpath('//dl[contains(@class,"pat-grid-list")]')
        if not pack_blocks:
            pack_blocks = [doc]

        rows: List[dict] = []
        for idx, block in enumerate(pack_blocks, start=1):
            active_substance = clean_single_line(
                block.xpath('string(.//dd[contains(@class,"medicine-active-substance")])')
            )
            formulation = clean_single_line(
                block.xpath('string(.//dd[contains(@class,"medicine-method")])')
            )
            strength_size = clean_single_line(
                block.xpath('string(.//dd[contains(@class,"medicine-strength")])')
            )
            manufacturer = clean_single_line(
                block.xpath('string(.//dd[contains(@class,"medicine-manufacturer")])')
            )
            local_pack_code = clean_single_line(
                block.xpath('string(.//dd[contains(@class,"medicine-rvg-number")])')
            )
            if not local_pack_code:
                local_pack_code = ""

            price_dd = block.xpath('.//dd[contains(@class,"medicine-price")]')
            unit_price = None
            ppp_vat = None
            reimbursement_message = ""
            reimbursable_status = "Unknown"
            reimbursable_rate = ""
            copay_price = None
            copay_percent = ""

            if price_dd:
                price_node = price_dd[0]
                unit_price = get_depends_amount(price_node, "piece")
                ppp_vat = get_depends_amount(price_node, "package")
                if ppp_vat is None:
                    ppp_vat = parse_euro_amount(price_node.text_content())

                message_divs = price_node.xpath('.//div[contains(@class,"pat-message")]')
                message_texts = [clean_single_line(div.text_content()) for div in message_divs]
                message_texts = [t for t in message_texts if t]
                reimbursement_message = " ".join(message_texts).strip()
                full_text = reimbursement_message.lower()

                has_success = any("success" in (div.get("class") or "") for div in message_divs)
                has_warning = any("warning" in (div.get("class") or "") for div in message_divs)

                if has_success or "volledig vergoed" in full_text:
                    reimbursable_status = "Fully reimbursed"
                    reimbursable_rate = "100%"
                elif has_warning:
                    reimbursable_status = "Partially reimbursed"

                warning_divs = price_node.xpath('.//div[contains(@class,"pat-message") and contains(@class,"warning")]')
                for wdiv in warning_divs:
                    copay_price = get_depends_amount(wdiv, "package")
                    if copay_price is None:
                        copay_price = parse_euro_amount(wdiv.text_content())
                    if copay_price is not None:
                        pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", wdiv.text_content(), re.IGNORECASE)
                        if pct:
                            copay_percent = f"{pct.group(1)}%"
                        break

            deductible = None
            deductible_nodes = block.xpath('.//dt[contains(@class,"not-reimbursed")]/following-sibling::dd[1]')
            if deductible_nodes:
                dnode = deductible_nodes[0]
                deductible = get_depends_amount(dnode, "package")
                if deductible is None:
                    deductible = parse_euro_amount(dnode.text_content())
                if deductible is None and "niets" in clean_single_line(dnode.text_content()).lower():
                    deductible = 0.0

            pack_desc_parts = [page_title, formulation, strength_size]
            local_pack_description = clean_single_line(" ".join([p for p in pack_desc_parts if p]))
            if not local_pack_description:
                local_pack_description = page_title

            if local_pack_description:
                first_word = local_pack_description.split()[0] if local_pack_description.split() else ""
                first_word = first_word.rstrip(",.;:()[]{}")
                product_group = first_word
            else:
                product_group = ""

            if not any([local_pack_description, active_substance, formulation, strength_size, local_pack_code]):
                continue

            ppp_ex_vat = (ppp_vat / 1.09) if ppp_vat is not None else None

            rows.append({
                'source_url': url,
                'collected_url_id': collected_url_id,
                'currency': 'EUR',
                'vat_percent': 9.0,
                'margin_rule': self.margin_rule,
                'start_date': date.today(),
                'end_date': None,
                'reimbursable_status': reimbursable_status,
                'reimbursable_rate': reimbursable_rate,
                'copay_price': copay_price,
                'copay_percent': copay_percent,
                'deductible': deductible,
                'ri_with_vat': deductible,
                'ppp_vat': ppp_vat,
                'ppp_ex_vat': ppp_ex_vat,
                'unit_price': unit_price,
                'product_group': product_group,
                'local_pack_description': local_pack_description,
                'active_substance': active_substance,
                'manufacturer': manufacturer,
                'formulation': formulation,
                'strength_size': strength_size,
                'local_pack_code': local_pack_code,
                'reimbursement_message': reimbursement_message,
            })

        return rows


if __name__ == "__main__":
    scraper = NetherlandsScraper()
    scraper.run()

