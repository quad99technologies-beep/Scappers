#!/usr/bin/env python3
"""
Netherlands Scraper - Selenium Edition
Uses Selenium for reliable URL collection and product scraping.
"""

import sys
import os
from pathlib import Path

# ---- Path wiring ----
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

_REPO_ROOT = _repo_root

import asyncio
import re
import time
import random
from datetime import date
from typing import List, Dict, Optional, Set
from urllib.parse import urlparse, parse_qs, quote, urlencode

import httpx
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException

from core.pipeline.base_scraper import BaseScraper
from db.repositories import NetherlandsRepository

# Text helpers
def clean_single_line(text: str) -> str:
    t = (text or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()

def first_euro_amount(text: str) -> str:
    if not text:
        return ""
    t = clean_single_line(text)
    m = re.search(r"[€]\s*\d[\d\.\,]*", t)
    if m:
        return "€" + m.group(0).strip()[1:].strip()
    return ""

def parse_total(page_html: str) -> Optional[int]:
    doc = html.fromstring(page_html)
    txt = doc.xpath('string(//*[@id="summary"]//strong)').strip()
    if not txt:
        return None
    compact = txt.replace(".", "").replace(" ", "")
    return int(compact) if compact.isdigit() else None


class NetherlandsSeleniumScraper(BaseScraper):
    def __init__(self, run_id: Optional[str] = None):
        if not run_id:
            run_id = os.environ.get("NL_RUN_ID")
        super().__init__("Netherlands", run_id)
        
        # Config
        self.base_url = self.config.get("BASE_URL", "https://www.medicijnkosten.nl")
        self.search_keyword = self.config.get("SEARCH_TERM", "632 Medicijnkosten Drugs4")
        self.margin_rule = self.config.get("MARGIN_RULE", "632 Medicijnkosten Drugs4")
        self.max_workers = int(self.config.get("MAX_WORKERS", "1"))
        self.page_delay = float(self.config.get("PAGE_DELAY", "2.0"))
        
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

    def create_driver(self) -> webdriver.Chrome:
        """Create a new Chrome driver instance."""
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Additional options to avoid detection
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-images")
        chrome_options.add_argument("--js-flags=--max-old-space-size=4096")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            '''
        })
        
        # Set cookie consent
        driver.get(self.base_url)
        time.sleep(2)
        driver.add_cookie({'name': 'cookieconsent', 'value': 'true', 'domain': '.medicijnkosten.nl'})

        # Track PIDs in DB for pipeline stop cleanup
        try:
            from core.browser.chrome_pid_tracker import get_chrome_pids_from_driver
            from core.browser.chrome_instance_tracker import ChromeInstanceTracker
            from core.db.postgres_connection import PostgresDB
            run_id = getattr(self, 'run_id', None)
            pids = get_chrome_pids_from_driver(driver)
            if pids and run_id:
                driver_pid = driver.service.process.pid if hasattr(driver.service, 'process') else list(pids)[0]
                db = PostgresDB("Netherlands")
                db.connect()
                try:
                    tracker = ChromeInstanceTracker("Netherlands", run_id, db)
                    tracker.register(step_number=1, pid=driver_pid, browser_type="chrome", child_pids=pids)
                finally:
                    db.close()
        except Exception:
            pass

        return driver

    def run(self):
        """Main entry point."""
        self.logger.info("Starting Netherlands Selenium Scraper")
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
        proxy = self.get_proxy(self.base_url)
        proxy_url = proxy.url if proxy else None
        
        # 1. Collect URLs
        self.logger.info("Phase 1: URL Collection")
        
        existing_urls = self.repo.get_collected_url_keys()
        self.logger.info(f"Loaded {len(existing_urls)} existing URLs to skip")
        
        collected_urls = await self.collect_urls_via_selenium(existing_urls)
        
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
            await self.scrape_products_concurrent(url_pairs, proxy_url)
            

        
        self.logger.info("Scraping finished")

    def extract_results_from_html(self, html_content: str) -> List[Dict]:
        """Extract product records from search results HTML."""
        doc = html.fromstring(html_content)
        results = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]')
        out = []
        for a in results:
            href = a.get("href", "")
            if not href or not href.startswith("/medicijn?"):
                continue
            if "pagenotfound" in href.lower() or href == "/medicijn?":
                continue
            
            url = self.base_url + href
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

    async def collect_urls_via_selenium(self, existing_urls: set) -> List[Dict]:
        """Collect URLs by iterating Strengths using Selenium."""
        self.logger.info("Starting Selenium URL Collection (Strategy: Strengths Global)...")
        
        seen_urls: Dict[str, Dict] = {}
        all_collected: List[Dict] = []
        
        # Browser restart settings
        BROWSER_RESTART_EVERY = 10  # Restart browser after every 10 strengths
        
        driver = self.create_driver()
        
        try:
            q_term = quote(self.search_keyword)
            filter_url = (f"{self.base_url}/zoeken?"
                          f"searchTerm={q_term}&"
                          f"type=medicine&"
                          f"searchTermHandover={q_term}&"
                          f"vorm=Alle%20vormen&"
                          f"sterkte=Alle%20sterktes")
            
            self.logger.info(f"Navigating to extract Strengths: {filter_url}")
            
            # Load page and extract strengths
            driver.get(filter_url)
            time.sleep(5)  # Wait longer for page to fully load including AJAX
            
            # Extract global count
            try:
                calculated_total = parse_total(driver.page_source)
                if calculated_total:
                    self.logger.info(f"GLOBAL TOTAL FOUND: {calculated_total}")
                    self.last_expected_total = calculated_total
            except Exception as e:
                self.logger.warning(f"Could not extract global count: {e}")
            
            # Click cookie consent if present
            try:
                cookie_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Akkoord')]")
                cookie_btn.click()
                time.sleep(0.5)
            except:
                pass
            
            # Extract strength options
            sterktes = []
            try:
                # Wait for the strength select to be present
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.NAME, "sterkte"))
                )
                
                # Try to open filters panel if needed
                try:
                    open_trigger = driver.find_element(By.ID, "open-trigger")
                    if open_trigger.is_displayed():
                        open_trigger.click()
                        time.sleep(1)
                except:
                    pass
                
                # Get strength options
                select = driver.find_element(By.NAME, "sterkte")
                options = select.find_elements(By.TAG_NAME, "option")
                sterktes = [opt.text.strip() for opt in options if opt.text.strip() and "Alle" not in opt.text]
                
                # If still no strengths, try parsing from HTML directly
                if not sterktes:
                    doc = html.fromstring(driver.page_source)
                    options = doc.xpath('//select[@name="sterkte"]/option/text()')
                    sterktes = [opt.strip() for opt in options if opt.strip() and "Alle" not in opt]
                    
            except Exception as e:
                self.logger.error(f"Failed to extract strengths: {e}")
                # Try parsing from HTML as fallback
                try:
                    doc = html.fromstring(driver.page_source)
                    options = doc.xpath('//select[@name="sterkte"]/option/text()')
                    sterktes = [opt.strip() for opt in options if opt.strip() and "Alle" not in opt]
                except Exception as e2:
                    self.logger.error(f"HTML parsing fallback also failed: {e2}")
                    return all_collected
            
            self.logger.info(f"Found {len(sterktes)} strengths global.")
            
            if not sterktes:
                self.logger.error("No strengths found - cannot continue")
                # Save page source for debugging
                try:
                    with open("debug_no_strengths.html", "w", encoding="utf-8") as f:
                        f.write(driver.page_source)
                    self.logger.info("Saved debug_no_strengths.html for debugging")
                except:
                    pass
                return all_collected
            
            # Process each strength
            total_sterktes = len(sterktes)
            for i, sterkte in enumerate(sterktes, 1):
                # Restart browser periodically
                if i > 1 and (i - 1) % BROWSER_RESTART_EVERY == 0:
                    self.logger.info(f"Restarting browser after {BROWSER_RESTART_EVERY} strengths...")
                    try:
                        driver.quit()
                    except:
                        pass
                    time.sleep(2)
                    driver = self.create_driver()
                    self.logger.info("Browser restarted successfully")
                
                self.logger.info(f"--- Strength {i}/{total_sterktes}: {sterkte} ---")
                
                # Small delay between strengths
                if i > 1:
                    time.sleep(0.5)
                
                s_url = (f"{self.base_url}/zoeken?"
                         f"searchTerm={q_term}&"
                         f"type=medicine&"
                         f"searchTermHandover={q_term}&"
                         f"vorm=Alle%20vormen&"
                         f"sterkte={quote(sterkte)}")
                
                # Insert parent combination
                parent_combo_id = self.repo.insert_combination("Alle vormen", sterkte, s_url)
                
                try:
                    # Navigate to strength page
                    driver.get(s_url)
                    time.sleep(2)  # Wait for AJAX content
                    
                    # Wait for results to load
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "a.result-item.medicine"))
                        )
                    except TimeoutException:
                        self.logger.warning(f"No results found for {sterkte}")
                        continue
                    
                    # Get count
                    website_count = 0
                    try:
                        count_text = driver.find_element(By.CSS_SELECTOR, "#summary strong").text
                        if count_text.strip().isdigit():
                            website_count = int(count_text.strip())
                        else:
                            match = re.search(r'(\d+)\s*zoekresultaten', driver.page_source, re.IGNORECASE)
                            if match:
                                website_count = int(match.group(1))
                        self.logger.info(f"Website says: {website_count} results for {sterkte}")
                    except Exception as e:
                        self.logger.warning(f"Count extraction warning: {e}")
                    
                    # Process pages
                    collected_count, inserted_count = self.process_strength_pages(
                        driver, s_url, sterkte, existing_urls, seen_urls, all_collected
                    )
                    
                    # Update DB
                    if parent_combo_id:
                        self.repo.mark_combination_completed(
                            parent_combo_id,
                            products_found=website_count,
                            urls_discovered=collected_count,
                            urls_fetched=collected_count,
                            urls_inserted=inserted_count,
                            urls_collected=collected_count
                        )
                    
                except Exception as e:
                    self.logger.error(f"Error processing strength {sterkte}: {e}")
                    if parent_combo_id:
                        self.repo.mark_combination_failed(parent_combo_id, str(e))
                    
        finally:
            try:
                driver.quit()
            except:
                pass
        
        return all_collected

    def process_strength_pages(self, driver, s_url: str, sterkte: str, 
                               existing_urls: set, seen_urls: Dict, 
                               all_collected: List[Dict]) -> tuple[int, int]:
        """Process all pages for a strength."""
        total_collected = 0
        total_inserted = 0
        page_num = 1
        consecutive_empty = 0
        
        while True:
            try:
                # Extract items from current page
                html_content = driver.page_source
                links_data = self.extract_results_from_html(html_content)
                
                new_count = 0
                for item in links_data:
                    url = item['url']
                    if url not in existing_urls and url not in seen_urls:
                        seen_urls[url] = item
                        new_count += 1
                
                total_collected += new_count
                self.logger.info(f"  Strength [{sterkte}] Pg {page_num}: Found {len(links_data)} items, {new_count} new")
                
                if not links_data:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        break
                else:
                    consecutive_empty = 0
                
                # Check for next page
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, "a.next[rel='next']")
                    if "disabled" in next_btn.get_attribute("class"):
                        break
                    next_btn.click()
                    page_num += 1
                    time.sleep(1.5)  # Wait for next page to load
                except:
                    # No next button or disabled - check if we can find page links
                    try:
                        # Try to find and click next page number
                        current_page = driver.find_element(By.CSS_SELECTOR, ".pagination .current")
                        next_page_num = int(current_page.text) + 1
                        next_page_link = driver.find_element(By.XPATH, f"//a[@class='page-link' and text()='{next_page_num}']")
                        next_page_link.click()
                        page_num += 1
                        time.sleep(1.5)
                    except:
                        break
                
                # Batch save
                if len(seen_urls) >= 500:
                    batch = list(seen_urls.values())
                    inserted = self.repo.insert_collected_urls(batch)
                    all_collected.extend(batch)
                    seen_urls.clear()
                    total_inserted += inserted
                    self.logger.info(f"  Saved batch: {inserted} inserted")
                    
            except Exception as e:
                self.logger.error(f"  Error on page {page_num}: {e}")
                break
        
        # Final save
        if seen_urls:
            batch = list(seen_urls.values())
            inserted = self.repo.insert_collected_urls(batch)
            all_collected.extend(batch)
            total_inserted += inserted
            self.logger.info(f"  Saved final batch: {inserted} inserted")
        
        return total_collected, total_inserted

    async def scrape_products_concurrent(self, url_pairs: List[tuple], proxy_url: str):
        """Scrape product details using httpx."""
        queue = asyncio.Queue()
        for u in url_pairs:
            queue.put_nowait(u)
            
        async def worker(w_id):
            async with httpx.AsyncClient(proxy=proxy_url, timeout=30.0, follow_redirects=True) as client:
                while not queue.empty():
                    if self._shutdown_requested:
                        break
                    
                    try:
                        url, c_id = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                        
                    try:
                        resp = await self._fetch_url(client, url)
                        
                        if resp and resp.status_code == 200:
                            data = self.extract_product_from_html(resp.text, url, c_id)
                            if self.repo.insert_packs([data]):
                                self.record_scraped_item(1, "products")
                            else:
                                self.record_error("db_insert_failed")
                        else:
                            code = resp.status_code if resp else "Error"
                            self.logger.warning(f"Failed to scrape {url} (Status: {code})")
                            self.record_error(f"http_{code}")
                            
                    except Exception as e:
                        self.logger.error(f"Worker {w_id} failed on {url}: {e}")
                        self.record_error("worker_exception")
                    finally:
                        queue.task_done()
                        await asyncio.sleep(self.page_delay + random.uniform(0.1, 1.0))

        workers = [worker(i) for i in range(self.max_workers)]
        await asyncio.gather(*workers)

    async def _fetch_url(self, client: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
        """Fetch URL with robust 429 backoff handling."""
        base_backoff = 10.0
        max_retries = 5
        
        for attempt in range(max_retries + 1):
            is_last = (attempt == max_retries)
            try:
                t0 = time.time()
                resp = await client.get(url)
                dur = (time.time()-t0)*1000
                
                if resp.status_code == 429:
                    if is_last:
                        self.logger.error(f"429 Rate Limit persisted after {max_retries} retries for {url}")
                        self.record_request_metric(429, dur, "GET")
                        return resp
                    
                    sleep_time = float(base_backoff * (2 ** attempt)) + random.uniform(1.0, 5.0)
                    self.logger.warning(f"Hit 429 on {url}. Retrying in {sleep_time:.1f}s (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(sleep_time)
                    continue
                
                self.record_request_metric(resp.status_code, dur, "GET")
                return resp
                
            except Exception as e:
                self.logger.error(f"Request error {url}: {e}")
                if is_last:
                    self.record_error("request_failed_final")
                    return None
                await asyncio.sleep(5)
                
        return None

    def extract_product_from_html(self, html_text: str, url: str, collected_url_id: int) -> dict:
        """Extract product data from HTML."""
        doc = html.fromstring(html_text)
        data = {
            'source_url': url,
            'collected_url_id': collected_url_id,
            'currency': 'EUR',
            'vat_percent': 9.0,
            'margin_rule': self.margin_rule,
            'start_date': date.today(),
            'end_date': None,
            'reimbursable_rate': '',
            'copay_price': '',
            'copay_percent': '',
            'deductible': '',
            'ri_with_vat': '',
        }
        
        local_pack = clean_single_line(doc.xpath('string(//h1)') or doc.xpath('string(//title)'))
        data['local_pack_description'] = local_pack
        data['product_group'] = local_pack.split()[0] if local_pack else ""
        data['active_substance'] = clean_single_line(doc.xpath('string(//dd[contains(@class,"medicine-active-substance")])'))
        data['manufacturer'] = clean_single_line(doc.xpath('string(//dd[contains(@class,"medicine-manufacturer")])'))
        data['local_pack_code'] = clean_single_line(doc.xpath('string(//dd[contains(@class,"medicine-rvg-number")])'))
        
        unit_price = ""
        ppp_vat = ""
        
        price_dd = doc.xpath('//dd[contains(@class,"medicine-price")]')
        if price_dd:
            pd = price_dd[0]
            pkg = pd.xpath('.//span[@data-pat-depends="inline-days=package"]')
            if pkg:
                ppp_vat = first_euro_amount(pkg[0].text_content())
            
        data['ppp_vat'] = ppp_vat
        data['unit_price'] = unit_price
        
        return data


if __name__ == "__main__":
    scraper = NetherlandsSeleniumScraper()
    scraper.run()
