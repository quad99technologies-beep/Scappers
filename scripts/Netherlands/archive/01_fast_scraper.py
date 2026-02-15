# -*- coding: utf-8 -*-
"""
Netherlands Fast Scraper — httpx + lxml (URL Collection + Product Scraping)

Features:
  - Optional Tor SOCKS5 proxy (TOR_ENABLED=1) with NEWNYM rotation every 12 min
  - Rate limiting: max 200 requests/min (configurable)
  - Periodic DB saves every batch (crash-safe)
  - Retry pass: failed URLs retried after main pass completes
  - Resume: skips already-scraped URLs from prior runs

Requires: pip install httpx[socks] lxml playwright
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote_plus, urlencode

import httpx
from lxml import html
from playwright.async_api import async_playwright

# ---- Path wiring ----
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

# ---- Database ----
try:
    from db.repositories import NetherlandsRepository
    from core.db.postgres_connection import get_db
except ImportError:
    print("[ERROR] Database not available")
    sys.exit(1)

# ---- Tor proxy ----
try:
    from core.network.tor_httpx import TorConfig, setup_tor, TorRotator, AsyncRateLimiter
    TOR_AVAILABLE = True
except ImportError:
    TOR_AVAILABLE = False

# ---- Config loader ----
try:
    from config_loader import getenv as _cfg_getenv
    _TOR_GETENV = _cfg_getenv
except ImportError:
    _TOR_GETENV = None

def _get_config(key: str, default: str = "") -> str:
    """Get config value from config_loader or return default."""
    if _TOR_GETENV:
        return _TOR_GETENV(key, default)
    return default

# ---- Constants ----
MARGIN_RULE = _get_config("MARGIN_RULE", "632 Medicijnkosten Drugs4")
SEARCH_KEYWORD = _get_config("SEARCH_TERM", "632 Medicijnkosten Drugs4")  # keyword used on the website search
BASE_URL = _get_config("BASE_URL", "https://www.medicijnkosten.nl")
MAX_WORKERS = int(_get_config("MAX_WORKERS", "10"))  # Reduced to avoid overwhelming the server
BATCH_SIZE = int(_get_config("BATCH_SIZE", "100"))
MAX_REQ_PER_MIN = int(_get_config("MAX_REQ_PER_MIN", "150"))  # Reduced rate limit
PAGE_DELAY = float(_get_config("PAGE_DELAY", "1.5"))  # Increased delay between pagination requests


# =====================================================================
# Text helpers (same logic as original Selenium scraper)
# =====================================================================

def clean_single_line(text: str) -> str:
    t = (text or "").replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", t).strip()


def first_euro_amount(text: str) -> str:
    if not text:
        return ""
    t = clean_single_line(text)
    # Try to match euro symbol (€) or corrupted version (�) followed by number
    m = re.search(r"[€�]\s*\d[\d\.\,]*", t)
    if m:
        # Return with proper euro symbol
        return "€" + m.group(0).strip()[1:].strip()
    return ""


def parse_total(page_html: str) -> int | None:
    doc = html.fromstring(page_html)
    txt = doc.xpath('string(//*[@id="summary"]//strong)').strip()
    return int(txt) if txt.isdigit() else None


def extract_results(fragment_html: str, base: str = BASE_URL) -> list[dict]:
    """Extract product records from search results HTML."""
    doc = html.fromstring(fragment_html)
    results = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]')
    out = []
    for a in results:
        href = a.get("href", "")
        # Skip invalid hrefs
        if not href or not href.startswith("/medicijn?"):
            continue
        # Skip known invalid patterns
        if "pagenotfound" in href.lower() or href == "/medicijn?":
            continue
        url = base + href
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


def extract_vorm_options(page_html: str) -> list[str]:
    """Extract vorm (dosage form) filter options from search results page."""
    doc = html.fromstring(page_html)
    options = doc.xpath('//select[@name="vorm"]/option/text()')
    return [opt.strip() for opt in options if opt.strip() and opt.strip() != "Alle vormen"]


# =====================================================================
# Phase 1: URL collection via vorm filters (bypasses 3100 cap)
# =====================================================================

async def collect_urls_via_vorm_filters(base_search_url: str, cookies: list, proxy_url: str,
                                         repo, existing_urls: set, run_id: str) -> list[dict]:
    """
    Collect all product URLs by splitting search by vorm (dosage form) filters.
    This bypasses the ~3100 pagination cap by making multiple smaller queries.
    
    Args:
        base_search_url: The main search URL (used to extract vorm options)
        cookies: Session cookies
        proxy_url: Tor proxy URL
        repo: NetherlandsRepository instance
        existing_urls: Set of URLs already collected
        run_id: Current run ID
        
    Returns:
        List of collected URL records
    """
    print(f"\n[HTTPX] Starting URL collection via vorm filters")
    
    jar = httpx.Cookies()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    
    # First, get the vorm options from the base search page
    async with httpx.AsyncClient(
        cookies=jar,
        headers={
            "Accept": "text/html",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
        },
        timeout=45.0,
        follow_redirects=True,
        proxy=proxy_url,
    ) as client:
        resp = await client.get(base_search_url)
        vormen = extract_vorm_options(resp.text)
        overall_total = parse_total(resp.text)
        print(f"[HTTPX] Overall product count: {overall_total or 'unknown'}")
        print(f"[HTTPX] Found {len(vormen)} dosage-form filters: {', '.join(vormen[:5])}{'...' if len(vormen) > 5 else ''}")
    
    if not vormen:
        print("[HTTPX] Could not extract vorm filters - falling back to single search")
        vormen = ["Alle vormen"]
    
    all_records: list[dict] = []
    seen_urls: dict[str, dict] = {}
    last_save_count = 0
    save_interval = 500
    
    search_kw_encoded = quote_plus(SEARCH_KEYWORD)
    
    for i, vorm in enumerate(vormen, 1):
        print(f"\n[HTTPX] Processing vorm {i}/{len(vormen)}: {vorm}")
        
        vorm_url = (
            f"{BASE_URL}/zoeken?searchTerm={search_kw_encoded}"
            f"&type=medicine&searchTermHandover={search_kw_encoded}"
            f"&vorm={quote_plus(vorm)}&sterkte=Alle%20sterktes"
        )
        
        async with httpx.AsyncClient(
            cookies=jar,
            headers={
                "Accept": "text/html",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
            },
            timeout=45.0,
            follow_redirects=True,
            proxy=proxy_url,
        ) as client:
            # Get first page
            response = await client.get(vorm_url)
            total = parse_total(response.text)
            print(f"[HTTPX]   Total for {vorm}: {total or 'unknown'}")
            
            vorm_records: dict[str, dict] = {}
            for rec in extract_results(response.text):
                if rec['url'] not in existing_urls and rec['url'] not in seen_urls:
                    vorm_records[rec['url']] = rec
            
            # Paginate if needed
            page_num = 1
            empty_count = 0
            max_pages = 50  # Safety limit
            
            while page_num < max_pages:
                page_num += 1
                params = {
                    "page": str(page_num),
                    "searchTerm": SEARCH_KEYWORD,
                    "vorm": vorm,
                    "sterkte": "Alle sterktes",
                    "sorting": "",
                    "debugMode": "",
                }
                page_url = f"{BASE_URL}/zoeken?" + urlencode(params)
                
                await asyncio.sleep(PAGE_DELAY)
                
                try:
                    resp = await client.get(page_url)
                    resp.raise_for_status()
                except Exception as e:
                    print(f"[HTTPX]   Page {page_num} error: {e}")
                    break
                
                records = extract_results(resp.text)
                new_count = 0
                for r in records:
                    if r['url'] not in vorm_records and r['url'] not in existing_urls and r['url'] not in seen_urls:
                        vorm_records[r['url']] = r
                        new_count += 1
                
                if new_count == 0:
                    empty_count += 1
                    if empty_count >= 3:
                        break
                else:
                    empty_count = 0
                
                if total and len(vorm_records) >= total:
                    break
            
            print(f"[HTTPX]   Collected {len(vorm_records)} URLs for {vorm}")
            
            # Add to global collection
            for url, rec in vorm_records.items():
                seen_urls[url] = rec
            
            # Incremental save
            if len(seen_urls) - last_save_count >= save_interval:
                print(f"[HTTPX] Saving {len(seen_urls)} total URLs to database...")
                repo.insert_collected_urls(list(seen_urls.values()))
                last_save_count = len(seen_urls)
                all_records.extend(seen_urls.values())
                seen_urls.clear()
    
    # Final save
    if seen_urls:
        print(f"[HTTPX] Final save: {len(seen_urls)} URLs")
        repo.insert_collected_urls(list(seen_urls.values()))
        all_records.extend(seen_urls.values())
    
    print(f"\n[HTTPX] URL collection complete: {len(all_records)} total unique URLs")
    return all_records


async def collect_urls_via_playwright_scroll(search_url: str, proxy_url: str,
                                              repo, existing_urls: set, 
                                              max_scrolls: int = 1000) -> list[dict]:
    """
    Collect URLs using Playwright with real browser infinite scrolling.
    This mimics manual scrolling and can collect more URLs than httpx pagination.
    
    Args:
        search_url: The main search URL
        proxy_url: Tor proxy URL (optional)
        repo: NetherlandsRepository instance
        existing_urls: Set of URLs already collected
        max_scrolls: Maximum scroll attempts
        
    Returns:
        List of collected URL records
    """
    print(f"\n[PLAYWRIGHT] Starting URL collection via real browser scrolling")
    print(f"[PLAYWRIGHT] Search URL: {search_url}")
    
    from playwright.async_api import async_playwright
    
    seen_urls: dict[str, dict] = {}
    last_save_count = 0
    save_interval = 500
    
    async with async_playwright() as p:
        # Launch browser with optional proxy
        browser_args = {}
        if proxy_url:
            browser_args['proxy'] = {'server': proxy_url}
        
        browser = await p.chromium.launch(headless=True, **browser_args)
        
        try:
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            page = await context.new_page()
            
            # Navigate to search page
            print(f"[PLAYWRIGHT] Loading search page...")
            await page.goto(search_url, wait_until='networkidle', timeout=60000)
            
            # Wait for results to load
            await page.wait_for_selector('a.result-item.medicine', timeout=30000)
            
            # Give initial load time
            await asyncio.sleep(2)
            
            scroll_count = 0
            no_change_count = 0
            last_url_count = 0
            
            while scroll_count < max_scrolls:
                # Extract current URLs from page
                links = await page.eval_on_selector_all('a.result-item.medicine', '''
                    elements => elements.map(a => ({
                        href: a.getAttribute('href'),
                        title: a.querySelector('.result-title')?.textContent?.trim() || '',
                        activeSubstance: a.querySelector('.active-substance')?.textContent?.trim() || '',
                        manufacturer: a.querySelector('.manufacturer')?.textContent?.trim() || '',
                        documentType: a.querySelector('.document-type')?.textContent?.trim() || '',
                        price: a.querySelector('.price')?.textContent?.trim() || '',
                        reimbursement: a.querySelector('.reimbursement')?.textContent?.trim() || ''
                    }))
                ''')
                
                new_count = 0
                for link in links:
                    href = link.get('href', '')
                    if not href or not href.startswith('/medicijn?'):
                        continue
                    if 'pagenotfound' in href.lower() or href == '/medicijn?':
                        continue
                    
                    url = BASE_URL + href
                    if url not in seen_urls and url not in existing_urls:
                        seen_urls[url] = {
                            'url': url,
                            'url_with_id': url,
                            'title': link.get('title', ''),
                            'active_substance': link.get('activeSubstance', ''),
                            'manufacturer': link.get('manufacturer', ''),
                            'document_type': link.get('documentType', ''),
                            'price_text': link.get('price', ''),
                            'reimbursement': link.get('reimbursement', ''),
                            'prefix': 'all_products',
                        }
                        new_count += 1
                
                current_count = len(seen_urls)
                
                # Print progress every 10 scrolls
                if scroll_count % 10 == 0 or new_count > 0:
                    print(f"[PLAYWRIGHT] Scroll {scroll_count}: {current_count} URLs total, {new_count} new")
                
                # Check if we've stopped getting new URLs
                if current_count == last_url_count:
                    no_change_count += 1
                    if no_change_count >= 10:
                        print(f"[PLAYWRIGHT] No new URLs for {no_change_count} scrolls, stopping")
                        break
                else:
                    no_change_count = 0
                
                last_url_count = current_count
                
                # Incremental save
                if current_count - last_save_count >= save_interval:
                    print(f"[PLAYWRIGHT] Saving {current_count} URLs to database...")
                    repo.insert_collected_urls(list(seen_urls.values()))
                    last_save_count = current_count
                    seen_urls.clear()
                
                # Scroll down to load more
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                # Wait for new content to load
                await asyncio.sleep(1.5)
                
                # Try to find and click "Load more" button if exists
                try:
                    load_more = await page.query_selector('button:has-text("Meer resultaten"):visible, button.load-more:visible')
                    if load_more:
                        await load_more.click()
                        await asyncio.sleep(1)
                except:
                    pass
                
                scroll_count += 1
            
            print(f"[PLAYWRIGHT] Scrolling complete after {scroll_count} scrolls, {len(seen_urls)} URLs in buffer")
            
        finally:
            await browser.close()
    
    # Final save
    if seen_urls:
        print(f"[PLAYWRIGHT] Final save: {len(seen_urls)} URLs")
        repo.insert_collected_urls(list(seen_urls.values()))
    
    print(f"\n[PLAYWRIGHT] URL collection complete: {last_save_count + len(seen_urls)} total unique URLs")
    return list(seen_urls.values())


async def collect_urls_via_httpx(search_url: str, cookies: list, proxy_url: str,
                                  repo, existing_urls: set, max_urls: int = 50000) -> list[dict]:
    """
    Collect URLs using httpx pagination (fast but may hit server-side limits).
    
    Args:
        search_url: The main search URL
        cookies: Session cookies
        proxy_url: Tor proxy URL
        repo: NetherlandsRepository instance
        existing_urls: Set of URLs already collected
        max_urls: Maximum URLs to collect (pagination cap)
        
    Returns:
        List of collected URL records
    """
    print(f"\n[HTTPX] Starting URL collection via HTTP pagination")
    print(f"[HTTPX] Search URL: {search_url}")
    print(f"[HTTPX] Max URLs: {max_urls}")
    
    jar = httpx.Cookies()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    
    seen_urls: dict[str, dict] = {}
    last_save_count = 0
    save_interval = 500
    
    async with httpx.AsyncClient(
        cookies=jar,
        headers={
            "Accept": "text/html",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": search_url,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
        },
        timeout=45.0,
        follow_redirects=True,
        proxy=proxy_url,
    ) as client:
        # Get first page
        response = await client.get(search_url)
        total = parse_total(response.text)
        print(f"[HTTPX] Total expected: {total or 'unknown'}")
        
        for rec in extract_results(response.text):
            if rec['url'] not in existing_urls:
                seen_urls[rec['url']] = rec
        
        print(f"[HTTPX] Page 1: {len(seen_urls)} URLs")
        
        # Paginate through remaining pages
        page_num = 1
        empty_count = 0
        
        while len(seen_urls) < max_urls:
            params = {
                "page": str(page_num),
                "searchTerm": SEARCH_KEYWORD,
                "vorm": "Alle vormen",
                "sterkte": "Alle sterktes",
                "sorting": "",
                "debugMode": "",
            }
            page_url = f"{BASE_URL}/zoeken?" + urlencode(params)
            
            await asyncio.sleep(PAGE_DELAY)
            
            try:
                resp = await client.get(page_url)
                resp.raise_for_status()
            except Exception as e:
                print(f"[HTTPX] Page {page_num} error: {e}")
                break
            
            records = extract_results(resp.text)
            new_count = 0
            for r in records:
                if r['url'] not in seen_urls and r['url'] not in existing_urls:
                    seen_urls[r['url']] = r
                    new_count += 1
            
            print(f"[HTTPX] Page {page_num}: {len(records)} links, {new_count} new, total: {len(seen_urls)}")
            
            # Incremental save
            if len(seen_urls) - last_save_count >= save_interval:
                print(f"[HTTPX] Saving {len(seen_urls)} URLs to database...")
                repo.insert_collected_urls(list(seen_urls.values()))
                last_save_count = len(seen_urls)
                seen_urls.clear()
            
            if new_count == 0:
                empty_count += 1
                if empty_count >= 3:
                    break
            else:
                empty_count = 0
            
            page_num += 1
            
            if total and len(seen_urls) >= total:
                break
    
    # Final save
    if seen_urls:
        print(f"[HTTPX] Final save: {len(seen_urls)} URLs")
        repo.insert_collected_urls(list(seen_urls.values()))
    
    print(f"[HTTPX] URL collection complete: {len(seen_urls)} total unique URLs")
    return list(seen_urls.values())


# =====================================================================
# Product extraction from HTML
# =====================================================================

def extract_product_from_html(html_text: str, url: str, collected_url_id: int = None) -> dict:
    """Extract product data from raw HTML using lxml. All data is server-rendered."""
    doc = html.fromstring(html_text)

    data = {
        'source_url': url,
        'collected_url_id': collected_url_id,
        'currency': 'EUR',
        'vat_percent': 9.0,
        'margin_rule': MARGIN_RULE,
        'start_date': date.today(),
        'end_date': None,
        'reimbursable_rate': '',
        'copay_price': '',
        'copay_percent': '',
        'deductible': '',
        'ri_with_vat': '',  # RI (Reimbursement Indicator) with VAT = deductible for package
    }

    # Basic fields
    local_pack_description = clean_single_line(
        doc.xpath('string(//h1)') or doc.xpath('string(//title)'))
    data['local_pack_description'] = local_pack_description
    
    # Product Group = first word of Local Pack Description
    # Handle exceptions like multi-word product names if needed
    if local_pack_description:
        # Split by space and take first word, remove any trailing punctuation
        first_word = local_pack_description.split()[0] if local_pack_description.split() else ""
        # Remove common punctuation from end
        first_word = first_word.rstrip(',.;:()[]{}')
        data['product_group'] = first_word
    else:
        data['product_group'] = ""
    
    data['active_substance'] = clean_single_line(
        doc.xpath('string(//dd[contains(@class,"medicine-active-substance")])'))
    data['formulation'] = clean_single_line(
        doc.xpath('string(//dd[contains(@class,"medicine-method")])'))
    data['strength_size'] = clean_single_line(
        doc.xpath('string(//dd[contains(@class,"medicine-strength")])'))
    data['manufacturer'] = clean_single_line(
        doc.xpath('string(//dd[contains(@class,"medicine-manufacturer")])'))
    data['local_pack_code'] = clean_single_line(
        doc.xpath('string(//dd[contains(@class,"medicine-rvg-number")])'))

    # Prices (both piece & package in HTML simultaneously)
    price_dd = doc.xpath('//dd[contains(@class,"medicine-price")]')
    ppp_vat = ""
    unit_price = ""
    if price_dd:
        pd = price_dd[0]
        pkg = pd.xpath('.//span[@data-pat-depends="inline-days=package"]')
        pce = pd.xpath('.//span[@data-pat-depends="inline-days=piece"]')
        if pkg:
            ppp_vat = first_euro_amount(pkg[0].text_content())
        if pce:
            unit_price = first_euro_amount(pce[0].text_content())
        if not ppp_vat:
            fb = pd.xpath('.//span[contains(@class,"pat-depends")]')
            if fb:
                ppp_vat = first_euro_amount(fb[0].text_content())

    data['ppp_vat'] = ppp_vat
    data['unit_price'] = unit_price
    if unit_price and ppp_vat and unit_price == ppp_vat:
        data['unit_price'] = ""

    # PPP ex-VAT
    data['ppp_ex_vat'] = ""
    if ppp_vat:
        try:
            p_str = ppp_vat.replace('€', '').replace('.', '').replace(',', '.').strip()
            data['ppp_ex_vat'] = f"€ {float(p_str) / 1.09:.2f}".replace('.', ',')
        except (ValueError, ZeroDivisionError):
            pass

    # Reimbursement
    banners = doc.xpath('//dd[contains(@class,"medicine-price")]//div[contains(@class,"pat-message")]')
    banner_texts = [clean_single_line(b.text_content()) for b in banners]
    banner_texts = [t for t in banner_texts if t]
    data['reimbursement_message'] = " ".join(banner_texts).strip()
    full_text = data['reimbursement_message'].lower()
    data['reimbursable_status'] = "Unknown"

    if "niet vergoed" in full_text or "not reimbursed" in full_text:
        data['reimbursable_status'] = "Not reimbursed"
        data['reimbursable_rate'] = "0%"
    elif "volledig vergoed" in full_text or "fully reimbursed" in full_text:
        data['reimbursable_status'] = "Fully reimbursed"
        data['reimbursable_rate'] = "100%"
    elif "deels vergoed" in full_text or "partially reimbursed" in full_text:
        data['reimbursable_status'] = "Partially reimbursed"
        pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", data['reimbursement_message'])
        data['reimbursable_rate'] = f"{pct.group(1)}%" if pct else "Partial"
    elif ("voorwaarde" in full_text or "voorwaarden" in full_text) and "vergoed" in full_text:
        data['reimbursable_status'] = "Reimbursed with conditions"
        data['reimbursable_rate'] = "100%"
    elif "vergoed" in full_text or "reimbursed" in full_text:
        data['reimbursable_status'] = "Reimbursed"
        data['reimbursable_rate'] = "100%"

    # Deductible & Copay from dt/dd pairs
    # NOTE: Some products have "Eigen risico" nested inside <div class="pat-message warning">
    # so we need to find ALL <dt> elements, not just direct children of <dl>
    dts = doc.xpath('//dt')
    for dt_elem in dts:
        label = clean_single_line(dt_elem.text_content()).lower()

        if not data['deductible'] and ("eigen risico" in label or "deductible" in label):
            dd_elem = dt_elem.getnext()
            if dd_elem is not None:
                # Try to extract per-package value first (same as price extraction logic)
                pkg_span = dd_elem.xpath('.//span[@data-pat-depends="inline-days=package"]')
                pce_span = dd_elem.xpath('.//span[@data-pat-depends="inline-days=piece"]')
                
                eur = ""
                if pkg_span:
                    eur = first_euro_amount(pkg_span[0].text_content())
                if not eur and pce_span:
                    eur = first_euro_amount(pce_span[0].text_content())
                if not eur:
                    # Fallback to full text
                    dd_text = clean_single_line(dd_elem.text_content())
                    eur = first_euro_amount(dd_text)
                
                if eur:
                    data['ri_with_vat'] = eur  # RI with VAT = deductible amount
                    data['deductible'] = eur
                    print(f"[DEBUG] Found Eigen risico: {eur} for {url}")
                elif "niets" in dd_elem.text_content().lower() or "nothing" in dd_elem.text_content().lower():
                    data['ri_with_vat'] = "€ 0,00"
                    data['deductible'] = "€ 0,00"
                else:
                    dd_text = clean_single_line(dd_elem.text_content())
                    if dd_text:
                        data['deductible'] = dd_text
                        print(f"[DEBUG] Eigen risico no euro amount: {dd_text[:50]} for {url}")
            else:
                print(f"[DEBUG] No dd element for Eigen risico: {url}")

        if not data['copay_price'] and any(
            kw in label for kw in ["eigen bijdrage", "copay", "co-pay", "bijbetaling"]
        ):
            dd_elem = dt_elem.getnext()
            if dd_elem is not None:
                # Try to extract per-package value first (same as deductible logic)
                pkg_span = dd_elem.xpath('.//span[@data-pat-depends="inline-days=package"]')
                pce_span = dd_elem.xpath('.//span[@data-pat-depends="inline-days=piece"]')
                
                eur = ""
                if pkg_span:
                    eur = first_euro_amount(pkg_span[0].text_content())
                if not eur and pce_span:
                    eur = first_euro_amount(pce_span[0].text_content())
                if not eur:
                    # Fallback to full text
                    dd_text = dd_elem.text_content()
                    eur = first_euro_amount(dd_text)
                
                if eur:
                    data['copay_price'] = eur
                    # Look for percentage in full dd text
                    pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", dd_elem.text_content(), re.IGNORECASE)
                    if pct:
                        data['copay_percent'] = f"{pct.group(1)}%"

    # Copay from warning banners (fallback)
    if not data['copay_price']:
        warn_banners = doc.xpath(
            '//dd[contains(@class,"medicine-price")]//div[contains(@class,"pat-message") and contains(@class,"warning")]')
        for wb in warn_banners:
            wb_text = wb.text_content()
            txt_low = clean_single_line(wb_text).lower()
            # Added "bij te betalen" - Dutch phrase for "to pay extra"
            if any(kw in txt_low for kw in ["additional", "eigen bijdrage", "zelf betalen",
                                             "bijbetalen", "bij te betalen", "extra", "you must pay"]):
                # Look for package price first (data-pat-depends="inline-days=package")
                pkg_span = wb.xpath('.//span[@data-pat-depends="inline-days=package" and contains(@class,"visible")]')
                if pkg_span:
                    eur = first_euro_amount(pkg_span[0].text_content())
                    if eur:
                        data['copay_price'] = eur
                        pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", pkg_span[0].text_content(), re.IGNORECASE)
                        if pct:
                            data['copay_percent'] = f"{pct.group(1)}%"
                        break
                # Fallback: package span without requiring visible class
                # (raw HTML from httpx may not have visible/hidden classes added by JS)
                pkg_span = wb.xpath('.//span[@data-pat-depends="inline-days=package"]')
                if pkg_span:
                    eur = first_euro_amount(pkg_span[0].text_content())
                    if eur:
                        data['copay_price'] = eur
                        pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", pkg_span[0].text_content(), re.IGNORECASE)
                        if pct:
                            data['copay_percent'] = f"{pct.group(1)}%"
                        break
                # Last fallback to first euro amount if no package span found at all
                eur = first_euro_amount(wb_text)
                if eur:
                    data['copay_price'] = eur
                    pct = re.search(r"(\d+(?:[.,]\d+)?)\s*%", wb_text, re.IGNORECASE)
                    if pct:
                        data['copay_percent'] = f"{pct.group(1)}%"
                    break

    return data


# =====================================================================
# Phase 2: Concurrent product scraping with rate limiting + retry
# =====================================================================

async def scrape_products_concurrent(
    url_pairs: list,
    repo,
    max_workers: int = MAX_WORKERS,
    batch_size: int = BATCH_SIZE,
    proxy_url: str = None,
    tor_cfg=None,
    cookies: list = None,
) -> int:
    """
    Scrape products with httpx + lxml.

    - Rate limited to MAX_REQ_PER_MIN requests/min
    - Periodic DB batch saves (crash-safe)
    - Retry pass for failed URLs after main pass
    - Optional Tor NEWNYM rotation
    """
    print(f"\n[SCRAPER] Starting httpx scraping with {max_workers} workers")
    if proxy_url:
        print(f"[SCRAPER] Using Tor proxy: {proxy_url}")
    print(f"[SCRAPER] Rate limit: {MAX_REQ_PER_MIN} req/min")
    print(f"[SCRAPER] Total URLs: {len(url_pairs)} | Batch size: {batch_size}")

    total_inserted = 0
    buffer: list[dict] = []
    buffer_lock = asyncio.Lock()
    completed_count = 0
    failed_items: list[tuple] = []   # (url, collected_url_id) — for retry pass
    start_time = time.time()

    # Rate limiter
    rate_limiter = None
    if TOR_AVAILABLE:
        rate_limiter = AsyncRateLimiter(max_per_minute=MAX_REQ_PER_MIN)
    else:
        # Inline simple rate limiter if tor_httpx not available
        class _SimpleLimiter:
            def __init__(self):
                self._interval = 60.0 / MAX_REQ_PER_MIN
                self._lock = asyncio.Lock()
                self._last = 0.0
                self.total_requests = 0
            async def acquire(self):
                async with self._lock:
                    now = time.monotonic()
                    if now - self._last < self._interval:
                        await asyncio.sleep(self._interval - (now - self._last))
                    self._last = time.monotonic()
                    self.total_requests += 1
        rate_limiter = _SimpleLimiter()

    # Reuse session cookies from Phase 1
    jar = httpx.Cookies()
    if cookies:
        for c in cookies:
            jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        print(f"[SCRAPER] Using {len(cookies)} session cookies. Launching {max_workers} workers...")
    else:
        print(f"[SCRAPER] No cookies provided. Launching {max_workers} workers...")

    # Tor NEWNYM rotation
    rotator = None
    if tor_cfg and TOR_AVAILABLE:
        rotator = TorRotator(tor_cfg)
        rotator.start()

    async def _flush_buffer():
        """Save buffered products to DB."""
        nonlocal total_inserted
        if not buffer:
            return
        try:
            inserted = repo.insert_packs(buffer.copy(), batch_size=batch_size)
            total_inserted += inserted
            elapsed = time.time() - start_time
            rate = completed_count / elapsed if elapsed > 0 else 0
            remaining = len(url_pairs) - completed_count - len(failed_items)
            eta_min = (remaining / rate / 60) if rate > 0 else 0
            print(f"[DB] Batch saved: {inserted} | Total: {total_inserted}/{len(url_pairs)} | "
                  f"{rate:.1f}/s | ETA: {eta_min:.0f}min")
            buffer.clear()
        except Exception as e:
            print(f"[DB ERROR] {e}")

    async def _run_scrape_pass(pairs: list, pass_name: str = "Main") -> list[tuple]:
        """Run one scraping pass over url_pairs. Returns list of failed (url, id) tuples."""
        nonlocal completed_count
        local_failed: list[tuple] = []

        url_queue: asyncio.Queue = asyncio.Queue()
        for pair in pairs:
            await url_queue.put(pair)

        async with httpx.AsyncClient(
            cookies=jar,
            headers={
                "Accept": "text/html,application/xhtml+xml",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0 Safari/537.36",
            },
            timeout=30.0,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=max_workers + 5, max_keepalive_connections=max_workers),
            proxy=proxy_url,
        ) as client:

            async def worker(wid: int):
                nonlocal completed_count
                while True:
                    try:
                        url, collected_url_id = url_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    await rate_limiter.acquire()

                    result = None
                    max_attempts = 3  # Increased from 2 to 3 attempts
                    for attempt in range(max_attempts):
                        try:
                            resp = await client.get(url)
                            resp.raise_for_status()
                            result = extract_product_from_html(resp.text, url, collected_url_id)
                            break
                        except httpx.HTTPStatusError as e:
                            # Handle 404 errors - these are permanently invalid URLs
                            if e.response.status_code == 404:
                                if len(local_failed) < 5:
                                    print(f"[{pass_name}] W{wid}: 404 Not Found - skipping {url[:80]}...")
                                # Mark as failed in DB immediately (don't add to local_failed for retry)
                                try:
                                    repo.update_url_status(collected_url_id, 'failed', error_message='404 Not Found')
                                except Exception as db_err:
                                    if len(local_failed) < 5:
                                        print(f"[{pass_name}] W{wid}: DB error marking 404: {db_err}")
                                break
                            # For other HTTP errors, retry with exponential backoff
                            if attempt < max_attempts - 1:
                                wait_time = 2 * (attempt + 1)  # 2s, 4s, 6s
                                await asyncio.sleep(wait_time)
                            else:
                                if len(local_failed) < 30:
                                    print(f"[{pass_name}] W{wid}: HTTP {e.response.status_code} - {url[:80]}...")
                                local_failed.append((url, collected_url_id))
                        except Exception as e:
                            if attempt < max_attempts - 1:
                                wait_time = 2 * (attempt + 1)
                                await asyncio.sleep(wait_time)
                            else:
                                if len(local_failed) < 30:
                                    print(f"[{pass_name}] W{wid}: {str(e).split(chr(10))[0][:100]}")
                                local_failed.append((url, collected_url_id))

                    if result:
                        async with buffer_lock:
                            buffer.append(result)
                            completed_count += 1
                            if len(buffer) >= batch_size:
                                await _flush_buffer()
                        # Mark URL as successfully scraped
                        try:
                            repo.update_url_status(collected_url_id, 'success')
                        except Exception as db_err:
                            print(f"[DB ERROR] Failed to mark URL {collected_url_id} as success: {db_err}")

            tasks = [asyncio.create_task(worker(i)) for i in range(max_workers)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check for exceptions in results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"[WORKER ERROR] Worker {i} failed with: {result}")

        return local_failed

    # ---- Main pass ----
    failed_items = await _run_scrape_pass(url_pairs, pass_name="MAIN")

    # ---- Retry pass for failed URLs ----
    if failed_items:
        print(f"\n[RETRY] Retrying {len(failed_items)} failed URLs...")
        retry_failed = await _run_scrape_pass(failed_items, pass_name="RETRY")
        if retry_failed:
            print(f"[RETRY] Still failed after retry: {len(retry_failed)}")
            # Mark permanently failed URLs in database
            for url, url_id in retry_failed:
                try:
                    repo.update_url_status(url_id, 'failed', error_message='Failed after retry')
                except Exception as db_err:
                    print(f"[DB ERROR] Failed to mark URL {url_id} as failed: {db_err}")
        else:
            print(f"[RETRY] All {len(failed_items)} retried successfully!")

    # Stop Tor rotator
    if rotator:
        rotator.stop()
        if rotator.rotation_count > 0:
            print(f"[TOR] Completed {rotator.rotation_count} NEWNYM rotations")

    # Final buffer flush
    async with buffer_lock:
        await _flush_buffer()

    elapsed = time.time() - start_time
    print(f"\n[SCRAPER] Complete! ({elapsed/60:.1f} min)")
    print(f"  Scraped: {completed_count}/{len(url_pairs)} | DB: {total_inserted}")
    if rate_limiter:
        print(f"  Total HTTP requests: {rate_limiter.total_requests}")

    return total_inserted


# =====================================================================
# Main entry point
# =====================================================================

async def main():
    print("=" * 80)
    print("NETHERLANDS FAST SCRAPER (URL Collection + Product Scraping)")
    print("=" * 80)

    # Run ID
    run_id = os.environ.get("NL_RUN_ID")
    if not run_id:
        run_id = f"nl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.environ["NL_RUN_ID"] = run_id

    try:
        _repo_root = Path(__file__).resolve().parent.parent.parent
        output_dir = _repo_root / "output" / "Netherlands"
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / ".current_run_id").write_text(run_id, encoding="utf-8")
    except Exception:
        pass

    print(f"Run ID: {run_id}")

    # ---- Tor proxy setup ----
    proxy_url = None
    tor_cfg = None
    if TOR_AVAILABLE:
        tor_cfg = TorConfig.from_env(getenv_fn=_TOR_GETENV)
        proxy_url = setup_tor(tor_cfg)
    else:
        print("[TOR] core.tor_httpx not available — running without Tor")

    # Database
    db = get_db("Netherlands")
    repo = NetherlandsRepository(db, run_id)
    repo.ensure_run_in_ledger(mode="resume")

    # ==================================================================
    # GET SESSION COOKIES (one-time, shared by both phases)
    # ==================================================================
    search_kw_encoded = quote_plus(SEARCH_KEYWORD)
    cookie_url = f"{BASE_URL}/zoeken?searchTerm={search_kw_encoded}&type=medicine"

    print("\n[PLAYWRIGHT] Getting session cookies (one-time)...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(cookie_url, wait_until="domcontentloaded", timeout=60000)
        try:
            await page.get_by_role("button", name=re.compile("Akkoord|Accept", re.I)).click(timeout=3000)
        except Exception:
            pass
        # Wait a moment for cookie to settle
        await asyncio.sleep(1)
        session_cookies = await context.cookies()
        await browser.close()
    print(f"[PLAYWRIGHT] Got {len(session_cookies)} cookies")

    # ==================================================================
    # PHASE 1: COLLECT URLS
    # ==================================================================
    print("\n" + "=" * 80)
    print("PHASE 1: COLLECTING PRODUCT URLS")
    print("=" * 80)

    url_id_map: dict[str, int] = {}

    # Load any URLs already saved from a prior (possibly interrupted) run
    existing_url_count = 0
    existing_urls: set[str] = set()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT url, id FROM nl_collected_urls WHERE run_id = %s", (run_id,))
            for row in cur.fetchall():
                url_id_map[row[0]] = row[1]
                existing_urls.add(row[0])
            existing_url_count = len(url_id_map)
    except Exception as e:
        print(f"[DB] Could not check existing URLs: {e}")

    # Helper: persist a batch of new records to DB immediately
    def _save_new_urls(records: list[dict]) -> int:
        """Insert records not yet in DB. Returns count of new rows inserted."""
        new_recs = [r for r in records if r['url'] not in existing_urls]
        if not new_recs:
            return 0
        inserted = repo.insert_collected_urls(new_recs)
        # Refresh url_id_map for newly inserted rows
        with db.cursor() as cur:
            placeholders = ','.join(['%s'] * len(new_recs))
            cur.execute(
                f"SELECT url, id FROM nl_collected_urls WHERE run_id = %s AND url IN ({placeholders})",
                [run_id] + [r['url'] for r in new_recs],
            )
            for row in cur.fetchall():
                url_id_map[row[0]] = row[1]
                existing_urls.add(row[0])
        return inserted

    # Check if we already have a completed URL collection from step_progress
    phase1_completed = repo.is_progress_completed(step_number=1, progress_key="url_collection_complete")

    if phase1_completed:
        print(f"[RESUME] Phase 1 already completed ({existing_url_count} URLs) — skipping")
    elif existing_url_count >= 22000:  # Assume complete if we have ~22K URLs
        print(f"[RESUME] Found {existing_url_count} URLs — assuming complete")
        repo.mark_progress(1, "url_collection", "url_collection_complete", "completed")
    else:
        if existing_url_count > 0:
            print(f"[RESUME] Found {existing_url_count} URLs from prior run — will collect remaining")

        # Get URL collection mode from config
        url_collection_mode = _get_config("URL_COLLECTION_MODE", "httpx").lower()
        
        search_url = (
            f"{BASE_URL}/zoeken?searchTerm={search_kw_encoded}"
            f"&type=medicine&searchTermHandover={search_kw_encoded}"
            "&vorm=Alle%20vormen&sterkte=Alle%20sterktes"
        )
        
        if url_collection_mode == "vorm_filters":
            # Use vorm filters to bypass the 3100 cap (recommended for full collection)
            print(f"[MODE] Using vorm filters (gets all ~22K URLs via multiple queries)")
            try:
                records = await collect_urls_via_vorm_filters(search_url, session_cookies, proxy_url,
                                                               repo, existing_urls, run_id)
            except Exception as e:
                print(f"[ERROR] URL collection failed: {e}")
                import traceback
                traceback.print_exc()
                raise
        elif url_collection_mode == "playwright_scroll":
            # Use Playwright with real browser scrolling (can get 4000+ URLs like manual browsing)
            max_scrolls = int(_get_config("URL_COLLECTION_MAX_SCROLLS", "1000"))
            print(f"[MODE] Using Playwright scroll (real browser, max {max_scrolls} scrolls)")
            try:
                records = await collect_urls_via_playwright_scroll(search_url, proxy_url,
                                                                    repo, existing_urls, max_scrolls)
            except Exception as e:
                print(f"[ERROR] URL collection failed: {e}")
                import traceback
                traceback.print_exc()
                raise
        else:
            # Use simple httpx single search (fast, but capped at ~3100 URLs)
            max_urls = int(_get_config("URL_COLLECTION_HTTpx_MAX_URLS", "50000"))
            print(f"[MODE] Using httpx single search (fast, max ~{max_urls} URLs)")
            try:
                records = await collect_urls_via_httpx(search_url, session_cookies, proxy_url, 
                                                        repo, existing_urls, max_urls)
            except Exception as e:
                print(f"[ERROR] URL collection failed: {e}")
                import traceback
                traceback.print_exc()
                raise
        
        # Reload url_id_map after collection
        with db.cursor() as cur:
            cur.execute("SELECT url, id FROM nl_collected_urls WHERE run_id = %s", (run_id,))
            for row in cur.fetchall():
                url_id_map[row[0]] = row[1]
        
        # Mark entire URL collection as complete
        repo.mark_progress(1, "url_collection", "url_collection_complete", "completed")
        print(f"\n[SUCCESS] Phase 1 complete — {len(url_id_map)} unique URLs in DB")

    # ==================================================================
    # PHASE 2: SCRAPE PRODUCT DETAILS
    # ==================================================================
    print("\n" + "=" * 80)
    print("PHASE 2: SCRAPING PRODUCT DETAILS")
    print("=" * 80)

    # Check if phase 2 is already completed
    phase2_completed = repo.is_progress_completed(step_number=2, progress_key="product_scraping_complete")
    if phase2_completed:
        print("[RESUME] Phase 2 already completed — skipping product scraping")
        repo.finish_run("completed", items_scraped=existing_url_count)
        return

    all_urls_list = list(url_id_map.keys())

    # Resume: skip already-scraped URLs
    print("[DB] Checking for already scraped products...")
    scraped_urls: set[str] = set()
    try:
        with db.cursor() as cur:
            cur.execute("SELECT DISTINCT source_url FROM nl_packs WHERE run_id = %s", (run_id,))
            scraped_urls = {row[0] for row in cur.fetchall()}
    except Exception as e:
        print(f"[DB] Could not check scraped URLs: {e}")

    if scraped_urls:
        print(f"[RESUME] {len(scraped_urls)} already scraped — skipping those")
        urls_to_scrape = [(url, url_id_map[url]) for url in all_urls_list if url not in scraped_urls]
    else:
        urls_to_scrape = [(url, url_id_map[url]) for url in all_urls_list]
    
    # Filter out invalid URLs (pagenotfound, etc.)
    invalid_patterns = ['pagenotfound', 'javascript:', 'mailto:', '#']
    original_count = len(urls_to_scrape)
    urls_to_scrape = [
        (url, url_id) for url, url_id in urls_to_scrape 
        if not any(pattern in url.lower() for pattern in invalid_patterns)
        and url != "https://www.medicijnkosten.nl/medicijn?"
    ]
    filtered_count = original_count - len(urls_to_scrape)
    if filtered_count > 0:
        print(f"[FILTER] Removed {filtered_count} invalid URLs (pagenotfound, etc.)")

    print(f"[SCRAPER] URLs to scrape: {len(urls_to_scrape)}/{len(all_urls_list)}")
    
    # Limit URLs per run to prevent memory issues
    max_urls_per_run = int(_get_config("MAX_URLS_PER_RUN", "50000"))
    if len(urls_to_scrape) > max_urls_per_run:
        print(f"[LIMIT] Limiting to {max_urls_per_run} URLs per run")
        urls_to_scrape = urls_to_scrape[:max_urls_per_run]

    if not urls_to_scrape:
        print("[SUCCESS] All products already scraped!")
        repo.mark_progress(2, "product_scraping", "product_scraping_complete", "completed")
        repo.finish_run("completed", items_scraped=len(scraped_urls))
        return

    try:
        inserted_count = await scrape_products_concurrent(
            urls_to_scrape, repo, max_workers=MAX_WORKERS, batch_size=BATCH_SIZE,
            proxy_url=proxy_url, tor_cfg=tor_cfg, cookies=session_cookies,
        )

        total_count = len(scraped_urls) + inserted_count
        print(f"\n{'='*80}")
        print(f"[SUCCESS] URLs: {len(url_id_map)} | Already: {len(scraped_urls)} | New: {inserted_count} | Total: {total_count}")
        print(f"{'='*80}")

        # Mark phase 2 as complete
        repo.mark_progress(2, "product_scraping", "product_scraping_complete", "completed")
        repo.finish_run("completed", items_scraped=total_count)
    except Exception as e:
        print(f"\n[ERROR] Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        repo.finish_run("failed", items_scraped=len(scraped_urls), error_message=str(e))
        raise


def main_sync():
    asyncio.run(main())


if __name__ == "__main__":
    from core.standalone_checkpoint import run_with_checkpoint
    # Allow step number to be configured via environment variable
    step_number = int(os.environ.get("NL_STEP_NUMBER", "1"))
    step_name = os.environ.get("NL_STEP_NAME", "Fast Scraper (DB-only)")
    run_with_checkpoint(main_sync, "Netherlands", step_number, step_name, output_files=[])
