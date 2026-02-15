# -*- coding: utf-8 -*-
"""
Netherlands URL Scraper - Combination-Based
Scrapes URLs for each vorm/sterkte combination from the database using Playwright + HTTP
"""

import asyncio
import sys
import os
from urllib.parse import urlencode
import re

import httpx
from lxml import html
from playwright.async_api import async_playwright

# Add parent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

# Database imports
try:
    from db.repositories import NetherlandsRepository
    from core.db.postgres_connection import get_db
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    print("[ERROR] Database not available")
    sys.exit(1)

SEARCH_TERM = "632 Medicijnkosten Drugs4"
BASE_URL = "https://www.medicijnkosten.nl"


def parse_total(page_html: str) -> int | None:
    """Parse total results count from page HTML"""
    doc = html.fromstring(page_html)
    txt = doc.xpath('string(//*[@id="summary"]//strong)').strip()
    return int(txt) if txt.isdigit() else None


def extract_links(fragment_html: str, base=BASE_URL) -> list[str]:
    """Extract product links from HTML fragment"""
    doc = html.fromstring(fragment_html)
    hrefs = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]/@href')
    out = []
    for h in hrefs:
        if h.startswith("/medicijn?"):
            out.append(base + h)
    return out


async def scrape_combination(combination: dict, cookies: list) -> list[str]:
    """
    Scrape URLs for a single vorm/sterkte combination
    
    Args:
        combination: Dict with vorm, sterkte, search_url
        cookies: Browser cookies from initial Playwright session
    
    Returns:
        List of product URLs
    """
    vorm = combination['vorm']
    sterkte = combination['sterkte']
    
    print(f"\n[COMBO] Processing: vorm={vorm}, sterkte={sterkte}")
    
    # Convert cookies to httpx format
    jar = httpx.Cookies()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    
    seen_urls = set()
    
    async with httpx.AsyncClient(
        cookies=jar,
        headers={
            "Accept": "text/html",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": combination['search_url'],
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
        },
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        
        # Get first page to get total and initial links
        response = await client.get(combination['search_url'])
        html0 = response.text
        
        total = parse_total(html0)
        print(f"[COMBO] Total expected: {total or 'unknown'}")
        
        # Extract links from first page
        for url in extract_links(html0):
            seen_urls.add(url)
        
        if total is None or total <= len(seen_urls):
            print(f"[COMBO] Collected {len(seen_urls)} URLs (complete)")
            return list(seen_urls)
        
        # Loop through pagination
        empty_count = 0
        page_num = 1
        
        while True:
            params = {
                "page": str(page_num),
                "searchTerm": SEARCH_TERM,
                "vorm": vorm,
                "sterkte": sterkte,
                "sorting": "",
                "debugMode": ""
            }
            url = f"{BASE_URL}/zoeken?" + urlencode(params)
            
            try:
                response = await client.get(url)
                links = extract_links(response.text)
                
                new_count = 0
                for link in links:
                    if link not in seen_urls:
                        seen_urls.add(link)
                        new_count += 1
                
                print(f"[COMBO] Page {page_num}: {len(links)} links, {new_count} new, total: {len(seen_urls)}")
                
                # Stop conditions
                if total and len(seen_urls) >= total:
                    break
                
                if new_count == 0:
                    empty_count += 1
                    if empty_count >= 3:
                        break
                else:
                    empty_count = 0
                
                page_num += 1
                
            except Exception as e:
                print(f"[COMBO] Error on page {page_num}: {e}")
                break
    
    print(f"[COMBO] Collected {len(seen_urls)} URLs for {vorm}/{sterkte}")
    return list(seen_urls)


async def main():
    """Main entry point"""
    print("=" * 80)
    print("NETHERLANDS URL SCRAPER - COMBINATION-BASED")
    print("=" * 80)
    
    # Get latest run ID from database
    db = get_db("Netherlands")
    
    # Get latest run with combinations
    with db.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT run_id 
            FROM nl_search_combinations 
            ORDER BY run_id DESC 
            LIMIT 1
        """)
        result = cur.fetchone()
        if not result:
            print("[ERROR] No combinations found in database")
            print("[ERROR] Run 01_load_combinations.py first")
            return
        
        run_id = result[0]
    
    print(f"Run ID: {run_id}")
    
    # Initialize repository
    repo = NetherlandsRepository(db, run_id)
    
    # Get pending combinations
    combinations = repo.get_search_combinations(status='pending')
    
    if not combinations:
        print("[INFO] No pending combinations found")
        print("[INFO] All combinations already processed")
        return
    
    print(f"[INFO] Found {len(combinations)} pending combinations")
    
    # Get cookies using Playwright (one-time)
    print("\n[PLAYWRIGHT] Getting cookies from browser session...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Navigate to any search page to get cookies
        test_url = f"{BASE_URL}/zoeken?searchTerm={SEARCH_TERM}&type=medicine"
        await page.goto(test_url, wait_until="networkidle")
        
        # Accept cookie banner if present
        try:
            await page.get_by_role("button", name=re.compile("Akkoord|Accept", re.I)).click(timeout=2000)
        except Exception:
            pass
        
        cookies = await context.cookies()
        await browser.close()
    
    print(f"[PLAYWRIGHT] Got {len(cookies)} cookies")
    
    # Process each combination
    all_urls = set()
    
    for i, combo in enumerate(combinations, 1):
        print(f"\n[PROGRESS] Combination {i}/{len(combinations)}")
        
        try:
            urls = await scrape_combination(combo, cookies)
            all_urls.update(urls)
            
            # Mark combination as completed
            repo.mark_combination_completed(combo['id'], len(urls))
            
        except Exception as e:
            print(f"[ERROR] Failed to process combination: {e}")
            repo.mark_combination_failed(combo['id'], str(e))
    
    # Save all URLs to file
    output_file = "medicijnkosten_links.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for url in sorted(all_urls):
            f.write(url + "\n")
    
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Collected {len(all_urls)} unique URLs")
    print(f"[SUCCESS] Saved to: {output_file}")
    print("=" * 80)
    
    # Insert URLs into database
    print("\n[DB] Inserting URLs into database...")
    url_records = [{'url': url, 'url_with_id': url} for url in all_urls]
    repo.insert_collected_urls(url_records)
    print(f"[DB] Inserted {len(url_records)} URLs")


if __name__ == "__main__":
    asyncio.run(main())
