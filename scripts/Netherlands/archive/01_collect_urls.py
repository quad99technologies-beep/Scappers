# -*- coding: utf-8 -*-
"""
Netherlands URL Collector - Simplified
Collects ALL product URLs from medicijnkosten.nl using a single search
"""

import asyncio
import sys
import os
from urllib.parse import urlencode

import httpx
from lxml import html
from playwright.async_api import async_playwright
import re
from pathlib import Path

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

# Add repo root to path for config_loader import
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from config_loader import getenv, get_input_dir, get_output_dir
from core.pipeline.standalone_checkpoint import run_with_checkpoint

SCRIPT_ID = "Netherlands"

# Constants from environment
SEARCH_TERM = getenv("SEARCH_TERM", "632 Medicijnkosten Drugs4")
BASE_URL = getenv("BASE_URL", "https://www.medicijnkosten.nl")

# Single search URL that covers ALL products
SEARCH_URL_TEMPLATE = getenv(
    "SEARCH_URL",
    f"{BASE_URL}/zoeken?searchTerm={{kw}}&type=medicine&searchTermHandover={{kw}}&vorm=Alle%20vormen&sterkte=Alle%20sterktes",
)
SEARCH_URL = SEARCH_URL_TEMPLATE.format(kw=SEARCH_TERM)


def parse_total(page_html: str) -> int | None:
    """Parse total results count from page HTML"""
    try:
        doc = html.fromstring(page_html)
        txt = doc.xpath('string(//*[@id="summary"]//strong)').strip()
        return int(txt) if txt.isdigit() else None
    except Exception:
        return None


def extract_links(fragment_html: str, base=BASE_URL) -> list[str]:
    """Extract product links from HTML fragment"""
    try:
        doc = html.fromstring(fragment_html)
        hrefs = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]/@href')
        out = []
        for h in hrefs:
            if h.startswith("/medicijn?"):
                out.append(base + h)
        return out
    except Exception:
        return []


async def main():
    """Main entry point"""
    print("=" * 80)
    print("NETHERLANDS URL COLLECTOR - SIMPLIFIED")
    print("=" * 80)
    print(f"Search URL: {SEARCH_URL}")
    print("=" * 80)
    
    # Get run ID from environment or generate new one
    run_id = os.environ.get("NL_RUN_ID")
    if not run_id:
        from datetime import datetime
        run_id = f"nl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Initialize database
    db = get_db("Netherlands")
    repo = NetherlandsRepository(db, run_id)
    repo.ensure_run_in_ledger(mode="url_collection")
    
    print(f"Run ID: {run_id}\n")
    
    # Get cookies using Playwright (one-time)
    print("[PLAYWRIGHT] Getting cookies from browser session...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Navigate to search page
        await page.goto(SEARCH_URL, wait_until="networkidle")
        
        # Accept cookie banner if present
        try:
            await page.get_by_role("button", name=re.compile("Akkoord|Accept", re.I)).click(timeout=2000)
            print("[PLAYWRIGHT] Accepted cookie banner")
        except Exception:
            pass
        
        # Get initial page HTML
        html0 = await page.content()
        
        # Get cookies
        cookies = await context.cookies()
        await browser.close()
    
    print(f"[PLAYWRIGHT] Got {len(cookies)} cookies\n")
    
    # Parse total from first page
    total = parse_total(html0)
    print(f"[INFO] Total expected: {total or 'unknown'}\n")
    
    # Convert cookies to httpx format
    jar = httpx.Cookies()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
    
    # Collect URLs using HTTP pagination
    seen_urls = set()
    
    # Extract from first page
    for url in extract_links(html0):
        seen_urls.add(url)
    
    print(f"[PAGE 0] Initial page: {len(seen_urls)} URLs")
    
    # HTTP loop for pagination
    async with httpx.AsyncClient(
        cookies=jar,
        headers={
            "Accept": "text/html",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": SEARCH_URL,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/144.0 Safari/537.36",
        },
        timeout=45.0,
        follow_redirects=True,
    ) as client:
        
        empty_count = 0
        page_num = 1
        
        while True:
            params = {
                "page": str(page_num),
                "searchTerm": SEARCH_TERM,
                "vorm": "Alle vormen",
                "sterkte": "Alle sterktes",
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
                
                print(f"[PAGE {page_num}] {len(links)} links, {new_count} new, total: {len(seen_urls)}")
                
                # Stop conditions
                if total and len(seen_urls) >= total:
                    print(f"\n[COMPLETE] Reached expected total: {total}")
                    break
                
                if new_count == 0:
                    empty_count += 1
                    if empty_count >= 3:
                        print(f"\n[COMPLETE] No new URLs for 3 consecutive pages")
                        break
                else:
                    empty_count = 0
                
                page_num += 1
                
                # Safety limit
                if page_num > 500:
                    print(f"\n[LIMIT] Reached page limit (500)")
                    break
                
            except Exception as e:
                print(f"[ERROR] Page {page_num}: {e}")
                break
    
    # Save to file
    output_file = "medicijnkosten_links.txt"
    with open(output_file, "w", encoding="utf-8") as f:
        for url in sorted(seen_urls):
            f.write(url + "\n")
    
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Collected {len(seen_urls)} unique URLs")
    print(f"[SUCCESS] Saved to: {output_file}")
    print("=" * 80)
    
    # Insert URLs into database
    print("\n[DB] Inserting URLs into database...")
    url_records = []
    for url in seen_urls:
        url_records.append({
            'url': url,
            'url_with_id': url,
            'prefix': 'medicijnkosten',
            'title': '',
            'active_substance': '',
            'manufacturer': '',
            'document_type': 'medicine',
            'price_text': '',
            'reimbursement': ''
        })
    
    repo.insert_collected_urls(url_records)
    print(f"[DB] Inserted {len(url_records)} URLs")
    
    # Update run ledger
    repo.finish_run(status="completed", items_scraped=len(url_records))
    
    print(f"\n[COMPLETE] URL collection finished")
    print(f"Run ID: {run_id}")


def run_main():
    """Wrapper to run async main() in a synchronous run_with_checkpoint."""
    asyncio.run(main())


if __name__ == "__main__":
    run_with_checkpoint(
        run_main,
        SCRIPT_ID,
        1,
        "Collect URLs",
        output_files=["medicijnkosten_links.txt"]
    )
