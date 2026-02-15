import asyncio
import os
import sys
import urllib.parse
import re
from playwright.async_api import async_playwright

# Configuration
SEARCH_TERM = "632 Medicijnkosten Drugs4"
BASE_URL = "https://www.medicijnkosten.nl"

async def check_counts():
    async with async_playwright() as p:
        print("Launching browser...")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        # 1. Check Global Count
        print("--- Global Search Pagination Check ---")
        q = urllib.parse.quote(SEARCH_TERM)
        # Construct URL exactly as user provided/scraper uses
        url = f"{BASE_URL}/zoeken?searchTerm={q}&type=medicine&searchTermHandover={q}&vorm=Alle%20vormen&sterkte=Alle%20sterktes"
        
        print(f"Navigating to: {url}")
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(3000)
        
        # Extract Total
        total_expected = 0
        try:
            content = await page.content()
            # Look for number before "zoekresultaten" or "resultaten"
            # Try specific selectors first
            try:
                text = await page.locator("h2").all_text_contents() 
                # e.g. "22206 zoekresultaten"
                for t in text:
                    m = re.search(r'(\d+)\s+zoekresultaten', t)
                    if m:
                        total_expected = int(m.group(1))
                        print(f"Found Total via H2: {total_expected}")
                        break
            except: pass
            
            if total_expected == 0:
                m = re.search(r'(\d+)\s+zoekresultaten', content)
                if m:
                    total_expected = int(m.group(1))
                    print(f"Found Total via Regex: {total_expected}")
        except Exception as e:
            print(f"Error extracting total: {e}")

        if total_expected == 0:
            print("Could not determine total expected count. Proceeding blindly.")

        # Paginate
        page_num = 1
        total_found = 0
        consecutive_empty = 0
        
        while True:
            # Construct page URL
            if page_num == 1:
                page_url = url
            else:
                page_url = f"{url}&page={page_num}"
                await page.goto(page_url, timeout=45000)
                await page.wait_for_timeout(1000)

            # Count items
            try:
                # Based on scraper selector: a.result-item.medicine
                items = await page.locator('a.result-item.medicine').count()
                print(f"Page {page_num}: Found {items} items")
                
                if items == 0:
                    print("  Zero items found.")
                    consecutive_empty += 1
                    if consecutive_empty >= 1: # Stop immediately on empty page
                        break
                else:
                    consecutive_empty = 0
                    total_found += items
                    
                # Check for stop conditions
                if total_found >= total_expected and total_expected > 0:
                    print("  Reached expected total.")
                    break
                    
                # Safety break for test
                if page_num > 60: # 60 pages * 10 = 600 items. Should test the 500 limit.
                    print("  Stopping at 60 pages for test.")
                    break
                    
            except Exception as e:
                print(f"  Error on page {page_num}: {e}")
                break
                
            page_num += 1

        print(f"\n--- Summary ---")
        print(f"Total Expected: {total_expected}")
        print(f"Total Found:    {total_found}")
        print(f"Pages Scraped:  {page_num}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(check_counts())
