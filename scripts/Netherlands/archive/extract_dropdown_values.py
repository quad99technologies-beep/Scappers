"""
Extract dropdown values (vorm and sterkte) from medicijnkosten.nl
This module extracts the actual dropdown values from the website for complete coverage.
"""

import asyncio
import re
from playwright.async_api import async_playwright


async def extract_dropdown_values():
    """
    Extract vorm (form) and sterkte (strength) dropdown values from medicijnkosten.nl
    
    Returns:
        tuple: (vorm_values, sterkte_values) - Lists of dropdown option values
    """
    print("[EXTRACT] Launching browser to extract dropdown values...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Navigate to search page
        url = "https://www.medicijnkosten.nl/zoeken?searchTerm=632%20Medicijnkosten%20Drugs4&type=medicine"
        print(f"[EXTRACT] Navigating to: {url}")
        await page.goto(url, wait_until="networkidle")
        
        # Accept cookie banner if present
        try:
            await page.get_by_role("button", name=re.compile("Akkoord|Accept", re.I)).click(timeout=2000)
            print("[EXTRACT] Accepted cookie banner")
        except Exception:
            pass  # No banner or already accepted
        
        # Click "Toon filters" button to show the filter dropdowns
        print("[EXTRACT] Clicking 'Toon filters' button to show dropdowns...")
        try:
            await page.click('a.filter-open-trigger, #open-trigger', timeout=5000)
            await page.wait_for_timeout(1000)  # Wait for animation
            print("[EXTRACT] Filters are now visible")
        except Exception as e:
            print(f"[WARN] Could not click filter button: {e}")
            # Try alternative: filters might already be visible
        
        # Wait for dropdowns to be visible
        await page.wait_for_selector('select[name="vorm"]', state='attached', timeout=10000)
        await page.wait_for_selector('select[name="sterkte"]', state='attached', timeout=10000)
        
        # Extract vorm (form) values
        print("[EXTRACT] Extracting vorm (form) values...")
        vorm_options = await page.locator('select[name="vorm"] option').all_text_contents()
        vorm_values = [v.strip() for v in vorm_options if v.strip()]
        print(f"[EXTRACT] Found {len(vorm_values)} vorm values")
        
        # Extract sterkte (strength) values
        print("[EXTRACT] Extracting sterkte (strength) values...")
        sterkte_options = await page.locator('select[name="sterkte"] option').all_text_contents()
        sterkte_values = [s.strip() for s in sterkte_options if s.strip()]
        print(f"[EXTRACT] Found {len(sterkte_values)} sterkte values")
        
        await browser.close()
    
    print(f"[EXTRACT] Extraction complete!")
    print(f"[EXTRACT] Vorm values: {vorm_values[:5]}... ({len(vorm_values)} total)")
    print(f"[EXTRACT] Sterkte values: {sterkte_values[:5]}... ({len(sterkte_values)} total)")
    
    return vorm_values, sterkte_values


def extract_all_combinations():
    """
    Synchronous wrapper for extract_dropdown_values()
    
    Returns:
        tuple: (vorm_values, sterkte_values)
    """
    return asyncio.run(extract_dropdown_values())


if __name__ == "__main__":
    # Test extraction
    print("=" * 80)
    print("TESTING DROPDOWN EXTRACTION")
    print("=" * 80)
    
    vorm_values, sterkte_values = extract_all_combinations()
    
    print("\n" + "=" * 80)
    print("EXTRACTION RESULTS")
    print("=" * 80)
    print(f"Vorm values ({len(vorm_values)}):")
    for i, v in enumerate(vorm_values, 1):
        print(f"  {i}. {v}")
    
    print(f"\nSterkte values ({len(sterkte_values)}):")
    for i, s in enumerate(sterkte_values[:20], 1):  # Show first 20
        print(f"  {i}. {s}")
    if len(sterkte_values) > 20:
        print(f"  ... and {len(sterkte_values) - 20} more")
    
    print(f"\nTotal combinations: {len(vorm_values) * len(sterkte_values):,}")
