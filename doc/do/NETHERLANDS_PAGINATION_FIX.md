# Netherlands Scraper - Pagination Fix Required

## Problem Identified

The current `01_fast_scraper.py` uses **infinite scroll** approach, but medicijnkosten.nl actually uses **page-based pagination**.

## Working Approach (from old script)

The old script (`archive/1-url scrapper.py`) correctly uses:

1. **httpx for HTTP requests** (not Playwright for scrolling)
2. **Page-based pagination**: `?page=1`, `?page=2`, `?page=3`...
3. **Correct XPath selector**: `//a[contains(@class,"result-item") and contains(@class,"medicine")]/@href`
4. **Stop after 3 empty pages**: No new URLs → increment counter, stop at 3

## Current Issues

1. **Wrong approach**: Trying to scroll infinite scroll (doesn't exist on this site)
2. **Wrong selector**: Looking for `a[href*='/medicijn?artikel=']`
   - Should be: `a.result-item.medicine`
3. **Deduplication bug**: `canonical_no_id()` was removing all params
4. **Only collecting 1 URL**: Because of above issues

## Correct Implementation Pattern

```python
import httpx
from lxml import html as lxml_html

async def collect_urls_with_pagination(search_url: str, cookies: list) -> List[str]:
    """Collect URLs using page-based pagination"""

    # Setup httpx client with cookies
    jar = httpx.Cookies()
    for c in cookies:
        jar.set(c["name"], c["value"], domain=c.get("domain"))

    async with httpx.AsyncClient(cookies=jar, timeout=45.0) as client:
        seen_urls = set()
        page_num = 0
        empty_count = 0

        while True:
            # Build pagination URL
            url = f"{search_url}&page={page_num}"

            # Get page HTML
            response = await client.get(url)
            doc = lxml_html.fromstring(response.text)

            # Extract product links
            hrefs = doc.xpath('//a[contains(@class,"result-item") and contains(@class,"medicine")]/@href')

            new_count = 0
            for href in hrefs:
                if href.startswith("/medicijn?"):
                    full_url = BASE_URL + href
                    if full_url not in seen_urls:
                        seen_urls.add(full_url)
                        new_count += 1

            print(f"Page {page_num}: {len(hrefs)} links, {new_count} new, total: {len(seen_urls)}")

            # Stop if no new URLs for 3 pages
            if new_count == 0:
                empty_count += 1
                if empty_count >= 3:
                    break
            else:
                empty_count = 0

            page_num += 1

        return list(seen_urls)
```

## Required Changes to 01_fast_scraper.py

### 1. Add httpx and lxml imports

```python
import httpx
from lxml import html as lxml_html
```

### 2. Replace smart_scroll_and_collect() function

Replace the entire function (lines 434-531) with pagination-based approach shown above.

### 3. Update main collection flow

In `main_async()` around line 810:
```python
# OLD (Playwright scroll):
await page.goto(single_search_url, wait_until="networkidle")
all_urls = await smart_scroll_and_collect(page, "all_products")

# NEW (httpx pagination):
await page.goto(single_search_url, wait_until="networkidle")
cookies = await context.cookies()
all_urls = await collect_urls_with_pagination(single_search_url, cookies)
```

### 4. Update URL record format

Make sure URLs are stored with proper structure:
```python
url_records = [
    {
        "prefix": "all_products",
        "url": url,  # Without ID
        "url_with_id": url,  # With ID
        "title": "",
    }
    for url in all_urls
]
```

## Benefits of This Approach

1. **Much faster**: No waiting for scroll animations
2. **More reliable**: Direct HTTP requests, no browser quirks
3. **Correct**: Matches how the site actually works
4. **Proven**: Old script worked with this approach

## Expected Result

With pagination approach:
- Should collect all 22,206 URLs
- Takes ~2-3 minutes (vs current stuck/1 URL)
- Database writes work correctly
- Can proceed to Phase 2 (scraping)

## Action Required

User needs to decide:
1. **Quick fix**: I can update just the URL collection function
2. **Full rewrite**: Adopt old script's entire approach
3. **Hybrid**: Keep current structure, just fix pagination

Recommend: **Quick fix** - update URL collection to use pagination, keep rest of fast_scraper.py structure.
