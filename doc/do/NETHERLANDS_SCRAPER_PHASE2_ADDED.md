# Netherlands Scraper - Phase 2 Product Scraping Added ✅

## Summary

Successfully added product detail scraping to `01_fast_scraper.py`. The script now performs **both URL collection AND product scraping** in a single run.

---

## Changes Made

### File Modified: `scripts/Netherlands/01_fast_scraper.py`

#### 1. Added Product Scraping Function

```python
async def scrape_product_detail(page, url: str) -> dict:
```

**Extracts the following data** (matching `nl_packs` schema):
- **Basic Info**: `local_pack_description` (h1 title), `manufacturer`, `local_pack_code` (RVG/EU number)
- **Product Details**: `active_substance`, `formulation`, `strength_size`
- **Pricing**: `unit_price` (per piece), `ppp_vat` (per package with VAT), `ppp_ex_vat` (calculated)
- **Reimbursement**: `reimbursable_status` (Fully/Partially/Not reimbursed), `reimbursement_message`
- **Metadata**: `currency` (EUR), `vat_percent` (9.0), `margin_rule` (632 Medicijnkosten Drugs4)

**Price Toggle Logic**:
- If `#inline-days` dropdown exists:
  1. Select "package" → extract package price → `ppp_vat`
  2. Select "piece" → extract piece price → `unit_price`
- If no dropdown: use visible price as `ppp_vat`

**Reimbursement Detection**:
- Checks `dd.medicine-price div.pat-message` for banner messages
- Classifies status based on keywords: "volledig vergoed", "deels vergoed", "niet vergoed"

#### 2. Added Concurrent Scraping Function

```python
async def scrape_products_concurrent(urls: list, max_workers: int = 6) -> list:
```

- **Concurrent processing**: 6 parallel workers by default (adjustable)
- **Semaphore control**: Prevents overwhelming the server
- **Progress tracking**: Prints progress every 50 products
- **Error handling**: Filters out failed scrapes, returns only valid results

#### 3. Updated Main Function

**NEW TWO-PHASE WORKFLOW**:

**Phase 1: URL Collection**
- Uses httpx + pagination (existing logic, working)
- Collects ~22,000 product URLs
- Saves to `medicijnkosten_links.txt`
- Inserts to `nl_collected_urls` table

**Phase 2: Product Scraping** (NEW)
- Reads URLs from Phase 1
- Scrapes product details concurrently (6 workers)
- Batch inserts to `nl_packs` table (500 records per batch)
- Reports success/failure counts

---

## How to Run

### Fresh Run (Both Phases)

```bash
cd d:\quad99\Scrappers\scripts\Netherlands
python run_pipeline_resume.py --fresh
```

This will:
1. ✅ Collect 22,000+ URLs (Phase 1) - **Already working**
2. ✅ Scrape 22,000+ product details (Phase 2) - **Newly added**
3. Run consolidation (existing step)

### Resume from Database

```bash
python run_pipeline_resume.py
```

- Skips Phase 1 if URLs already collected
- Proceeds to Phase 2 if `nl_packs` is empty

### Run Directly (Bypass Pipeline)

```bash
python 01_fast_scraper.py
```

---

## Expected Output

### Phase 1: URL Collection (Already Working)

```
================================================================================
PHASE 1: COLLECTING PRODUCT URLS
================================================================================
[PLAYWRIGHT] Getting cookies from browser session...
[PLAYWRIGHT] Got 3 cookies

[COMBO] Processing: vorm=Alle vormen, sterkte=Alle sterktes
[COMBO] Total expected: 22206
[COMBO] Page 0: 100 links, 100 new, total: 100
[COMBO] Page 1: 100 links, 100 new, total: 200
...
[COMBO] Page 218: 100 links, 100 new, total: 21900
[COMBO] Collected 21900 URLs for Alle vormen/Alle sterktes

================================================================================
[SUCCESS] Collected 21900 unique URLs
[SUCCESS] Saved to: medicijnkosten_links.txt
================================================================================

[DB] Inserting URLs into database...
[DB] Inserted 21900 URLs
```

### Phase 2: Product Scraping (NEW)

```
================================================================================
PHASE 2: SCRAPING PRODUCT DETAILS
================================================================================

[SCRAPER] Starting product scraping with 6 workers
[SCRAPER] Total URLs to scrape: 21900

[SCRAPER] Progress: 50/21900 (0%)
[SCRAPER] Progress: 100/21900 (0%)
[SCRAPER] Progress: 150/21900 (0%)
...
[SCRAPER] Progress: 21900/21900 (100%)

[SCRAPER] Successfully scraped 21850/21900 products

[DB] Inserting 21850 products into database...
[DB] Successfully inserted 21850 products

================================================================================
[SUCCESS] Scraping complete!
  URLs collected: 21900
  Products scraped: 21850
================================================================================
```

---

## Performance Expectations

### Phase 1: URL Collection
- **Speed**: ~100 URLs per page, ~220 pages
- **Time**: 5-10 minutes (httpx pagination, very fast)
- **Status**: ✅ Already working (tested)

### Phase 2: Product Scraping
- **Speed**: 6 concurrent workers, ~2-3 seconds per product page
- **Throughput**: ~120-180 products per minute
- **Time for 22,000 products**: ~2-3 hours
- **Status**: ✅ Newly added (ready to test)

**Total Pipeline Time**: ~2.5-3.5 hours (for 22,000 products)

---

## Database Tables Populated

### `nl_collected_urls` (Phase 1)
- `run_id`: Current run identifier
- `prefix`: "all_products"
- `url`: Product URL
- `url_with_id`: Same as url
- `title`: Empty (not extracted during URL collection)

### `nl_packs` (Phase 2) - NEW
- `run_id`: Current run identifier
- `source_url`: Product URL
- `local_pack_description`: Product name/title
- `active_substance`: Generic name
- `formulation`: Form (tablet, capsule, etc.)
- `strength_size`: Strength (e.g., "10mg", "500mg/5ml")
- `manufacturer`: Company name
- `local_pack_code`: RVG or EU number
- `unit_price`: Price per piece (if available)
- `ppp_vat`: Price per package (with VAT)
- `ppp_ex_vat`: Price per package (without VAT)
- `currency`: "EUR"
- `vat_percent`: 9.0
- `reimbursable_status`: "Fully reimbursed", "Partially reimbursed", etc.
- `reimbursement_message`: Full banner text
- `margin_rule`: "632 Medicijnkosten Drugs4"
- `scraped_at`: Timestamp (auto-generated by DB)

---

## Verification Steps

### 1. Check URL Collection (Phase 1)

```sql
-- Check URLs collected
SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = 'nl_20260209_XXXXXX';
-- Expected: ~22,000

-- Sample URLs
SELECT * FROM nl_collected_urls LIMIT 5;
```

### 2. Check Product Details (Phase 2)

```sql
-- Check products scraped
SELECT COUNT(*) FROM nl_packs WHERE run_id = 'nl_20260209_XXXXXX';
-- Expected: ~22,000

-- Sample products with pricing
SELECT
    local_pack_description,
    manufacturer,
    unit_price,
    ppp_vat,
    reimbursable_status
FROM nl_packs
WHERE run_id = 'nl_20260209_XXXXXX'
LIMIT 10;

-- Check reimbursement distribution
SELECT reimbursable_status, COUNT(*)
FROM nl_packs
WHERE run_id = 'nl_20260209_XXXXXX'
GROUP BY reimbursable_status;
```

### 3. Check Consolidation Output

```bash
# Should now have data (previously was 0 rows)
wc -l output/Netherlands/consolidated_products.csv
```

---

## Next Steps

1. **Run the scraper** to test Phase 2:
   ```bash
   cd scripts/Netherlands
   python 01_fast_scraper.py
   ```

2. **Monitor progress** in real-time:
   - Watch console output for Phase 1 and Phase 2 progress
   - Check database row counts periodically:
     ```sql
     SELECT
         (SELECT COUNT(*) FROM nl_collected_urls WHERE run_id = 'nl_LATEST') as urls,
         (SELECT COUNT(*) FROM nl_packs WHERE run_id = 'nl_LATEST') as packs;
     ```

3. **After successful run**, verify consolidation:
   ```bash
   python 03_Consolidate_Results.py
   ```

4. **Review data quality**:
   - Check for missing prices
   - Verify reimbursement status distribution
   - Spot-check a few products manually on medicijnkosten.nl

---

## Troubleshooting

### "No URLs to scrape" Error

**Cause**: Phase 1 didn't collect URLs or `nl_collected_urls` table is empty

**Fix**:
```bash
# Check if URLs exist
psql -d netherlands -c "SELECT COUNT(*) FROM nl_collected_urls;"

# If 0, run Phase 1 only (URL collection works, tested)
```

### Playwright Browser Errors

**Cause**: Chromium not installed

**Fix**:
```bash
pip install playwright
playwright install chromium
```

### Slow Scraping Speed

**Cause**: Default 6 workers might be conservative

**Fix**: Edit line in `01_fast_scraper.py`:
```python
products = await scrape_products_concurrent(urls_to_scrape, max_workers=10)  # Increase to 10
```

**Warning**: Higher workers = more aggressive, may trigger rate limiting

### Database Insert Errors

**Cause**: Schema mismatch or missing fields

**Fix**: Check error message, verify `nl_packs` table schema matches expected fields

---

## Technical Notes

### Why Playwright for Scraping (Not httpx)?

- **Dynamic prices**: Toggle between "piece" and "package" requires JavaScript execution
- **Pat-depends system**: Prices update via AJAX when dropdown changes
- **Complex selectors**: Reimbursement banners use dynamic CSS classes

### Price Extraction Logic

1. Check if `#inline-days` dropdown exists
2. If yes:
   - Select "package" → wait 500ms → extract `span[data-pat-depends="inline-days=package"]`
   - Select "piece" → wait 500ms → extract `span[data-pat-depends="inline-days=piece"]`
3. If no dropdown: extract first visible `span.pat-depends` with €

### Concurrent Architecture

- **Semaphore**: Limits to 6 parallel browsers (prevents memory issues)
- **Isolated contexts**: Each worker has own browser instance (no shared state)
- **Exception handling**: Individual failures don't crash entire batch
- **Gather pattern**: `asyncio.gather(*tasks, return_exceptions=True)` collects all results

---

## Success Criteria

✅ **Phase 1** (URL Collection): Working, tested, collected 21,900 URLs
✅ **Phase 2** (Product Scraping): Implemented, ready to test
✅ **Database integration**: Using `insert_packs()` with batch inserts
✅ **Business logic preserved**: All extraction logic from old Selenium scraper
✅ **Concurrent processing**: 6 workers for faster scraping
✅ **Error handling**: Graceful failure for individual products

---

## Ready to Test!

The scraper is now complete with both URL collection and product detail scraping. Run it and monitor the console output to see Phase 1 (URLs) and Phase 2 (products) in action.

**Estimated total time**: 2.5-3.5 hours for 22,000 products (Phase 1: 5-10 min, Phase 2: 2-3 hours)
