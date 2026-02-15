# Netherlands Scraper - Complete Documentation

## Overview
The Netherlands scraper collects pharmaceutical pricing data from **medicijnkosten.nl**, a Dutch government website that publishes official drug prices and reimbursement information.

---

## Architecture

### Pipeline Structure
The scraper runs as a 3-step pipeline:

```
Step 1: Backup and Clean (00_backup_and_clean.py)
   ↓
Step 2: Fast Scraper (01_fast_scraper.py) ← Main scraping logic
   ↓
Step 3: Consolidate Results (03_Consolidate_Results.py)
```

**Execution**: `run_pipeline_resume.py` orchestrates all steps

---

## Main Scraper: `01_fast_scraper.py`

### Two-Phase Architecture

#### **PHASE 1: URL Collection**
Collects all product URLs from search results

**Process:**
1. **Get Session Cookies** (Playwright)
   - Opens browser to get valid session cookies
   - Accepts cookie consent banner
   - Cookies are reused for all HTTP requests

2. **Single Search Query**
   ```
   URL: /zoeken?searchTerm=632%20Medicijnkosten%20Drugs4
        &type=medicine
        &searchTermHandover=632%20Medicijnkosten%20Drugs4
        &vorm=Alle%20vormen
        &sterkte=Alle%20sterktes
   ```
   - `vorm=Alle vormen` = All forms (tablets, liquids, etc.)
   - `sterkte=Alle sterktes` = All strengths
   - This single query covers ALL ~22,000 products

3. **Pagination Loop**
   - Extracts 100 URLs per page
   - Uses HTTP requests (httpx) for speed
   - **Critical**: Pagination params do NOT include `type=medicine`
   
   ```python
   # Pagination URL params (pages 1, 2, 3...)
   params = {
       "page": "1",
       "searchTerm": "632 Medicijnkosten Drugs4",
       "vorm": "Alle vormen",
       "sterkte": "Alle sterktes",
       "sorting": "",
       "debugMode": ""
   }
   # Note: NO "type" parameter in pagination!
   ```

4. **URL Extraction**
   - XPath: `//a[contains(@class,"result-item") and contains(@class,"medicine")]/@href`
   - Filters: Only URLs starting with `/medicijn?`
   - Skips: Invalid URLs like `/pagenotfound`

5. **Database Storage**
   - Saves to `nl_collected_urls` table
   - Each URL gets a unique ID for tracking

**Output**: ~22,206 unique product URLs

---

#### **PHASE 2: Product Scraping**

**Process:**
1. **Resume Support**
   - Checks `nl_packs` table for already-scraped URLs
   - Only scrapes new/missing products

2. **Concurrent Scraping**
   - **15 workers** running in parallel
   - **Rate limiting**: 200 requests/min (to avoid blocking)
   - **Tor proxy**: Optional (rotates IP every 12 minutes)

3. **HTTP Requests**
   - Uses httpx (async HTTP client)
   - Reuses session cookies from Phase 1
   - Timeout: 30 seconds per request

4. **HTML Parsing** (lxml)
   - Extracts all data from server-rendered HTML
   - No JavaScript execution needed
   - Fast and reliable

5. **Batch Saving**
   - Saves every 100 products to database
   - Crash-safe: Can resume from last batch

6. **Retry Logic**
   - Failed URLs are retried once
   - Errors logged but don't stop the scraper

**Output**: Product data in `nl_packs` table

---

## Data Extraction

### Product Page Structure
Each product URL (e.g., `/medicijn?id=12345`) contains:

#### **Basic Information**
```python
# Product name/description
local_pack_description = <h1> tag content

# Active ingredient
active_substance = <dd class="medicine-active-substance">

# Form (tablet, liquid, etc.)
formulation = <dd class="medicine-method">

# Strength (e.g., "10mg")
strength_size = <dd class="medicine-strength">

# Manufacturer
manufacturer = <dd class="medicine-manufacturer">

# RVG number (Dutch registration code)
local_pack_code = <dd class="medicine-rvg-number">
```

#### **Pricing Information**
```python
# Package price (incl. VAT)
ppp_vat = <span data-pat-depends="inline-days=package">

# Unit price (per piece)
unit_price = <span data-pat-depends="inline-days=piece">

# Package price (excl. VAT) - calculated
ppp_ex_vat = ppp_vat / 1.09  # 9% VAT rate
```

#### **Reimbursement Information**
Extracted from banner messages:

```python
# Reimbursement status
"niet vergoed" → "Not reimbursed"
"volledig vergoed" → "Fully reimbursed" (100%)
"deels vergoed" → "Partially reimbursed" (extract %)
"voorwaarden" → "Reimbursed with conditions"

# Deductible (eigen risico)
deductible = <dd> next to <dt> containing "eigen risico"

# Copay (eigen bijdrage)
copay_price = <dd> next to <dt> containing "eigen bijdrage"
copay_percent = extracted from same field
```

---

## Database Schema

### Tables Used

#### 1. `nl_collected_urls`
Stores all product URLs from Phase 1
```sql
- id (primary key)
- run_id (links to run_ledger)
- url (product URL)
- url_with_id (same as url)
- prefix (category)
- title, active_substance, manufacturer (preview data)
- created_at
```

#### 2. `nl_packs`
Stores complete product data from Phase 2
```sql
- id (primary key)
- run_id
- collected_url_id (foreign key to nl_collected_urls)
- source_url
- local_pack_description
- active_substance
- formulation
- strength_size
- manufacturer
- local_pack_code (RVG number)
- ppp_vat, ppp_ex_vat, unit_price
- currency (EUR)
- vat_percent (9.0)
- reimbursable_status
- reimbursable_rate
- copay_price, copay_percent
- deductible
- reimbursement_message
- margin_rule
- start_date, end_date
- created_at
```

#### 3. `run_ledger`
Tracks scraper runs
```sql
- run_id (primary key)
- country (Netherlands)
- status (running/completed/failed)
- mode (resume/fresh)
- items_scraped
- started_at, finished_at
```

---

## Key Features

### 1. **Tor Integration** (Optional)
- **Purpose**: Avoid IP blocking on large scrapes
- **Configuration**: `TOR_ENABLED=1` in config
- **Rotation**: New IP every 12 minutes (NEWNYM signal)
- **Proxy**: `socks5://127.0.0.1:9050`

### 2. **Rate Limiting**
- **Default**: 200 requests/minute
- **Mechanism**: AsyncRateLimiter enforces delays
- **Purpose**: Respectful scraping, avoid server overload

### 3. **Resume Capability**
- **URL Collection**: Skips if URLs already in database
- **Product Scraping**: Only scrapes new URLs
- **Benefit**: Can stop/restart without losing progress

### 4. **Error Handling**
- **Network errors**: Retry once with exponential backoff
- **404 errors**: Logged but don't stop scraper
- **Proxy errors**: Automatic retry
- **Database errors**: Logged, batch continues

### 5. **Progress Tracking**
```
[DB] Batch saved: 100 | Total: 1200/22206 | 3.1/s | ETA: 45min
```
- Shows: batch size, total progress, rate, estimated time

---

## Configuration

### Environment Variables (Netherlands.env.json)
```json
{
  "SEARCH_TERM": "632 Medicijnkosten Drugs4",
  "BASE_URL": "https://www.medicijnkosten.nl",
  "MARGIN_RULE": "632 Medicijnkosten Drugs4",
  "MAX_WORKERS": "15",
  "BATCH_SIZE": "100",
  "MAX_REQ_PER_MIN": "200",
  "PAGE_DELAY": "0.8",
  "TOR_ENABLED": "1",
  "TOR_CONTROL_PORT": "9051",
  "TOR_SOCKS_PORT": "9050"
}
```

---

## Common Issues & Solutions

### Issue 1: Only ~12,000 URLs Collected (Expected ~22,000)
**Cause**: Including `type=medicine` in pagination params
**Solution**: Remove `type` and `searchTermHandover` from pagination
```python
# ✅ Correct pagination params
params = {
    "page": str(page_num),
    "searchTerm": SEARCH_KEYWORD,
    "vorm": "Alle vormen",
    "sterkte": "Alle sterktes",
    "sorting": "",
    "debugMode": ""
}

# ❌ Wrong - breaks pagination
params = {
    "page": str(page_num),
    "searchTerm": SEARCH_KEYWORD,
    "type": "medicine",  # Don't include this!
    ...
}
```

### Issue 2: 404 Errors for `/pagenotfound`
**Cause**: Invalid hrefs in search results
**Solution**: Filter URLs during extraction
```python
if not href or not href.startswith("/medicijn?"):
    continue
if "pagenotfound" in href.lower():
    continue
```

### Issue 3: Tor Proxy Errors
**Cause**: Tor not running or wrong port
**Solution**: 
1. Check Tor is running: `tor --version`
2. Verify ports in config match Tor settings
3. Test connection: `curl --socks5 127.0.0.1:9050 https://check.torproject.org`

### Issue 4: Slow Scraping
**Causes & Solutions**:
- **Too many workers**: Reduce MAX_WORKERS to 10-15
- **Rate limiting**: Increase MAX_REQ_PER_MIN (carefully!)
- **Tor overhead**: Disable Tor for faster scraping
- **Network issues**: Check internet connection

---

## Execution Flow

### Full Pipeline Run
```bash
python run_pipeline_resume.py --fresh
```

**Steps:**
1. Generates run_id: `nl_20260210_094428`
2. **Step 1**: Backs up previous results
3. **Step 2**: Runs fast scraper
   - Phase 1: Collects ~22,206 URLs (~5 min)
   - Phase 2: Scrapes products (~60-90 min)
4. **Step 3**: Consolidates results to CSV/Excel

### Resume Run
```bash
python run_pipeline_resume.py
```
- Uses existing run_id
- Skips already-collected URLs
- Continues from last scraped product

---

## Output Files

### During Scraping
- `.current_run_id` - Current run identifier
- `medicijnkosten_links.txt` - All collected URLs (optional)

### After Consolidation
- `netherlands_products_YYYYMMDD_HHMMSS.csv`
- `netherlands_products_YYYYMMDD_HHMMSS.xlsx`

---

## Performance Metrics

### Typical Run (Full Scrape)
- **URLs Collected**: ~22,206
- **Phase 1 Time**: 5-10 minutes
- **Phase 2 Time**: 60-90 minutes
- **Total Time**: ~70-100 minutes
- **Success Rate**: >99%
- **Database Size**: ~50MB per run

### Resource Usage
- **CPU**: Low (async I/O bound)
- **Memory**: ~200-500MB
- **Network**: ~100-200 requests/min
- **Database**: ~22,000 inserts

---

## Technical Stack

### Libraries
- **httpx**: Async HTTP client (with SOCKS proxy support)
- **lxml**: Fast HTML parsing
- **playwright**: Browser automation (cookies only)
- **asyncio**: Async/await concurrency
- **psycopg2**: PostgreSQL database

### Why This Stack?
- **httpx vs requests**: Async support, faster
- **lxml vs BeautifulSoup**: 10x faster parsing
- **Playwright vs Selenium**: Modern, faster, better API
- **No JavaScript execution**: Site is server-rendered, saves time

---

## Maintenance Notes

### When Website Changes
1. **Check XPath selectors** in `extract_product_from_html()`
2. **Verify URL structure** in `extract_results()`
3. **Test pagination** - ensure params still work
4. **Update dropdown values** if vorm/sterkte options change

### Database Cleanup
```bash
python cleanup_database_auto.py
```
- Removes old runs
- Keeps last N runs (configurable)

### Monitoring
- Check `run_ledger` table for run status
- Review error logs for patterns
- Monitor success rate per run

---

## Best Practices

1. **Always use --fresh for new data collection**
2. **Enable Tor for large scrapes** (>10k products)
3. **Monitor rate limits** to avoid blocking
4. **Keep backups** of successful runs
5. **Test changes** on small subset first
6. **Review logs** after each run

---

## Troubleshooting Commands

```bash
# Check database connection
python -c "from core.db.postgres_connection import get_db; print(get_db('Netherlands'))"

# View current run status
python show_run_details.py

# Clean old data
python cleanup_database_auto.py

# Test URL extraction
python test_url.py

# Manual scraper run (no pipeline)
python 01_fast_scraper.py
```

---

## Summary

The Netherlands scraper is a **two-phase, async, database-backed system** that:
1. Collects all product URLs via pagination (Phase 1)
2. Scrapes product details concurrently (Phase 2)
3. Supports resume, rate limiting, and Tor proxy
4. Stores structured data in PostgreSQL
5. Handles ~22,000 products in ~70-100 minutes

**Key Success Factor**: Correct pagination parameters (no `type=medicine` in pagination!)
