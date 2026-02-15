# 🇳🇱 Netherlands Scraper - Quick Start Guide

**Last Updated:** 2026-02-09  
**Status:** ✅ Production Ready

---

## 📋 WORKFLOW SUMMARY

The Netherlands scraper follows this sequence:

```
1. BACKUP → 2. CREATE COMBINATIONS → 3. GRAB URLs → 4. GRAB PRODUCT DATA → 5. CONSOLIDATE
```

### Detailed Steps:

#### **Step 0: Backup & Clean** 
```bash
python 00_backup_and_clean.py
```
- Backs up previous run data
- Cleans output directory

#### **Step 1: Create Combinations**
```bash
python 01_load_combinations.py
```
- Extracts vorm (form) and sterkte (strength) values from website
- Generates all combinations (vorm × sterkte)
- Stores in `nl_search_combinations` table

#### **Step 2: Grab URLs - FAST Playwright Method** ⚡
```bash
python "1-url scrapper.py"
```
- Uses **Playwright** to get cookies/session (like real browser)
- Then uses **HTTP XHR pagination** (no scrolling!)
- **10-20x faster** than Selenium scrolling
- Outputs: `medicijnkosten_links.txt`

**Performance:**
- Old method (Selenium scrolling): 30-60 minutes
- New method (Playwright + HTTP): 2-5 minutes 🚀

#### **Step 3: Grab Product Data - Selenium Multi-threaded**
```bash
python 01_get_medicijnkosten_data.py
```
- Loads URLs from file or database
- Multi-threaded scraping (configurable workers)
- Extracts pricing and reimbursement data
- Stores in `nl_packs` table

#### **Step 4: Reimbursement Extraction**
```bash
python 02_reimbursement_extraction.py
```
- Extracts detailed reimbursement data
- Stores in `nl_reimbursement` table

#### **Step 5: Consolidation**
```bash
python 03_Consolidate_Results.py
```
- Merges data from all tables
- Exports to final format

---

## 🚀 QUICK START

### Full Pipeline:

```bash
cd d:\quad99\Scrappers\scripts\Netherlands

# Run all steps
python 00_backup_and_clean.py
python 01_load_combinations.py
python "1-url scrapper.py"
python 01_get_medicijnkosten_data.py
python 02_reimbursement_extraction.py
python 03_Consolidate_Results.py
```

### Just Collect URLs (Fast):

```bash
cd d:\quad99\Scrappers\scripts\Netherlands
python "1-url scrapper.py"
# Output: medicijnkosten_links.txt
```

---

## 🔑 KEY FEATURES

### 1. **Playwright URL Scraper** (`1-url scrapper.py`)

**Why it's fast:**
- ✅ Uses Playwright once to get cookies/session
- ✅ Then switches to pure HTTP requests
- ✅ No scrolling needed - uses XHR pagination
- ✅ 10-20x faster than Selenium

**How it works:**
```python
# Phase 1: Get cookies with Playwright (one-time)
- Launch browser
- Navigate to search page
- Accept cookie banner
- Get cookies
- Close browser

# Phase 2: HTTP loop (FAST!)
- Use cookies from Phase 1
- Loop through pages using HTTP GET
- Extract links from each page
- Stop when: total reached OR 3 empty pages
```

### 2. **Multi-threaded Product Scraper** (`01_get_medicijnkosten_data.py`)

**Features:**
- ✅ Configurable worker threads (default: 4)
- ✅ Each worker has own browser instance
- ✅ Automatic retry with exponential backoff
- ✅ Timeout guards prevent infinite loops
- ✅ Data validation before database insertion
- ✅ Resume capability

### 3. **Database-First Architecture**

**All data stored in PostgreSQL:**
- `nl_search_combinations` - vorm/sterkte combinations
- `nl_collected_urls` - Product URLs
- `nl_packs` - Product pricing data
- `nl_reimbursement` - Reimbursement details
- `nl_consolidated` - Merged final data

**Benefits:**
- ✅ No CSV files to manage
- ✅ Easy to query and analyze
- ✅ Resume capability built-in
- ✅ Progress tracking

---

## ⚙️ CONFIGURATION

### Key Environment Variables:

```bash
# Threading
SCRAPE_THREADS=4                   # Number of parallel workers

# Browser
HEADLESS_SCRAPE=true               # Hide browser windows

# Timeouts
ABSOLUTE_TIMEOUT_MINUTES=300       # Max time for operations (5 hours)
PAGELOAD_TIMEOUT=90                # Page load timeout (seconds)

# Network Retry
NETWORK_RETRY_MAX=3                # Max retry attempts
NETWORK_RETRY_DELAY=5              # Base delay (uses exponential backoff)
```

---

## 📊 MONITORING PROGRESS

### Real-time Logs:

```
[PLAYWRIGHT] Total expected: 5,234
[PLAYWRIGHT] page=1 status=200 links=50 new=50 total_seen=50
[PLAYWRIGHT] page=2 status=200 links=50 new=50 total_seen=100
...
[PLAYWRIGHT] Saved: 5,234 links to medicijnkosten_links.txt

[SCRAPE] Worker 1: Processing URL 123/5234
[SCRAPE] Worker 2: Processing URL 124/5234
[DB] Inserted 1 pack record
[PROGRESS] 50.0% complete (2,617/5,234 URLs)
```

### Database Queries:

```sql
-- Check URL collection status
SELECT 
    COUNT(*) as total_urls,
    SUM(CASE WHEN packs_scraped THEN 1 ELSE 0 END) as scraped,
    SUM(CASE WHEN NOT packs_scraped THEN 1 ELSE 0 END) as pending
FROM nl_collected_urls
WHERE run_id = 'your_run_id';

-- Check pack data count
SELECT COUNT(*) FROM nl_packs WHERE run_id = 'your_run_id';

-- Check errors
SELECT * FROM nl_errors WHERE run_id = 'your_run_id' ORDER BY created_at DESC;
```

---

## 🐛 TROUBLESHOOTING

### Issue: Playwright script fails

**Solution:**
```bash
# Install Playwright browsers
pip install playwright
playwright install chromium
```

### Issue: "Too Many Requests" (429)

**Solution:**
```bash
# Reduce threads
set SCRAPE_THREADS=2
python 01_get_medicijnkosten_data.py
```

### Issue: Chrome crashes

**Solution:**
```bash
# Kill all Chrome instances
.\killChrome.bat

# Restart scraper
python 01_get_medicijnkosten_data.py
```

### Issue: Database connection error

**Solution:**
```bash
# Check database is running
# Check config/Netherlands.env.json credentials
# Test connection:
python -c "from core.db.postgres_connection import get_db; db = get_db(); print('Connected!')"
```

---

## 📈 PERFORMANCE COMPARISON

### URL Collection:

| Method | Time | Speed |
|--------|------|-------|
| **Old: Selenium Scrolling** | 30-60 min | Slow 🐌 |
| **New: Playwright + HTTP** | 2-5 min | **10-20x faster!** 🚀 |

### Why Playwright is Faster:

1. **No scrolling needed** - Uses XHR pagination endpoint
2. **Pure HTTP requests** - After initial cookie grab
3. **No browser overhead** - Only Playwright once, then httpx
4. **Parallel-ready** - Can run multiple instances easily

---

## 📚 FILES OVERVIEW

### Main Scripts:

- `00_backup_and_clean.py` - Backup previous run
- `01_load_combinations.py` - Create vorm/sterkte combinations
- **`1-url scrapper.py`** - **FAST URL collection (Playwright)**
- `01_get_medicijnkosten_data.py` - Multi-threaded product scraping
- `02_reimbursement_extraction.py` - Reimbursement data
- `03_Consolidate_Results.py` - Merge and export

### Utilities:

- `config_loader.py` - Configuration management
- `data_validator.py` - Data validation
- `url_builder.py` - URL construction
- `scraper_utils.py` - Common utilities

### Database:

- `db/schema.py` - Table definitions
- `db/repositories.py` - Database operations

---

## 🎯 BEST PRACTICES

1. **Always backup first**: Run `00_backup_and_clean.py`
2. **Use Playwright for URLs**: Much faster than Selenium scrolling
3. **Start with 2-4 threads**: For stability
4. **Monitor logs**: Watch for errors
5. **Use resume mode**: Don't restart from scratch

---

## 📞 NEED HELP?

1. Check `NETHERLANDS_SCRAPER_OVERVIEW.md` for detailed documentation
2. Review error logs in `output/Netherlands/`
3. Check database: `SELECT * FROM nl_errors`
4. Run health check: `python health_check.py`

---

**End of Quick Start Guide**

For detailed documentation, see: `NETHERLANDS_SCRAPER_OVERVIEW.md`
