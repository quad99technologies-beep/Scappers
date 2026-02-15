# Netherlands Scraper - Critical Fixes Required

## Issue 1: PPP Pricing Logic Bug ⚠️ CRITICAL

### Problem:
Line 1792 in `01_get_medicijnkosten_data.py` incorrectly assigns deductible to PPP:

```python
# WRONG:
ppp_vat = deductible_value(driver)  # This is "Eigen risico" (€2.38), NOT PPP!
```

### Fix:
```python
# CORRECT:
ppp_vat = pack_price_vat  # This is "Gemiddelde prijs per..." (€4.81) = actual PPP
```

### Impact:
- ALL scraped data has wrong PPP values
- Deductible values are stored as PPP
- Actual PPP values are ignored

---

## Issue 2: Step 2 Workflow Mismatch ⚠️ CRITICAL

### Problem:
The pipeline runs:
1. Step 1: `01_collect_urls.py` → Collects 22,206 URLs ✅
2. Step 2: `01_get_medicijnkosten_data.py` → Looks for combinations ❌

But `01_get_medicijnkosten_data.py` expects combinations, not collected URLs!

### Current Behavior:
```
[Step 2/3] Scrape Products...
[MODE] Using COMBINATION-BASED collection
[COMBO] No pending combinations found.
[STATS] URLs scraped: 0  ← Nothing scraped!
```

### Root Cause:
`01_get_medicijnkosten_data.py` is a **dual-purpose script** that:
- Collects URLs (if combinations exist)
- Scrapes products (from collected URLs)

Since we now have separate `01_collect_urls.py`, the script finds no combinations and exits.

### Solution Options:

#### Option A: Update 01_get_medicijnkosten_data.py (Recommended)
Make it skip URL collection and go straight to product scraping:

```python
# Skip combination-based collection
# Go directly to scraping from nl_collected_urls table
```

#### Option B: Use Different Run IDs
Make Step 2 use the same run_id as Step 1:

```python
# In 01_get_medicijnkosten_data.py
# Use run_id from Step 1 instead of generating new one
```

#### Option C: Simplify Pipeline (Best Long-term)
Create a dedicated product scraper that only reads from `nl_collected_urls`:

```python
# New file: 02_scrape_products.py
# Only scrapes products from URLs in database
# No URL collection logic
```

---

## Immediate Fixes Needed:

### Fix 1: PPP Logic (5 minutes)
File: `01_get_medicijnkosten_data.py`
Line: 1792-1794

Change:
```python
ppp_vat = deductible_value(driver)
ppp_vat_float = euro_str_to_float(ppp_vat)
ppp_ex_vat = fmt_float(ppp_vat_float / (1.0 + VAT_RATE) if ppp_vat_float is not None else None)
```

To:
```python
# PPP is the package price, not deductible
ppp_vat = pack_price_vat  # Already extracted from prices.get("package")
ppp_vat_float = euro_str_to_float(ppp_vat)
ppp_ex_vat = fmt_float(ppp_vat_float / 1.09 if ppp_vat_float is not None else None)

# Store deductible separately (add to PackRow dataclass)
deductible_vat = deductible_value(driver)
deductible_vat_float = euro_str_to_float(deductible_vat)
deductible_ex_vat = fmt_float(deductible_vat_float / 1.09 if deductible_vat_float is not None else None)
```

### Fix 2: Run ID Continuity (10 minutes)
File: `01_get_medicijnkosten_data.py`
Find where run_id is generated, change to:

```python
# Try to get latest run_id from nl_collected_urls first
with db.cursor() as cur:
    cur.execute("""
        SELECT DISTINCT run_id 
        FROM nl_collected_urls 
        ORDER BY created_at DESC 
        LIMIT 1
    """)
    result = cur.fetchone()
    if result:
        run_id = result[0]
        print(f"[INFO] Using existing run_id from URL collection: {run_id}")
    else:
        # Generate new run_id if no URLs collected
        run_id = f"nl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"[INFO] Generated new run_id: {run_id}")
```

### Fix 3: Skip Combination Collection (5 minutes)
File: `01_get_medicijnkosten_data.py`
Find the combination collection logic and add:

```python
# Check if URLs already collected
urls_count = repo.get_collected_url_count()
if urls_count > 0:
    print(f"[INFO] Found {urls_count} URLs already collected")
    print(f"[INFO] Skipping combination-based collection")
    # Skip to product scraping phase
else:
    # Original combination logic
    ...
```

---

## Testing Plan:

1. Apply Fix 1 (PPP logic)
2. Apply Fix 2 (Run ID continuity)  
3. Apply Fix 3 (Skip combination collection)
4. Run pipeline:
   ```bash
   cd d:\quad99\Scrappers\scripts\Netherlands
   .\run_pipeline.bat
   ```
5. Verify:
   - Step 1 collects 22,206 URLs ✅
   - Step 2 uses same run_id and scrapes products ✅
   - Step 3 consolidates data ✅

---

## Priority:

1. **Fix 1 (PPP)** - CRITICAL - Wrong data being stored
2. **Fix 2 (Run ID)** - CRITICAL - Pipeline broken
3. **Fix 3 (Skip combinations)** - HIGH - Prevents scraping

Apply all three fixes before next run!
