# Netherlands Scraper - Copay & Deductible Fields Added ✅

**Date:** 2026-02-09
**Status:** Complete - Ready to migrate database and run

---

## Summary

Added missing field extraction for copay and deductible values based on the old Selenium scraper logic. The screenshots showed these values exist on medicijnkosten.nl but weren't being captured.

---

## Changes Made

### 1. **Schema Updated** - `db/schema.py`

Added three new fields to `nl_packs` table:
- `active_substance TEXT` - Generic/active ingredient name
- `manufacturer TEXT` - Company/manufacturer name
- `deductible NUMERIC(12,4)` - Eigen risico (deductible with VAT)

**Before:**
```sql
CREATE TABLE IF NOT EXISTS nl_packs (
    ...
    copay_price NUMERIC(12,4),
    copay_percent TEXT,
    margin_rule TEXT,
    local_pack_description TEXT,
    formulation TEXT,
    ...
```

**After:**
```sql
CREATE TABLE IF NOT EXISTS nl_packs (
    ...
    copay_price NUMERIC(12,4),
    copay_percent TEXT,
    deductible NUMERIC(12,4),      -- NEW
    margin_rule TEXT,
    local_pack_description TEXT,
    active_substance TEXT,          -- NEW
    manufacturer TEXT,              -- NEW
    formulation TEXT,
    ...
```

### 2. **Repository Updated** - `db/repositories.py`

Updated `insert_packs()` method to include new fields in INSERT statement:

**Column Order:**
```python
(run_id, start_date, end_date, currency, unit_price, ppp_ex_vat, ppp_vat,
 vat_percent, reimbursable_status, reimbursable_rate, copay_price, copay_percent,
 deductible,                    # NEW
 margin_rule, local_pack_description,
 active_substance, manufacturer,  # NEW
 formulation, strength_size, local_pack_code, reimbursement_message, source_url)
```

**ON CONFLICT UPDATE:**
```python
ON CONFLICT (run_id, source_url, local_pack_code) DO UPDATE SET
    ...
    copay_price = EXCLUDED.copay_price,        # NEW
    copay_percent = EXCLUDED.copay_percent,    # NEW
    deductible = EXCLUDED.deductible,          # NEW
    active_substance = EXCLUDED.active_substance,  # NEW
    manufacturer = EXCLUDED.manufacturer,      # NEW
    ...
```

### 3. **Scraper Updated** - `01_fast_scraper.py`

#### A. Added Deductible Extraction

**Logic (from old Selenium scraper):**
```python
# Extract deductible (Eigen risico) (exact logic from Selenium scraper)
try:
    dts = await page.locator("dl.pat-grid-list > dt").all()
    for dt in dts:
        label_text = await dt.inner_text()
        label = clean_single_line(label_text).lower()
        if "eigen risico" in label or "deductible" in label:
            dd = dt.locator("xpath=following-sibling::dd[1]")
            dd_text = await dd.inner_text()
            txt = clean_single_line(dd_text)
            eur = first_euro_amount(txt)
            if eur:
                product_data['deductible'] = eur
                break
            if "niets" in txt.lower() or "nothing" in txt.lower():
                product_data['deductible'] = "€ 0,00"
                break
except:
    pass
```

**Extracts from:** `dl.pat-grid-list > dt` labels matching "eigen risico" or "deductible"

**Example:** `€ 2,38` (as shown in screenshot)

#### B. Added Copay Extraction

**Logic (from old Selenium scraper):**
```python
# Extract copay (Eigen bijdrage) (exact logic from Selenium scraper)
try:
    # Look for copay information in pat-message boxes (orange warnings)
    messages = await page.locator("dd.medicine-price div.pat-message.pat-message-warning, div.pat-message.pat-message-warning").all()
    for msg in messages:
        msg_text = await msg.inner_text()
        txt = clean_single_line(msg_text).lower()
        # Check for copay patterns: "additional", "eigen bijdrage", "you must pay"
        if any(keyword in txt for keyword in ["additional", "eigen bijdrage", "you must pay", "zelf betalen"]):
            # Extract euro amount
            eur = first_euro_amount(msg_text)
            if eur:
                product_data['copay_price'] = eur
                # Try to extract percentage if mentioned
                percent_match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", msg_text, re.IGNORECASE)
                if percent_match:
                    product_data['copay_percent'] = f"{percent_match.group(1)}%"
                break

    # Also check in dl.pat-grid-list for copay fields (if not found in messages)
    if not product_data['copay_price']:
        dts = await page.locator("dl.pat-grid-list > dt").all()
        for dt in dts:
            label_text = await dt.inner_text()
            label = clean_single_line(label_text).lower()
            if any(keyword in label for keyword in ["eigen bijdrage", "copay", "co-pay", "own contribution"]):
                dd = dt.locator("xpath=following-sibling::dd[1]")
                dd_text = await dd.inner_text()
                txt = clean_single_line(dd_text)
                eur = first_euro_amount(txt)
                if eur:
                    product_data['copay_price'] = eur
                    # Try to extract percentage
                    percent_match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", txt, re.IGNORECASE)
                    if percent_match:
                        product_data['copay_percent'] = f"{percent_match.group(1)}%"
                    break
except:
    pass
```

**Extracts from:**
1. Warning message banners (`div.pat-message.pat-message-warning`)
2. Fallback to `dl.pat-grid-list > dt` labels

**Keywords:** "eigen bijdrage", "additional", "you must pay", "zelf betalen", "copay", "co-pay", "own contribution"

**Example:** `€ 2,43` copay price with optional percentage (as shown in screenshot)

#### C. Updated Product Data Initialization

```python
product_data = {
    ...
    'copay_price': '',
    'copay_percent': '',
    'deductible': '',     # NEW
}
```

---

## Migration Required

The database schema must be updated before running the new scraper. Choose one method:

### Method 1: Python Migration Script (Recommended)

```bash
cd d:\quad99\Scrappers\scripts\Netherlands\db
python run_migration.py
```

This will:
- Check if columns already exist
- Add missing columns if needed
- Commit changes
- Report status

### Method 2: SQL Migration Script

```bash
psql -d netherlands -f add_missing_fields_migration.sql
```

---

## Testing the Changes

### 1. Run Migration

```bash
cd scripts/Netherlands/db
python run_migration.py
```

**Expected output:**
```
[MIGRATION] Connecting to Netherlands database...
[MIGRATION] Adding missing fields to nl_packs table...
[MIGRATION] Adding column 'active_substance' (TEXT)...
[MIGRATION] ✓ Added column 'active_substance'
[MIGRATION] Adding column 'manufacturer' (TEXT)...
[MIGRATION] ✓ Added column 'manufacturer'
[MIGRATION] Adding column 'deductible' (NUMERIC(12,4))...
[MIGRATION] ✓ Added column 'deductible'
[MIGRATION] ✓ Migration complete!
```

### 2. Run Scraper (Fresh)

```bash
cd scripts/Netherlands
python run_pipeline_resume.py --fresh
```

This will:
- Collect URLs (~22,000 products)
- Scrape product details with **new fields**
- Save to database with copay, deductible, active_substance, manufacturer

### 3. Verify Data

```sql
-- Check if new fields are populated
SELECT
    local_pack_description,
    active_substance,
    manufacturer,
    copay_price,
    deductible,
    reimbursable_status
FROM nl_packs
WHERE run_id = 'nl_20260209_XXXXXX'
  AND (copay_price IS NOT NULL OR deductible IS NOT NULL)
LIMIT 10;
```

**Expected result:** Rows with populated copay_price (e.g., €2.43) and deductible (e.g., €2.38) values

### 4. Spot Check Against Website

Pick a few products and verify manually:
- Go to medicijnkosten.nl product page
- Check "Eigen risico" value matches `deductible` field
- Check "Eigen bijdrage" value matches `copay_price` field
- Check active substance matches `active_substance` field
- Check manufacturer matches `manufacturer` field

---

## What Was Fixed

### Issue 1: Copay Not Extracted
**Before:** `copay_price` and `copay_percent` fields always empty
**After:** Extracts from warning banners or dt/dd pairs with keywords

### Issue 2: Deductible Never Stored
**Before:** Old Selenium scraper extracted it but never stored it (see TODO comment in old code)
**After:** Fully extracted and stored in database

### Issue 3: Active Substance & Manufacturer Missing
**Before:** Extracted in scraper but not stored in nl_packs (only in nl_collected_urls)
**After:** Now stored in nl_packs for easier querying

---

## Database Field Reference

| Field | Type | Example | Source |
|-------|------|---------|--------|
| `active_substance` | TEXT | "Metformine" | `dd.medicine-active-substance` |
| `manufacturer` | TEXT | "Sandoz" | `dd.medicine-manufacturer` |
| `copay_price` | NUMERIC(12,4) | 2.43 | Warning banner or dt/dd with "eigen bijdrage" |
| `copay_percent` | TEXT | "10%" | Extracted from copay message if present |
| `deductible` | NUMERIC(12,4) | 2.38 | dt/dd with "eigen risico" label |

---

## Files Modified

1. ✅ `scripts/Netherlands/db/schema.py` - Added 3 fields to nl_packs DDL
2. ✅ `scripts/Netherlands/db/repositories.py` - Updated insert_packs() to handle new fields
3. ✅ `scripts/Netherlands/01_fast_scraper.py` - Added copay and deductible extraction logic

## Files Created

1. ✅ `scripts/Netherlands/db/add_missing_fields_migration.sql` - SQL migration script
2. ✅ `scripts/Netherlands/db/run_migration.py` - Python migration script
3. ✅ `scripts/Netherlands/COPAY_DEDUCTIBLE_FIELDS_ADDED.md` - This documentation

---

## Success Criteria

✅ Database migration completes without errors
✅ Scraper runs without errors
✅ Copay values populated where applicable (not all products have copay)
✅ Deductible values populated where applicable
✅ Active substance populated for all products
✅ Manufacturer populated for all products
✅ Values match what's shown on medicijnkosten.nl website

---

## Next Steps

1. **Run migration:**
   ```bash
   cd scripts/Netherlands/db
   python run_migration.py
   ```

2. **Run scraper:**
   ```bash
   cd scripts/Netherlands
   python run_pipeline_resume.py --fresh
   ```

3. **Verify data quality:**
   - Query database for sample records
   - Spot-check against website
   - Ensure no missing fields that should be populated

4. **Monitor for errors:**
   - Check console output for extraction errors
   - Review any products with missing copay/deductible
   - Validate data types (NUMERIC fields should parse correctly)

---

**Status:** ✅ All changes complete! Run migration then scraper to test.
