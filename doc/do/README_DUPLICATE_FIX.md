# India Scraper — Quick Start

## ⚠️ IMPORTANT: Fix Duplicate Data First!

The error you're seeing is because **existing data in the database has duplicates**, preventing the unique indexes from being created.

### 🔧 One-Time Fix (Run This First)

```bash
cd d:\quad99\Scrappers
python scripts\India\fix_duplicates.py
```

This will:
1. ✅ Remove all duplicate rows from India tables
2. ✅ Create the required unique indexes
3. ✅ Verify everything is clean

**Expected output:**
```
============================================================
India: Fixing Duplicate Data
============================================================

Step 1: Checking for duplicates...
  in_sku_mrp: 123 duplicates found
  in_med_details: 45 duplicates found
  in_brand_alternatives: 678 duplicates found
  in_sku_main: 234 duplicates found

Step 2: Removing 1080 duplicate rows...
✓ Duplicates removed and indexes created successfully!

Step 3: Verifying fix...
  in_sku_mrp: Clean ✓
  in_med_details: Clean ✓
  in_brand_alternatives: Clean ✓
  in_sku_main: Clean ✓

============================================================
SUCCESS: All India tables are now duplicate-free!
You can now run the scraper without duplicate key errors.
============================================================
```

---

## 🚀 After Fixing Duplicates

### Run the Scraper

```bash
# Fresh run with 5 workers
python scripts\India\run_pipeline_scrapy.py --fresh --workers 5

# Resume interrupted run
python scripts\India\run_pipeline_scrapy.py --workers 5
```

---

## 📋 What Changed?

The India scraper **already uses dedicated tables** with the `in_` prefix:
- `in_sku_main` — Main SKU data
- `in_sku_mrp` — MRP details
- `in_brand_alternatives` — Alternative brands
- `in_med_details` — Medicine details
- `in_formulation_status` — Work queue
- `in_input_formulations` — Input data

**These tables are India-specific and separate from other countries.**

The issue was that:
1. Old runs created duplicate data (before the duplicate prevention fix)
2. The unique indexes couldn't be created because duplicates existed
3. The fix script removes duplicates and creates the indexes

---

## ✅ Improvements Made

1. **Duplicate Prevention** — Fixed bug that allowed duplicates
2. **Circuit Breaker** — Pauses when NPPA API is down
3. **DB Reconnection** — Auto-reconnects on network failures
4. **Crash Logging** — Saves crash details to `crash_log.json`
5. **Performance** — 5-10x faster batch writes with `execute_values`
6. **Transaction Safety** — Atomic writes per formulation with savepoints

See `IMPROVEMENT_PLAN.md` for full details.

---

## 🐛 Still Having Issues?

1. **Check the crash log**: `output\India\crash_log.json`
2. **Check worker logs**: `output\India\logs\worker_*.log`
3. **Verify database connection**: Run `python scripts\India\health_check.py`

---

**Questions? Check `IMPROVEMENT_PLAN.md` for detailed documentation.**
