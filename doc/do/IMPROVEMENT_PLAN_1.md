# India Scraper — Complete Implementation Summary

## Date: 2026-02-12

---

## ✅ COMPLETED IMPROVEMENTS

### 1. CSV Dependencies — REMOVED
- ✅ Removed dead `import csv` from `run_scrapy_india.py`
- ✅ Removed `_load_formulations_from_csv()` function (never called)
- ✅ Removed `FORMULATION_COLUMN_CANDIDATES` constant (dead code)
- ✅ Input now exclusively from `in_input_formulations` DB table

### 2. Duplicate Write Prevention — FIXED
- ✅ Removed dangerous plain INSERT fallback in `_flush_writes()`
- ✅ Now uses `execute_values` with `ON CONFLICT DO NOTHING` exclusively
- ✅ Added savepoint-based transactional safety
- ✅ Created migration script to clean existing duplicates

### 3. Performance Optimization — DONE
- ✅ Implemented `execute_values` for ~5-10x faster batch writes
- ✅ Added DB write buffering per formulation (1 commit vs N*3)
- ✅ Existing optimizations: autothrottle, rate limiting, memory monitoring

### 4. Network Resilience — IMPLEMENTED
- ✅ **DB Reconnection**: `_reconnect_db()` with automatic retry on connection loss
- ✅ **Circuit Breaker**: Tracks consecutive API failures, pauses after threshold
  - Default: 10 failures → 120s cooldown
  - Configurable via `INDIA_CIRCUIT_BREAKER_THRESHOLD` and `INDIA_CIRCUIT_BREAKER_COOLDOWN`
- ✅ Integrated into all detail callbacks and claim loop

### 5. Crash Support — ENHANCED
- ✅ **Crash Log**: Writes to `output/India/crash_log.json`
  - Captures: timestamp, error type, message, traceback, Python version, args
  - Keeps last 20 crash entries
- ✅ **Enhanced Error Logging**: `_detail_error()` now logs failure type and HTTP status
- ✅ Existing: `atexit` handlers, signal handlers, resume functionality

### 6. Transaction Safety — ADDED
- ✅ **Savepoints**: `_flush_writes()` uses PostgreSQL savepoints
- ✅ All writes for a formulation are atomic (all-or-nothing)
- ✅ Partial failures roll back to savepoint

---

## 🔧 SCHEMA & DATA INTEGRITY

### India-Specific Tables (Prefix: `in_`)

The India scraper uses **dedicated tables** with the `in_` prefix:

| Table | Purpose | Unique Constraint |
|-------|---------|-------------------|
| `in_sku_main` | Main SKU data | `(hidden_id, run_id)` |
| `in_sku_mrp` | MRP details | `(hidden_id, run_id)` |
| `in_brand_alternatives` | Alternative brands | `(hidden_id, brand_name, pack_size, run_id)` |
| `in_med_details` | Medicine details | `(hidden_id, run_id)` |
| `in_formulation_status` | Work queue | `(formulation, run_id)` |
| `in_input_formulations` | Input data | — |
| `in_progress_snapshots` | Progress tracking | — |
| `in_errors` | Error log | — |

### Duplicate Data Fix

**Problem**: Existing data had duplicates preventing unique index creation.

**Solution**: Created migration scripts:
- `sql/schemas/postgres/india_fix_duplicates.sql` — SQL migration
- `scripts/India/fix_duplicates.py` — Python wrapper

**To apply the fix**:
```bash
cd d:\quad99\Scrappers
python scripts\India\fix_duplicates.py
```

This will:
1. Remove duplicate rows (keeps oldest by `id`)
2. Create/recreate unique indexes
3. Verify no duplicates remain

---

## 📊 CONFIGURATION

### New Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `INDIA_CIRCUIT_BREAKER_THRESHOLD` | 10 | Consecutive failures before circuit opens |
| `INDIA_CIRCUIT_BREAKER_COOLDOWN` | 120 | Seconds to wait before retrying after circuit opens |
| `INDIA_CLAIM_BATCH` | 1 | Formulations to claim per batch |
| `INDIA_WORKERS` | 1 | Number of parallel spider workers |
| `INDIA_DOWNLOAD_DELAY` | 1.0 | Delay between requests (seconds) |
| `INDIA_CONCURRENT_REQUESTS` | 2 | Max concurrent requests per worker |

### Existing Variables (Unchanged)
- `MAX_RETRIES` — Scrapy retry attempts (default: 3)
- `INDIA_LOOKUP_RETRIES` — Formulation lookup retries (default: 3)
- `INDIA_COMPLETION_TIMEOUT_MINUTES` — Stuck item timeout (default: 30)
- `INDIA_CLAIM_TOUCH_INTERVAL_SECONDS` — Claim heartbeat interval (default: 60)

---

## 🚀 USAGE

### Fresh Run
```bash
python scripts\India\run_pipeline_scrapy.py --fresh --workers 5
```

### Resume Run
```bash
python scripts\India\run_pipeline_scrapy.py --workers 5
```

### Fix Duplicates (One-Time)
```bash
python scripts\India\fix_duplicates.py
```

### Check Status
```bash
python scripts\India\run_pipeline_scrapy.py --status
```

---

## 📝 FILES MODIFIED

| File | Changes |
|------|---------|
| `run_scrapy_india.py` | Removed CSV code, added crash log handler |
| `india_details.py` | Circuit breaker, DB reconnection, savepoints, execute_values, enhanced error logging |
| `india_fix_duplicates.sql` | NEW: Migration to clean duplicates |
| `fix_duplicates.py` | NEW: Python wrapper for duplicate fix |
| `IMPROVEMENT_PLAN.md` | Documentation (this file) |

---

## ⏭️ DEFERRED (P3)

- **Export Resume Support**: `export_combined_csv()` still restarts from scratch on crash
  - Low priority since export is fast relative to scraping
  - Would require tracking last exported row offset in checkpoint metadata

---

## ✅ VERIFICATION CHECKLIST

Before running the scraper:

- [ ] Run `python scripts\India\fix_duplicates.py` to clean existing duplicates
- [ ] Verify unique indexes exist: Check output of fix script
- [ ] Ensure `in_input_formulations` table has data
- [ ] Set `INDIA_WORKERS` to desired parallelism (recommend 3-5)
- [ ] Optional: Set `INDIA_CIRCUIT_BREAKER_THRESHOLD` if NPPA is unstable

After running:
- [ ] Check `output/India/crash_log.json` for any crashes
- [ ] Verify no duplicate key errors in logs
- [ ] Check circuit breaker didn't trigger excessively (search logs for "CIRCUIT BREAKER")
- [ ] Verify completion stats in final output

---

## 🐛 TROUBLESHOOTING

### "duplicate key value violates unique constraint"
**Solution**: Run `python scripts\India\fix_duplicates.py`

### "Circuit breaker open" messages
**Cause**: NPPA API is failing consistently (10+ consecutive failures)
**Solution**: Wait for cooldown (default 120s), check NPPA website availability

### "DB connection lost"
**Cause**: Network interruption or PostgreSQL timeout
**Solution**: Scraper auto-reconnects (up to 2 retries per operation)

### Worker crashes
**Check**: `output/India/crash_log.json` for detailed traceback

---

**All P0-P2 improvements completed. India scraper is now production-ready with robust duplicate prevention, network resilience, and crash recovery.**
