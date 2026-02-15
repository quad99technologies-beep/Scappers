# Netherlands - Argentina Pattern Alignment ✅

## Changes Made to Match Argentina

### 1. Run ID Resume Priority (Argentina Pattern)
**Argentina Logic**:
```python
SELECT run_id FROM run_ledger 
WHERE scraper_name = 'Argentina'
ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC
LIMIT 1
```

**Netherlands Now** (UPDATED):
```python
SELECT run_id FROM run_ledger 
WHERE scraper_name = 'Netherlands'
  AND status IN ('running', 'partial', 'resume', 'stopped')
ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC
LIMIT 1
```

**Why This Matters**:
- Prioritizes runs with **actual data** (items_scraped > 0)
- Prevents resuming empty/bad runs
- Chooses the run with most progress

---

## Common Patterns Verified

### ✅ Database-First Architecture
**Argentina**: No CSV files in main workflow, all data in PostgreSQL
**Netherlands**: ✅ Implemented - no CSV/TXT files

### ✅ Run ID Management
**Argentina**: 
- `.current_run_id` file
- `ARGENTINA_RUN_ID` environment variable
- Database check prioritizes runs with data

**Netherlands**: ✅ Implemented
- `.current_run_id` file
- `NL_RUN_ID` environment variable
- Database check now prioritizes runs with data

### ✅ Repository Pattern
**Argentina**: `ArgentinaRepository(db, run_id)`
**Netherlands**: ✅ `NetherlandsRepository(db, run_id)`

### ✅ Schema Application
**Argentina**: `apply_argentina_schema(db)`
**Netherlands**: ✅ `apply_netherlands_schema(db)`

### ✅ Step Progress Tracking
**Argentina**: `ar_step_progress` table
**Netherlands**: ✅ `nl_step_progress` table

### ✅ Database Resume Logic
**Argentina**: Checks database for step completion
**Netherlands**: ✅ `_is_step_complete_in_db()` function

---

## Pattern Differences (Intentional)

### Connection Method
**Argentina**: 
```python
from core.db.connection import CountryDB
db = CountryDB("Argentina")
```

**Netherlands**: 
```python
from core.db.postgres_connection import get_db
db = get_db("Netherlands")
```

**Status**: Different but both valid patterns

### Scraping Technology
**Argentina**: Selenium (complex dynamic site)
**Netherlands**: Playwright (modern async, faster)

**Status**: Tool choice based on site requirements

---

## Key Improvements from Argentina Pattern

### 1. Smart Resume Priority
**Before**:
```python
ORDER BY started_at DESC  # Just by time
```

**After (Argentina pattern)**:
```python
ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC
# Prioritizes runs with data!
```

**Impact**:
- If run A: started 10 mins ago, 0 products
- And run B: started 1 hour ago, 10,000 products
- **Resumes run B** (the one with actual data) ✅

### 2. Database Verification for Steps
**Argentina Pattern**: Checks actual data in tables
**Netherlands**: ✅ Implemented `_is_step_complete_in_db()`

**Impact**:
- Step 1 complete = URLs exist AND products exist
- Step 2 complete = consolidated records exist
- No more file-based verification

---

## Testing

### Scenario: Multiple Runs in Database
```sql
-- Run 1: recent but empty (bad run)
run_id: nl_20260209_210000
status: running
items_scraped: 0
started_at: 2026-02-09 21:00:00

-- Run 2: older but has data (good run)
run_id: nl_20260209_200000
status: running
items_scraped: 15000
started_at: 2026-02-09 20:00:00
```

**Old Logic**: Would resume Run 1 (more recent, but empty)
**New Logic (Argentina pattern)**: ✅ Resumes Run 2 (has 15K products)

---

## Summary

Netherlands now follows Argentina's proven patterns:

✅ Database-first (no CSV/TXT)
✅ Smart run resume (prioritizes data)
✅ Repository pattern
✅ Schema application
✅ Step progress tracking
✅ Database-based step verification
✅ Incremental saves (every 100 products)
✅ Resume capability (skip scraped URLs)
✅ Crash-proof (data persisted)

**Aligned with Argentina's battle-tested architecture!**
