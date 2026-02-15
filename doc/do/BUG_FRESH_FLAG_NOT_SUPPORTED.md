# CRITICAL BUG REPORT - Russia Scraper

**Date:** 2026-02-12  
**Severity:** HIGH  
**Status:** BLOCKING FRESH RUNS

---

## Issue Description

The Russia scraper **does not support the `--fresh` flag** even though the pipeline runner passes it. This causes the scraper to always attempt to resume from the last run, even when a fresh run is explicitly requested.

---

## Observed Behavior

```
Command: python run_pipeline_resume.py --fresh

Expected: Start a completely fresh run with new run_id
Actual: Attempts to resume from existing run_id 20260203_174925_1cdf6ef0

Error:
[INFO] Total pages to scrape: 1144
[INFO] 1144 pages already completed, 0 pages remaining
[WARNING] get_next_pages_to_scrape() returned empty (attempt 1/10)
[WARNING] get_next_pages_to_scrape() returned empty (attempt 2/10)
...
================================================================================
Execution failed with return code 1
```

---

## Root Cause

The file `01_russia_farmcom_scraper.py` does NOT have:
1. Command-line argument parsing for `--fresh` flag
2. Logic to skip resume and force a new run_id

The scraper always tries to resume from the most recent run with completed pages.

---

## Impact

- **Cannot start fresh runs** - Users are stuck with old run_ids
- **Testing is blocked** - Cannot test changes with clean data
- **Data quality issues** - Cannot re-scrape if previous run had errors
- **User frustration** - `--fresh` flag appears to be ignored

---

## Files Affected

1. `scripts/Russia/01_russia_farmcom_scraper.py` - Missing `--fresh` flag support
2. `scripts/Russia/02_russia_farmcom_excluded_scraper.py` - Likely has same issue

---

## Required Fix

### Add Command-Line Argument Parsing

```python
import argparse

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Russia VED Scraper")
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Start a fresh run (ignore previous runs)"
    )
    parser.add_argument(
        "--run-id",
        type=str,
        help="Use specific run_id (for resume)"
    )
    return parser.parse_args()

# In main():
args = parse_args()
```

### Modify Resume Logic

```python
def main():
    args = parse_args()
    
    # ... existing code ...
    
    # BEFORE: Always try to resume
    # run_id = repo.get_best_run_to_resume()
    
    # AFTER: Check --fresh flag
    if args.fresh:
        print("[INIT] --fresh flag detected, starting new run", flush=True)
        run_id = generate_run_id("Russia")
        run_ledger_start(db, run_id, "Russia", step_count=6)
    elif args.run_id:
        print(f"[INIT] Using specified run_id: {args.run_id}", flush=True)
        run_id = args.run_id
    else:
        # Try to resume from best run
        run_id = repo.get_best_run_to_resume()
        if run_id:
            print(f"[INIT] Resuming run: {run_id}", flush=True)
        else:
            print("[INIT] No previous run found, starting new run", flush=True)
            run_id = generate_run_id("Russia")
            run_ledger_start(db, run_id, "Russia", step_count=6)
```

---

## Workaround (Temporary)

Until fixed, users must manually delete or rename the previous run data:

```sql
-- Option 1: Delete previous run data (DESTRUCTIVE)
DELETE FROM ru_ved_products WHERE run_id = '20260203_174925_1cdf6ef0';
DELETE FROM ru_step_progress WHERE run_id = '20260203_174925_1cdf6ef0';

-- Option 2: Mark previous run as completed (safer)
UPDATE run_ledger 
SET status = 'completed', finished_at = NOW() 
WHERE run_id = '20260203_174925_1cdf6ef0';
```

---

## Testing Checklist

After implementing fix:

- [ ] `python 01_russia_farmcom_scraper.py` - Should resume by default
- [ ] `python 01_russia_farmcom_scraper.py --fresh` - Should start new run
- [ ] `python 01_russia_farmcom_scraper.py --run-id XXXXX` - Should use specified run_id
- [ ] `python run_pipeline_resume.py --fresh` - Should pass --fresh to all scripts
- [ ] Verify new run_id is generated when --fresh is used
- [ ] Verify old run data is not loaded when --fresh is used

---

## Priority

**CRITICAL** - This blocks normal operation and testing.

Should be fixed BEFORE implementing other improvements.

---

## Related Issues

- Belarus scraper likely has the same issue
- All scrapers should have consistent `--fresh` flag support
- Pipeline runner should document which flags are supported

---

**End of Bug Report**
