# Russia Scraper - Fresh Flag Implementation

**Date:** 2026-02-12  
**Status:** ✅ IMPLEMENTED

---

## Changes Made

### 1. Enhanced Argument Parser (`01_russia_farmcom_scraper.py`)

**Function:** `_parse_args()` (renamed from `_parse_retry_range()`)

**New Capabilities:**
- `--fresh` flag: Start a completely new run (ignore previous runs)
- `--run-id <ID>` flag: Use a specific run_id
- `--start N --end M`: Retry specific page range (existing functionality)

**Returns:** `(retry_pages, fresh_run, run_id)`

### 2. Updated Main Function Logic

**Before:**
```python
# Always tried to resume from best run
resume_page, best_run_id = get_resume_page_and_run_id(lookup_repo)
if resume_page > 1 and best_run_id:
    _run_id = best_run_id  # ALWAYS RESUMED
```

**After:**
```python
if fresh_run:
    # --fresh flag: Always start a new run
    _run_id = generate_run_id("Russia")
    _repo.start_run("fresh")
elif specified_run_id:
    # --run-id specified: Use the specified run_id
    _run_id = specified_run_id
    # Check if run exists and resume or start fresh
else:
    # Default behavior: Try to resume from best run
    resume_page, best_run_id = get_resume_page_and_run_id(lookup_repo)
    if resume_page > 1 and best_run_id:
        _run_id = best_run_id
        _repo.resume_run()
    else:
        _run_id = generate_run_id("Russia")
        _repo.start_run("fresh")
```

### 3. Excluded Products Scraper (`02_russia_farmcom_excluded_scraper.py`)

Added same `--fresh` and `--run-id` support for consistency.

---

## Usage Examples

### Start Fresh Run
```bash
python 01_russia_farmcom_scraper.py --fresh
python 02_russia_farmcom_excluded_scraper.py --fresh
```

### Resume from Specific Run ID
```bash
python 01_russia_farmcom_scraper.py --run-id 20260212_112857_abc123
```

### Retry Specific Pages
```bash
python 01_russia_farmcom_scraper.py --start 100 --end 150
```

### Pipeline with Fresh Flag
```bash
python run_pipeline_resume.py --fresh
```

---

## Testing Checklist

- [x] Code changes implemented
- [ ] Test `--fresh` flag creates new run_id
- [ ] Test `--run-id` uses specified run_id
- [ ] Test default behavior still resumes correctly
- [ ] Test pipeline runner passes --fresh to scripts
- [ ] Verify no data from previous runs is loaded with --fresh

---

## Benefits

1. **Unblocks Current Issue:** Can now start fresh runs without manual DB cleanup
2. **Better Testing:** Easy to test changes with clean data
3. **Flexibility:** Can resume specific runs or start fresh as needed
4. **Consistency:** Both VED and Excluded scrapers have same behavior

---

## Files Modified

1. `scripts/Russia/01_russia_farmcom_scraper.py`
   - Renamed `_parse_retry_range()` to `_parse_args()`
   - Added `--fresh` and `--run-id` support
   - Updated main() logic

2. `scripts/Russia/02_russia_farmcom_excluded_scraper.py`
   - Added `--fresh` and `--run-id` support
   - Updated main() logic

---

## Next Steps

1. Test the implementation with: `python run_pipeline_resume.py --fresh`
2. Verify new run_id is generated
3. Confirm scraping starts from page 1
4. Check that old run data is not loaded

---

**Status:** Ready for testing!
