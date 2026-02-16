# Canada Quebec Scraper - Path Fix

## Issue
**Error**: `Pipeline script not found: D:\quad99\Scrappers\scripts\CanadaQuebec\run_pipeline.bat`

**Root Cause**: The scraper registry had the wrong folder name.

---

## Problem

In `services/scraper_registry.py`:
```python
"CanadaQuebec": {
    "path": "scripts/CanadaQuebec",  # ← WRONG! Folder doesn't exist
```

**Actual folder name**: `scripts/canada_quebec` (lowercase with underscore)

---

## Fix Applied

Changed path in `scraper_registry.py`:
```python
"CanadaQuebec": {
    "path": "scripts/canada_quebec",  # ← FIXED!
```

---

## Verification

```bash
$ python -c "from services.scraper_registry import SCRAPER_CONFIGS; print(SCRAPER_CONFIGS['CanadaQuebec']['path'])"
scripts/canada_quebec

$ ls scripts/canada_quebec/run_pipeline.bat
Exists: True ✓
```

---

## Status
✅ **Fixed** - Canada Quebec scraper can now start successfully

**Files exist**:
- ✅ `scripts/canada_quebec/` folder
- ✅ `scripts/canada_quebec/run_pipeline.bat`
- ✅ `scripts/canada_quebec/run_pipeline_resume.py`
- ✅ All 7 step scripts (00-06)

---

**Fixed**: 2026-02-15 17:37  
**Ready to run**: Canada Quebec scraper ✓
