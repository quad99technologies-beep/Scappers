# Canada Quebec - Additional Import Path Fix

## Issue
After fixing the config_loader, another import error appeared:
```
ModuleNotFoundError: No module named 'scripts.CanadaQuebec'
```

## Location
**File**: `scripts/canada_quebec/db_handler.py`  
**Line**: 24

## Root Cause
The db_handler had a hardcoded import path using the old folder name:
```python
from scripts.CanadaQuebec.config_loader import DB_ENABLED, SCRAPER_ID_DB
```

But the actual folder is `scripts/canada_quebec` (lowercase with underscore).

## Fix Applied
Changed line 24:
```python
from scripts.canada_quebec.config_loader import DB_ENABLED, SCRAPER_ID_DB
```

## Verification
✅ Import test passed:
```bash
$ python -c "from scripts.canada_quebec.db_handler import DBHandler; ..."
Canada Quebec db_handler: OK
```

---

## Canada Quebec - Complete Fix Summary

**Issues Fixed**:
1. ✅ Scraper registry path: `CanadaQuebec` → `canada_quebec`
2. ✅ Config loader: undefined `get_config_resolver()`
3. ✅ DB handler import: `scripts.CanadaQuebec` → `scripts.canada_quebec`

**Status**: ✅ Ready to run from GUI

---

**Fixed**: 2026-02-15 17:46  
**All imports**: Working ✓
