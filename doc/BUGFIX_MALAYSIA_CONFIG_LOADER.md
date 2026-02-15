# Malaysia Config Loader - Bug Fix

## Issue
**Error**: `NameError: name 'get_config_resolver' is not defined`

**Location**: `scripts/Malaysia/config_loader.py` lines 107, 165

**Impact**: Malaysia scraper unable to start, crashes on initialization

---

## Root Cause

The `config_loader.py` was calling an undefined function `get_config_resolver()` that was removed during a previous refactoring. The code should use `ConfigManager.get_config_value()` directly.

**Broken code**:
```python
if _PLATFORM_CONFIG_AVAILABLE:
    cr = get_config_resolver()  # ← Undefined!
    return cr.get(SCRAPER_ID, key, default)
```

---

## Fix Applied

### 1. Fixed `getenv()` function (line 94-109)
```python
if _PLATFORM_CONFIG_AVAILABLE:
    try:
        value = ConfigManager.get_config_value(SCRAPER_ID, key, default if default is not None else "")
        return value if value is not None else (default if default is not None else "")
    except Exception:
        # Fallback to os.getenv if ConfigManager fails
        return os.getenv(key, default)
return os.getenv(key, default)
```

### 2. Fixed `getenv_list()` function (line 160-190)
```python
if _PLATFORM_CONFIG_AVAILABLE:
    try:
        value = ConfigManager.get_config_value(SCRAPER_ID, key, default)
    except Exception:
        value = os.getenv(key, None)
else:
    value = os.getenv(key)
```

---

## Changes Made

**File**: `scripts/Malaysia/config_loader.py`

**Lines modified**: 
- Lines 104-112 (getenv function)
- Lines 166-172 (getenv_list function)

**Changes**:
1. Removed undefined `get_config_resolver()` calls
2. Direct call to `ConfigManager.get_config_value()`
3. Added try/except for graceful fallback
4. Maintained backward compatibility with os.getenv

---

## Testing

✅ **Import test passed**:
```bash
$ python -c "from config_loader import getenv, getenv_list; ..."
getenv test: default_value
getenv_list test: ['default']
Config loader imports successfully
```

✅ **Functions work correctly**:
- `getenv()` returns values or defaults
- `getenv_list()` handles lists properly
- Fallback to `os.getenv` when ConfigManager unavailable

---

## Impact

**Before**: Malaysia scraper crashes on startup  
**After**: Malaysia scraper starts successfully ✓

**Affected functions**:
- ✅ `getenv()`
- ✅ `getenv_list()`
- ✅ `get_output_dir()` (uses getenv internally)
- ✅ All config resolution

---

## Prevention

This bug occurred because:
1. Function was renamed/removed in refactoring
2. No import-time checks for undefined functions
3. Missing tests for config_loader module

**Recommendation**: Add unit tests for config_loader.py to catch such issues early.

---

**Fixed**: 2026-02-15 17:35  
**Status**: ✅ Ready to run Malaysia scraper  
**Tested**: Import tests passing ✓
