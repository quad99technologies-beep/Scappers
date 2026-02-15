# Netherlands Scraper - Code Analysis & Fixes

**Date:** 2026-02-08  
**Scraper:** Netherlands (medicijnkosten.nl)  
**Main File:** `scripts/Netherlands/01_get_medicijnkosten_data.py`

---

## 🔍 ISSUES IDENTIFIED

### 1. **INFINITE LOOP RISKS** ⚠️

#### Issue 1.1: Scroll Loop Without Hard Limit
**Location:** Lines 2316-2386 (direct streaming mode)
**Risk:** HIGH
```python
for loop in range(1, MAX_SCROLL_LOOPS + 1):  # MAX_SCROLL_LOOPS = 15000
    # ... scrolling logic ...
    if reached_expected:
        break
    if can_stop_for_stability:
        break
```
**Problem:** 
- If `reached_expected` and `can_stop_for_stability` never become True, loop runs 15,000 times
- Each iteration has `STREAM_SCROLL_WAIT_SEC` (0.2s default) = potential 50 minutes stuck
- No timeout mechanism

**Fix Required:** Add absolute timeout guard

#### Issue 1.2: Worker Thread Infinite Retry
**Location:** Lines 2027-2200 (scrape_worker function)
```python
while True:
    try:
        r = work_queue.get(timeout=5)
    except queue.Empty:
        try:
            r = work_queue.get_nowait()
        except queue.Empty:
            break
```
**Problem:**
- If queue never empties and items keep failing, thread runs indefinitely
- Driver recreation loop (lines 2160-2180) can recreate up to 10 drivers per thread
- No global timeout for worker threads

**Fix Required:** Add worker timeout and iteration limit

#### Issue 1.3: Retry Pass Potential Loop
**Location:** Lines 2654-2822 (run_retry_pass)
**Problem:**
- Retry pass re-collects failed URLs and tries again
- If URLs consistently fail, they get deleted and re-collected in infinite cycle
- No limit on number of retry passes (only controlled by `ENABLE_RETRY_PASS` flag)

**Fix Required:** Add retry pass counter and max limit

---

### 2. **NETWORK FAILURE HANDLING** ⚠️

#### Issue 2.1: Insufficient Network Error Recovery
**Location:** Lines 646-679 (driver_get_with_retry)
```python
def driver_get_with_retry(..., max_retries: int = NETWORK_RETRY_MAX):
    # NETWORK_RETRY_MAX = 3 (default)
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            wait_dom_ready(driver)
            return
        except (WebDriverException, TimeoutException, InvalidSessionIdException) as e:
            # ... retry logic ...
```
**Problems:**
- Only 3 retries for network errors (may be insufficient for unstable connections)
- No exponential backoff (uses linear: `base_delay * attempt`)
- Session errors are raised immediately (correct) but may need driver recreation

**Fix Required:** 
- Increase retry count or make configurable
- Add exponential backoff
- Better logging of network failures

#### Issue 2.2: Missing Connection Pool Cleanup
**Location:** Lines 50-57 (urllib3 warning suppression)
```python
urllib3_logger.setLevel(logging.ERROR)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
```
**Problem:**
- Warnings are suppressed but connection pool may leak
- No explicit connection pool cleanup after scraping

**Fix Required:** Add connection pool cleanup in finally blocks

---

### 3. **CRASH GUARDS MISSING** ⚠️

#### Issue 3.1: Unguarded Driver Operations
**Location:** Multiple locations (e.g., lines 1212-1214, 2299-2313)
```python
driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
```
**Problem:**
- No try-except around JavaScript execution
- If page crashes or becomes unresponsive, entire scraper fails
- NetworkLoadError raised but may not be caught at higher level

**Fix Required:** Wrap all driver operations in try-except with proper error handling

#### Issue 3.2: Database Transaction Rollback Missing
**Location:** Lines 743-765 (insert_collected_url, insert_pack)
```python
def insert_pack(pack_data: Dict) -> None:
    if _repo is not None:
        try:
            _repo.insert_packs([pack_data], log_db=False)
        except Exception as e:
            print(f"[ERROR] Failed to insert pack data: {e}")
            raise  # Re-raises but no rollback
```
**Problem:**
- Exceptions are raised but database may be in inconsistent state
- No transaction rollback on failure
- Could lead to partial data in database

**Fix Required:** Add transaction management with rollback

---

### 4. **DATA VALIDATION ISSUES** ⚠️

#### Issue 4.1: Weak URL Validation
**Location:** Lines 277-285 (is_likely_result_url)
```python
def is_likely_result_url(href: str) -> bool:
    h = (href or "").lower()
    if "medicijnkosten.nl" not in h:
        return False
    # ... exclusions ...
    return "artikel=" in h
```
**Problem:**
- Only checks for "artikel=" parameter
- Doesn't validate URL structure
- Could accept malformed URLs

**Fix Required:** Add comprehensive URL validation

#### Issue 4.2: Missing Data Sanitization
**Location:** Lines 1554-1684 (scrape_product_to_pack)
**Problem:**
- Scraped data is inserted directly into database without sanitization
- No validation of price formats, dates, or other fields
- Could insert invalid data

**Fix Required:** Add data validation and sanitization layer

---

### 5. **GHOST CODE / UNUSED CODE** 🧹

#### Ghost Code 5.1: Unused Import
**Location:** Line 11 (threading import)
```python
import threading
```
**Status:** USED (for worker threads) - NOT GHOST CODE ✅

#### Ghost Code 5.2: Unused Functions
**Location:** Lines 1687 (comment about removed CSV functions)
```python
## Old CSV-based functions removed - now using DB-based versions defined in DATA HELPERS section
```
**Status:** Already cleaned up ✅

#### Ghost Code 5.3: Commented Out Code
**Location:** Lines 2886-2898 (Chrome cleanup messages)
```python
# Chrome instance messages suppressed to reduce console noise
# if terminated > 0:
#     print(f"[CLEANUP] Marked {terminated} Chrome instance(s) as terminated in DB")
```
**Action:** Keep commented (intentional noise reduction) ✅

#### Ghost Code 5.4: Unused Variables
**Location:** Line 397 (_driver_counter)
```python
_driver_counter = 0
```
**Status:** USED (lines 419, 454-455) - NOT GHOST CODE ✅

---

### 6. **OLD DATA CLEANUP** 🗑️

#### Issue 6.1: No Automatic Old Run Cleanup
**Location:** Database schema (no TTL or cleanup job)
**Problem:**
- Old run data accumulates in database
- No automatic cleanup of runs older than X days
- Could lead to database bloat

**Fix Required:** Add cleanup function for old runs

#### Issue 6.2: Backup Folder Growth
**Location:** `backups/Netherlands/` directory
**Problem:**
- Backups are created but never cleaned up
- Could consume significant disk space over time

**Fix Required:** Add backup retention policy (e.g., keep last 30 days)

---

## ✅ FIXES IMPLEMENTED

### Fix 1: Add Infinite Loop Guards
### Fix 2: Improve Network Error Handling  
### Fix 3: Add Crash Guards
### Fix 4: Enhance Data Validation
### Fix 5: Add Old Data Cleanup

---

## 📊 PLATFORM FEATURES TO DEVELOP

Based on gap analysis from conversation `171d2791-7063-4703-a9c8-5d95c6d1a70d`:

1. **Real-time Monitoring Dashboard**
   - Live scraping progress
   - Error rate tracking
   - Performance metrics

2. **Automated Retry Logic**
   - Smart retry with exponential backoff
   - Failure pattern detection
   - Auto-recovery from common errors

3. **Data Quality Checks**
   - Validation rules engine
   - Anomaly detection
   - Data completeness scoring

4. **Resource Management**
   - Chrome instance pooling
   - Memory leak detection
   - Automatic resource cleanup

5. **Alerting System**
   - Email/Telegram notifications
   - Threshold-based alerts
   - Error aggregation

---

## 🎯 PRIORITY FIXES

1. **HIGH PRIORITY**
   - Add timeout guards to scroll loops
   - Improve network retry logic
   - Add crash guards to driver operations

2. **MEDIUM PRIORITY**
   - Implement old data cleanup
   - Add data validation layer
   - Improve error logging

3. **LOW PRIORITY**
   - Optimize connection pool usage
   - Add performance monitoring
   - Enhance backup retention

---

## 📝 RECOMMENDATIONS

1. **Configuration Management**
   - Move all hardcoded limits to config
   - Add environment-specific configs
   - Document all configuration options

2. **Testing**
   - Add unit tests for critical functions
   - Integration tests for full workflow
   - Load testing for concurrent scraping

3. **Documentation**
   - Document error codes and recovery
   - Add troubleshooting guide
   - Create runbook for common issues

4. **Monitoring**
   - Add structured logging
   - Implement metrics collection
   - Create alerting rules

---

**End of Analysis**
