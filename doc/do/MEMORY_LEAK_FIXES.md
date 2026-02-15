# Memory Leak and Resource Lock Fixes

## Critical Issues Found

### 1. Unbounded Set Growth (CRITICAL)
**Location:** `scripts/Russia/01_russia_farmcom_scraper.py` line 1293, 1016
**Problem:** `existing_ids` set loads ALL item_ids from ALL runs and keeps growing
**Impact:** Can consume GBs of memory over time

**Fix:** Use DB-backed deduplication or bounded set with periodic clearing

### 2. Database Connection Pool Leaks (HIGH)
**Location:** Multiple regions
**Problem:** Connections may not be returned to pool properly
**Impact:** Connection pool exhaustion, database locks

**Fix:** Ensure all DB operations use context managers

### 3. Browser Instance Accumulation (HIGH)
**Location:** Multiple regions with `_active_drivers` lists
**Problem:** Driver objects accumulate in lists
**Impact:** Memory growth, orphaned browser processes

**Fix:** Periodic cleanup and proper untracking

### 4. File Handle Leaks (MEDIUM)
**Location:** File operations without proper closing
**Problem:** File handles not closed properly
**Impact:** OS resource exhaustion

**Fix:** Use context managers for all file operations

### 5. Thread Accumulation (MEDIUM)
**Location:** Argentina and other regions
**Problem:** Daemon threads created but not cleaned up
**Impact:** Memory growth, thread limit exhaustion

**Fix:** Use thread pools or explicit cleanup

## Implementation Plan

### Phase 1: Critical Fixes (Immediate)

1. **Fix Russia `existing_ids` Set**
   - Replace in-memory set with DB-backed deduplication
   - Or implement bounded set with periodic clearing

2. **Add Resource Monitoring**
   - Integrate `core/resource_monitor.py` into all regions
   - Periodic checks every 5 minutes

3. **Fix Database Connection Leaks**
   - Audit all DB operations
   - Ensure context managers are used

### Phase 2: Browser Cleanup (High Priority)

4. **Enhanced Browser PID Cleanup**
   - Periodic cleanup every N operations
   - Pre-run and post-run cleanup (already added)

5. **Driver List Management**
   - Ensure drivers are untracked when closed
   - Periodic cleanup of stale driver references

### Phase 3: Memory Management (Medium Priority)

6. **Periodic Garbage Collection**
   - Force GC every N operations
   - After browser restarts
   - After large data operations

7. **Bounded Data Structures**
   - Limit set/list sizes
   - Periodic clearing of old entries
   - Use DB for large datasets

## Files to Update

1. `scripts/Russia/01_russia_farmcom_scraper.py` - Fix existing_ids set
2. `scripts/Russia/02_russia_farmcom_excluded_scraper.py` - Fix existing_ids set
3. All pipeline runners - Add resource monitoring
4. All scraper scripts - Add periodic cleanup
5. Database repositories - Ensure proper connection handling
