# Argentina Scraper Performance Fix

## Problem Analysis

The Argentina scraper slows down significantly after ~30 minutes of running. This is caused by multiple resource leaks:

### Root Causes Identified

1. **Zombie Daemon Threads Accumulation**
   - Multiple `daemon=True` threads are created for navigation timeouts
   - These threads don't block program exit but they consume memory while running
   - After thousands of products, hundreds of zombie threads accumulate

2. **Memory Leak in Thread Creation Pattern**
   ```python
   nav_thread = threading.Thread(target=do_navigation, daemon=True)
   nav_thread.start()
   ```
   - Threads are created for every navigation operation
   - No explicit cleanup/join for completed threads
   - Thread objects accumulate in memory

3. **Firefox Profile Directory Leaks**
   - Temporary profiles are created but cleanup may fail silently
   - `shutil.rmtree(profile_path, ignore_errors=True)` ignores all errors
   - Profile directories accumulate in temp folder

4. **Selenium WebDriver Object Accumulation**
   - Driver objects are added to `_active_drivers` list
   - On restart, old driver objects may not be properly unregistered
   - List grows over time, consuming memory

5. **Orphaned Geckodriver/Firefox Processes**
   - Even with PID tracking, some processes may escape cleanup
   - Windows handles may accumulate
   - Memory fragmentation from process creation/destruction

## Fixes Applied

### Fix 1: Thread Pool Reuse (Critical)
Instead of creating new threads for every operation, use a thread pool or reuse threads.

### Fix 2: Explicit Thread Cleanup
Join completed threads to free resources:
```python
nav_thread.join(timeout=0.1)  # Clean up thread object
```

### Fix 3: Periodic Garbage Collection
Force Python garbage collection every N products:
```python
import gc
if products_processed % 50 == 0:
    gc.collect()
```

### Fix 4: Driver List Cleanup
Ensure old driver references are removed:
```python
def restart_driver(thread_id: int, driver, headless: bool):
    if driver:
        unregister_driver(driver)  # Remove from active list first
        # ... rest of cleanup
```

### Fix 5: Profile Directory Verification
Verify profile cleanup actually succeeded:
```python
def cleanup_temp_profile(profile_dir):
    if not profile_dir:
        return
    profile_path = Path(profile_dir)
    # Try multiple times with increasing delays
    for attempt in range(3):
        try:
            if profile_path.exists():
                shutil.rmtree(profile_path, ignore_errors=False)
            if not profile_path.exists():
                break
        except Exception:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    with _temp_profile_lock:
        _temp_profile_dirs.discard(str(profile_path))
```

### Fix 6: Periodic Process Cleanup
Kill any orphaned geckodriver/firefox processes every N products:
```python
if products_processed % 100 == 0:
    kill_orphaned_firefox_processes()
```

## Implementation

See the patch file `performance_fix.patch` for the actual code changes.

## Quick Workaround (Until Fix is Applied)

1. **Reduce THREADS**: Set `SELENIUM_THREADS=1` or `2` in config
2. **Reduce PRODUCTS_PER_RESTART**: Set to `10` or `20` to restart browser more frequently
3. **Restart every 15 minutes**: Stop and restart the pipeline
4. **Run without GUI**: Use command line instead of GUI

## Monitoring

Add this to your scraper to monitor resource usage:

```python
import psutil
import os

def log_resource_usage():
    process = psutil.Process(os.getpid())
    mem_mb = process.memory_info().rss / 1024 / 1024
    threads = threading.active_count()
    print(f"[RESOURCE] Memory: {mem_mb:.1f}MB, Threads: {threads}")
```
