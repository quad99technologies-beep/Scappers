# Memory Leak Fixes and Enhancements Summary

## Overview
This document summarizes the memory leak fixes, anti-bot measures, and UI/UX improvements implemented for Malaysia, India, and Russia scrapers based on the Argentina implementation.

## 1. Memory Leak Fixes (Argentina Pattern)

### Common Features Added to All Scrapers:

#### A. Resource Monitoring (`scraper_utils.py`)
- **Memory Usage Tracking**: `get_memory_usage_mb()` - monitors process memory using psutil
- **Memory Limit Check**: `check_memory_limit()` - triggers when memory exceeds 2GB
- **Force Cleanup**: `force_cleanup()` - aggressive garbage collection
- **Resource Logging**: `log_resource_usage()` - logs memory and thread counts

#### B. Process Management
- **Tracked Chrome PIDs**: Track Chrome process IDs for targeted cleanup
- **Kill Tracked Processes**: `kill_tracked_chrome_processes()` - kills only scraper-owned Chrome
- **Kill All Chrome (Nuclear)**: `kill_all_chrome_processes()` - system-wide Chrome cleanup
- **Orphaned Process Cleanup**: `cleanup_orphaned_chrome_processes()` - removes zombie processes

#### C. Driver Management
- **Register/Unregister Drivers**: Track active browser instances
- **Close All Drivers**: Graceful shutdown with PID tracking
- **Signal Handlers**: SIGINT/SIGTERM handling for graceful shutdown
- **atexit Registration**: Ensures cleanup on script exit

### Country-Specific Implementations:

#### Malaysia (Playwright-based)
**File**: `scripts/Malaysia/scrapers/base.py`
- Added memory monitoring globals (`_shutdown_requested`, `_active_browsers`, `_tracked_chrome_pids`)
- Enhanced `browser_session()` context manager with cleanup
- Modified `new_page()` to check memory every 50 pages
- Automatic context rotation when memory limit exceeded
- Force GC after browser session closes

#### Russia (Selenium-based)
**File**: `scripts/Russia/01_russia_farmcom_scraper.py`
- Added memory monitoring imports from `scraper_utils`
- Enhanced `cleanup_all_chrome()` with PID tracking
- Memory check every 100 pages in main scraping loop
- Automatic Chrome restart when memory exceeds 2GB
- Progress logging includes memory usage

#### India (Scrapy-based)
**File**: `scripts/India/scraper_utils.py` (new file)
- Full resource monitoring utilities
- Process management for Chrome instances
- Signal handlers for graceful shutdown
- Progress tracking utilities

## 2. Anti-Bot Humanized Measures

### Malaysia (Already Implemented)
**File**: `scripts/Malaysia/scrapers/base.py`
- Playwright stealth context with `_STEALTH_INIT_SCRIPT`
- Webdriver property hiding
- Mock plugins array
- Mock languages and chrome runtime
- Random user agents pool
- Human-like delays (`pause()`, `long_pause()`, `type_delay_ms()`)
- Human typing simulation (`human_type()`)
- Cloudflare detection and waiting
- Table stability waiting

### Russia (Enhanced)
**File**: `scripts/Russia/01_russia_farmcom_scraper.py`
- CDP anti-detection script execution
- Webdriver property removal
- Mock plugins and languages
- User agent rotation via config
- Human actions via `core.human_actions.pause()`

### India (Via Scrapy Settings)
- Download delays
- User agent rotation
- Concurrent request limiting

## 3. UI/UX Improvements

### Progress Tracking
All scrapers now output:
```
[PROGRESS] Pipeline Step: X/Y (Z%) - Descriptive message
[TIMING] Step N completed in Xm Ys
[PAUSE] Waiting 10 seconds before next step...
```

### Pipeline Execution Plan
All `run_pipeline_resume.py` files display:
```
================================================================================
PIPELINE EXECUTION PLAN
================================================================================
Step 1/N: Step Name - SKIPPED (already completed)
Step 2/N: Step Name - WILL RUN NOW (starting from here)
Step 3/N: Step Name - WILL RUN AFTER previous steps complete
================================================================================
```

### Step Descriptions
Each step has descriptive messages:
- **Malaysia**:
  - Step 0: "Preparing: Backing up previous results, initializing DB..."
  - Step 1: "Scraping: Fetching product registration numbers from MyPriMe..."
  - etc.

- **India**:
  - Step 0: "Scraping: Fetching drug pricing data from NPPA (Scrapy)..."
  - Step 1: "Processing: QC validation and CSV export generation..."

- **Russia**:
  - Step 0: "Preparing: Backing up previous results and cleaning output directory..."
  - Step 1: "Scraping: Extracting VED drug pricing data from farmcom.info..."
  - etc.

## 4. Env/IO Folders Structure

All scrapers use consistent folder structure via `config_loader.py`:

```
Documents/ScraperPlatform/
├── input/
│   ├── Malaysia/
│   ├── India/
│   └── Russia/
├── output/
│   ├── Malaysia/
│   ├── India/
│   └── Russia/
└── exports/
    ├── Malaysia/
    ├── India/
    └── Russia/
```

### Config Loader Features:
- Platform config integration (`platform_config.py`)
- Environment variable loading from `config/{Country}.env.json`
- Fallback to dotenv if platform config unavailable
- Path resolution with fallback to local directories

## 5. Files Modified/Created

### New Files:
1. `scripts/Malaysia/scraper_utils.py` - Resource monitoring utilities
2. `scripts/India/scraper_utils.py` - Resource monitoring utilities
3. `scripts/Russia/scraper_utils.py` - Resource monitoring utilities

### Modified Files:
1. `scripts/Malaysia/scrapers/base.py` - Memory leak fixes, enhanced cleanup
2. `scripts/Russia/01_russia_farmcom_scraper.py` - Memory monitoring, enhanced cleanup
3. `scripts/India/run_pipeline_scrapy.py` - UI/UX improvements

## 6. Key Configuration Variables

All scrapers support these environment variables (via config files):

### Memory Management:
- `MEMORY_LIMIT_MB` - Memory limit before restart (default: 2048)
- `MEMORY_CHECK_INTERVAL` - Pages between memory checks

### Anti-Bot:
- `SCRIPT_01_CHROME_USER_AGENT` - Custom user agent
- `SCRIPT_01_HEADLESS` - Run in headless mode

### VPN (where applicable):
- `VPN_REQUIRED` - Whether VPN is required
- `VPN_CHECK_ENABLED` - Enable VPN connectivity check
- `VPN_CHECK_HOST` - Host to check for VPN connectivity

## 7. Testing Recommendations

1. **Memory Monitoring**: Run scrapers with large datasets to verify memory stays below 2GB
2. **Cleanup Verification**: Check that Chrome processes are killed after scraper exits
3. **Signal Handling**: Test Ctrl+C during scraping to verify graceful shutdown
4. **Progress Output**: Verify [PROGRESS] and [TIMING] messages appear in logs
5. **Resume Functionality**: Test checkpoint resume after interruption

## 8. GUI and Long-Run Stability (GUI Hang After 2–3 Hours)

### Problem
After 2–3 hours of running a pipeline from the GUI, the interface could become non-responsive or hang due to:
- **Unbounded log storage**: `scraper_logs[scraper_name]` grew without limit as pipeline stdout was appended.
- **Huge log widget updates**: The log ScrolledText was updated with the full in-memory log; replacing millions of characters blocked the main thread.
- **Periodic refresh**: `schedule_log_refresh` (every 500 ms) re-read and re-rendered the full log, worsening freezes as log size grew.
- **DB connection leaks**: Startup recovery and `stop_pipeline` opened DB connections that were not always closed on exception.

### Fixes Applied

#### A. Log size caps (`scraper_gui.py`)
- **In-memory cap**: `MAX_LOG_CHARS = 1_500_000` (~1.5 MB per scraper). When appending pipeline output, content is truncated from the start when over this limit (`_cap_scraper_log`).
- **Display cap**: `MAX_DISPLAY_LOG_CHARS = 500_000`. Only the last 500k characters are shown in the log widget (`_truncate_log_tail`), so `update_log_display` and `schedule_log_refresh` never insert huge strings.
- **Widget line cap**: When appending live lines, the log widget is trimmed to the last ~6k lines (drop oldest when over 8k lines) so the widget itself does not grow unbounded between full refreshes.

#### B. DB connection cleanup
- **Startup recovery** (`scraper_gui.py`): Postgres recovery loop now uses `try/finally` so `db.close()` is always called, even on exception.
- **Stop pipeline** (`shared_workflow_runner.py`): Both Windows and Unix branches that open `CountryDB` to mark run as resumable now use `try/finally` and close the DB in `finally`.

### Scraper-Side Notes (No Change Required for GUI Hang)
- **Russia VED**: Loads `existing_ids` from all runs via `get_existing_item_ids_all_runs()`; set can be large but lives in the scraper subprocess, not the GUI.
- **Russia Excluded**: Uses a single `existing_ids` set per run (not persisted across runs); no dedup, so memory is bounded by one run’s items.
- **Argentina/Malaysia/Russia**: Driver cleanup, `gc.collect()`, and memory checks are already in place in scraper scripts; the main long-run issue was GUI-side log growth and DB leaks.

## 9. Future Enhancements

Potential improvements based on Argentina patterns:
- Tor proxy support for enhanced anonymity
- Surfshark VPN integration
- Rotation coordination across multiple workers
- Load time monitoring with automatic restart on slow pages
- Auto-restart wrapper for automatic recovery
