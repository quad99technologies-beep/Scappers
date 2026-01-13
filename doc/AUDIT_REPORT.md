# Production Scraping Platform Engineering Audit Report

**Date:** 2026-01-12  
**Auditor:** Platform Engineering Review  
**Scope:** Engineering standards audit (no business logic changes)  
**Repository:** Scappers Platform

---

## Executive Summary

This audit evaluates the repository against 8 production-quality engineering standards categories. The platform has good foundational infrastructure (checkpoint system, browser managers, config management) but requires improvements in progress tracking, resource cleanup, logging consistency, and time tracking.

**Overall Status:** PARTIAL - Infrastructure exists but needs hardening for production reliability.

---

## Repository Structure Overview

### Key Components
- **Entrypoints:**
  - `shared_workflow_runner.py` - Unified orchestrator (WorkflowRunner)
  - `scripts/{Scraper}/run_pipeline_resume.py` - Scraper-specific pipeline runners
  - `scraper_gui.py` - GUI interface
  - `scripts/{Scraper}/run_pipeline.bat` - Legacy batch runners

- **Progress Storage:**
  - Step-level: `output/{scraper}/.checkpoints/pipeline_checkpoint.json`
  - Item-level: `output/{scraper}/alfabeta_progress.csv` (Argentina), varies by scraper
  - Run-level: `runs/{run_id}/logs/run.log` (when using WorkflowRunner)

- **Browser Lifecycle:**
  - `core/chrome_manager.py` - ChromeManager (singleton)
  - `core/chrome_pid_tracker.py` - PID tracking per scraper
  - Argentina uses Firefox (not Chrome) - no equivalent manager
  - Cleanup in `shared_workflow_runner.py`, `run_pipeline_resume.py`

- **Config Management:**
  - `core/config_manager.py` - ConfigManager (centralized)
  - `platform_config.py` - PathManager/ConfigResolver (backward compatibility)
  - `config/{scraper}.env.json` - Scraper configs
  - `config/{scraper}.env.example` - Templates

---

## Audit Results by Category

### 1. Progress & Status Reporting

**Status:** PARTIAL ❌

**Evidence:**
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Lines 135-173: `log_progress_with_step()` exists but only logs after item completion
  - Line 1841: Progress printed every N items, not per item
  - No deterministic progress persistence during loops
  
- **File:** `scripts/Argentina/scraper_utils.py`
  - Lines 169-173: `append_progress()` writes to CSV after each item (good)
  - No progress file flushed frequently enough to survive crashes
  
- **File:** `scripts/Argentina/run_pipeline_resume.py`
  - Lines 54, 96: Step-level progress (% complete)
  - No item-level counts within steps
  
- **File:** `scripts/Malaysia/02_Product_Details.py`
  - Lines 505-506: Progress per item exists
  - Lines 521-522: Saves after each item (good for resilience)
  
- **File:** `scripts/Argentina/04_alfabeta_api_scraper.py`
  - Lines 509-511: Progress logged every 10 items (not per item)

**Issues:**
1. **Progress not deterministic:** Progress files not flushed frequently (CSV append mode, OS buffers)
2. **No ETA calculation:** Only shows counts/percent, no time estimates
3. **Progress reset risk:** If script crashes mid-loop, progress may reset (no atomic writes)
4. **Inconsistent format:** Different progress formats across scrapers (`[PROGRESS] X/Y (Z%)` vs plain text)
5. **No partial step tracking:** If step 3 crashes at item 500/1000, resume starts from step 4 (loses item-level progress)

**Recommendation:**
- Add `core/progress_store.py` with atomic JSON writes (write to temp, rename)
- Flush progress after every N items (configurable, default=1)
- Add ETA calculation (items/sec, remaining items)
- Standardize progress format: `[PROGRESS] {step}: {item_completed}/{item_total} ({percent}%) ETA: {hh:mm:ss}`
- Persist item-level progress in checkpoint JSON (not just step completion)

**Business Logic Unchanged:** Only adds progress tracking layer, no parsing/selectors changed.

---

### 2. Browser Instance Management

**Status:** PARTIAL ⚠️

**Evidence:**
- **File:** `core/chrome_manager.py`
  - Lines 74-83: `register_driver()` - WeakSet tracking (good)
  - Lines 100-133: `cleanup_all()` - Closes all drivers
  - Lines 42-64: Signal handlers registered (SIGTERM, SIGINT)
  
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Lines 675-772: `setup_driver()` - Creates Firefox instance
  - Line 682: Creates temporary profile per driver (good)
  - Line 742: `register_driver(drv)` called (good)
  - **CRITICAL:** Firefox uses temporary profile, but profile path not isolated per thread/worker
  
- **File:** `scripts/Netherlands/01_collect_urls.py`
  - Line 256: `user_data_dir` created per instance with PID+timestamp (good)
  
- **File:** `scripts/Malaysia/01_Product_Registration_Number.py`
  - Lines 58-61: Chrome created but no explicit user-data-dir
  - Lines 64-67: ChromeManager registration exists
  
- **File:** `core/chrome_pid_tracker.py`
  - Lines 234-298: `terminate_chrome_pids()` - Scraper-specific cleanup
  - Lines 261-267: Cross-scraper PID conflict detection (good)
  
- **File:** `shared_workflow_runner.py`
  - Lines 802-809, 828-834: Chrome cleanup on success/error (good)
  - **ISSUE:** Argentina uses Firefox, not Chrome - cleanup may miss Firefox instances

**Issues:**
1. **Firefox not managed:** Argentina uses Firefox but ChromeManager only tracks Chrome
2. **Profile isolation:** Firefox profiles are temporary but not explicitly isolated per worker ID (thread-safe but not deterministic)
3. **Chrome user-data-dir:** Malaysia Chrome doesn't set explicit user-data-dir (uses default, may conflict)
4. **Max instances logic:** `scripts/Argentina/03_alfabeta_selenium_scraper.py:1610` uses `min(4, eligible_count)` - hardcoded, not configurable
5. **Timeout/retry scattered:** Timeouts hardcoded in scripts (e.g., `PAGE_LOAD_TIMEOUT`, `max_retries=3`) - not centralized
6. **Orphan process risk:** Firefox/geckodriver processes may not be tracked if script crashes before registration

**Recommendation:**
- Add `core/browser_pool.py` with unified Chrome/Firefox/Playwright manager
- Require explicit `user-data-dir` for all browsers (per worker ID)
- Add `core/retry_config.py` with centralized timeouts/retries
- Track browser PIDs at creation time (before any operations)
- Add `max_instances = min(config.max_instances, remaining_items)` logic
- Verify cleanup in finally blocks and signal handlers

**Business Logic Unchanged:** Only improves resource management, no parsing/selectors changed.

---

### 3. Resume / Checkpoint / Run Ledger

**Status:** PARTIAL ⚠️

**Evidence:**
- **File:** `core/pipeline_checkpoint.py`
  - Lines 84-108: `mark_step_complete()` - Step-level checkpoint
  - Lines 110-113: `is_step_complete()` - Check completion
  - Lines 209-233: `should_skip_step()` - Verify outputs exist
  - **ISSUE:** No item-level checkpoint (only step-level)
  
- **File:** `scripts/Argentina/run_pipeline_resume.py`
  - Lines 145-162: Verifies output files exist before skipping (good)
  - Lines 186-214: Skips completed steps (good)
  - **ISSUE:** If step 3 crashes at item 500/1000, entire step re-runs
  
- **File:** `scripts/Argentina/scraper_utils.py`
  - Lines 156-167: `combine_skip_sets()` - Loads progress CSV to skip items (item-level resume exists!)
  - **ISSUE:** Skip set loaded at start, not integrated with checkpoint system
  
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Lines 1708-1712: Uses skip_set to skip items (good)
  - **ISSUE:** Progress file not verified on resume (if progress.csv corrupt, items re-scraped)

**Issues:**
1. **Step-level only:** Checkpoint tracks steps, not items within steps
2. **Idempotency risk:** Re-running may duplicate output rows if progress file missing
3. **Dedup key inconsistency:** Argentina uses `(company, product)` tuple, but output uses `(input_company, input_product_name)` - keys may not match
4. **Single source of truth missing:** Progress CSV and checkpoint JSON are separate - no unified "completed items" store
5. **Partial step not tracked:** If step crashes mid-execution, checkpoint doesn't record "step 3 in progress, item 500/1000"

**Recommendation:**
- Extend `PipelineCheckpoint` to track item-level progress: `step_3_items_completed: ["key1", "key2", ...]`
- Integrate skip_set loading with checkpoint verification (checkpoint JSON as source of truth, CSV as backup)
- Add dedup key validation: ensure skip_set keys match output CSV keys
- Add "partial step" state: checkpoint can record `step_3: {status: "in_progress", item: 500, total: 1000}`
- Atomic checkpoint writes: write to temp file, rename (prevents corruption)

**Business Logic Unchanged:** Only improves checkpoint/resume logic, no parsing/selectors changed.

---

### 4. Pipeline Management

**Status:** PARTIAL ⚠️

**Evidence:**
- **File:** `shared_workflow_runner.py`
  - Lines 703-851: `run()` method - Unified orchestrator (good)
  - Lines 752-774: Step execution via `scraper.run_steps()` (interface-based)
  - **ISSUE:** WorkflowRunner not used by all scrapers (Argentina uses `run_pipeline_resume.py`)
  
- **File:** `scripts/Argentina/run_pipeline_resume.py`
  - Lines 132-142: Step definitions (hardcoded list)
  - Lines 30-104: `run_step()` - Executes subprocess, marks checkpoint
  - **ISSUE:** Steps can't be run standalone (no CLI flags per step)
  
- **File:** `scripts/Malaysia/run_pipeline_resume.py`
  - Lines 126-135: Step definitions (similar structure)
  
- **File:** `scripts/CanadaQuebec/run_pipeline_resume.py`
  - Lines 147-158: Step definitions (similar structure)
  - Line 152: Optional step (allow_failure) - good design

**Issues:**
1. **Multiple orchestrators:** `shared_workflow_runner.py` exists but not used by Argentina/Malaysia/CanadaQuebec (they use `run_pipeline_resume.py`)
2. **Step boundaries not explicit:** Steps are Python scripts, but no step_id/step_name/input/output schema defined in code
3. **No standalone step execution:** Can't run `python 03_alfabeta_selenium_scraper.py --step-id=3` independently
4. **Step success criteria:** Steps succeed if subprocess returns 0, but no explicit output validation
5. **Hidden side effects:** Steps may modify shared state (e.g., `Productlist_with_urls.csv`) not documented in step contract

**Recommendation:**
- Create unified `core/pipeline_orchestrator.py` used by all scrapers
- Define step schema: `{step_id, step_name, input_files, output_files, success_criteria, checkpoint_write}`
- Add CLI flags to each step script: `--step-id=N --standalone` (optional)
- Add step validation: verify output files exist and are non-empty before marking complete
- Document step contracts: inputs, outputs, side effects

**Business Logic Unchanged:** Only standardizes orchestration, no parsing/selectors changed.

---

### 5. Console/Log Management

**Status:** PARTIAL ⚠️

**Evidence:**
- **File:** `shared_workflow_runner.py`
  - Lines 675-701: `setup_logging()` - File + console handlers (good)
  - Lines 688-691: Consistent formatter (good)
  
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Lines 600-671: Extensive `print()` statements (not logging)
  - Line 150: `logging.basicConfig()` - Logging configured
  - **ISSUE:** Mix of `print()` and `logging.*` (inconsistent)
  
- **File:** `scripts/Malaysia/02_Product_Details.py`
  - Lines 497-531: All `print()` statements (no logging)
  
- **File:** `scripts/Argentina/scraper_utils.py`
  - Uses `log.info()`, `log.warning()`, `log.error()` (good)
  
- **File:** `scraper_gui.py`
  - Lines 1314-1391: Parses log content for progress (expects specific format)
  - **ISSUE:** GUI expects `[PROGRESS]` format, but scripts use inconsistent formats

**Issues:**
1. **Inconsistent logging:** Mix of `print()` and `logging.*` across scripts
2. **No log format standard:** Some use `[PREFIX]`, some don't; thread/worker ID not consistently included
3. **Log levels misuse:** Many scripts use `log.info()` for everything (no WARN/ERROR distinction)
4. **Log files not always created:** Some scripts only log to console (e.g., Malaysia scripts)
5. **Sensitive data risk:** Config files use `.env.json` but values may be logged (need to verify)
6. **GUI parsing fragility:** GUI regex expects specific format - scripts must match exactly

**Recommendation:**
- Create `core/logger.py` with standard formatter: `[{level}] [{scraper}] [{step}] [thread-{id}] {message}`
- Replace all `print()` with `logger.info()` (except startup banners)
- Use log levels properly: INFO (normal), WARNING (recoverable), ERROR (failure)
- Ensure all scripts write to file (not just console)
- Add sensitive value masking: never log passwords/tokens (check ConfigManager secrets)
- Standardize progress format: `[PROGRESS] {step}: {completed}/{total} ({percent}%)`

**Business Logic Unchanged:** Only standardizes logging format, no parsing/selectors changed.

---

### 6. .env / Config Management

**Status:** OK ✅ (with minor issues)

**Evidence:**
- **File:** `core/config_manager.py`
  - Lines 126-196: `load_env()` - Centralized config loading (good)
  - Lines 108-123: `env_paths()` - Deterministic paths (good)
  - Lines 184-194: Required keys validation (good)
  
- **File:** `config/Argentina.env.example`
  - Lines 1-40: Well-documented template (good)
  - All variables documented (good)
  
- **File:** `platform_config.py`
  - Lines 36-202: PathManager - Centralized paths (good)
  - **ISSUE:** Some hardcoded paths in fallback logic (lines 89-100)
  
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Line 562: Hardcoded path: `"C:\\Program Files"` (fallback, but still hardcoded)
  
- **File:** `scripts/Argentina/config_loader.py`
  - Line 11: Comment mentions "Hardcoded defaults" - need to verify

**Issues:**
1. **Hardcoded paths:** Some fallback logic uses `D:\quad99\Scappers` or `C:\Program Files` (platform_config.py fallbacks, Firefox binary detection)
2. **Config loading not always used:** Some scripts may use `os.getenv()` directly instead of ConfigManager
3. **Silent defaults:** Some config values may fall back to unsafe defaults (need to audit each scraper's config_loader.py)
4. **Path policy:** PathManager exists but some scripts construct paths manually

**Recommendation:**
- Audit all scripts for direct `os.getenv()` usage (should use ConfigManager)
- Remove hardcoded paths from fallback logic (use Path.home(), tempfile)
- Add config validation: fail fast if required keys missing (no silent defaults)
- Enforce PathManager usage: all path construction via PathManager (no manual Path() joins in logic layer)

**Business Logic Unchanged:** Only improves config hygiene, no parsing/selectors changed.

---

### 7. Script Naming / UI-UX References

**Status:** OK ✅ (mostly consistent)

**Evidence:**
- **File:** `scripts/Argentina/` - Files: `00_backup_and_clean.py`, `01_getProdList.py`, `02_prepare_urls.py`, etc. (consistent)
- **File:** `scripts/Malaysia/` - Files: `00_backup_and_clean.py`, `01_Product_Registration_Number.py`, etc. (consistent)
- **File:** `scripts/CanadaQuebec/` - Files: `00_backup_and_clean.py`, `01_split_pdf_into_annexes.py`, etc. (consistent)
- **File:** `scripts/Argentina/run_pipeline_resume.py`
  - Lines 42-51: Step descriptions match script names (good)
- **File:** `scraper_gui.py`
  - GUI labels reference step names (need to verify match)

**Issues:**
1. **Minor inconsistency:** Argentina step 1 is `01_getProdList.py` (camelCase) vs others use `snake_case`
2. **Documentation may lag:** README.md may reference old script names (need to verify)
3. **UI labels:** GUI step labels should match script names exactly (need to verify)

**Recommendation:**
- Standardize script naming: all use `snake_case` (rename `01_getProdList.py` to `01_get_prod_list.py` - BREAKING, coordinate with team)
- Verify README.md step names match actual script names
- Add script name validation: check that GUI labels match script filenames (unit test)

**Business Logic Unchanged:** Only renames files, no logic changes.

---

### 8. Tor Browser Manager

**Status:** PARTIAL ⚠️

**Evidence:**
- **File:** `scripts/Argentina/03_alfabeta_selenium_scraper.py`
  - Lines 513-539: `check_tor_running()` - Checks ports 9050/9150 (good)
  - Lines 510-511: `TOR_PROXY_PORT` global variable (stores detected port)
  - Lines 702-709: Tor proxy configuration in Firefox profile (good)
  - **ISSUE:** No Tor start/stop manager (relies on external Tor)
  
- **File:** `scripts/Argentina/01_getProdList.py`
  - Lines 160-209: Tor check before starting (good)
  - **ISSUE:** If Tor not running, script fails (no fallback or actionable error)

**Issues:**
1. **No Tor manager:** Scripts check if Tor is running but don't start/stop it
2. **Port conflict detection:** Scripts detect port but don't handle conflicts (if port in use by another process)
3. **Profile isolation:** Tor proxy is global per script run (all workers share same proxy) - not isolated per worker
4. **Failure path:** If Tor not running, script fails immediately (no graceful fallback)
5. **Health checks:** Only checks port availability, not actual proxy functionality (may be listening but broken)
6. **Resume compatibility:** If script resumes, Tor check runs again (good), but no state persistence (which port was used last time)

**Recommendation:**
- Create `core/tor_manager.py` with: `start_tor()`, `stop_tor()`, `check_tor_health()`, `get_tor_port()`
- Add config flag: `TOR_ENABLED=true/false` (if false, skip Tor, use direct connection)
- Add port conflict detection: if 9050/9150 in use, try alternative ports or fail with clear error
- Add health check: test proxy with actual request (not just port check)
- Add retry logic: if Tor connection fails, retry with backoff (not just fail immediately)
- Log Tor status clearly: `[TOR] Enabled: true, Port: 9150, Health: OK`

**Business Logic Unchanged:** Only improves Tor management, no parsing/selectors changed.

---

## Top 10 Highest-Risk Issues

### 1. ❌ CRITICAL: Progress Not Resilient to Crashes
**Risk:** Data Loss, Duplicate Work  
**Category:** Progress & Status Reporting  
**Evidence:** `scripts/Argentina/scraper_utils.py:169-173` - Progress CSV appended but not flushed atomically  
**Impact:** If script crashes, progress may be lost, causing duplicate scraping  
**Fix:** Add atomic progress writes (temp file + rename), flush after every item

### 2. ❌ CRITICAL: Firefox Processes Not Tracked
**Risk:** Orphan Processes, Resource Leaks  
**Category:** Browser Instance Management  
**Evidence:** `scripts/Argentina/03_alfabeta_selenium_scraper.py:675-772` - Firefox used but ChromeManager only tracks Chrome  
**Impact:** Firefox/geckodriver processes may remain after crashes, consuming resources  
**Fix:** Extend ChromeManager to Firefox/Playwright, or create unified BrowserManager

### 3. ❌ CRITICAL: Item-Level Resume Not Integrated with Checkpoint
**Risk:** Data Loss, Duplicate Work  
**Category:** Resume / Checkpoint  
**Evidence:** `core/pipeline_checkpoint.py` tracks steps only, not items within steps  
**Impact:** If step 3 crashes at item 500/1000, entire step re-runs (wastes time)  
**Fix:** Extend checkpoint to track item-level progress, integrate with skip_set

### 4. ⚠️ HIGH: Inconsistent Logging (print vs logging)
**Risk:** Poor Observability, GUI Parsing Failures  
**Category:** Console/Log Management  
**Evidence:** Mix of `print()` and `logging.*` across scripts  
**Impact:** Logs not searchable, GUI progress parsing may fail  
**Fix:** Standardize on `logging.*`, create `core/logger.py` with standard formatter

### 5. ⚠️ HIGH: No Time Tracking for Steps/Pipeline
**Risk:** Poor UX, No Performance Metrics  
**Category:** Progress & Status Reporting (NEW REQUIREMENT)  
**Evidence:** No time tracking found in codebase  
**Impact:** Users can't see how long steps/pipeline take, can't estimate completion time  
**Fix:** Add time tracking to step execution, persist to checkpoint JSON, display in GUI

### 6. ⚠️ HIGH: Hardcoded Timeouts/Retries
**Risk:** Brittle Error Handling, Magic Numbers  
**Category:** Browser Instance Management  
**Evidence:** `scripts/Argentina/03_alfabeta_selenium_scraper.py:476` - `timeout=30` hardcoded  
**Impact:** Can't tune timeouts without code changes, inconsistent retry logic  
**Fix:** Create `core/retry_config.py` with centralized timeouts/retries

### 7. ⚠️ MEDIUM: Chrome user-data-dir Not Explicit
**Risk:** Profile Conflicts, State Leakage  
**Category:** Browser Instance Management  
**Evidence:** `scripts/Malaysia/01_Product_Registration_Number.py:58-61` - Chrome created without explicit user-data-dir  
**Impact:** Multiple instances may share profile, causing conflicts  
**Fix:** Require explicit user-data-dir per worker ID

### 8. ⚠️ MEDIUM: Step Success Criteria Not Validated
**Risk:** Silent Failures, Corrupt Outputs  
**Category:** Pipeline Management  
**Evidence:** `scripts/Argentina/run_pipeline_resume.py:64-68` - Step succeeds if subprocess returns 0, no output validation  
**Impact:** Steps may "succeed" but produce empty/corrupt files  
**Fix:** Add output validation before marking step complete (file exists, non-empty, valid format)

### 9. ⚠️ MEDIUM: Tor Failure Path Not Graceful
**Risk:** Script Failures, Poor UX  
**Category:** Tor Browser Manager  
**Evidence:** `scripts/Argentina/01_getProdList.py:207` - Script fails if Tor not running  
**Impact:** No fallback option, user must manually start Tor  
**Fix:** Add `TOR_ENABLED` config flag, graceful fallback or clear error message

### 10. ⚠️ MEDIUM: Dedup Key Inconsistency
**Risk:** Duplicate Output Rows  
**Category:** Resume / Checkpoint  
**Evidence:** Argentina uses `(company, product)` for skip_set but output uses `(input_company, input_product_name)`  
**Impact:** Keys may not match, causing duplicates  
**Fix:** Standardize dedup keys, add validation that skip_set keys match output keys

---

## Missing Standard Modules

### Should Exist But Missing:
1. **`core/progress_store.py`** - Atomic progress writes, ETA calculation, standard format
2. **`core/browser_pool.py`** - Unified Chrome/Firefox/Playwright manager (or extend ChromeManager)
3. **`core/retry_config.py`** - Centralized timeouts, retries, backoff
4. **`core/tor_manager.py`** - Tor start/stop/health checks
5. **`core/logger.py`** - Standard logging formatter (optional, can use logging directly)

### Exists But Not Fully Used:
1. **`core/chrome_manager.py`** - Exists but Argentina uses Firefox (not tracked)
2. **`core/pipeline_checkpoint.py`** - Exists but only step-level (needs item-level)
3. **`core/config_manager.py`** - Exists but some scripts use `os.getenv()` directly
4. **`shared_workflow_runner.py`** - Exists but not used by all scrapers

---

## Time Tracking Implementation Plan

### Requirement:
Track time taken for each step and entire pipeline, display in UI (last run completion time).

### Current State:
- No time tracking found in codebase
- Checkpoint stores `completed_at` timestamp but not duration
- GUI doesn't display timing information

### Implementation Plan:

**Files to Touch:**
1. `core/pipeline_checkpoint.py` - Add `step_duration` and `pipeline_duration` fields
2. `scripts/{Scraper}/run_pipeline_resume.py` - Track step start/end time
3. `shared_workflow_runner.py` - Track pipeline start/end time
4. `scraper_gui.py` - Display timing in UI

**Functions to Add/Change:**

**File: `core/pipeline_checkpoint.py`**
```python
def mark_step_complete(self, step_number: int, step_name: str, output_files: List[str] = None, duration_seconds: float = None):
    # Add duration_seconds parameter
    # Store in step_outputs: {"duration_seconds": duration_seconds, ...}

def get_pipeline_timing(self) -> Dict:
    # Calculate total pipeline duration from step durations
    # Return: {"total_duration_seconds": X, "step_durations": {...}}
```

**File: `scripts/{Scraper}/run_pipeline_resume.py`**
```python
def run_step(step_num: int, script_name: str, step_name: str, output_files: list = None):
    start_time = time.time()
    try:
        # ... existing subprocess.run() ...
        duration = time.time() - start_time
        cp.mark_step_complete(step_num, step_name, output_files, duration_seconds=duration)
    except:
        # Track duration even on failure
        duration = time.time() - start_time
        raise
```

**File: `scraper_gui.py`**
```python
def update_progress_from_log(self, log_content: str, scraper_name: str, update_display: bool = True):
    # Parse timing from checkpoint JSON
    # Display: "Last run: 2h 15m 30s" or "Step 3: 45m 12s"
    # Format: "{hours}h {minutes}m {seconds}s" or "{minutes}m {seconds}s" if < 1 hour
```

**Business Logic Unchanged:** Only adds timing metadata, no parsing/selectors changed.

---

## Summary Table

| Category | Status | Evidence Files | Fix Priority |
|----------|--------|----------------|--------------|
| 1. Progress & Status Reporting | PARTIAL ❌ | `scripts/Argentina/03_alfabeta_selenium_scraper.py`, `scraper_utils.py`, `run_pipeline_resume.py` | CRITICAL |
| 2. Browser Instance Management | PARTIAL ⚠️ | `core/chrome_manager.py`, `scripts/Argentina/03_alfabeta_selenium_scraper.py`, `scripts/Malaysia/01_Product_Registration_Number.py` | HIGH |
| 3. Resume / Checkpoint | PARTIAL ⚠️ | `core/pipeline_checkpoint.py`, `scripts/Argentina/run_pipeline_resume.py`, `scraper_utils.py` | HIGH |
| 4. Pipeline Management | PARTIAL ⚠️ | `shared_workflow_runner.py`, `scripts/{Scraper}/run_pipeline_resume.py` | MEDIUM |
| 5. Console/Log Management | PARTIAL ⚠️ | `scripts/Argentina/03_alfabeta_selenium_scraper.py`, `scripts/Malaysia/02_Product_Details.py`, `scraper_gui.py` | HIGH |
| 6. .env / Config Management | OK ✅ | `core/config_manager.py`, `platform_config.py`, `config/*.env.example` | LOW |
| 7. Script Naming / UI-UX | OK ✅ | `scripts/{Scraper}/*.py`, `README.md` | LOW |
| 8. Tor Browser Manager | PARTIAL ⚠️ | `scripts/Argentina/03_alfabeta_selenium_scraper.py`, `01_getProdList.py` | MEDIUM |

---

## Next Steps

1. **Immediate (Week 1):**
   - Fix progress resilience (atomic writes)
   - Add Firefox tracking to browser manager
   - Add time tracking (steps + pipeline)

2. **Short-term (Week 2-3):**
   - Standardize logging (replace print with logging)
   - Centralize timeouts/retries
   - Add item-level checkpoint tracking

3. **Medium-term (Month 1):**
   - Create unified browser pool
   - Add Tor manager
   - Standardize pipeline orchestration

4. **Long-term (Month 2+):**
   - Refactor script naming (snake_case)
   - Add step validation
   - Performance optimization based on timing data

---

**END OF AUDIT REPORT**
