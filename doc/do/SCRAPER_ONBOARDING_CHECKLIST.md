# Master Scraper Onboarding Checklist

**Date:** February 6, 2026  
**Purpose:** Comprehensive checklist that every new scraper must pass before being onboarded to the platform

**Reference Implementation:** See `scripts/Malaysia/run_pipeline_resume.py` and `core/integration_example.py`

**Standardized Features:**
- Chrome instance tracking: Use `core.chrome_instance_tracker.ChromeInstanceTracker`
- Stealth/anti-bot: Use `core.stealth_profile` (excludes human typing)

---

## ✅ Pre-Onboarding Requirements

### 1. Repository Structure
- [ ] Scraper directory created in `scripts/[ScraperName]/`
- [ ] Main orchestrator file: `run_pipeline_resume.py` exists
- [ ] Step scripts organized (preferably in `steps/` subdirectory)
- [ ] Configuration file: `config/[ScraperName].env.json` exists
- [ ] Documentation directory: `doc/[ScraperName]/README.md` exists

### 2. Database Schema
- [ ] PostgreSQL schema created: `sql/schemas/postgres/[prefix]_schema.sql`
- [ ] Schema includes required tables:
  - [ ] `[prefix]_step_progress` table (with enhanced columns)
  - [ ] `[prefix]_export_reports` table
  - [ ] `[prefix]_errors` table (optional but recommended)
  - [ ] `[prefix]_chrome_instances` table (MANDATORY - for browser tracking)
  - [ ] Input tables (never deleted/truncated)
- [ ] Schema migration created: `sql/migrations/postgres/XXX_add_[prefix]_schema.sql`
- [ ] Schema version recorded in `_schema_versions` table

---

## ✅ Pipeline Orchestration (MANDATORY)

### 3. Pipeline Runner Structure
- [ ] Uses `run_pipeline_resume.py` as main orchestrator
- [ ] Implements checkpoint/resume system (`core.pipeline_checkpoint.PipelineCheckpoint`)
- [ ] Supports `--fresh` flag (start from step 0)
- [ ] Supports `--step N` flag (start from specific step)
- [ ] Run ID management (environment variable + `.current_run_id` file)
- [ ] Step numbering starts at 0 (step 0 = backup/clean)

### 4. Stale Pipeline Recovery
- [ ] Imports `recover_stale_pipelines` from `shared_workflow_runner`
- [ ] Calls `recover_stale_pipelines(scraper_name)` in `main()` before determining start step
- [ ] Wrapped in try/except (non-blocking)
- [ ] Logs recovery actions

### 5. Browser PID Cleanup
- [ ] Imports `terminate_scraper_pids` from `core.chrome_pid_tracker`
- [ ] Pre-run cleanup: Calls `terminate_scraper_pids(scraper_name, repo_root)` before pipeline starts
- [ ] Post-run cleanup: Calls `terminate_scraper_pids(scraper_name, repo_root)` after pipeline completes
- [ ] Both wrapped in try/except (non-blocking)
- [ ] Logs cleanup actions

---

## ✅ Database Standards (MANDATORY)

### 6. Postgres-Only Policy
- [ ] ✅ PostgreSQL is the ONLY source of truth
- [ ] ❌ No SQLite databases used
- [ ] ❌ No CSV files used as primary input or source of truth
- [ ] ✅ CSV allowed ONLY for final exports (and must be persisted to Postgres)
- [ ] ✅ Input tables NEVER deleted/truncated
- [ ] ✅ Output cleanup only via safe, scoped deletion (by `run_id`/country/source)

### 7. Step Progress Logging
- [ ] Imports `log_step_progress` from `core.step_progress_logger`
- [ ] Logs step start: `log_step_progress(..., status="in_progress")`
- [ ] Logs step completion: `log_step_progress(..., status="completed")`
- [ ] Logs step failure: `log_step_progress(..., status="failed", error_message=...)`
- [ ] Uses enhanced metrics (see Step 8)
- [ ] Calls `update_run_ledger_step_count()` after step completes

### 8. Enhanced Step Metrics (MANDATORY)
- [ ] Logs `duration_seconds` (calculated from start_time to end_time)
- [ ] Logs `rows_read` (rows read from input)
- [ ] Logs `rows_processed` (rows processed/transformed)
- [ ] Logs `rows_inserted` (new rows inserted)
- [ ] Logs `rows_updated` (existing rows updated)
- [ ] Logs `rows_rejected` (rows rejected/failed validation)
- [ ] Logs `browser_instances_spawned` (if applicable)
- [ ] Logs `log_file_path` (path to step log file if available)
- [ ] All metrics passed to `log_step_progress()` function

### 9. Run-Level Aggregation
- [ ] Calls `update_run_ledger_aggregation(scraper_name, run_id)` after all steps complete
- [ ] Populates `run_ledger` columns:
  - [ ] `total_runtime_seconds`
  - [ ] `slowest_step_number`
  - [ ] `slowest_step_name`
  - [ ] `failure_step_number` (if failed)
  - [ ] `failure_step_name` (if failed)
  - [ ] `recovery_step_number` (if resumed)

---

## ✅ Foundation Contracts Integration (MANDATORY)

### 10. Preflight Health Checks
- [ ] Imports `PreflightChecker` from `core.preflight_checks`
- [ ] Creates checker: `PreflightChecker(scraper_name, run_id)`
- [ ] Calls `checker.run_all_checks()` in `main()` before pipeline starts
- [ ] Logs all check results
- [ ] Blocks pipeline if `checker.has_critical_failures()` returns True
- [ ] Exits with error code if critical checks fail

### 11. Step Event Hooks
- [ ] Imports `StepHookRegistry` and `StepMetrics` from `core.step_hooks`
- [ ] Creates `StepMetrics` object before each step execution
- [ ] Calls `StepHookRegistry.emit_step_start(metrics)` before step execution
- [ ] Calls `StepHookRegistry.emit_step_end(metrics)` after successful step completion
- [ ] Calls `StepHookRegistry.emit_step_error(metrics, error)` on step failure
- [ ] Populates `StepMetrics` with all available data (duration, row counts, etc.)

### 12. Alerting Integration
- [ ] Imports `setup_alerting_hooks` from `core.alerting_integration`
- [ ] Calls `setup_alerting_hooks()` once at startup (in `main()`)
- [ ] Alerting automatically triggers on step failures and anomalies

### 13. Data Quality Checks
- [ ] Imports `DataQualityChecker` from `core.data_quality_checks`
- [ ] Runs pre-flight checks: `dq_checker.run_preflight_checks()` before pipeline starts
- [ ] Runs post-run checks: `dq_checker.run_postrun_checks()` after pipeline completes
- [ ] Saves results: `dq_checker.save_results_to_db()` after checks
- [ ] Validates exports: `dq_checker.validate_export(export_file)` for each export file

### 14. Audit Logging
- [ ] Imports `audit_log` from `core.monitoring.audit_logger`
- [ ] Logs `run_started` event at pipeline start
- [ ] Logs `run_completed` event on successful completion
- [ ] Logs `run_failed` event on failure
- [ ] Includes relevant context (run_id, step_number, error_message if applicable)

### 15. Performance Benchmarking
- [ ] Imports `record_step_benchmark` from `core.benchmarking`
- [ ] Calls `record_step_benchmark(scraper_name, step_name, duration_seconds, rows_processed)` after each step completes
- [ ] Enables performance regression detection

---

## ✅ Browser Lifecycle Management (MANDATORY)

### 16. Browser Session Management
- [ ] Uses context manager for browser sessions (`browser_session()` or equivalent)
- [ ] Ensures browser instances are closed even on errors (try/finally or context manager)
- [ ] Tracks browser instance count per step
- [ ] Logs `browser_instances_spawned` in step metrics

### 17. Chrome Instance Tracking (MANDATORY)
- [ ] `[prefix]_chrome_instances` table created in schema
- [ ] Table includes columns: `run_id`, `step_number`, `thread_id`, `pid`, `parent_pid`, `browser_type`, `started_at`, `terminated_at`, `termination_reason`
- [ ] Registers browser instances when spawned: `register_chrome_instance(step_number, thread_id, pid, ...)`
- [ ] Marks instances as terminated on cleanup: `mark_chrome_terminated(instance_id, reason)`
- [ ] Tracks instances per step/thread for multi-threaded scrapers
- [ ] Enables orphan detection (instances running >2 hours)
- [ ] Uses repository method or shared utility for tracking

### 18. Browser Cleanup
- [ ] No orphaned browser processes after crashes
- [ ] Pre-run cleanup terminates any existing browser instances
- [ ] Post-run cleanup terminates all browser instances
- [ ] Uses `core.chrome_pid_tracker.terminate_scraper_pids()` for cleanup
- [ ] Chrome instance table updated on cleanup (mark as terminated)

---

## ✅ Error Handling & Logging

### 18. Error Tracking
- [ ] Errors logged to `[prefix]_errors` table (optional but recommended)
- [ ] Step failures logged with full error message and traceback reference
- [ ] Error messages include context (step_number, run_id, timestamp)
- [ ] Non-fatal errors don't block pipeline execution (wrapped in try/except)

### 19. Logging Standards
- [ ] Uses Python `logging` module (not print statements)
- [ ] Logs include `run_id` and `step_number` in context
- [ ] Log files saved to `output/[ScraperName]/logs/` or similar
- [ ] Log file path stored in `log_file_path` column of step_progress table
- [ ] Log levels appropriate (DEBUG, INFO, WARNING, ERROR)

---

## ✅ Configuration Management

### 20. Configuration File
- [ ] Configuration file: `config/[ScraperName].env.json`
- [ ] Follows standard structure:
  ```json
  {
    "scraper": {
      "id": "ScraperName",
      "enabled": true
    },
    "config": {
      "SCRIPT_00_*": "...",
      "SCRIPT_01_*": "..."
    },
    "secrets": {
      "API_KEY": "...",
      "PASSWORD": "..."
    }
  }
  ```
- [ ] No hardcoded values in scripts
- [ ] Uses `config_loader.py` or equivalent for configuration loading

### 21. Environment Variables
- [ ] Run ID stored in environment variable: `[SCRAPER_NAME]_RUN_ID`
- [ ] Database connection via environment variables (not hardcoded)
- [ ] Sensitive data in `secrets` section (not in code)

---

## ✅ Export Standards

### 22. Export Generation
- [ ] Exports generated in CSV format
- [ ] Exports saved to `exports/` subdirectory within output directory
- [ ] Export files follow naming convention: `[scraper]_[report_type]_[date].csv`
- [ ] Exports persisted to `[prefix]_export_reports` table in Postgres
- [ ] Export metadata includes: `run_id`, `export_type`, `file_path`, `row_count`, `generated_at`

### 23. Export Types (Standard)
- [ ] PCID-mapped products (`*_mapping.csv` or `*_mapped.csv`)
- [ ] PCID-missing products (`*_missing.csv` or `*_not_mapped.csv`)
- [ ] No-data products (`*_no_data.csv`)
- [ ] Out-of-stock products (`*_oos.csv`) - if applicable

---

## ✅ PCID Mapping Standards

### 24. PCID Mapping Contract
- [ ] Uses `core.pcid_mapping_contract.get_pcid_mapping()` for PCID lookups
- [ ] Uses shared `pcid_mapping` table (not country-specific tables)
- [ ] Mapping logic follows standard pattern:
  - [ ] Extract key fields (company, product, generic, pack_desc, etc.)
  - [ ] Normalize fields (lowercase, strip whitespace)
  - [ ] Lookup in `pcid_mapping` table
  - [ ] Handle OOS products via `is_oos_product()` check

### 25. Deduplication
- [ ] Uses database UNIQUE constraints for deduplication
- [ ] Unique key includes `run_id` (allows same product across runs)
- [ ] Unique constraint on appropriate fields (e.g., `UNIQUE(run_id, product_url)` or `UNIQUE(run_id, registration_no)`)
- [ ] Handles duplicate key violations gracefully (upsert or skip)

---

## ✅ Anti-Bot & Stealth Features (MANDATORY)

### 26. Stealth/Anti-Bot Implementation
- [ ] Uses `core.stealth_profile` module for stealth features
- [ ] Playwright scrapers: Calls `apply_playwright(context_kwargs)` before creating context
- [ ] Selenium scrapers: Calls `apply_selenium(options)` before creating driver
- [ ] Stealth features include:
  - [ ] Webdriver property hiding (`navigator.webdriver = undefined`)
  - [ ] Mock plugins array (Chrome-like plugins)
  - [ ] Mock languages (`navigator.languages`)
  - [ ] Mock chrome runtime (`window.chrome`)
  - [ ] User agent rotation (random selection from pool)
  - [ ] Automation-controlled flag disabled (`--disable-blink-features=AutomationControlled`)
- [ ] Stealth init script injected into Playwright contexts
- [ ] User agent pool defined (minimum 3-4 realistic user agents)
- [ ] Human-like delays (`pause()`, `long_pause()`) for page loads
- [ ] ❌ **EXCLUDED**: Human-like typing simulation (`human_type()`, `type_delay_ms()`) - NOT required

### 27. Code Structure
- [ ] Follows existing pipeline patterns (see Malaysia as reference)
- [ ] Code is modular (separate functions for each major operation)
- [ ] No business logic in orchestrator (orchestrator only coordinates)
- [ ] Step scripts are independent (can run standalone for testing)

### 28. Error Handling
- [ ] All database operations wrapped in try/except
- [ ] All file operations wrapped in try/except
- [ ] All network requests wrapped in try/except
- [ ] Errors logged with context (step, run_id, timestamp)
- [ ] Non-critical errors don't crash pipeline (continue to next step/item)

### 29. Idempotency
- [ ] Steps are idempotent (can be rerun safely)
- [ ] Uses `run_id` scoping for all writes (prevents cross-run conflicts)
- [ ] Checkpoint system allows resume from any step
- [ ] No side effects from rerunning steps

---

## ✅ Documentation Requirements

### 30. README Documentation
- [ ] `doc/[ScraperName]/README.md` exists
- [ ] Documents pipeline steps (step numbers, names, descriptions)
- [ ] Documents input requirements (tables, files, formats)
- [ ] Documents output format (CSV structure, column descriptions)
- [ ] Documents configuration options
- [ ] Documents PCID mapping strategy
- [ ] Documents error handling and recovery

### 31. Code Documentation
- [ ] Functions have docstrings
- [ ] Complex logic has inline comments
- [ ] Step scripts have header comments explaining purpose
- [ ] Configuration options documented in code comments

---

## ✅ Testing Requirements

### 32. Smoke Tests
- [ ] Pipeline can run end-to-end without errors (even with empty input)
- [ ] Each step can run independently (for debugging)
- [ ] Checkpoint/resume works correctly
- [ ] Error handling works (test with invalid input)

### 33. Data Validation
- [ ] Input data validated before processing
- [ ] Output data validated after processing
- [ ] Row counts match expected ranges
- [ ] Required columns present in exports

---

## ✅ Integration Checklist

### 34. Foundation Contracts Integration
- [ ] Preflight checks integrated and tested
- [ ] Step hooks integrated and emitting events
- [ ] Alerting hooks registered
- [ ] Data quality checks running
- [ ] Audit logging active
- [ ] Benchmarking active

### 35. Database Integration
- [ ] Schema created and migrated
- [ ] Step progress logging working
- [ ] Enhanced metrics populated
- [ ] Run-level aggregation working
- [ ] Export reports persisted

### 36. Platform Integration
- [ ] Scraper added to `scraper_gui.py` (if GUI integration needed)
- [ ] Scraper added to `create_checkpoint.py` PIPELINE_STEPS dict
- [ ] Configuration file added to `config/` directory
- [ ] Documentation added to `doc/[ScraperName]/`

---

## ✅ Final Verification

### 37. End-to-End Test
- [ ] Full pipeline run completes successfully
- [ ] All steps logged to database
- [ ] Enhanced metrics populated correctly
- [ ] Exports generated and persisted
- [ ] No orphaned browser processes
- [ ] No errors in logs

### 38. Performance Verification
- [ ] Step durations reasonable (no performance regressions)
- [ ] Memory usage stable (no leaks)
- [ ] Database queries efficient
- [ ] Browser instances cleaned up properly

### 39. Documentation Verification
- [ ] README is complete and accurate
- [ ] Code comments are helpful
- [ ] Configuration options documented
- [ ] Error scenarios documented

---

## 📋 Checklist Summary

**Total Items:** 39 categories, ~160+ individual checks

**Critical (Must Pass):**
- ✅ Pipeline Orchestration (Items 3-5)
- ✅ Database Standards (Items 6-9)
- ✅ Foundation Contracts (Items 10-15)
- ✅ Step Tracking (Item 8)
- ✅ Chrome Instance Tracking (Item 17)
- ✅ Stealth/Anti-Bot Features (Item 26)
- ✅ Export Standards (Items 22-23)

**Important (Should Pass):**
- ✅ Browser Management (Items 16, 18)
- ✅ Error Handling (Items 19-20)
- ✅ Configuration (Items 21-22)
- ✅ PCID Mapping (Items 24-25)

**Recommended (Good to Have):**
- ✅ Code Quality (Items 26-28)
- ✅ Documentation (Items 29-30)
- ✅ Testing (Items 31-32)

---

## 🎯 Quick Reference

**Reference Implementation:**
- `scripts/Malaysia/run_pipeline_resume.py` - Full integration example
- `core/integration_example.py` - Integration pattern reference

**Key Files to Review:**
- `doc/project/GAP_ANALYSIS_MALAYSIA_ARGENTINA_NETHERLANDS.md` - Standards reference
- `doc/implementation/IMPLEMENTATION_COMPLETE.md` - Feature list
- `doc/deployment/DEPLOY_NOW.md` - Deployment guide

**Support:**
- See `doc/general/DEVELOPER_ONBOARDING_GUIDE.md` for detailed developer guide

---

## ✅ Sign-Off

**Scraper Name:** _________________  
**Developer:** _________________  
**Date:** _________________  
**Status:** ☐ Passed | ☐ Needs Work | ☐ Blocked

**Reviewer:** _________________  
**Review Date:** _________________  
**Approved:** ☐ Yes | ☐ No

---

**Last Updated:** February 6, 2026  
**Version:** 1.0
