# Scraper Core Features Audit

**Purpose**: Identify duplicated vs shared core features across all scrapers to ensure uniformity and reduce maintenance burden.

**Date**: 2026-02-16

---

## Executive Summary

| Feature | Status | Notes |
|---------|--------|-------|
| **Config loading** | Duplicated | 12 scrapers each have `config_loader.py` with nearly identical logic; only `SCRAPER_ID` differs |
| **Cleanup lock** | Duplicated + inconsistent | Each scraper has its own; some use `get_env_int` (doesn't exist), some use `get_lock_paths`, some hardcode lock path |
| **Backup & clean** | Shared + duplicated | All use `core.utils.shared_utils` but each has its own `00_backup_and_clean.py` |
| **Run pipeline** | Duplicated | Each scraper has its own `run_pipeline_resume.py` with similar orchestration logic |
| **Core modules** | Shared | ConfigManager, pipeline_start_lock, chrome_manager, tor_manager, DB, etc. are used correctly |

---

## 1. Config Loader (config_loader.py)

### Current State
- **12 scrapers** each have their own `config_loader.py`:
  - Argentina, Belarus, Netherlands, Russia, canada_ontario, canada_quebec, tender_chile, tender_brazil, Taiwan, India, north_macedonia, Malaysia

### Duplication
- All implement: `getenv`, `getenv_int`, `getenv_float`, `getenv_bool`, `get_output_dir`, `get_backup_dir`, `get_input_dir`, `get_central_output_dir`, `load_env_file`
- All delegate to `core.config.config_manager.ConfigManager` for paths
- **Only difference**: `SCRAPER_ID` constant and scraper-specific config keys (e.g. Argentina has `ALFABETA_USER`, Russia has `SCRIPT_01_REGION_VALUE`)

### Inconsistencies
| Scraper | getenv vs get_env | .env.json support |
|---------|-------------------|-------------------|
| canada_quebec | Uses `get_env` + `get_env_int` (underscore) | Yes |
| All others | Use `getenv` + `getenv_int` (no underscore) | Belarus, Russia: Yes; Others: No |

### Recommendation
- Create `core.config.scraper_config` with a factory: `get_config_loader(scraper_id: str)` that returns a thin wrapper
- Each scraper keeps a 1-line `config_loader.py` that re-exports from core with its `SCRAPER_ID`
- Or: single `core.config.scraper_config` module with `getenv(scraper_id, key, default)` etc.

---

## 2. Cleanup Lock (cleanup_lock.py)

### Current State
- **11 scrapers** have `cleanup_lock.py` (India/Netherlands use archive version or different pattern)

### Duplication
- All remove lock file from `ConfigManager.get_sessions_dir() / "{Scraper}.lock"` or `get_lock_paths(scraper_name, repo_root)`
- All clean old lock `.{Scraper}_run.lock` in repo root

### Inconsistencies

| Scraper | Pattern | Bug / Note |
|---------|---------|-------------|
| Argentina, Malaysia, Russia, canada_quebec | Import `get_env_int`, `get_env_float` | **Bug**: Argentina/Malaysia/Russia config_loaders have `getenv_int` not `get_env_int` → Import fails, falls back to defaults |
| canada_quebec | Import `get_env_int`, `get_env_float` | OK: canada_quebec has `get_env_int` |
| Belarus, Taiwan, north_macedonia, Netherlands (archive), India (archive) | Use `get_lock_paths("Scraper", repo_root)` | Clean, no retry |
| canada_ontario | Uses `getenv_int`, `getenv_float`, `getenv_bool` + `--force` arg | Most feature-rich |
| Russia | Uses `ConfigManager.get_sessions_dir() / "Russia.lock"` directly | Bypasses `get_lock_paths` |
| Argentina, Malaysia | Retry loop + old lock cleanup | Runs at import (no `if __name__`) |
| Belarus, Taiwan, north_macedonia | `main()` + `if __name__` | Called as script |

### Recommendation
- Add shared `core.pipeline.cleanup_lock.run_cleanup(scraper_id: str)` that:
  - Uses `get_lock_paths(scraper_id, repo_root)`
  - Supports retries via env `MAX_RETRIES_CLEANUP`, `CLEANUP_RETRY_DELAY_BASE`
  - Cleans both new and old lock locations
- Each scraper's `cleanup_lock.py` becomes: `from core.pipeline.cleanup_lock import run_cleanup; run_cleanup("Argentina")` (or via `SCRAPER_ID` from config)
- Fix `get_env_int` → `getenv_int` in Argentina, Malaysia, Russia cleanup_lock (or add alias in config_loaders)

---

## 3. Backup and Clean (00_backup_and_clean.py)

### Current State
- **10+ scrapers** have `00_backup_and_clean.py`; Malaysia has `steps/step_00_backup_clean.py`

### Shared Usage
- All use `core.utils.shared_utils.backup_output_folder`, `clean_output_folder` ✓

### Duplication
- Each file has its own `main()` that:
  - Gets output dir from config_loader
  - Calls `backup_output_folder(output_dir, ...)` and `clean_output_folder(output_dir, ...)`
  - Minor differences in backup naming, exclude patterns

### Recommendation
- Add `core.utils.shared_utils.run_backup_and_clean(scraper_id: str)` that uses `ConfigManager.get_output_dir(scraper_id)` and shared logic
- Each `00_backup_and_clean.py` becomes a thin entry point calling the shared function

---

## 4. Run Pipeline Resume (run_pipeline_resume.py)

### Current State
- **11 scrapers** each have their own `run_pipeline_resume.py` (India uses `run_pipeline_scrapy.py`)

### Duplication
- All implement similar flow:
  - Path setup (script dir, repo root, clear `config_loader` from `sys.modules` for multi-scraper GUI)
  - Step list definition (0=backup, 1=step1, ...)
  - Checkpoint manager integration
  - Lock acquisition, cleanup on exit
  - Subprocess or direct execution of step scripts

### Shared Usage
- `core.pipeline.pipeline_checkpoint.get_checkpoint_manager`
- `core.pipeline.pipeline_start_lock`
- `core.config.config_manager.ConfigManager`
- `core.browser.chrome_pid_tracker.terminate_scraper_pids`
- `core.utils.step_progress_logger`
- Some use: `core.pipeline.preflight_checks`, `core.monitoring.alerting_integration`, `core.data.data_quality_checks`, `core.monitoring.prometheus_exporter`

### Inconsistencies
- Russia, tender_chile, canada_quebec: Full monitoring stack (preflight, alerting, data quality, prometheus)
- Argentina, Belarus, Malaysia, Taiwan, north_macedonia: Minimal or no monitoring
- Step list format varies: `(0, "00_backup_and_clean.py", "Backup and Clean", None)` vs `{"script": "00_backup_and_clean.py"}`

### Recommendation
- Create `core.pipeline.pipeline_runner` that takes `scraper_id`, `steps: List[Tuple]`, and optional monitoring flags
- Each `run_pipeline_resume.py` defines only its step list and calls the shared runner
- Standardize step format and monitoring hooks across all scrapers

---

## 5. Core Modules (Shared Resources)

### Correctly Used
| Module | Usage |
|--------|-------|
| `core.config.config_manager.ConfigManager` | Paths, env loading |
| `core.pipeline.pipeline_start_lock` | Lock acquisition, `get_lock_paths` |
| `core.pipeline.pipeline_checkpoint` | Checkpoint/resume |
| `core.browser.chrome_manager` | Chrome driver, PID tracking |
| `core.browser.chrome_instance_tracker` | DB-backed instance tracking |
| `core.browser.chrome_pid_tracker` | PID extraction from driver |
| `core.browser.stealth_profile` | Anti-detection |
| `core.network.tor_manager` | Tor proxy, auto-start |
| `core.db.connection.CountryDB` | SQLite (legacy) |
| `core.db.postgres_connection.PostgresDB` | PostgreSQL |
| `core.utils.shared_utils` | backup_output_folder, clean_output_folder, build_product_key |
| `core.resource_monitor` | Memory/resource checks |

### Scraper-Specific (Acceptable)
- `scripts/*/db/schema.py` – each scraper has its own schema; some use `core.db.models.apply_common_schema`
- Scraper-specific step scripts (e.g. `01_belarus_rceth_extract.py`) – domain logic, not core

---

## 6. Naming Inconsistencies

| Feature | Variant A | Variant B | Scrapers |
|---------|-----------|------------|----------|
| Config getter | `getenv_int` | `get_env_int` | canada_quebec uses `get_env_*` |
| Cleanup import | `getenv_int` | `get_env_int` | cleanup_lock imports `get_env_int` but config has `getenv_int` (Argentina, Malaysia, Russia) |

---

## 7. Action Items (Priority Order) – IMPLEMENTED 2026-02-16

1. ~~**Fix cleanup_lock import bug**~~ DONE: Created `core.pipeline.cleanup_lock.run_cleanup()`; all scrapers use it.
2. ~~**Unify cleanup_lock**~~ DONE: Shared `run_cleanup(scraper_id)`; thin wrappers in each scraper.
3. ~~**Unify config_loader**~~ DONE: Created `core.config.scraper_config.ScraperConfig`; Taiwan migrated as example.
4. ~~**Unify 00_backup_and_clean**~~ DONE: Added `run_backup_and_clean(scraper_id)`; Russia, Taiwan, Belarus, tender_chile, north_macedonia, Argentina refactored.
5. ~~**Unify run_pipeline_resume**~~ DONE: Created `core.pipeline.pipeline_runner.run_pipeline()`; scrapers can migrate incrementally.
6. ~~**Standardize naming**~~ DONE: canada_quebec has `getenv_*` aliases; cleanup uses shared module (no config_loader import).

---

## 8. Scraper Inventory

| Scraper | config_loader | cleanup_lock | 00_backup | run_pipeline |
|---------|---------------|--------------|-----------|---------------|
| Argentina | ✓ | ✓ | ✓ | ✓ |
| Belarus | ✓ | ✓ | ✓ | ✓ |
| Netherlands | ✓ | archive | - | ✓ |
| Russia | ✓ | ✓ | ✓ | ✓ |
| canada_ontario | ✓ | ✓ | ✓ | ✓ |
| canada_quebec | ✓ | ✓ | ✓ | ✓ |
| tender_chile | ✓ | ✓ | ✓ | ✓ |
| tender_brazil | ✓ | - | - | - |
| Taiwan | ✓ | ✓ | ✓ | ✓ |
| India | ✓ | archive | - | run_pipeline_scrapy |
| north_macedonia | ✓ | ✓ | ✓ | ✓ |
| Malaysia | ✓ | ✓ | step_00 | ✓ |
