# Argentina Unused Files Archive

**Archive Date:** 2026-02-15

## Summary

This archive contains Python scripts from `scripts/Argentina/` that are no longer used by the Argentina pipeline.

## Archived Files

### 1. `mock_step.py` - DEPRECATED
- **Reason:** Temporary mock step used during early pipeline development
- **Last Used:** Legacy checkpoint migration only (referenced in `_has_legacy_mock_step_checkpoint`)
- **Replacement:** Real step implementations (steps 0-10)

### 2. `auto_restart_wrapper.py` - ORPHAN
- **Reason:** Standalone wrapper for auto-restarting scraper, never integrated into current pipeline
- **Last Used:** Not integrated into `run_pipeline_resume.py`
- **Replacement:** Pipeline has built-in resume/restart logic

### 3. `scraper.py` - LEGACY
- **Reason:** Old scraper implementation using BaseScraper class
- **Last Used:** Superseded by `run_pipeline_resume.py` pipeline architecture
- **Replacement:** `run_pipeline_resume.py` with step-based execution

## Active Scripts (Not Archived)

### Core Pipeline Scripts (Used by run_pipeline_resume.py)
- `00_backup_and_clean.py` - Step 0
- `01_getProdList.py` - Step 1
- `02_prepare_urls.py` - Step 2
- `03_alfabeta_selenium_scraper.py` - Step 3
- `03b_alfabeta_selenium_company_search.py` - Step 4
- `04_alfabeta_api_scraper.py` - Step 5
- `05_TranslateUsingDictionary.py` - Step 6
- `06_GenerateOutput.py` - Step 7
- `07_scrape_no_data_pipeline.py` - Step 8
- `08_refresh_export.py` - Step 9
- `08_stats_and_validation.py` - Step 10

### Utility Scripts (Imported by other scripts)
- `config_loader.py` - Configuration management
- `scraper_utils.py` - Shared scraping utilities
- `03_alfabeta_selenium_worker.py` - Selenium worker (called by step 3 & 7)
- `smart_locator.py` - Smart element locator
- `state_machine.py` - Navigation state machine
- `cleanup_lock.py` - Lock file cleanup

### Diagnostic/Maintenance Tools (Run manually)
- `scrape_no_data_products.py` - Manual no-data product scraper
- `check_pending_products.py` - Check pending products
- `delete_no_data_products.py` - Delete no-data products
- `delete_step8_products_now.py` - Quick delete step 8 products
- `cleanup_step8_products.py` - Cleanup step 8 products
- `requeue_missing_products.py` - Requeue missing products
- `fix_api_marked_for_selenium_retry.py` - Fix API/Selenium marking
- `health_check.py` - Health check script

## Database Tables Used by Argentina

### Core Tables (Active)
- `ar_product_index` - Product queue
- `ar_products` - Scraped products
- `ar_products_translated` - Translated products
- `ar_step_progress` - Step progress tracking
- `ar_dictionary` - ES->EN translations
- `ar_export_reports` - Export tracking
- `ar_errors` - Error logging
- `ar_scrape_stats` - Scrape statistics

### Supporting Tables
- `ar_ignore_list` - Products to skip
- `ar_artifacts` - Screenshots/artifacts
- `ar_oos_urls` - Out-of-scope URLs

## Notes

- The Argentina pipeline uses `run_pipeline_resume.py` as the main orchestrator
- All core steps are executed as subprocesses by the pipeline runner
- Log files are now persisted in `output/Argentina/logs/` (fixed 2026-02-15)
