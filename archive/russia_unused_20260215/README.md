# Russia Unused Files Archive

**Archive Date:** 2026-02-15

## Archived Files

### 1. `scraper.py` - ALTERNATIVE/LEGACY
- **Reason:** Alternative pipeline runner using BaseScraper class
- **Primary Runner:** `run_pipeline_resume.py` is the main pipeline runner
- **Note:** This script provides a wrapper around the same step scripts but uses BaseScraper architecture
- **Can be restored if:** BaseScraper approach is preferred over the resume-based pipeline

## Active Scripts (Not Archived)

### Core Pipeline Scripts
- `00_backup_and_clean.py` - Step 0
- `01_russia_farmcom_scraper.py` - Step 1 (VED pricing)
- `02_russia_farmcom_excluded_scraper.py` - Step 2 (excluded list)
- `03_retry_failed_pages.py` - Step 3 (retry failed)
- `04_process_and_translate.py` - Step 4 (translation)
- `05_format_for_export.py` - Step 5 (export)

### Utility Scripts
- `config_loader.py` - Configuration management
- `smart_locator.py` - Element location
- `state_machine.py` - Navigation state
- `scraper_utils.py` - Shared utilities
- `cleanup_lock.py` - Lock cleanup

### Standalone Utilities
- `health_check.py` - Health diagnostics
- `init_db.py` - DB initialization
- `migrate_add_url_column.py` - DB migration

## Pipeline Flow
```
run_pipeline_resume.py → Step 0 → Step 1 → Step 2 → Step 3 → Step 4 → Step 5
```

## Notes
- All 15 remaining scripts are actively used
- Log persistence fixed 2026-02-15
