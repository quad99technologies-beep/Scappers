# North Macedonia Unused Files Archive

**Archive Date:** 2026-02-15

## Summary

Archived 10 unused/deprecated scripts from `scripts/North Macedonia/`

## Archived Files

### ORPHAN (Unused)
| File | Reason |
|------|--------|
| `scraper_utils.py` | Not imported by any script |
| `progress_ui.py` | Standalone utility, not called |

### DEPRECATED (Old Versions)
| File | Reason |
|------|--------|
| `01_collect_urls_simple.py` | Simplified version, not used |
| `01_fast_collect_urls.py` | HTTP version, not used (using Selenium version) |
| `01_fast_collect_urls_fixed.py` | Fixed HTTP version, not used |
| `01_fast_collect_urls_original.py` | Original HTTP version, not used |
| `02_scrape_details.py` | Old Selenium version (replaced by 02_fast_scrape_details.py) |

### ALTERNATIVE SCRAPERS (Not in Pipeline)
| File | Reason |
|------|--------|
| `03_map_pcids.py` | Standalone PCID mapper (PCID mapping done in 06_generate_export.py) |
| `03_scrape_zdravstvo.py` | Alternative zdravstvo scraper, not in pipeline |
| `03a_scrape_maxprices_parallel.py` | Max prices scraper, not in pipeline |

## Active Scripts (Not Archived)

### Core Pipeline Scripts
- `00_backup_and_clean.py` - Step 0
- `01_collect_urls.py` - Step 1 (URL collection - **Selenium-based**)
- `02_fast_scrape_details.py` - Step 2 (Detail scraping - **HTTP-based**)
- `04_translate_using_dictionary.py` - Step 3
- `05_stats_and_validation.py` - Step 4
- `06_generate_export.py` - Step 5

### Utility Scripts
- `config_loader.py` - Configuration
- `state_machine.py` - Navigation state
- `smart_locator.py` - Element location
- `cleanup_lock.py` - Lock cleanup

### Standalone Utilities
- `health_check.py` - Diagnostics
- `check_schema.py` - Schema check
- `migrate_schema.py` - DB migration

## Pipeline Flow
```
run_pipeline_resume.py → Step 0 → Step 1 → Step 2 → Step 3 → Step 4 → Step 5
```

## Key Points
- **Step 1** uses Selenium-based `01_collect_urls.py` (not the HTTP "fast" versions)
- **Step 2** uses HTTP-based `02_fast_scrape_details.py` (not the Selenium version)
- Log persistence fixed 2026-02-15
