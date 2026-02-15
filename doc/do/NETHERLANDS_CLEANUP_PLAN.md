# Netherlands Scraper - Cleanup Plan

## SIMPLIFIED WORKFLOW

```
Step 0: Backup & Clean
Step 1: Collect URLs (single search: Alle vormen + Alle sterktes)
Step 2: Scrape Products (medicijnkosten.nl data)
Step 3: Consolidate & Export
```

## FILES TO RENAME

### Core Scripts:
- `1-url scrapper.py` → `01_collect_urls.py`
- `01_get_medicijnkosten_data.py` → `02_scrape_products.py`
- `03_Consolidate_Results.py` → `03_consolidate.py`

### Keep As-Is:
- `00_backup_and_clean.py`
- `config_loader.py`
- `data_validator.py`
- `scraper_utils.py`
- `url_builder.py`
- `smart_locator.py`
- `state_machine.py`
- `health_check.py`
- `cleanup_lock.py`
- `run_pipeline.bat`

## FILES TO DELETE

- `01_load_combinations.py` - No longer needed (single URL)
- `02_reimbursement_extraction.py` - Merged into main scraper
- `extract_dropdown_values.py` - No longer needed
- `.env.example` - If not used

## DATABASE TABLES

### KEEP (Core Tables):
```sql
- nl_collected_urls      -- Product URLs from single search
- nl_packs              -- Product pricing data
- nl_consolidated       -- Final merged output
- nl_chrome_instances   -- Browser cleanup tracking
- nl_errors             -- Error logging
```

### REMOVE (Unused Tables):
```sql
- nl_search_combinations  -- No longer needed (single URL)
- nl_details             -- Not used in current workflow
- nl_costs               -- Not used in current workflow
- nl_products            -- Legacy table
- nl_reimbursement       -- Legacy table
- nl_step_progress       -- Not used
- nl_export_reports      -- Not used
```

## UPDATED PIPELINE

```batch
Step 0: Backup & Clean (00_backup_and_clean.py)
Step 1: Collect URLs (01_collect_urls.py)
Step 2: Scrape Products (02_scrape_products.py)
Step 3: Consolidate (03_consolidate.py)
```

## IMPLEMENTATION ORDER

1. Simplify `01_collect_urls.py` (remove combination logic)
2. Update `run_pipeline.bat` (3 steps instead of 5)
3. Update database schema (remove unused tables)
4. Delete unused files
5. Update documentation
