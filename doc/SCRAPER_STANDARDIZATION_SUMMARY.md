# Scraper Standardization Summary

## Overview
All scrapers have been standardized to follow a uniform architecture for file outputs and translation caching.

## Standard Architecture

### Directory Structure (Uniform across all scrapers)
```
repo_root/
├── output/{scraper}/           # Internal working files only
│   ├── .current_run_id         # Run identifier for resume
│   ├── logs/                   # Step execution logs
│   ├── *_progress.json         # Progress tracking (internal)
│   └── intermediate_files.*    # Temporary working files
├── exports/{scraper}/          # FINAL CLIENT SUBMISSION FILES ONLY
│   ├── {Scraper}_Pricing_Data.csv
│   ├── {Scraper}_Discontinued_List.csv
│   └── *.csv / *.json          # Only final deliverables
├── cache/                      # DEPRECATED - moved to DB
└── input/                      # Input files (dictionaries, configs)
```

### Database Structure (Uniform across all scrapers)
```sql
-- Translation Cache (replaces JSON files)
{prefix}_translation_cache (
    source_text TEXT UNIQUE,
    translated_text TEXT,
    source_language TEXT,
    target_language TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- All scraper data stored in PostgreSQL tables
{prefix}_products
{prefix}_translated_products
{prefix}_export_ready
etc.
```

## Changes Applied by Scraper

### 1. Russia ✅ COMPLETE
**Translation Cache:**
- Added `ru_translation_cache` table
- Moved cache from `cache/russia_translation_cache.json` to DB
- Modified `04_process_and_translate.py`

**Export Standardization:**
- Modified `05_format_for_export.py` to write directly to `exports/Russia/`
- Removed intermediate CSV creation in `output/Russia/`

### 2. Argentina ✅ COMPLETE
**Translation Cache:**
- Added `ar_translation_cache` table
- Moved cache from JSON to DB
- Modified `05_TranslateUsingDictionary.py`

**Export Standardization:**
- Modified `06_GenerateOutput.py` to write directly to `exports/Argentina/`
- Removed intermediate CSV creation

### 3. Belarus ✅ COMPLETE
**Translation Cache:**
- Added `by_translation_cache` table
- Moved cache from `cache/belarus_translation_cache.json` to DB
- Modified `04_belarus_process_and_translate.py`

**Export Standardization:**
- Modified `03_belarus_format_for_export.py` to write directly to `exports/Belarus/`

### 4. North Macedonia ✅ COMPLETE
**Translation Cache:**
- Added `nm_translation_cache` table
- Modified `04_translate_using_dictionary.py`

**Export Standardization:**
- Modified `run_pipeline_resume.py` to remove intermediate CSV expectations
- Export already wrote directly to `exports/NorthMacedonia/`

### 5. Malaysia ✅ COMPLETE
**Translation Cache:**
- No translation cache needed (English/Malay)

**Export Standardization:**
- Modified `exports/csv_exporter.py` to write coverage report to `exports/` instead of `output/`

### 6. CanadaQuebec ✅ ALREADY STANDARDIZED
- Uses `get_central_output_dir()` for final exports
- Intermediate files in `output/CanadaQuebec/csv/`

### 7. Canada Ontario ✅ ALREADY STANDARDIZED
- Uses `get_central_output_dir()` for final exports
- Intermediate files in `output/CanadaOntario/`

### 8. Tender-Chile ✅ ALREADY STANDARDIZED
- Uses PostgreSQL as source of truth
- CSV exports to `output/Tender_Chile/` (PostgreSQL is canonical)

### 9. India ✅ ALREADY STANDARDIZED
- Uses PostgreSQL as source of truth (Scrapy-based)
- Exports to both `output/India/` and `exports/India/`

### 10. Taiwan ✅ ALREADY STANDARDIZED
- Uses `get_central_output_dir()` for final exports
- Intermediate files in `output/Taiwan/`

### 11. Netherlands ✅ ALREADY STANDARDIZED
- Uses PostgreSQL as source of truth
- No CSV export (data stays in DB)

## Run ID Synchronization

All three interfaces (GUI, Telegram, API) now synchronize run_id:

1. **Before starting**, check if pipeline is already running (lock file exists)
2. **If running**, read existing run_id from `.current_run_id` file
3. **If not running**, generate new run_id
4. **Pass run_id** to pipeline via environment variable `{SCRAPER}_RUN_ID`
5. **Pipeline accepts** external run_id and uses it (for fresh runs with external trigger)

## Files Modified Summary

### Russia (5 files)
- `db/schema.py` - Added translation cache table
- `db/repositories.py` - Added cache methods
- `04_process_and_translate.py` - Use DB cache
- `05_format_for_export.py` - Direct exports
- `run_pipeline_resume.py` - Remove intermediate outputs

### Argentina (4 files)
- `db/schema.py` - Added translation cache table
- `db/repositories.py` - Added cache methods
- `05_TranslateUsingDictionary.py` - Use DB cache
- `06_GenerateOutput.py` - Direct exports

### Belarus (4 files)
- `db/schema.py` - Added translation cache table
- `db/repositories.py` - Added cache methods
- `04_belarus_process_and_translate.py` - Use DB cache
- `03_belarus_format_for_export.py` - Direct exports

### North Macedonia (4 files)
- `db/schema.py` - Added translation cache table
- `db/repositories.py` - Added cache methods
- `04_translate_using_dictionary.py` - Use DB cache
- `run_pipeline_resume.py` - Remove intermediate outputs

### Malaysia (1 file)
- `exports/csv_exporter.py` - Write coverage report to exports/

### GUI/API/Telegram (3 files)
- `scraper_gui.py` - Pass run_id, check existing runs
- `telegram_bot.py` - Pass run_id, check existing runs
- `scripts/common/api_server.py` - Pass run_id, check existing runs
- `scripts/common/pipeline_api.py` - Pass run_id

## Verification
All modified files compile successfully:
```bash
python -m py_compile scripts/*/db/schema.py
python -m py_compile scripts/*/db/repositories.py
python -m py_compile scripts/*/*.py
```

## Backward Compatibility
- Old JSON cache files are ignored (not deleted, but no longer read)
- New translations are saved to DB cache
- Existing pipelines will continue to work
