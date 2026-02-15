# Malaysia Unused Files Archive

**Archive Date:** 2026-02-15

## Summary

This archive contains Python scripts from `scripts/Malaysia/` that are no longer used by the Malaysia pipeline.

## Archived Files

### 1. `scraper_utils.py` - ORPHAN (for Malaysia)
- **Reason:** Not imported by any Malaysia script
- **Used By:** Argentina and Russia scrapers only
- **Malaysia Replacement:** Uses `core.selector_healer` and `core.pipeline_checkpoint` instead

### 2. `smart_locator.py` - ORPHAN (for Malaysia)
- **Reason:** Not imported by any Malaysia script
- **Used By:** Other countries (Russia, North Macedonia, Canada Ontario, Netherlands, Belarus)
- **Malaysia Replacement:** Uses `core.selector_healer` instead

### 3. `state_machine.py` - ORPHAN (for Malaysia)
- **Reason:** Not imported by any Malaysia script
- **Used By:** Other countries (Russia, North Macedonia, Canada Ontario, Netherlands, Belarus)
- **Malaysia Replacement:** Uses `core.pipeline_checkpoint` and DB-backed state

## Active Scripts (Not Archived)

### Core Pipeline Scripts (Used by run_pipeline_resume.py)
- `steps/step_00_backup_clean.py` - Step 0
- `steps/step_01_registration.py` - Step 1
- `steps/step_02_product_details.py` - Step 2
- `steps/step_03_consolidate.py` - Step 3
- `steps/step_04_reimbursable.py` - Step 4
- `steps/step_05_pcid_export.py` - Step 5

### Utility Scripts
- `config_loader.py` - Configuration management (widely used)
- `cleanup_lock.py` - Lock file cleanup

### Diagnostic/Maintenance Tools (Run manually)
- `health_check.py` - Health check (manual)
- `data_quality_check.py` - Data quality check (manual)
- `clear_step_data.py` - Clear step data (manual/GUI)
- `import_pcid_mapping.py` - Import PCID mapping (manual)
- `import_search_keywords.py` - Import search keywords (manual)
- `migrate_pcid_constraint.py` - DB migration (manual)

## Database Tables Used by Malaysia

### Core Tables (Active)
- `my_products` - Registration numbers from MyPriMe
- `my_product_details` - Product details from Quest3+
- `my_consolidated_products` - Deduplicated products
- `my_reimbursable_drugs` - FUKKM reimbursable drugs
- `my_pcid_mappings` - Final PCID-mapped output
- `my_step_progress` - Step progress tracking
- `my_bulk_search_counts` - Bulk search tracking
- `my_export_reports` - Export tracking

### Supporting Tables
- `my_pcid_reference` - PCID reference data
- `my_input_products` - Input products

## Key Differences from Argentina

Malaysia uses a **DB-first architecture**:
- No CSV file handling (unlike Argentina)
- Uses `core.selector_healer` instead of `smart_locator.py`
- Uses `core.pipeline_checkpoint` instead of `state_machine.py`
- All state stored in PostgreSQL, not files

## Notes

- The Malaysia pipeline uses `run_pipeline_resume.py` as the main orchestrator
- All core steps are executed as subprocesses by the pipeline runner
- Log files are now persisted in `output/Malaysia/logs/` (fixed 2026-02-15)
