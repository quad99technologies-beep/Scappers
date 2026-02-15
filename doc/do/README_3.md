# Belarus Scraper Runbook

This is the single source of truth for the Belarus RCETH drug price scraper.

## Pipeline at a glance
- Steps (run via `python scripts/Belarus/run_pipeline_resume.py`):
  0. Backup & DB init (`00_backup_and_clean.py`)
  1. RCETH data extraction (`01_belarus_rceth_extract.py`) - scrapes drug price registry
  2. PCID mapping (`02_belarus_pcid_mapping.py`) - maps to PCID codes for export
- Resume-safe: checkpoints stored by `core.pipeline_checkpoint`.
- Run ID persisted at `output/Belarus/.current_run_id`.

## Prereqs
- Env: `config/Belarus.env.json` (loaded by `config_loader.py`).
- Input files:
  - `input/Belarus/Generic Name.csv` (generic name list for search)
  - `input/Belarus/Belarus PCID Mapping.csv` (PCID mapping reference)
- DB: PostgreSQL reachable per env; schema auto-migrates on step 0.
- Chrome: Required for Selenium-based scraping.

## How to run
```bash
cd scripts/Belarus
python run_pipeline_resume.py --fresh   # full run
# or resume from a step
python run_pipeline_resume.py --step 2  # start at PCID mapping
```

## Outputs
- DB tables (prefix `by_`): rceth_data, pcid_mappings, final_output, step_progress, export_reports, errors.
- Files:
  - Exports: `exports/Belarus/` (PCID mapped/unmapped CSVs)
  - Health: `python health_check.py` for system diagnostics

## Health check
```bash
cd scripts/Belarus
python health_check.py
```
Checks DB connectivity, disk space, and Chrome availability.

## Common issues
- Chrome not found: ensure Chrome is installed and chromedriver is on PATH.
- DB connection failed: verify PostgreSQL credentials in `config/Belarus.env.json`.
- Stale checkpoint: use `--fresh` to clear all checkpoints and start over.
