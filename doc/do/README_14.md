# Taiwan Scraper Runbook

This is the single source of truth for the Taiwan NHI drug code scraper.

## Pipeline at a glance
- Steps (run via `python scripts/Taiwan/run_pipeline_resume.py`):
  0. Backup & DB init (`00_backup_and_clean.py`)
  1. Collect drug code URLs (`01_taiwan_collect_drug_code_urls.py.py`) - scrape NHI drug code listing
  2. Extract drug code details (`02_taiwan_extract_drug_code_details.py`) - scrape FDA certificate details
- Resume-safe: checkpoints stored by `core.pipeline_checkpoint`.

## Prereqs
- Env: `config/Taiwan.env.json` (loaded by `config_loader.py`).
- Input files:
  - `input/Taiwan/ATC_L3_L4_Prefixes.csv` (ATC code prefixes for filtering)
- DB: PostgreSQL reachable per env; schema auto-migrates on step 0.
- Chrome: Required for Selenium-based scraping.

## How to run
```bash
cd scripts/Taiwan
python run_pipeline_resume.py --fresh   # full run
# or resume from a step
python run_pipeline_resume.py --step 2  # start at detail extraction
# or use the batch file
run_pipeline.bat
```

## Outputs
- DB tables (prefix `tw_`): drug_codes, drug_details, step_progress, export_reports, errors.
- Files:
  - `taiwan_drug_code_urls.csv` - collected drug code URLs (Step 1)
  - `taiwan_drug_code_details.csv` - extracted certificate details (Step 2)
  - Progress tracking: `seen_drug_codes.txt`, `seen_licids.txt`, `seen_companies.txt`

## Health check
```bash
cd scripts/Taiwan
python health_check.py
```
Checks DB connectivity, disk space, Chrome availability, and input file presence.

## Common issues
- Chrome not found: ensure Chrome is installed and chromedriver is on PATH.
- NHI site slow: the scraper tracks progress and can resume; use `--step 1` or `--step 2` to continue.
- Stale checkpoint: use `--fresh` to clear all checkpoints and start over.
