# Malaysia Scraper Runbook (2026-01-30)

This replaces all previous Malaysia docs. Keep this as the single source of truth.

## Pipeline at a glance
- Steps (run via `python scripts/Malaysia/run_pipeline_resume.py`):
  0. Backup & DB init (`steps/step_00_backup_clean.py`)
  1. Product registration numbers (MyPriMe, Playwright)
  2. Product details (Quest3+, bulk + individual)
  3. Consolidate results (DB merge)
  4. Fully reimbursable list (FUKKM)
  5. PCID mapping + exports
- Resume-safe: checkpoints stored by `core.pipeline_checkpoint`.
- Run ID persisted at `output/Malaysia/.current_run_id`.

## Prereqs
- Env: `config/Malaysia.env.json` (or `.env` loaded by `config_loader.py`).
- Input files:
  - `input/Malaysia/products.csv` (keywords for bulk search)
  - `input/Malaysia/PCID Mapping - Malaysia.csv` (columns: `LOCAL_PACK_CODE`, `PCID Mapping`)
- DB: PostgreSQL reachable per env; schema auto-migrates on step 0/5.

## How to run
```bash
cd scripts/Malaysia
python run_pipeline_resume.py --fresh   # full run
# or resume from a step
python run_pipeline_resume.py --step 4  # start at reimbursable
```

## Key behaviors / fixes (current)
- Quest3 bulk counts: `page_rows` is forced to `csv_rows` if the DataTables info under-reports (e.g., shows 10). Diff now zeroes when CSV is complete.
- PCID mapping join: strips non‑alphanumerics on both reg no and local pack code; accepts header `PCID Mapping`.
- Exports audit: `my_export_reports` logs mapped/not-mapped/coverage/diff with run_id.

## Outputs
- DB tables (prefix `my_`): products, product_details, consolidated_products, reimbursable_drugs, pcid_reference, pcid_mappings, bulk_search_counts, export_reports, step_progress.
- Files:
  - Exports: `exports/Malaysia/malaysia_pcid_mapped_*.csv`, `malaysia_pcid_not_mapped_*.csv`, diff + coverage report.
  - Health: `exports/Malaysia/health_check/health_check_*.txt|json`.

## Health check
```bash
cd scripts/Malaysia
python health_check.py
```
Checks URL reachability (MyPriMe, Quest3+, FUKKM), presence of PCID file, and selector config including `SCRIPT_02_INFO_SELECTOR` (`#searchTable_info` default).

## Common issues
- Counts mismatch in `my_bulk_search_counts`: rerun step 2; ensure CSV downloaded; page_rows now aligns automatically.
- PCID missing: add mapping in `input/Malaysia/PCID Mapping - Malaysia.csv` and rerun step 5.
- Stale checkpoint/run_id: `--fresh` clears; removes `.current_run_id`.

## Maintenance tips
- When selectors change, update env vars rather than code where possible; run `health_check.py` to confirm.
- Keep input mappings in UTF-8; avoid stray BOM.
- Before deploying, spot-check `my_export_reports` for the latest run to verify exports were written.
