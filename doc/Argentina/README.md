# Argentina Scraper (DB-Only)

Date: 2026-01-30

This pipeline is now PostgreSQL-first (aligned with Malaysia). No CSV inputs or tracking are used during the run. The only CSVs created are the final exports.

## End-to-end flow
1) Step 0 - Backup & DB init
   - Creates backup, cleans output, applies schema, creates run_id.
   - Seeds dictionary + PCID reference + ignore list into DB.
2) Step 1 - Product list
   - Scrapes AlfaBeta index and inserts into `ar_product_index`.
   - Count check: extracted == DB count.
3) Step 2 - Prepare URLs
   - Builds URLs in DB for every product.
   - Count check: URL count == product_index count.
4) Step 3 - Selenium
   - Reads pending rows from DB, writes `ar_products`, updates `ar_product_index`.
5) Step 4 - API
   - Reads DB for items with 0 records after Selenium max runs.
   - Writes `ar_products`, updates `ar_product_index`.
6) Step 5 - Translation
   - Translates `ar_products` into `ar_products_translated` using `ar_dictionary`.
7) Step 6 - Export
   - Strict 4-key PCID match using `ar_pcid_reference`.
   - Exports final CSVs and stores a copy in central exports.

## Required inputs
These are loaded into DB during Step 0:
- `input/Argentina/Dictionary.csv`
- `input/Argentina/PCID Mapping - Argentina.csv`
- `input/Argentina/ignore_list.csv` (optional)

## Outputs (CSV only)
Generated in `output/Argentina/` and mirrored to central exports:
- `alfabeta_Report_<date>_pcid_mapping.csv`
- `alfabeta_Report_<date>_pcid_missing.csv`
- `alfabeta_Report_<date>_pcid_oos.csv` (empty placeholder)
- `alfabeta_Report_<date>_pcid_no_data.csv`

## Run
```bash
cd scripts/Argentina
python run_pipeline_resume.py --fresh
```

## DB tables (Argentina)
- `ar_product_index` (queue)
- `ar_products` (scraped rows)
- `ar_products_translated`
- `ar_dictionary`
- `ar_pcid_reference`
- `ar_pcid_mappings`
- `ar_export_reports`
