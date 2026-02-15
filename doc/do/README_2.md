# Argentina Scraper (DB-Only)

Date: 2026-02-05

This pipeline is now PostgreSQL-first (aligned with Malaysia). No CSV inputs or tracking are used during the run. The only CSVs created are the final exports.

## End-to-end flow (10 steps)

| Step | Script | Description |
|------|--------|-------------|
| 0 | `00_backup_and_clean.py` | Backup & DB init - Creates backup, cleans output, applies schema, creates run_id |
| 1 | `01_getProdList.py` | Product list - Scrapes AlfaBeta index and inserts into `ar_product_index` |
| 2 | `02_prepare_urls.py` | Prepare URLs - Builds URLs in DB for every product |
| 3 | `03_alfabeta_selenium_scraper.py` | **Selenium Product Search** - Searches by product name, writes `ar_products` |
| 4 | `03b_alfabeta_selenium_company_search.py` | **Selenium Company Search** - For products with `total_records=0`, searches by company name |
| 5 | `04_alfabeta_api_scraper.py` | API Scraper - For remaining products with 0 records after Selenium |
| 6 | `05_TranslateUsingDictionary.py` | Translation - Translates `ar_products` into `ar_products_translated` |
| 7 | `06_GenerateOutput.py` | Export - Strict 4-key PCID match, exports final CSVs |
| 8 | `07_scrape_no_data_pipeline.py` | No-Data Retry (AUTO x2) - Retries no-data products twice using Selenium |
| 9 | `08_stats_and_validation.py` | Statistics & Validation - Detailed stats report |

### Step 4: Selenium Company Search (NEW)

This step targets products that still have `total_records=0` after the product search (Step 3).

**Strategy:**
1. Search by **company name** in "Índice de Laboratorios" field
2. Click on the exact company match to see all products for that company
3. Find and click on the exact product from the company's product list
4. Extract the product data

**Why this helps:**
- Some products have common names that return too many search results
- Company names are often more unique than product names
- Searching by company first narrows down the results

### Scrape Source Tracking

The pipeline now tracks which step scraped each product:
- `scrape_source` column in `ar_product_index`: `selenium_product`, `selenium_company`, `api`, `step7`
- `source` column in `ar_products`: `selenium`, `selenium_product`, `selenium_company`, `api`, `step7`, `manual`

The Statistics step (Step 9) provides a detailed breakdown of how many products were scraped by each method.

## Required inputs
These live in PostgreSQL and are typically uploaded via the GUI:
- `pcid_mapping` (`source_country='Argentina'`)
- `ar_dictionary`
- `ar_ignore_list` (optional)

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
- `ar_errors` (all errors logged in DB)
- `ar_step_progress` (sub-step resume)
- `ar_dictionary`
- **`pcid_mapping`** (shared table, `source_country='Argentina'`) – **single source** for PCID reference; used by both GUI Input page and pipeline (Step 0 + Step 6)
- `ar_pcid_mappings` (run-specific PCID-matched export rows)
- `ar_export_reports`
- `ar_artifacts` (screenshots before API, etc. – all logged in DB)## PCID: one table
- Argentina uses the **shared** table **`pcid_mapping`** with `source_country = 'Argentina'` as the **single source** for PCID reference.
- **Input page upload:** Uploading "PCID Mapping" for Argentina in the GUI writes to `pcid_mapping`; the next pipeline run (Step 6) uses that data.
- **Step 0 from file:** Running Step 0 with `input/Argentina/PCID Mapping - Argentina.csv` replaces Argentina rows in `pcid_mapping` (same table). No separate `ar_pcid_reference` is used.## Artifacts
- Before moving a product to API (total_records=0 after max loops), a screenshot is taken and saved under `output/Argentina/artifacts/`; the path is logged in `ar_artifacts` with type `screenshot_before_api`.