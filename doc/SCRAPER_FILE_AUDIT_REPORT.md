# Scraper File Audit Report

**Date:** 2026-02-15  
**Scope:** All scrapers in `d:\quad99\Scrappers\scripts`  
**Focus:** File creation patterns, output locations, translation caches

---

## Summary by Scraper

### 1. Argentina

**Pipeline:** `run_pipeline_resume.py` (11 steps: 0-10)

**Files in output/Argentina/:**
- `alfabeta_Report_<date>_pcid_mapping.csv` - Products with PCID matches
- `alfabeta_Report_<date>_pcid_missing.csv` - Products without PCID
- `alfabeta_Report_<date>_pcid_oos.csv` - Out-of-scope PCIDs
- `alfabeta_Report_<date>_pcid_no_data.csv` - PCIDs in mapping but not in scraped data
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs

**Files in exports/Argentina/:**
- Same CSV files copied from output/ (central export location)

**Translation Cache:**
- **Location:** `cache/argentina_translation_cache.json`
- **Format:** JSON dict `{"normalized_term": "translated_term"}`
- **Used in:** `05_TranslateUsingDictionary.py`
- **Purpose:** Caches Google/OpenAI translations to avoid duplicate API calls

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/Argentina/`
- Uses `get_central_output_dir()` → `exports/Argentina/`
- DB-first architecture (PostgreSQL with `ar_` prefix tables)

---

### 2. Russia

**Pipeline:** `run_pipeline_resume.py` (6 steps: 0-5)

**Files in output/Russia/:**
- `russia_pricing_data.csv` - VED pricing data export
- `russia_discontinued_list.csv` - Excluded/discontinued products
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs
- `russia_scraper_progress.json` - Progress tracking (Step 0 clears this)
- `russia_excluded_scraper_progress.json` - Excluded list progress

**Files in exports/Russia/:**
- `Russia_Pricing_Data.csv` - Central export copy
- `Russia_Discontinued_List.csv` - Central export copy

**Translation Cache:**
- **Location:** `cache/russia_translation_cache.json`
- **Format:** JSON dict
- **Used in:** `04_process_and_translate.py`

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/Russia/`
- Uses `get_central_output_dir()` → `exports/Russia/`
- DB-first architecture (PostgreSQL with `ru_` prefix tables)

---

### 3. Malaysia

**Pipeline:** `run_pipeline_resume.py` (6 steps: 0-5, steps in `steps/` subdirectory)

**Files in output/Malaysia/:**
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs

**Files in exports/Malaysia/:**
- `malaysia_pcid_mapped_<date>.csv` - Products with PCID
- `malaysia_pcid_not_mapped_<date>.csv` - Products without PCID
- `malaysia_pcid_no_data_<date>.csv` - PCIDs not in scraped data
- `malaysia_coverage_report_<date>.csv` - Coverage statistics
- `malaysia_diff_report_<date>.csv` - Differential report

**Translation Cache:**
- **None found** (uses dictionary table `my_input_dictionary` in DB)

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/Malaysia/`
- Uses `get_central_output_dir()` → `exports/Malaysia/`
- DB-first architecture (PostgreSQL with `my_` prefix tables)

---

### 4. India

**Pipeline:** `run_pipeline_scrapy.py` (3 steps: 0-2)

**Files in output/India/:**
- `details_combined_001.csv` - Final combined export
- `qc_report.json` - Quality control report
- `.current_run_id` - Run identifier file

**Files in exports/India/:**
- Not explicitly defined (uses output/ as primary)

**Translation Cache:**
- **None** (English source data, no translation needed)

**Standard Pattern:** ⚠️ PARTIAL
- Uses Scrapy framework (different architecture)
- DB-first (PostgreSQL with `in_` prefix tables)
- Output files are CSV + JSON

---

### 5. CanadaQuebec

**Pipeline:** `run_pipeline_resume.py` (7 steps: 0-6)

**Files in output/CanadaQuebec/:**
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs
- `split_pdfs/` - Split PDF files (annexe_iv1.pdf, annexe_iv2.pdf, annexe_v.pdf)

**Files in exports/CanadaQuebec/:**
- `CanadaQuebec_merged_<date>.csv` - Final merged output
- `annexe_iv1_extracted.csv` - Extracted Annexe IV.1 data
- `annexe_iv2_extracted.csv` - Extracted Annexe IV.2 data
- `annexe_v_extracted.csv` - Extracted Annexe V data

**Translation Cache:**
- **None** (French/English bilingual source, no translation step)

**Standard Pattern:** ✅ YES
- Uses `get_csv_output_dir()` → `output/CanadaQuebec/`
- Uses `get_central_output_dir()` → `exports/CanadaQuebec/`
- DB-enabled (SQLite/PostgreSQL hybrid)

---

### 6. Canada Ontario

**Pipeline:** `run_pipeline_resume.py` (4 steps: 0-3)

**Files in output/Canada Ontario/:**
- `products.csv` - Extracted product details
- `manufacturer_master.csv` - Manufacturer reference
- `completed_letters.json` - Progress tracking
- `ontario_eap_prices.csv` - EAP pricing data
- `.current_run_id` - Run identifier file
- `logs/pipeline.log` - Pipeline logs

**Files in exports/CanadaOntario/:**
- `canadaontarioreport_<date>.csv` - Final report with standardized columns

**Translation Cache:**
- **None** (English source data)

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/Canada Ontario/`
- Uses `get_central_output_dir()` → `exports/CanadaOntario/`
- DB-first architecture (PostgreSQL)

---

### 7. Belarus

**Pipeline:** `run_pipeline_resume.py` (5 steps: 0-4)

**Files in output/Belarus/:**
- `belarus_rceth_raw.csv` - Raw scraped data
- `BELARUS_PCID_MAPPED_OUTPUT.csv` - PCID mapped data
- `belarus_pricing_data.csv` - Final export
- `.current_run_id` - Run identifier file

**Files in exports/Belarus/:**
- Not explicitly separated (output/ used as primary)

**Translation Cache:**
- **Location:** `cache/belarus_translation_cache.json`
- **Format:** JSON dict
- **Used in:** `04_belarus_process_and_translate.py`

**Standard Pattern:** ⚠️ PARTIAL
- Uses `get_output_dir()` → `output/Belarus/`
- No central exports/ directory usage
- DB-enabled but also writes intermediate CSVs

---

### 8. North Macedonia

**Pipeline:** `run_pipeline_resume.py` (6 steps: 0-5)

**Files in output/North Macedonia/:**
- `north_macedonia_drug_register.csv` - Scraped drug register data
- `manual_translation_needed.csv` - Missing terms report (from translation step)
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs

**Files in exports/NorthMacedonia/:**
- `north_macedonia_pcid_mapped_<date>.csv` - Products with PCID
- `north_macedonia_pcid_not_mapped_<date>.csv` - Products without PCID
- `north_macedonia_pcid_no_data_<date>.csv` - PCIDs not in scraped data

**Translation Cache:**
- **None found** (uses in-memory cache `_google_translate_cache` only)
- Dictionary stored in DB (`nm_input_dictionary`)

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/North Macedonia/`
- Uses hardcoded `exports/NorthMacedonia/` path
- DB-first architecture (PostgreSQL with `nm_` prefix tables)

---

### 9. Taiwan

**Pipeline:** `run_pipeline_resume.py` (3 steps: 0-2)

**Files in output/Taiwan/:**
- `taiwan_drug_code_urls.csv` - Collected URLs
- `.current_run_id` - Run identifier file

**Files in exports/Taiwan/:**
- `taiwan_drug_code_details.csv` - Final extracted details

**Translation Cache:**
- **None** (Traditional Chinese source, no translation step)

**Standard Pattern:** ✅ YES
- Uses `get_output_dir()` → `output/Taiwan/`
- Uses `get_central_output_dir()` → `exports/Taiwan/`
- Simple 3-step pipeline

---

### 10. Tender-Chile

**Pipeline:** `run_pipeline_resume.py` (5 steps: 0-4)

**Files in output/Tender- Chile/:**
- `tender_redirect_urls.csv` - Redirect URLs from step 1
- `tender_details.csv` - Tender details from step 2
- `mercadopublico_supplier_rows.csv` - Supplier data from step 3
- `mercadopublico_lot_summary.csv` - Lot summaries from step 3
- `final_tender_data.csv` - Final merged output from step 4
- `.current_run_id` - Run identifier file

**Files in exports/Tender_Chile/:**
- Not explicitly separated (output/ used as primary)

**Translation Cache:**
- **None** (Spanish source, no translation step)

**Standard Pattern:** ⚠️ PARTIAL
- Uses `get_output_dir()` → `output/Tender- Chile/`
- No central exports/ directory usage
- DB-first architecture (PostgreSQL with `tc_` prefix tables)

---

### 11. Netherlands

**Pipeline:** `run_pipeline_resume.py` (2 steps: 0-1)

**Files in output/Netherlands/:**
- `.current_run_id` - Run identifier file
- `logs/step_*.log` - Step execution logs

**Files in exports/Netherlands/:**
- Not explicitly defined (DB-only output)

**Translation Cache:**
- **None** (Dutch/English source, no translation step)

**Standard Pattern:** ⚠️ PARTIAL
- Uses `get_output_dir()` → `output/Netherlands/`
- No CSV exports (DB-only)
- DB-first architecture (PostgreSQL with `nl_` prefix tables)

---

## Translation Cache Summary

| Scraper | Cache Location | Format | Purpose |
|---------|---------------|--------|---------|
| Argentina | `cache/argentina_translation_cache.json` | JSON dict | Google/OpenAI translation cache |
| Russia | `cache/russia_translation_cache.json` | JSON dict | Translation cache |
| Belarus | `cache/belarus_translation_cache.json` | JSON dict | Translation cache |
| North Macedonia | In-memory only | Python dict | Google Translate cache (not persisted) |
| Malaysia | DB table `my_input_dictionary` | PostgreSQL | Dictionary storage |

---

## Standard Pattern Compliance

### ✅ Fully Standard (8/11)
- Argentina
- Russia
- Malaysia
- CanadaQuebec
- Canada Ontario
- North Macedonia
- Taiwan
- India (Scrapy variant)

### ⚠️ Partial (3/11)
- Belarus - No exports/ directory
- Tender-Chile - No exports/ directory
- Netherlands - DB-only, no CSV exports

---

## Key File Patterns

### Output Directory Structure (Standard)
```
output/<ScraperName>/
├── .current_run_id
├── logs/
│   └── step_*.log
└── *.csv (intermediate files)
```

### Exports Directory Structure (Standard)
```
exports/<ScraperName>/
├── *_pcid_mapped_*.csv
├── *_pcid_not_mapped_*.csv
├── *_pcid_no_data_*.csv
└── *report_*.csv (final reports)
```

### Cache Directory Structure
```
cache/
├── argentina_translation_cache.json
├── russia_translation_cache.json
└── belarus_translation_cache.json
```

---

## Database Table Prefixes

| Scraper | Table Prefix |
|---------|-------------|
| Argentina | `ar_` |
| Russia | `ru_` |
| Malaysia | `my_` |
| India | `in_` |
| Belarus | `by_` |
| North Macedonia | `nm_` |
| Tender-Chile | `tc_` |
| Netherlands | `nl_` |
| Canada Ontario | `co_` |
| CanadaQuebec | `cq_` |
| Taiwan | `tw_` |

---

## Recommendations

1. **Belarus, Tender-Chile, Netherlands:** Add central exports/ directory usage for consistency
2. **North Macedonia:** Consider persisting translation cache to JSON file
3. **All scrapers:** Consider standardizing on `get_central_output_dir()` from config_loader
4. **Translation caches:** Consider unifying cache format and location across all scrapers
