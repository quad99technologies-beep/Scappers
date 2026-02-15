# Master Approval Checklist - Scraper Platform

**Generated:** 2026-02-13  
**Purpose:** Final verification steps for repository approval and release.

---

## 🏗 Level 0: Platform Hygiene & Environment

- [ ] **Dependencies**: `pip install -r requirements.txt` runs without conflict.
- [ ] **Health Check**: Run `python doctor.py` - must return "ALL SYSTEMS GO! ✓".
- [ ] **GUI Launch**: Run `run_gui.bat` (or `python scraper_gui.py`) - GUI must open without crashing.
- [ ] **Config**: `config/platform.env` exists and contains valid paths.
- [ ] **Secrets**: `config/*.env.json` files have placeholders or valid keys (do NOT commit real keys if public repo).
- [ ] **Gitignore**: Verify `.env`, `__pycache__`, `output/`, `logs/` are ignored.

---

## 🧩 Level 1: Core Feature Integration (All Scrapers)

Check that *all* active scrapers implement these shared components:
- [ ] **Proxy Manager**: Uses `core.proxy_manager`.
- [ ] **Geo Router**: Uses `core.geo_router` (if applicable).
- [ ] **Schema Validation**: Uses shared DB schema from `db/`.
- [ ] **Metrics**: Prometheus metrics initialized (if applicable).
- [ ] **Resume Capability**: Scripts support resuming from last step/checkpoint.

---

## 🌍 Level 2: Per-Scraper Verification

### 🇦🇷 Argentina (AlfaBeta)
- [ ] **Zero Rows Alert**: Verify the "Zero Rows Processed" alert does not trigger falsely.
- [ ] **Company Search**: Verify "VITALIX" mapping in `ar_product_index`.
- [ ] **Pipeline**: Runs end-to-end with `run_pipeline.bat`.

### 🇧🇾 Belarus
- [ ] **Tor Connection**: Tor proxy works and connects to RCETH.
- [ ] **Validation**: `05_stats_and_validation.py` runs and produces report.
- [ ] **Schema**: `by_validation_results` table exists.

### 🇨🇦 Canada Ontario
- [ ] **CSV Handling**: `01_extract_product_details.py` correctly reads/writes CSVs.
- [ ] **Input/Output**: Tab functionalities implemented and working.
- [ ] **Pipeline Error**: "it is not still using csv" error resolved.

### 🇨🇦 Canada Quebec
- [ ] **PDF Extraction**: "Annexe V" PDF extracted with high accuracy.
- [ ] **AI Fallback**: Logic exists to use AI for ambiguous lines.
- [ ] **Data Cleaning**: Unit prices and strengths normalized.

### 🇨🇱 Chile (Tender)
- [ ] **Award URLs**: Correctly formatted and accessible.
- [ ] **Data Field**: "No award data" issue resolved.

### 🇮🇳 India
- [ ] **Schema Isolation**: Tables use `india_` prefix (e.g., `india_sku_main`).
- [ ] **Unique Index**: No duplicate `(hidden_id, run_id)` errors.
- [ ] **Pipeline**: Runs without crashing on shared table conflicts.

### 🇮🇹 Italy
- [ ] **Pagination**: Scrapes *all* records (not just first page/API limit).
- [ ] **Price Reductions**: "Riduzione di prezzo" data fully captured.
- [ ] **API Logic**: Handles sub-queries for details.

### 🇲🇾 Malaysia
- [ ] **Crash Recovery**: "Page crashed" errors trigger retry/reload, not pipeline failure.
- [ ] **Discovery**: URL discovery finds all products.

### 🇳🇱 Netherlands
- [ ] **Combinations**: `01_load_combinations_smart.py` generates granular search queries.
- [ ] **Large Data**: Handles >5000 results by splitting queries.
- [ ] **Stability**: Browser session reuse works without crashing.

### 🇲🇰 North Macedonia
- [ ] **Database**: Connection (`psycopg2`) is stable.
- [ ] **Translation**: `04_translate_using_dictionary.py` runs without "server closed connection".
- [ ] **Pipeline**: Refactored to use new DB infrastructure.

### 🇷🇺 Russia
- [ ] **Fresh Run**: `--fresh` flag clears previous state/data correctly.
- [ ] **Validation**: Validation info added to pipeline.

---

## 📊 Level 3: Code & Data Quality Standards

- [ ] **Logging**: All scripts use `logging` module (not just `print`).
- [ ] **Hardcoding**: No absolute paths in scripts (use `platform_config`).
- [ ] **Table Schemas**: All tables have primary keys and appropriate indexes.
- [ ] **Docstrings**: Main functions have basic docstrings explaining purpose.
- [ ] **Cleanliness**: No commented-out blocks of dead code > 10 lines.

---

## 🚀 Level 4: Final Release Steps

- [ ] **Backup**: Create a full backup of `data/` and `sql/` before final approval.
- [ ] **Documentation**: Update `README.md` with any new setup instructions.
- [ ] **Version**: update `package.json` or `setup.py` version to `1.0.0` (or target version).
- [ ] **Tag**: Git tag the release commit.
