# Netherlands FK Reimbursement Scraper - Integration Plan

## Overview
Integrate `new source.py` (Farmacotherapeutisch Kompas reimbursement scraper) into the Netherlands pipeline as Steps 2-5. Existing Steps 0-1 (backup + medicijnkosten.nl pricing) remain **untouched**.

**Data flow:**
```
Step 2: Playwright → Expand + Scroll → Collect ~4000 detail URLs → nl_fk_urls
Step 3: httpx AsyncClient + asyncio.Queue workers → Parse HTML → nl_fk_reimbursement (raw Dutch)
Step 4: Load nl_fk_dictionary → Dictionary match → Google Translate fallback → Batch save → Update nl_fk_reimbursement
Step 5: Read nl_fk_reimbursement → PCID lookup → Write CSV to exports/Netherlands/
```

---

## Files to Create (4 new scripts)

### 1. `scripts/Netherlands/02_fk_collect_urls.py` — FK URL Collection (Step 2)
- Uses **async Playwright** to open FK listing, expand collapsible sections, scroll, collect ~4000 detail URLs
- Tracks Chrome PIDs via `core.browser.chrome_pid_tracker` → saves to `nl_chrome_instances`
- Stores URLs in `nl_fk_urls` table (status=pending)
- **Resume**: If `nl_fk_urls` already has data for this run_id, skip collection
- Uses: `core.utils.logger`, `core.db.postgres_connection`, `core.pipeline.standalone_checkpoint`

### 2. `scripts/Netherlands/03_fk_scrape_reimbursement.py` — FK Detail Scraping (Step 3)
- Replaces `requests` + `ThreadPoolExecutor` with **`httpx.AsyncClient` + `asyncio.Queue`** worker pool
- Reads pending URLs from `nl_fk_urls`, fetches HTML, parses composition/indications/reimbursement
- Stores raw Dutch text in `nl_fk_reimbursement` (translation deferred to Step 4)
- Marks each URL as success/failed, increments retry_count on failure
- Retries failed URLs up to `FK_MAX_RETRIES` (default 3)
- **Resume**: Only processes URLs with status=pending or retryable failed
- Uses: `core.utils.logger`, `core.db.postgres_connection`, `core.pipeline.standalone_checkpoint`

### 3. `scripts/Netherlands/04_fk_translate.py` — FK Translation (Step 4)
- Seeds `nl_fk_dictionary` with ~90 hardcoded Dutch→English terms from `new source.py`
- Follows **universal translation pattern**: Dictionary first → Google Translate fallback → batch INSERT every 50 entries
- Splits joined indications on ` ; `, translates each bullet separately, re-joins
- Updates `nl_fk_reimbursement.indication_en` and `translation_status`
- **Resume**: Only processes rows with `translation_status='pending'`
- Uses: `core.utils.logger`, `core.translation.service.TranslationService`, `core.db.postgres_connection`

### 4. `scripts/Netherlands/05_fk_generate_export.py` — FK Export (Step 5)
- Reads translated data from `nl_fk_reimbursement`
- Applies PCID mapping via `core.data.pcid_mapping_contract`
- Writes CSV to `exports/Netherlands/fk_reimbursement_export.csv`
- Logs export to `nl_export_reports`
- **Resume**: Always re-generates (idempotent, fast)

---

## Files to Modify (5 existing files)

### 5. `scripts/Netherlands/db/schema.py` — Add 3 new table DDLs
- `nl_fk_urls` — URL tracking with status/retry_count/error_message
- `nl_fk_reimbursement` — Reimbursement rows with indication_nl/indication_en split
- `nl_fk_dictionary` — Dutch→English dictionary (grows over time)
- Append to existing `NETHERLANDS_SCHEMA_DDL` list

### 6. `scripts/Netherlands/db/repositories.py` — Add ~15 FK repository methods
- `insert_fk_urls()`, `get_pending_fk_urls()`, `get_retryable_fk_urls()`, `mark_fk_url_status()`, `get_fk_url_stats()`
- `insert_fk_reimbursement_batch()`, `get_untranslated_fk_rows()`, `update_fk_translations_batch()`, `get_all_fk_reimbursement_for_export()`
- `load_fk_dictionary()`, `seed_fk_dictionary()`, `upsert_fk_dictionary_batch()`

### 7. `scripts/Netherlands/run_pipeline_resume.py` — Add Steps 2-5
- Extend `PIPELINE_STEPS` and `STEP_DESCRIPTIONS`
- Update `_get_db_step_status()` to query FK tables
- Update `_is_step_complete()` with FK completion logic

### 8. `services/scraper_registry.py` — Add Steps 2-5 to Netherlands config
- Add 4 new step entries to the `"Netherlands"` steps list

### 9. `config/Netherlands.env.json` — Add FK config keys
- `FK_LISTING_URL`, `FK_SCRAPE_WORKERS`, `FK_BATCH_SIZE`, `FK_MAX_RETRIES`, `FK_SLEEP_BETWEEN`, `FK_PLAYWRIGHT_HEADLESS`, `FK_ENABLE_GOOGLE_TRANSLATE`

---

## DB Schema (3 new tables, all `nl_fk_` prefix)

### `nl_fk_urls`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | |
| run_id | TEXT FK→run_ledger | |
| url | TEXT NOT NULL | |
| generic_slug | TEXT | Extracted from URL path |
| status | TEXT DEFAULT 'pending' | pending/success/failed/skipped |
| error_message | TEXT | |
| retry_count | INTEGER DEFAULT 0 | |
| scraped_at | TIMESTAMP | |
| UNIQUE(run_id, url) | | Dedup constraint |

### `nl_fk_reimbursement`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | |
| run_id | TEXT FK→run_ledger | |
| fk_url_id | INTEGER FK→nl_fk_urls | |
| generic_name, brand_name, manufacturer | TEXT | |
| dosage_form, strength | TEXT | |
| patient_population | TEXT | ADULTS/ELDERLY/CHILDREN/INFANTS |
| indication_nl | TEXT | Original Dutch |
| indication_en | TEXT | Translated English (Step 4) |
| reimbursement_status | TEXT | REIMBURSED/NOT REIMBURSED/CONDITIONAL/OTC |
| reimbursable_text | TEXT | Human-readable |
| route_of_administration, pack_details | TEXT | |
| translation_status | TEXT DEFAULT 'pending' | pending/translated/no_dutch/failed |
| source_url | TEXT NOT NULL | |
| UNIQUE(run_id, source_url, brand_name, strength, patient_population) | | |

### `nl_fk_dictionary`
| Column | Type | Notes |
|--------|------|-------|
| id | SERIAL PK | |
| source_term | TEXT NOT NULL | Dutch term (normalized) |
| translated_term | TEXT NOT NULL | English translation |
| category | TEXT DEFAULT 'manual' | manual/google_auto/hardcoded_seed |
| UNIQUE(source_term, source_lang, target_lang) | | |

---

## Implementation Order
1. `db/schema.py` (tables first)
2. `db/repositories.py` (CRUD methods)
3. `config/Netherlands.env.json` (config keys)
4. `02_fk_collect_urls.py` (URL collection)
5. `03_fk_scrape_reimbursement.py` (detail scraping)
6. `04_fk_translate.py` (translation)
7. `05_fk_generate_export.py` (export)
8. `run_pipeline_resume.py` (pipeline wiring)
9. `services/scraper_registry.py` (GUI registration)

## Key Platform Rules Followed
- All logging via `core.utils.logger.get_logger()` — NO `print()`
- DB via `core.db.postgres_connection.get_db("Netherlands")` — NOT direct psycopg2
- Config via `config_loader` facade — NOT hardcoded
- Chrome PID tracking via `core.browser.chrome_pid_tracker`
- Step checkpoints via `core.pipeline.standalone_checkpoint.run_with_checkpoint()`
- Translation: dictionary-first, Google fallback, batch INSERT every 50
- httpx async + asyncio.Queue worker pool (NOT requests + ThreadPoolExecutor)
- All data stored in DB first, CSV export as final step
