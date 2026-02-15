# Region audit: Checkpoint, instance tracking, DB, resume, GUI (India, Malaysia, Argentina, Russia)

**Date:** 2025-02-03  
**Scope:** India, Malaysia, Argentina, Russia — no changes to scraping/business logic; E2E and tracking only.

---

## 1. Checkpoint (die/manager)

| Region     | Checkpoint (file) | Manager (GUI) | Notes |
|-----------|-------------------|---------------|--------|
| **India** | ✅ `output/India/.checkpoints/pipeline_checkpoint.json` | ✅ View/Clear/Manage | Uses `run_pipeline_scrapy.py`; checkpoint in `core.pipeline_checkpoint` |
| **Malaysia** | ✅ Same pattern | ✅ View/Clear/Manage | `run_pipeline_resume.py`; step 0–5 |
| **Argentina** | ✅ Same pattern | ✅ View/Clear/Manage | `run_pipeline_resume.py`; step 0–6; `ar_step_progress` in DB |
| **Russia** | ✅ Same pattern | ✅ View/Clear/Manage | `run_pipeline_resume.py`; step 0–5; **fixed:** metadata + `mark_as_completed` |

- **Stale recovery:** All four regions included in `recover_all_stale_checkpoints` and `recover_stale_pipelines` (Russia was missing → **fixed**).
- **Manager (rollback):** Unchecking a step in “Manage Checkpoint” unchecks all later steps; Apply clears checkpoint and re-marks only selected steps → next run starts from first uncompleted step. Backward step works.

---

## 2. Chrome / Tor instance tracking

| Region     | Chrome tracking | Tor/Firefox tracking | PID file |
|-----------|-----------------|----------------------|----------|
| **India** | N/A (Scrapy, no browser) | N/A | — |
| **Malaysia** | ✅ `core.chrome_pid_tracker` (Playwright); `save_chrome_pids` in base scraper | N/A | `.Malaysia_chrome_pids.json` |
| **Argentina** | N/A | ✅ `core.firefox_pid_tracker`; `save_firefox_pids` in worker | `.Argentina_firefox_pids.json` |
| **Russia** | ✅ `get_chrome_pids_from_driver`, `save_chrome_pids` in 01/02 scrapers | N/A | `.Russia_chrome_pids.json` |

- GUI “Kill browser” uses `terminate_scraper_pids` (Chrome + Firefox by scraper).
- No change to scraping logic; tracking already in place.

---

## 3. DB schema (run_ledger + region tables)

- **Common:** `run_ledger` (run_id, scraper_name, started_at, ended_at, status, step_count, items_scraped, …) in `core.db.models`.
- **India:** PostgresDB; `in_*` tables (e.g. `in_formulation_status`, `in_sku_main`); schema in `sql/schemas/postgres/india.sql`.
- **Malaysia:** CountryDB; `my_*` (my_products, my_product_details, my_consolidated_products, my_reimbursable_drugs, my_pcid_mappings, my_step_progress, …).
- **Argentina:** CountryDB; `ar_*` (ar_product_index, ar_products, ar_step_progress, ar_errors, …).
- **Russia:** CountryDB; `ru_*` (ru_ved_products, ru_excluded_products, ru_translated_products, ru_export_ready, ru_step_progress, ru_failed_pages).

**Optional DB tracking improvements (not implemented):**

- Add `run_ledger.current_step` / `current_step_name` for live UI (checkpoint metadata already has this).
- Log pipeline start/end in `run_ledger` for Russia (Argentina/Malaysia already use run_ledger + run_id).

---

## 4. Resume pipeline logic (no new instance on resume)

| Region     | Run ID source | Prevents new instance? |
|-----------|----------------|------------------------|
| **India** | DB (`in_formulation_status`) + checkpoint; `run_pipeline_scrapy.py` uses existing run when resumable | ✅ |
| **Malaysia** | `_ensure_resume_run_id(start_step)`: checkpoint → run_ledger → `.current_run_id`; sets `MALAYSIA_RUN_ID` | ✅ |
| **Argentina** | Same pattern; `ARGENTINA_RUN_ID`; `_mark_run_ledger_active_if_resume` | ✅ |
| **Russia** | No run_id in checkpoint/run_ledger for pipeline; steps use DB by run_id from backup/clean. | ✅ (single output dir; step 0 creates run_id) |

- **Backward step:** If user rolls back in Manage Checkpoint, next run starts from the first uncompleted step; output-file verification may force re-run of earlier steps if files are missing. No new run_id created when resuming.

---

## 5. Progress bar and execution summary

- **Console:** All four pipelines print `[PROGRESS] Pipeline Step: X/Y (pct%) - description` and `[TIMING] Step N completed in …`.
- **GUI:** Progress bar and label updated from log parsing (`[PROGRESS]`); DB Activity panel shows DB-related log lines; execution summary is in checkpoint timing + console.
- **Russia:** Now updates checkpoint metadata (`current_step`, `current_step_name`, `status`) before each step and calls `mark_as_completed()` at end so progress/status align with other regions.

---

## 6. DB activity, console, Input/Output, Config, Telegram

- **DB Activity:** Shown in Dashboard; filtered by selected scraper/market.
- **Console:** Execution log with colors; “Open in Cursor” etc.
- **Input page:** Input Management tab (CSV/input tables + PCID mapping).
- **Output page:** Output tab (DB table browser by scraper).
- **Configuration:** Config tab; env from config loader.
- **Telegram:** Telegram Bot section on Dashboard; start/stop and status.

No scraper/business logic changed; existing GUI behavior kept.

---

## 7. Health check (with DB)

- **Argentina:** ✅ Added DB check: `PostgreSQL (run_ledger)` via CountryDB("Argentina").
- **Malaysia:** ✅ Added DB check: `PostgreSQL (run_ledger)` via CountryDB("Malaysia").
- **Russia:** ✅ Added DB check: `PostgreSQL (run_ledger)` via CountryDB("Russia").
- **India:** ✅ New `scripts/India/health_check.py` with DB-only check (PostgresDB("India"), run_ledger).

All health checks now include a DB connectivity + run_ledger check as requested.

---

## 8. Pipeline page (E2E, no business logic changes)

- Pipeline tab shows steps per scraper; Run / Fresh Run / Stop use shared workflow runner and checkpoint.
- **Fixes applied (E2E/tracking only):**
  - Russia in default scraper list for checkpoint and run-ledger recovery.
  - Russia `run_pipeline_resume.py`: set checkpoint metadata before each step and call `mark_as_completed()` at end.
  - Health checks: DB check added for Argentina, Malaysia, Russia; India health_check.py added with DB check.

---

## 9. Files touched

- `core/pipeline_checkpoint.py` — add Russia to default scraper list for recovery.
- `shared_workflow_runner.py` — add Russia to default scraper lists for recovery and resumable pipelines.
- `scripts/Russia/run_pipeline_resume.py` — checkpoint metadata before step, `mark_as_completed()` on success.
- `scripts/Argentina/health_check.py` — DB connectivity check.
- `scripts/Malaysia/health_check.py` — DB connectivity check.
- `scripts/Russia/health_check.py` — DB connectivity check.
- `scripts/India/health_check.py` — new file; DB-only health check.

---

## 10. Summary

- Checkpoint and manager behavior are consistent across India, Malaysia, Argentina, Russia; Russia included in recovery and completion marking.
- Chrome (Malaysia, Russia) and Firefox/Tor (Argentina) tracking unchanged and correct.
- DB schemas and run_ledger usage are in place; optional extra columns left for later.
- Resume logic does not create a new instance when resuming; backward step in Manage Checkpoint works.
- Progress bar and execution summary come from logs and checkpoint; Russia aligned with others.
- Health checks for all four regions now include a DB (run_ledger) check.
- No scraping or business logic changes; only E2E, tracking, and health-check additions.
