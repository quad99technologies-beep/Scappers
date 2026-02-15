# Docs Feature Verification Report

## 1) Executive Summary

- Total claims found: 47
- Verified implemented (with evidence): 36
- Partially implemented (missing parts): 5
- Not implemented / doc-only: 5
- Risky / unclear (needs manual confirmation): 1

Recent markdown sources audited (git-recency filtered, then scoped to requested markets and platform docs):
- `doc/Malaysia/README.md`
- `doc/Argentina/README.md`
- `doc/Argentina/ARGENTINA_ROUND_ROBIN_RETRY.md`
- `doc/Argentina/PERFORMANCE_FIX.md`
- `doc/Netherlands/README.md`
- `doc/Tender_Chile/README.md`
- `doc/general/MEMORY_LEAK_FIXES_SUMMARY.md`
- `doc/general/REGION_AUDIT_CHECKPOINT_TRACKING.md`
- `doc/UPGRADE_SUMMARY.md`
- `doc/project/STANDARDIZATION_COMPLETE_SUMMARY.md`

Verification method used per claim:
- Confirmed executable code path (module/function/class)
- Confirmed reachability from pipeline entrypoint
- Added reproducible command + expected artifacts/log checks (tests are largely absent for these claims)

## 2) Market-wise Sections

### Malaysia

| Feature Claim | Source .md + line(s) | Verification Status | Evidence (file paths, functions/classes, tests, CLI entrypoints) | How to reproduce/run | Notes / Gaps |
|---|---|---|---|---|---|
| Pipeline has steps 0-5 run via `run_pipeline_resume.py` | `doc/Malaysia/README.md:6-13` | Implemented | Entrypoint step map in `scripts/Malaysia/run_pipeline_resume.py:602`; step scripts wired `scripts/Malaysia/run_pipeline_resume.py:603`..`scripts/Malaysia/run_pipeline_resume.py:608` | `python scripts/Malaysia/run_pipeline_resume.py --fresh`; verify step logs show 6-step plan and step script execution | Reachable and executable from CLI |
| Resume-safe checkpoint via `core.pipeline_checkpoint` | `doc/Malaysia/README.md:13` | Implemented | Checkpoint manager import/use: `scripts/Malaysia/run_pipeline_resume.py:29`, metadata writes `scripts/Malaysia/run_pipeline_resume.py:220`, checkpoint implementation `core/pipeline_checkpoint.py:99`, `core/pipeline_checkpoint.py:132` | Run once, stop mid-run, rerun without `--fresh`; verify resume step from checkpoint | No automated tests found |
| Run ID persisted to `.current_run_id` | `doc/Malaysia/README.md:14` | Implemented | Write on step0 `scripts/Malaysia/steps/step_00_backup_clean.py:111`; read/reuse in runner `scripts/Malaysia/run_pipeline_resume.py:124`, resume lock `scripts/Malaysia/run_pipeline_resume.py:165` | Run step0, confirm `output/Malaysia/.current_run_id` exists and is reused on resume | Works with env fallback |
| DB schema auto-migrates on step 0/5 | `doc/Malaysia/README.md:21` | Partial | Step0 runs schema apply `scripts/Malaysia/steps/step_00_backup_clean.py:95`..`scripts/Malaysia/steps/step_00_backup_clean.py:99`; step5 does defensive `ALTER TABLE IF NOT EXISTS` in repo `scripts/Malaysia/db/repositories.py:556` | `python scripts/Malaysia/steps/step_00_backup_clean.py`; optionally run step5 and inspect DB schema | Full schema migration is primarily step0; step5 only patches selected columns |
| Quest3 under-report fix (`page_rows` forced to CSV row count) | `doc/Malaysia/README.md:32` | Implemented | Logic in scraper `scripts/Malaysia/scrapers/quest3_scraper.py:418`..`scripts/Malaysia/scrapers/quest3_scraper.py:420`; run path via step2 `scripts/Malaysia/run_pipeline_resume.py:605` | `python scripts/Malaysia/run_pipeline_resume.py --step 2`; inspect bulk count output/logs for mismatches resolving | No dedicated test; behavior observed via code path |
| PCID join normalizes non-alphanumerics and supports `PCID Mapping` header | `doc/Malaysia/README.md:33` | Implemented | Header fallback `scripts/Malaysia/db/repositories.py:533`..`scripts/Malaysia/db/repositories.py:534`; normalized join key `scripts/Malaysia/db/repositories.py:547`, `scripts/Malaysia/db/repositories.py:554`; join call `scripts/Malaysia/db/repositories.py:569`..`scripts/Malaysia/db/repositories.py:570` | `python scripts/Malaysia/run_pipeline_resume.py --step 5`; inspect mapped/unmapped outputs and DB rows | Implementation is DB-side SQL normalization |
| Export audit tracked in `my_export_reports` | `doc/Malaysia/README.md:34` | Implemented | Exporter writes audit rows `scripts/Malaysia/exports/csv_exporter.py:48`, `scripts/Malaysia/exports/csv_exporter.py:61`, `scripts/Malaysia/exports/csv_exporter.py:84`, `scripts/Malaysia/exports/csv_exporter.py:97`; repo sink `scripts/Malaysia/db/repositories.py:824` | `python scripts/Malaysia/run_pipeline_resume.py --step 5`; query `my_export_reports` for current `run_id` | No tests; reproducible through export step |
| Health check validates URLs and selector config including info selector | `doc/Malaysia/README.md:47` | Implemented | Health checks include URL + selector checks in `scripts/Malaysia/health_check.py:166` onward; info selector handling in `scripts/Malaysia/health_check.py:225`..`scripts/Malaysia/health_check.py:239` | `python scripts/Malaysia/health_check.py`; verify PASS/FAIL matrix and saved reports | Also includes DB check now (beyond README text) |
| `--fresh` clears checkpoint and removes stale run_id file | `doc/Malaysia/README.md:52` | Implemented | Fresh handling in `scripts/Malaysia/run_pipeline_resume.py:574`..`scripts/Malaysia/run_pipeline_resume.py:584` | `python scripts/Malaysia/run_pipeline_resume.py --fresh`; verify checkpoint reset and `.current_run_id` recreation from step0 | Behavior matches claim |

### Argentina

| Feature Claim | Source .md + line(s) | Verification Status | Evidence (file paths, functions/classes, tests, CLI entrypoints) | How to reproduce/run | Notes / Gaps |
|---|---|---|---|---|---|
| Pipeline is PostgreSQL-first and does not use CSV tracking during run | `doc/Argentina/README.md:5` | Partial | DB-first pipeline is wired in `scripts/Argentina/run_pipeline_resume.py:878`; however step8 reads exported `*_pcid_no_data.csv` (`scripts/Argentina/07_scrape_no_data_pipeline.py:1`, `scripts/Argentina/07_scrape_no_data_pipeline.py:85`) | `python scripts/Argentina/run_pipeline_resume.py --fresh`; then inspect step8 reading no-data CSV | Core flow is DB-first, but step8 consumes CSV export for retry set |
| End-to-end flow has 10 steps | `doc/Argentina/README.md:7-21` | Implemented | Step list in entrypoint `scripts/Argentina/run_pipeline_resume.py:878`..`scripts/Argentina/run_pipeline_resume.py:887` | `python scripts/Argentina/run_pipeline_resume.py --fresh`; verify execution plan shows 10 steps | Reachable via CLI |
| Step4 company-search script is part of pipeline | `doc/Argentina/README.md:15` | Implemented | Wired at `scripts/Argentina/run_pipeline_resume.py:883`; target script exists `scripts/Argentina/03b_alfabeta_selenium_company_search.py:7` | `python scripts/Argentina/run_pipeline_resume.py --step 4`; verify company-search log output | Reachable from entrypoint |
| Company-search strategy uses laboratorios/company path and exact product match | `doc/Argentina/README.md:24-35` | Implemented | Strategy explicitly coded in `scripts/Argentina/03b_alfabeta_selenium_company_search.py:10`..`scripts/Argentina/03b_alfabeta_selenium_company_search.py:12`, and functions around `scripts/Argentina/03b_alfabeta_selenium_company_search.py:215`, `scripts/Argentina/03b_alfabeta_selenium_company_search.py:529`, `scripts/Argentina/03b_alfabeta_selenium_company_search.py:984` | `python scripts/Argentina/run_pipeline_resume.py --step 4`; inspect logs for company match and product click | Behavior is code-backed |
| Scrape source tracking columns (`scrape_source`, `source`) are used | `doc/Argentina/README.md:37-42` | Implemented | Schema columns `scripts/Argentina/db/schema.py:30`; updates in company step `scripts/Argentina/03b_alfabeta_selenium_company_search.py:881`; step7 marks `scripts/Argentina/07_scrape_no_data_pipeline.py:231`; repository writes `scripts/Argentina/db/repositories.py:322` | Run steps 3/4/5/8 and query `ar_product_index.scrape_source` and `ar_products.source` | Reachable through normal pipeline |
| Stats step reports source-wise scraping breakdown | `doc/Argentina/README.md:43` | Implemented | Stats queries include source breakdown in `scripts/Argentina/08_stats_and_validation.py:224`..`scripts/Argentina/08_stats_and_validation.py:227`, printed section `scripts/Argentina/08_stats_and_validation.py:451`..`scripts/Argentina/08_stats_and_validation.py:468` | `python scripts/Argentina/run_pipeline_resume.py --step 9`; verify printed source breakdown | No dedicated tests |
| Output CSV set (`pcid_mapping/missing/oos/no_data`) | `doc/Argentina/README.md:51-57` | Implemented | Output naming + writes in `scripts/Argentina/06_GenerateOutput.py:426`..`scripts/Argentina/06_GenerateOutput.py:428`; report logging `scripts/Argentina/06_GenerateOutput.py:460`..`scripts/Argentina/06_GenerateOutput.py:462` | `python scripts/Argentina/run_pipeline_resume.py --step 7`; check files under `output/Argentina/exports/` | Claim matches implementation |
| Shared `pcid_mapping` table is used as PCID source in output step | `doc/Argentina/README.md:71-76` | Implemented | `PCIDMapping("Argentina")` in `scripts/Argentina/06_GenerateOutput.py:320`; data read from shared table by contract | `python scripts/Argentina/run_pipeline_resume.py --step 7`; check logs for PCID rows loaded | Implemented in step6/export stage |
| Step0 from file replaces Argentina rows in shared `pcid_mapping` | `doc/Argentina/README.md:77` | Doc-only | Step0 explicitly states reference data is not loaded from CSV `scripts/Argentina/00_backup_and_clean.py:12`, repeated at `scripts/Argentina/00_backup_and_clean.py:186`; seed functions exist but are not called | N/A | Claim contradicts current code wiring |
| Screenshot is captured before API fallback and logged in `ar_artifacts` | `doc/Argentina/README.md:78` | Implemented | Screenshot + DB artifact log `scripts/Argentina/03_alfabeta_selenium_worker.py:1191`..`scripts/Argentina/03_alfabeta_selenium_worker.py:1203`; artifact dir setup `scripts/Argentina/03_alfabeta_selenium_worker.py:967`..`scripts/Argentina/03_alfabeta_selenium_worker.py:971`; repository insert `scripts/Argentina/db/repositories.py:708` | Run step3 with failing products; verify PNGs in `output/Argentina/artifacts` and rows in `ar_artifacts` | Reachable in worker API fallback path |
| Step8 no-data retry runs AUTO x2 | `doc/Argentina/README.md:19`, `doc/Argentina/README.md:19` | Implemented | Default rounds in `scripts/Argentina/07_scrape_no_data_pipeline.py:61`; loop over rounds `scripts/Argentina/07_scrape_no_data_pipeline.py:314`; wired in pipeline `scripts/Argentina/run_pipeline_resume.py:887` | `python scripts/Argentina/run_pipeline_resume.py --step 8`; observe round logs `Round 1/2`, `Round 2/2` | Env override `NO_DATA_MAX_ROUNDS` supported |
| Step8 reruns translation and export after retries | `doc/Argentina/README.md:19` | Implemented | Final reruns in `scripts/Argentina/07_scrape_no_data_pipeline.py:342`..`scripts/Argentina/07_scrape_no_data_pipeline.py:345` | Run step8 and verify it invokes translation/export scripts at end | Matches claim intent |
| Round-robin retry mode and max attempts per product supported | `doc/Argentina/ARGENTINA_ROUND_ROBIN_RETRY.md:5`, `doc/Argentina/ARGENTINA_ROUND_ROBIN_RETRY.md:30-34` | Implemented | Config flags `scripts/Argentina/config_loader.py:386`..`scripts/Argentina/config_loader.py:387`; attempt tracking/requeue/max-attempt logic `scripts/Argentina/03_alfabeta_selenium_worker.py:418`..`scripts/Argentina/03_alfabeta_selenium_worker.py:525` | `SELENIUM_ROUND_ROBIN_RETRY=true SELENIUM_MAX_ATTEMPTS_PER_PRODUCT=5 python scripts/Argentina/03_alfabeta_selenium_scraper.py`; inspect `[ROUND_ROBIN]` logs | Reachable in Selenium worker path |
| Performance fixes from `PERFORMANCE_FIX.md` are fully applied | `doc/Argentina/PERFORMANCE_FIX.md:38-92` | Partial | Multiple fixes exist: join timeouts `scripts/Argentina/03_alfabeta_selenium_worker.py:1993`, `scripts/Argentina/03_alfabeta_selenium_worker.py:2287`; GC/memory checks `scripts/Argentina/03_alfabeta_selenium_worker.py:3294`..`scripts/Argentina/03_alfabeta_selenium_worker.py:3318`; unregister/profile cleanup `scripts/Argentina/03_alfabeta_selenium_worker.py:4819`, `scripts/Argentina/03_alfabeta_selenium_worker.py:4860` | `python scripts/Argentina/03_alfabeta_selenium_scraper.py`; monitor resource logs and long-run behavior | Doc says complete fix; code has many mitigations, but no benchmark/test proving issue fully resolved |

### Netherlands

| Feature Claim | Source .md + line(s) | Verification Status | Evidence (file paths, functions/classes, tests, CLI entrypoints) | How to reproduce/run | Notes / Gaps |
|---|---|---|---|---|---|
| Pipeline steps are run via `run_pipeline_resume.py` with step0/1/2/3/5 | `doc/Netherlands/README.md:6-12` | Implemented | Entrypoint step map `scripts/Netherlands/run_pipeline_resume.py:676`..`scripts/Netherlands/run_pipeline_resume.py:689` | `python scripts/Netherlands/run_pipeline_resume.py --fresh`; verify execution plan | Step2 is disabled by default unless env enables |
| Step1 script is `01_collect_urls.py` | `doc/Netherlands/README.md:8` | Doc-only | Actual wired script is `scripts/Netherlands/run_pipeline_resume.py:678` -> `01_get_medicijnkosten_data.py` | N/A | Documentation mismatch |
| Architecture uses Playwright (not Selenium) | `doc/Netherlands/README.md:15` | Doc-only | Step1 is Selenium-heavy (`scripts/Netherlands/01_get_medicijnkosten_data.py:41`..`scripts/Netherlands/01_get_medicijnkosten_data.py:47`); Step2 uses Playwright (`scripts/Netherlands/02_reimbursement_extraction.py:61`) | `python scripts/Netherlands/run_pipeline_resume.py --step 1` and inspect Selenium startup | Actual architecture is mixed Selenium + Playwright |
| Anti-detection via stealth profile and human actions | `doc/Netherlands/README.md:16` | Implemented | Step1 stealth/human actions `scripts/Netherlands/01_get_medicijnkosten_data.py:76`, `scripts/Netherlands/01_get_medicijnkosten_data.py:96`, `scripts/Netherlands/01_get_medicijnkosten_data.py:398`; Step2 stealth/human `scripts/Netherlands/02_reimbursement_extraction.py:75`, `scripts/Netherlands/02_reimbursement_extraction.py:95`, `scripts/Netherlands/02_reimbursement_extraction.py:616` | Run step1/step2 and confirm logs for driver/context setup and page pacing | Claim is supported |
| Smart locator + state machine are used | `doc/Netherlands/README.md:17` | Implemented | Imports/use in step1 `scripts/Netherlands/01_get_medicijnkosten_data.py:51`, `scripts/Netherlands/01_get_medicijnkosten_data.py:52`, `scripts/Netherlands/01_get_medicijnkosten_data.py:800`; step2 `scripts/Netherlands/02_reimbursement_extraction.py:70`, `scripts/Netherlands/02_reimbursement_extraction.py:71`, `scripts/Netherlands/02_reimbursement_extraction.py:649` | Run step1/step2 and inspect navigation logs/state transitions | Reachable from entrypoint |
| Schema auto-migrates on step0 | `doc/Netherlands/README.md:24` | Implemented | Step0 applies schema `scripts/Netherlands/00_backup_and_clean.py:37`, `scripts/Netherlands/00_backup_and_clean.py:79`; run pipeline includes step0 | `python scripts/Netherlands/run_pipeline_resume.py --fresh` | Claim is supported |
| Health check includes DB connectivity | `doc/Netherlands/README.md:45` | Doc-only | Current checks list in `scripts/Netherlands/health_check.py:206`..`scripts/Netherlands/health_check.py:210` includes URL/layout/file checks, not DB/run_ledger query | `python scripts/Netherlands/health_check.py` | DB connectivity is not currently checked |
| Exports are generated under `exports/Netherlands` | `doc/Netherlands/README.md:38` | Implemented | Pipeline step5 calls `05_Generate_PCID_Mapped.py` via `scripts/Netherlands/run_pipeline_resume.py:689`; output path handled in market exporter flow | `python scripts/Netherlands/run_pipeline_resume.py --step 5`; inspect export files | Supported by step wiring |

### Chile (Tender_Chile)

| Feature Claim | Source .md + line(s) | Verification Status | Evidence (file paths, functions/classes, tests, CLI entrypoints) | How to reproduce/run | Notes / Gaps |
|---|---|---|---|---|---|
| 5-step pipeline (`0..4`) with resume/fresh/step options | `doc/Tender_Chile/README.md:13-70`, `doc/Tender_Chile/README.md:127-138` | Implemented | Step map in entrypoint `scripts/Tender- Chile/run_pipeline_resume.py:637`..`scripts/Tender- Chile/run_pipeline_resume.py:642`; CLI args `scripts/Tender- Chile/run_pipeline_resume.py:467`..`scripts/Tender- Chile/run_pipeline_resume.py:468` | `python "scripts/Tender- Chile/run_pipeline_resume.py" --fresh` or `--step 2` | Reachable via CLI |
| Step outputs: redirect/details/awards/final CSV filenames | `doc/Tender_Chile/README.md:24`, `doc/Tender_Chile/README.md:31`, `doc/Tender_Chile/README.md:49-50`, `doc/Tender_Chile/README.md:70` | Implemented | Output filenames in scripts: `scripts/Tender- Chile/01_get_redirect_urls.py:68`, `scripts/Tender- Chile/02_extract_tender_details.py:120`, `scripts/Tender- Chile/03_extract_tender_awards.py:69`..`scripts/Tender- Chile/03_extract_tender_awards.py:70`, `scripts/Tender- Chile/04_merge_final_csv.py:65` | Run pipeline; verify files in `output/Tender_Chile/` | Matches documented files |
| Config keys `MAX_TENDERS`, `HEADLESS`, `WAIT_SECONDS` control scraping | `doc/Tender_Chile/README.md:95-100` | Implemented | Used in step1 `scripts/Tender- Chile/01_get_redirect_urls.py:60`..`scripts/Tender- Chile/01_get_redirect_urls.py:62`; step2 `scripts/Tender- Chile/02_extract_tender_details.py:123`..`scripts/Tender- Chile/02_extract_tender_details.py:124`; step3 `scripts/Tender- Chile/03_extract_tender_awards.py:73`..`scripts/Tender- Chile/03_extract_tender_awards.py:74` | `MAX_TENDERS=5 HEADLESS=false WAIT_SECONDS=30 python "scripts/Tender- Chile/run_pipeline_resume.py" --step 1` | Claim is supported |
| Optional OpenAI translation when API key configured | `doc/Tender_Chile/README.md:100`, `doc/Tender_Chile/README.md:228` | Doc-only | No translation/OpenAI usage found in market code; only dependency reference `scripts/Tender- Chile/requirements.txt:6` | N/A | Documentation overstates current implementation |
| Lock cleanup script exists and is usable | `doc/Tender_Chile/README.md:209-214` | Implemented | Script exists and is invoked post-run in runner `scripts/Tender- Chile/run_pipeline_resume.py:790` | `python "scripts/Tender- Chile/cleanup_lock.py"` | Claim is supported |
| Checkpoint system saves progress per step, `--fresh` resets | `doc/Tender_Chile/README.md:230` | Implemented | Checkpoint manager use `scripts/Tender- Chile/run_pipeline_resume.py:30`; mark complete per step `scripts/Tender- Chile/run_pipeline_resume.py:320`; clear on fresh `scripts/Tender- Chile/run_pipeline_resume.py:560`; final mark `scripts/Tender- Chile/run_pipeline_resume.py:724` | Interrupt and rerun without `--fresh`; then run with `--fresh` to reset | Reachable and implemented |
| Includes request pacing/rate limiting delays | `doc/Tender_Chile/README.md:229` | Implemented | Time-based waits in step scripts, e.g. `scripts/Tender- Chile/01_get_redirect_urls.py:328`, `scripts/Tender- Chile/02_extract_tender_details.py:585`, `scripts/Tender- Chile/03_extract_tender_awards.py:614` | Run steps and inspect logs/timing between requests | Broad claim; exact per-endpoint policy not centralized |
| GUI + CLI usage paths are supported | `doc/Tender_Chile/README.md:120-145` | Implemented | CLI runner is executable (`scripts/Tender- Chile/run_pipeline_resume.py`); GUI-run compatibility through shared workflow + step logging hooks (`scripts/Tender- Chile/run_pipeline_resume.py:51`) | CLI: `python "scripts/Tender- Chile/run_pipeline_resume.py"`; GUI: run scraper from app Pipeline tab | No explicit automated E2E test found |

## 3) Cross-cutting Platform Features

| Feature Claim | Source .md + line(s) | Verification Status | Evidence (file paths, functions/classes, tests, CLI entrypoints) | How to reproduce/run | Notes / Gaps |
|---|---|---|---|---|---|
| Stale pipeline recovery is wired for core regions | `doc/general/REGION_AUDIT_CHECKPOINT_TRACKING.md:17`, `doc/general/REGION_AUDIT_CHECKPOINT_TRACKING.md:99-103` | Implemented | Recovery functions include target lists in `shared_workflow_runner.py:1201`, `shared_workflow_runner.py:1231`; checkpoint stale recovery defaults in `core/pipeline_checkpoint.py:423`, `core/pipeline_checkpoint.py:436`; market runners call recovery (`scripts/Malaysia/run_pipeline_resume.py:494`, `scripts/Argentina/run_pipeline_resume.py:742`, `scripts/Tender- Chile/run_pipeline_resume.py:488`) | Start a pipeline, force stop/crash, rerun and observe recovery logs | Implemented for audited markets |
| Health checks include DB run_ledger checks (Malaysia/Argentina) | `doc/general/REGION_AUDIT_CHECKPOINT_TRACKING.md:87-93` | Implemented | Malaysia DB check `scripts/Malaysia/health_check.py:151`..`scripts/Malaysia/health_check.py:159`; Argentina DB check `scripts/Argentina/health_check.py:88`..`scripts/Argentina/health_check.py:96` | `python scripts/Malaysia/health_check.py`; `python scripts/Argentina/health_check.py` | True for Malaysia/Argentina; not true for Netherlands |
| GUI long-run log memory fix (caps + truncation) is implemented | `doc/general/MEMORY_LEAK_FIXES_SUMMARY.md:192-196` | Implemented | Log caps in `scraper_gui.py:256`..`scraper_gui.py:257`; truncation helpers `scraper_gui.py:5229`, `scraper_gui.py:5238`; periodic refresh uses truncated content `scraper_gui.py:5267`, `scraper_gui.py:5279` | Run long pipeline in GUI and monitor memory/UI responsiveness | No benchmark test in repo |
| DB connection cleanup in GUI startup/stop paths is implemented | `doc/general/MEMORY_LEAK_FIXES_SUMMARY.md:197-200` | Implemented | GUI startup close in finally `scraper_gui.py:148`..`scraper_gui.py:151`; workflow runner stop path closes DB in finally `shared_workflow_runner.py:562`..`shared_workflow_runner.py:565`, `shared_workflow_runner.py:694`..`shared_workflow_runner.py:697` | Trigger startup recovery and stop actions, observe no leaked DB handles | Code-level verification only |
| Migrations 005/006/007 are deployed | `doc/UPGRADE_SUMMARY.md:8-12`, `doc/UPGRADE_SUMMARY.md:35-36`, `doc/UPGRADE_SUMMARY.md:52-53` | Unclear | Migration SQL exists (`sql/migrations/postgres/005_add_step_tracking_columns.sql`, `sql/migrations/postgres/006_add_chrome_instances_table.sql`, `sql/migrations/postgres/007_add_run_ledger_live_fields.sql`) and deploy/verify scripts exist (`scripts/deploy_all_migrations.py:75`..`scripts/deploy_all_migrations.py:77`, `scripts/verify_migrations.py:25`) | Run migration deployment + verification against target DB | Repo proves scripts exist, not that production DBs were actually migrated |
| Enhanced step metrics logging is integrated | `doc/UPGRADE_SUMMARY.md:14-30`, `doc/UPGRADE_SUMMARY.md:207-221` | Implemented | Shared logger supports metric fields `core/step_progress_logger.py:25`..`core/step_progress_logger.py:32`; upsert SQL includes enhanced columns `core/step_progress_logger.py:110`..`core/step_progress_logger.py:128`; run-level aggregation `core/step_progress_logger.py:191` | Run any market pipeline and inspect `{prefix}_step_progress` + `run_ledger` fields | Requires migrated schema to persist all columns |
| Live dashboard via `run_ledger.current_step/current_step_name` | `doc/UPGRADE_SUMMARY.md:55-60` | Partial | Columns are added by migration `sql/migrations/postgres/007_add_run_ledger_live_fields.sql:8`..`sql/migrations/postgres/007_add_run_ledger_live_fields.sql:9`; market runners mostly update checkpoint metadata (e.g. `scripts/Malaysia/run_pipeline_resume.py:220`) and `step_count`, not run_ledger current step fields | Query `run_ledger.current_step/current_step_name` during active run | Column exists, but active updating path is not clearly wired in audited market runners |
| Browser PID cleanup added to all regions | `doc/project/STANDARDIZATION_COMPLETE_SUMMARY.md:21-24` | Partial | Implemented in Malaysia/Argentina/Tender_Chile runners (`scripts/Malaysia/run_pipeline_resume.py:645`, `scripts/Argentina/run_pipeline_resume.py:926`, `scripts/Tender- Chile/run_pipeline_resume.py:631`, plus post-run cleanup `scripts/Tender- Chile/run_pipeline_resume.py:784`) | Run pipelines and confirm pre/post cleanup logs | Not consistently present in Netherlands runner path |

## 4) Action Items

Concrete code tasks to make doc claims true:

1. Netherlands docs and implementation alignment.
- Update `doc/Netherlands/README.md` to reflect real step1 script (`01_get_medicijnkosten_data.py`) and mixed Selenium+Playwright architecture.
- Either add DB check to `scripts/Netherlands/health_check.py` (run_ledger connectivity) or remove DB-check claim from docs.
- Document that step2 is disabled by default unless `NL_ENABLE_STEP2=true`.

2. Argentina README PCID step0 claim fix.
- Either wire CSV seeding into `scripts/Argentina/00_backup_and_clean.py` (call `seed_pcid_reference`) or remove `Step 0 from file replaces pcid_mapping` from docs.
- Keep one source of truth statement accurate across docs.

3. Chile translation claim correction.
- If translation is intended: implement a concrete translation module and wire it into step4 or a dedicated step.
- If not intended: remove OpenAI translation claims from `doc/Tender_Chile/README.md`.

4. Run-ledger live fields wiring.
- Add explicit updates to `run_ledger.current_step` and `run_ledger.current_step_name` in shared step progress utilities or per-market runners.
- Keep backward compatibility with checkpoint metadata.

5. Evidence-grade migration verification.
- Add a CI/ops check to run `scripts/verify_migrations.py` against target DB and publish artifact output.
- Update `doc/UPGRADE_SUMMARY.md` "deployed" wording to "migration scripts present" unless DB verification artifact exists.

Suggested test cases to prove each feature:

1. Resume and checkpoint E2E (all 4 markets).
- Start pipeline, stop after step N, restart without `--fresh`, assert next executed step == N+1 and same run_id reused.

2. Malaysia Quest3 count reconciliation test.
- Mock/fixture where page count under-reports and CSV has full rows; assert `page_rows` is clamped to CSV count and mismatch diff is zero.

3. Argentina no-data retry round count test.
- Seed `*_pcid_no_data.csv`, run step8 with `NO_DATA_MAX_ROUNDS=2`, assert exactly two rounds max and translation/export rerun.

4. Argentina step0 PCID source-of-truth test.
- If CSV seeding enabled, assert step0 replaces only `source_country='Argentina'` rows in `pcid_mapping`; if not, assert docs do not claim it.

5. Netherlands health check DB test.
- Add and test `CountryDB("Netherlands")` + `SELECT 1 FROM run_ledger ...` check; assert health matrix includes pass/fail row.

6. Chile translation behavior test (if implemented).
- With/without API key, assert deterministic translated fields or explicit no-translation behavior and log message.

7. Cross-cutting GUI long-run stability test.
- Simulate sustained log stream >2MB per scraper; assert `MAX_LOG_CHARS`/display truncation enforcement and no GUI freeze.

8. Run-ledger live-step update test.
- During active run, poll `run_ledger.current_step/current_step_name`; assert values advance per step and clear/finalize correctly.
