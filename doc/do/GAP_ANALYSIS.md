# Argentina vs Malaysia - Gap Analysis (2026-01-30)

## Malaysia baseline (what it has)
- DB-first pipeline with `steps/` wrappers and resume checkpoints.
- Centralized repository layer for all DB operations.
- SQL-based PCID mapping + exporter.
- Export tracking + data-quality checks.
- Run ledger updates + input upload logging.

## Argentina current state (after DB migration)
- DB-first pipeline with run_id, `ar_` schema, repository, and DB-only steps.
- Selenium/API read/write DB; translation + export are DB-only.
- Input uploads (dictionary/PCID) logged into `input_uploads`.
- Count checks: product list and URL preparation must match.
- Final exports only (CSV).

## Remaining differences (optional)
- Step wrappers under `scripts/Argentina/steps/` (Malaysia style).
- Data-quality guard (Malaysia has `data_quality_check.py`).
- Export diff summary report (Malaysia writes diff for mapping).
- Sub-step progress tracking (`ar_step_progress`) is defined but not yet used.

## Recommended follow-ups
1) Add Argentina `steps/` wrappers mirroring Malaysia (cleaner resume control).
2) Add a DB-backed data-quality script (counts, completeness, coverage).
3) Implement optional export diff summary in `06_GenerateOutput.py`.
4) Use `ar_step_progress` for per-product resume if needed.
