# India Scraper Runbook

This is the single source of truth for the India pharma scraper (Scrapy-based).

## Pipeline at a glance
- Steps (run via `python scripts/India/run_pipeline_scrapy.py`):
  0. Backup & clean (`00_backup_and_clean.py`)
  1. Scrapy spider run (`run_scrapy_india.py`) - crawls CDSCO/formulations data
  2. QC & export (`05_qc_and_export.py`) - quality checks and final export
- Resume-safe: checkpoints stored by `core.pipeline_checkpoint`.
- Run ID persisted at `output/India/last_run_id.json`.

## Architecture
Unlike other regions that use Selenium, India uses the **Scrapy** framework:
- Spider: `scrapy_project/pharma/spiders/india_details.py`
- Pipeline runner: `scripts/India/run_scrapy_india.py`
- This provides better throughput for the large Indian drug database.

## Prereqs
- Env: `config/India.env.json` (loaded by `config_loader.py`).
- Input files:
  - `input/India/formulations_part1.csv` through `formulations_part5.csv`
- DB: PostgreSQL reachable per env; schema defined in `sql/schemas/postgres/india.sql`.

## How to run
```bash
cd scripts/India
python run_pipeline_scrapy.py --fresh   # full run
# or use the batch file
run_pipeline.bat
```

## Outputs
- DB tables: India-specific schema (see `sql/schemas/postgres/india.sql`).
- Files:
  - `output/India/details_combined.csv` - combined scrape results
  - `output/India/qc_report.json` - quality check report
  - `exports/India/` - final export files

## Health check
```bash
cd scripts/India
python health_check.py
```
Checks DB connectivity, disk space, and Scrapy availability.

## Speed tuning

To increase scraping speed, set these in `config/India.env` (or `India.env.json`). Start conservative and increase only if the NPPA site does not return 429/errors.

| Variable | Default | Effect | Suggested for speed |
|----------|---------|--------|----------------------|
| `INDIA_WORKERS` | 1 | Parallel Scrapy processes (each claims from the same queue) | 3–5 |
| `INDIA_CLAIM_BATCH` | 1 | Formulations claimed per batch per worker (fewer DB round-trips) | 5–10 |
| `INDIA_CONCURRENT_REQUESTS` | 2 | Concurrent HTTP requests per spider | 4–6 |
| `INDIA_DOWNLOAD_DELAY` | 1.0 | Seconds between requests (lower = faster, higher 429 risk) | 0.5 (if server allows) |
| `INDIA_AUTOTHROTTLE` | true | Scrapy auto-adjusts delay from server latency | `false` for fixed delay + higher throughput (more 429 risk) |

**CLI override:**  
`python run_pipeline_scrapy.py --fresh --workers 5` (overrides `INDIA_WORKERS` for that run).

**Caveats:** NPPA may throttle or block if you send too many requests. If you see 429 or frequent timeouts, reduce workers, concurrency, or increase download delay.

## Common issues
- Scrapy not installed: `pip install scrapy`
- Connection throttled: Scrapy respects DOWNLOAD_DELAY settings in spider.
- Stale checkpoint: use `--fresh` to clear checkpoints.
