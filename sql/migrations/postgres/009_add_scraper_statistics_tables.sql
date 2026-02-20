-- Migration 009: Add shared scraper statistics tables
-- Creates:
--   - scraper_run_statistics  (one JSON summary per run)
--   - scraper_step_statistics (one JSON snapshot per step)

CREATE TABLE IF NOT EXISTS scraper_run_statistics (
    id SERIAL PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    stats_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scraper_name, run_id)
);

CREATE INDEX IF NOT EXISTS idx_srs_scraper ON scraper_run_statistics(scraper_name);
CREATE INDEX IF NOT EXISTS idx_srs_run ON scraper_run_statistics(run_id);

CREATE TABLE IF NOT EXISTS scraper_step_statistics (
    id SERIAL PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    step_number INTEGER NOT NULL,
    step_name TEXT,
    status TEXT,
    error_message TEXT,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scraper_name, run_id, step_number)
);

CREATE INDEX IF NOT EXISTS idx_sss_scraper ON scraper_step_statistics(scraper_name);
CREATE INDEX IF NOT EXISTS idx_sss_run ON scraper_step_statistics(run_id);
CREATE INDEX IF NOT EXISTS idx_sss_step ON scraper_step_statistics(scraper_name, step_number);

INSERT INTO _schema_versions (version, filename)
VALUES (9, '009_add_scraper_statistics_tables.sql')
ON CONFLICT (version) DO NOTHING;

