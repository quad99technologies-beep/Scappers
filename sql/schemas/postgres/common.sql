-- Common schema for PostgreSQL (shared across all countries)
-- All statements are idempotent (IF NOT EXISTS).

-- Run ledger: one row per pipeline execution
CREATE TABLE IF NOT EXISTS run_ledger (
    run_id TEXT PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK(status IN ('running', 'completed', 'failed', 'cancelled', 'partial', 'resume', 'stopped')),
    step_count INTEGER DEFAULT 0,
    items_scraped INTEGER DEFAULT 0,
    items_exported INTEGER DEFAULT 0,
    error_message TEXT,
    git_commit TEXT,
    config_hash TEXT,
    metadata_json TEXT,
    mode TEXT DEFAULT 'fresh',
    thread_count INTEGER,
    totals_json TEXT
);

-- HTTP request log
CREATE TABLE IF NOT EXISTS http_requests (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    url TEXT NOT NULL,
    method TEXT DEFAULT 'GET',
    status_code INTEGER,
    response_bytes INTEGER,
    elapsed_ms REAL,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_req_run ON http_requests(run_id);
CREATE INDEX IF NOT EXISTS idx_req_url ON http_requests(url);

-- Generic scraped items (countries add specific tables)
CREATE TABLE IF NOT EXISTS scraped_items (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    source_url TEXT,
    item_json TEXT NOT NULL,
    item_hash TEXT,
    parse_status TEXT DEFAULT 'ok'
        CHECK(parse_status IN ('ok', 'partial', 'error')),
    error_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_items_run ON scraped_items(run_id);
CREATE INDEX IF NOT EXISTS idx_items_hash ON scraped_items(item_hash);

-- Schema version tracking
CREATE TABLE IF NOT EXISTS _schema_versions (
    version INTEGER PRIMARY KEY,
    filename TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Input uploads tracking (shared)
CREATE TABLE IF NOT EXISTS input_uploads (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    replaced_previous INTEGER DEFAULT 0,
    uploaded_by TEXT DEFAULT 'gui',
    source_country TEXT
);
