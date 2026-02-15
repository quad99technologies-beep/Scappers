-- Russia scraper: PostgreSQL schema with ru_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- VED Products from farmcom.info (Step 1)
CREATE TABLE IF NOT EXISTS ru_ved_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn TEXT,
    inn TEXT,
    manufacturer_country TEXT,
    release_form TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    page_number INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_ved_item ON ru_ved_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_run ON ru_ved_products(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_page ON ru_ved_products(page_number);

-- Excluded products (Step 2)
CREATE TABLE IF NOT EXISTS ru_excluded_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn TEXT,
    inn TEXT,
    manufacturer_country TEXT,
    release_form TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    page_number INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_excl_item ON ru_excluded_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_excl_run ON ru_excluded_products(run_id);

-- Translated/processed products (Step 3)
CREATE TABLE IF NOT EXISTS ru_translated_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn_ru TEXT,
    tn_en TEXT,
    inn_ru TEXT,
    inn_en TEXT,
    manufacturer_country_ru TEXT,
    manufacturer_country_en TEXT,
    release_form_ru TEXT,
    release_form_en TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    start_date_iso DATE,
    translation_method TEXT CHECK(translation_method IN ('dictionary', 'ai', 'none')),
    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_trans_item ON ru_translated_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_trans_run ON ru_translated_products(run_id);

-- Export-ready formatted data (Step 4)
CREATE TABLE IF NOT EXISTS ru_export_ready (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    trade_name_en TEXT,
    inn_en TEXT,
    manufacturer_country_en TEXT,
    dosage_form_en TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_iso DATE,
    source_type TEXT CHECK(source_type IN ('ved', 'excluded')),
    formatted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_export_run ON ru_export_ready(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_export_type ON ru_export_ready(source_type);

-- Step progress tracking (sub-step resume)
CREATE TABLE IF NOT EXISTS ru_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped', 'ean_missing')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    log_details TEXT,
    url TEXT,
    rows_found INTEGER DEFAULT 0,
    ean_found INTEGER DEFAULT 0,
    rows_scraped INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    ean_missing INTEGER DEFAULT 0,
    db_count_before INTEGER DEFAULT 0,
    db_count_after INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_ru_prog_run ON ru_step_progress(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_prog_step ON ru_step_progress(step_number, status);

-- Failed pages tracking (for retry mechanism)
CREATE TABLE IF NOT EXISTS ru_failed_pages (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    page_number INTEGER NOT NULL,
    source_type TEXT CHECK(source_type IN ('ved', 'excluded')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,
    status TEXT CHECK(status IN ('pending', 'retrying', 'failed_permanently')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, page_number, source_type)
);
CREATE INDEX IF NOT EXISTS idx_ru_failed_run ON ru_failed_pages(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_failed_status ON ru_failed_pages(status);

-- Export reports: track generated files per run
CREATE TABLE IF NOT EXISTS ru_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_export_reports_run ON ru_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_export_reports_type ON ru_export_reports(report_type);

-- Error tracking
CREATE TABLE IF NOT EXISTS ru_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_errors_run ON ru_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_errors_step ON ru_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_ru_errors_type ON ru_errors(error_type);
