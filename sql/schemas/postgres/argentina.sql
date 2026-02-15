-- Argentina scraper: PostgreSQL schema with ar_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Product index: Product + company pairs sourced from AlfaBeta (prep/queue)
CREATE TABLE IF NOT EXISTS ar_product_index (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product TEXT NOT NULL,
    company TEXT NOT NULL,
    url TEXT,
    loop_count INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    scraped_by_selenium BOOLEAN DEFAULT FALSE,
    scraped_by_api BOOLEAN DEFAULT FALSE,
    scrape_source TEXT,  -- Tracks which step scraped: 'selenium_product', 'selenium_company', 'api', 'step7'
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending','in_progress','completed','failed','skipped')),
    last_attempt_at TIMESTAMP,
    last_attempt_source TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, company, product)
);
CREATE INDEX IF NOT EXISTS idx_ar_product_index_run ON ar_product_index(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_product_index_status ON ar_product_index(status);

-- Products: Scraped product details (selenium/api)
CREATE TABLE IF NOT EXISTS ar_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    record_hash TEXT,
    input_company TEXT,
    input_product_name TEXT,
    company TEXT,
    product_name TEXT,
    active_ingredient TEXT,
    therapeutic_class TEXT,
    description TEXT,
    price_ars REAL,
    price_raw TEXT,
    date TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sifar_detail TEXT,
    pami_af TEXT,
    pami_os TEXT,
    ioma_detail TEXT,
    ioma_af TEXT,
    ioma_os TEXT,
    import_status TEXT,
    coverage_json TEXT,
    source TEXT CHECK(source IN ('selenium','selenium_product','selenium_company','api','step7','manual')) DEFAULT 'selenium',
    UNIQUE(run_id, record_hash)
);
CREATE INDEX IF NOT EXISTS idx_ar_products_run ON ar_products(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_products_company_prod ON ar_products(input_company, input_product_name);
CREATE INDEX IF NOT EXISTS idx_ar_products_run_input ON ar_products(run_id, input_company, input_product_name);

-- Products translated: English-normalised view after dictionary translation
CREATE TABLE IF NOT EXISTS ar_products_translated (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_id INTEGER REFERENCES ar_products(id) ON DELETE CASCADE,
    company TEXT,
    product_name TEXT,
    active_ingredient TEXT,
    therapeutic_class TEXT,
    description TEXT,
    price_ars REAL,
    date TEXT,
    sifar_detail TEXT,
    pami_af TEXT,
    pami_os TEXT,
    ioma_detail TEXT,
    ioma_af TEXT,
    ioma_os TEXT,
    import_status TEXT,
    coverage_json TEXT,
    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    translation_source TEXT,
    UNIQUE(run_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_ar_products_translated_run ON ar_products_translated(run_id);

-- Errors: Per-product error log
CREATE TABLE IF NOT EXISTS ar_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    input_company TEXT,
    input_product_name TEXT,
    error_type TEXT,
    error_message TEXT,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_errors_run ON ar_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_errors_step ON ar_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_ar_errors_type ON ar_errors(error_type);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS ar_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending','in_progress','completed','failed','skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_ar_progress_run_step ON ar_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_ar_progress_status ON ar_step_progress(status);

-- Dictionary: ES->EN dictionary entries
CREATE TABLE IF NOT EXISTS ar_dictionary (
    id SERIAL PRIMARY KEY,
    es TEXT NOT NULL,
    en TEXT NOT NULL,
    source TEXT DEFAULT 'file',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(es)
);
CREATE INDEX IF NOT EXISTS idx_ar_dictionary_es ON ar_dictionary(es);

-- Ignore list: Products to skip
CREATE TABLE IF NOT EXISTS ar_ignore_list (
    id SERIAL PRIMARY KEY,
    company TEXT NOT NULL,
    product TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company, product)
);
CREATE INDEX IF NOT EXISTS idx_ar_ignore_company_product ON ar_ignore_list(company, product);

-- Export reports: Generated export/report tracking
CREATE TABLE IF NOT EXISTS ar_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_export_reports_run ON ar_export_reports(run_id);

-- Artifacts: Screenshots and artifacts
CREATE TABLE IF NOT EXISTS ar_artifacts (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    input_company TEXT NOT NULL,
    input_product_name TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_artifacts_run ON ar_artifacts(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_artifacts_type ON ar_artifacts(artifact_type);

-- OOS URLs: Out of Scope products excluded from scraping
CREATE TABLE IF NOT EXISTS ar_oos_urls (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT NOT NULL,
    company TEXT NOT NULL,
    product TEXT NOT NULL,
    generic_name TEXT,
    local_pack_description TEXT,
    url TEXT,
    reason TEXT DEFAULT 'OOS in PCID mapping',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, company, product)
);
CREATE INDEX IF NOT EXISTS idx_ar_oos_urls_run ON ar_oos_urls(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_oos_urls_pcid ON ar_oos_urls(pcid);
CREATE INDEX IF NOT EXISTS idx_ar_oos_urls_url ON ar_oos_urls(url);
