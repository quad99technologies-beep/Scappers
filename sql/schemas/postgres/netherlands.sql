-- Netherlands scraper: PostgreSQL schema with nl_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Products: Collected product URLs and basic info (Step 1)
CREATE TABLE IF NOT EXISTS nl_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_url TEXT,
    product_name TEXT,
    brand_name TEXT,
    generic_name TEXT,
    atc_code TEXT,
    dosage_form TEXT,
    strength TEXT,
    pack_size TEXT,
    manufacturer TEXT,
    source_prefix TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, product_url)
);
CREATE INDEX IF NOT EXISTS idx_nl_products_run ON nl_products(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_products_url ON nl_products(product_url);
CREATE INDEX IF NOT EXISTS idx_nl_products_atc ON nl_products(atc_code);

-- Reimbursement: Pricing and reimbursement data (Step 2)
CREATE TABLE IF NOT EXISTS nl_reimbursement (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_url TEXT,
    product_name TEXT,
    reimbursement_price REAL,
    pharmacy_purchase_price REAL,
    list_price REAL,
    supplement REAL,
    currency TEXT DEFAULT 'EUR',
    reimbursement_status TEXT,
    indication TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, product_url, product_name)
);
CREATE INDEX IF NOT EXISTS idx_nl_reimb_run ON nl_reimbursement(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_reimb_url ON nl_reimbursement(product_url);
CREATE INDEX IF NOT EXISTS idx_nl_reimb_name ON nl_reimbursement(product_name);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS nl_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_nl_progress_run_step ON nl_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_nl_progress_status ON nl_step_progress(status);

-- Export reports
CREATE TABLE IF NOT EXISTS nl_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nl_export_reports_run ON nl_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_export_reports_type ON nl_export_reports(report_type);

-- Errors
CREATE TABLE IF NOT EXISTS nl_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nl_errors_run ON nl_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_errors_step ON nl_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_nl_errors_type ON nl_errors(error_type);
