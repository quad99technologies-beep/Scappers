-- Tender Chile scraper: PostgreSQL schema with tc_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Tender redirects: Redirect URLs from tender list (Step 1)
CREATE TABLE IF NOT EXISTS tc_tender_redirects (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    redirect_url TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id)
);
CREATE INDEX IF NOT EXISTS idx_tc_redirects_run ON tc_tender_redirects(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_redirects_tender ON tc_tender_redirects(tender_id);

-- Tender details: Tender details from MercadoPublico (Step 2)
CREATE TABLE IF NOT EXISTS tc_tender_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    tender_name TEXT,
    tender_status TEXT,
    publication_date TEXT,
    closing_date TEXT,
    organization TEXT,
    province TEXT,
    contact_info TEXT,
    description TEXT,
    currency TEXT DEFAULT 'CLP',
    estimated_amount REAL,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id)
);
CREATE INDEX IF NOT EXISTS idx_tc_details_run ON tc_tender_details(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_details_tender ON tc_tender_details(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_details_status ON tc_tender_details(tender_status);

-- Tender awards: Tender award information (Step 3)
-- Stores ALL bidders (both winning and non-winning) for complete tender analysis
CREATE TABLE IF NOT EXISTS tc_tender_awards (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    lot_number TEXT,
    lot_title TEXT,
    un_classification_code TEXT,
    buyer_specifications TEXT,
    lot_quantity TEXT,
    supplier_name TEXT,
    supplier_rut TEXT,
    supplier_specifications TEXT,
    unit_price_offer REAL,
    awarded_quantity TEXT,
    total_net_awarded REAL,
    award_amount REAL,
    award_date TEXT,
    award_status TEXT,
    is_awarded TEXT,
    awarded_unit_price REAL,
    source_url TEXT,
    source_tender_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_awards_run ON tc_tender_awards(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_awards_tender ON tc_tender_awards(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_awards_supplier ON tc_tender_awards(supplier_name);

-- Final output: EVERSANA format
CREATE TABLE IF NOT EXISTS tc_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    tender_id TEXT NOT NULL,
    tender_name TEXT,
    tender_status TEXT,
    organization TEXT,
    contact_info TEXT,
    lot_number TEXT,
    lot_title TEXT,
    supplier_name TEXT,
    supplier_rut TEXT,
    currency TEXT DEFAULT 'CLP',
    estimated_amount REAL,
    award_amount REAL,
    publication_date TEXT,
    closing_date TEXT,
    award_date TEXT,
    description TEXT,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, tender_id, lot_number, supplier_rut)
);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_run ON tc_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_tender ON tc_final_output(tender_id);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_status ON tc_final_output(tender_status);
CREATE INDEX IF NOT EXISTS idx_tc_final_output_supplier ON tc_final_output(supplier_name);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS tc_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_tc_progress_run_step ON tc_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_tc_progress_status ON tc_step_progress(status);

-- Export reports
CREATE TABLE IF NOT EXISTS tc_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_export_reports_run ON tc_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_export_reports_type ON tc_export_reports(report_type);

-- Errors
CREATE TABLE IF NOT EXISTS tc_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_errors_run ON tc_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_tc_errors_step ON tc_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_tc_errors_type ON tc_errors(error_type);
