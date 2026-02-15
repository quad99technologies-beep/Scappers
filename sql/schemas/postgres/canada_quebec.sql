-- Canada Quebec scraper: PostgreSQL schema with cq_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Annexe data: Extracted pharmaceutical data from RAMQ PDF annexes
CREATE TABLE IF NOT EXISTS cq_annexe_data (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    annexe_type TEXT,
    generic_name TEXT,
    formulation TEXT,
    strength TEXT,
    fill_size TEXT,
    din TEXT,
    brand TEXT,
    manufacturer TEXT,
    price REAL,
    price_type TEXT,
    currency TEXT DEFAULT 'CAD',
    local_pack_code TEXT,
    local_pack_description TEXT,
    source_page INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, annexe_type)
);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_run ON cq_annexe_data(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_din ON cq_annexe_data(din);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_type ON cq_annexe_data(annexe_type);
CREATE INDEX IF NOT EXISTS idx_cq_annexe_generic ON cq_annexe_data(generic_name);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS cq_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_cq_progress_run_step ON cq_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_cq_progress_status ON cq_step_progress(status);

-- Export reports
CREATE TABLE IF NOT EXISTS cq_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cq_export_reports_run ON cq_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_export_reports_type ON cq_export_reports(report_type);

-- Errors
CREATE TABLE IF NOT EXISTS cq_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cq_errors_run ON cq_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_cq_errors_step ON cq_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_cq_errors_type ON cq_errors(error_type);
