-- Belarus scraper: PostgreSQL schema with by_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- RCETH Data: Raw drug price registry data (Step 1)
CREATE TABLE IF NOT EXISTS by_rceth_data (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    inn TEXT,
    inn_en TEXT,
    trade_name TEXT,
    trade_name_en TEXT,
    manufacturer TEXT,
    manufacturer_country TEXT,
    dosage_form TEXT,
    dosage_form_en TEXT,
    strength TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    registration_number TEXT,
    registration_date TEXT,
    registration_valid_to TEXT,
    producer_price REAL,
    producer_price_vat REAL,
    wholesale_price REAL,
    wholesale_price_vat REAL,
    retail_price REAL,
    retail_price_vat REAL,
    import_price REAL,
    import_price_currency TEXT,
    currency TEXT DEFAULT 'BYN',
    atc_code TEXT,
    who_atc_code TEXT,
    pharmacotherapeutic_group TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_number, trade_name, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_by_rceth_run ON by_rceth_data(run_id);
CREATE INDEX IF NOT EXISTS idx_by_rceth_atc ON by_rceth_data(atc_code);
CREATE INDEX IF NOT EXISTS idx_by_rceth_inn ON by_rceth_data(inn);
CREATE INDEX IF NOT EXISTS idx_by_rceth_reg ON by_rceth_data(registration_number);

-- PCID Mappings: PCID mapped data for export (Step 2)
CREATE TABLE IF NOT EXISTS by_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT NOT NULL,
    local_pack_code TEXT,
    presentation TEXT,
    inn TEXT,
    inn_en TEXT,
    trade_name TEXT,
    trade_name_en TEXT,
    manufacturer TEXT,
    manufacturer_country TEXT,
    atc_code TEXT,
    who_atc_code TEXT,
    retail_price REAL,
    retail_price_vat REAL,
    currency TEXT DEFAULT 'BYN',
    country TEXT DEFAULT 'BELARUS',
    region TEXT DEFAULT 'EUROPE',
    source TEXT DEFAULT 'PRICENTRIC',
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, pcid, trade_name, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_by_pcid_run ON by_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_by_pcid_code ON by_pcid_mappings(pcid);
CREATE INDEX IF NOT EXISTS idx_by_pcid_atc ON by_pcid_mappings(atc_code);

-- Final Output: EVERSANA format merged output
CREATE TABLE IF NOT EXISTS by_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    country TEXT DEFAULT 'BELARUS',
    region TEXT DEFAULT 'EUROPE',
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    generic_name_en TEXT,
    dosage_form TEXT,
    dosage_form_en TEXT,
    strength TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    producer_price REAL,
    producer_price_vat REAL,
    wholesale_price REAL,
    wholesale_price_vat REAL,
    retail_price REAL,
    retail_price_vat REAL,
    currency TEXT DEFAULT 'BYN',
    atc_code TEXT,
    who_atc_code TEXT,
    pharmacotherapeutic_group TEXT,
    registration_number TEXT,
    registration_date TEXT,
    registration_valid_to TEXT,
    source_type TEXT CHECK(source_type IN ('rceth', 'pcid_mapped')),
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_number, local_product_name, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_by_final_run ON by_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_by_final_pcid ON by_final_output(pcid);
CREATE INDEX IF NOT EXISTS idx_by_final_atc ON by_final_output(atc_code);
CREATE INDEX IF NOT EXISTS idx_by_final_reg ON by_final_output(registration_number);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS by_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_by_progress_run_step ON by_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_by_progress_status ON by_step_progress(status);

-- Export reports: Generated export/report tracking
CREATE TABLE IF NOT EXISTS by_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_export_reports_run ON by_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_by_export_reports_type ON by_export_reports(report_type);

-- Errors: Error tracking
CREATE TABLE IF NOT EXISTS by_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_errors_run ON by_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_by_errors_step ON by_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_by_errors_type ON by_errors(error_type);
