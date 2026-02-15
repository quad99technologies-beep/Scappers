-- North Macedonia scraper: PostgreSQL schema with nm_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Drug register: Drug register data from MoH (Steps 1-2)
CREATE TABLE IF NOT EXISTS nm_drug_register (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    registration_number TEXT,
    product_name TEXT,
    product_name_en TEXT,
    generic_name TEXT,
    generic_name_en TEXT,
    dosage_form TEXT,
    strength TEXT,
    pack_size TEXT,
    manufacturer TEXT,
    marketing_authorisation_holder TEXT,
    atc_code TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_run ON nm_drug_register(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_drug_reg_number ON nm_drug_register(registration_number);

-- Max prices: Max prices data from zdravstvo.gov.mk (Step 3)
CREATE TABLE IF NOT EXISTS nm_max_prices (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_name TEXT,
    product_name_en TEXT,
    generic_name TEXT,
    generic_name_en TEXT,
    dosage_form TEXT,
    strength TEXT,
    pack_size TEXT,
    manufacturer TEXT,
    max_price REAL,
    currency TEXT DEFAULT 'MKD',
    effective_date TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nm_max_prices_run ON nm_max_prices(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_max_prices_product ON nm_max_prices(product_name);

-- Final output: EVERSANA format
CREATE TABLE IF NOT EXISTS nm_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    country TEXT DEFAULT 'NORTH MACEDONIA',
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    generic_name_en TEXT,
    description TEXT,
    strength TEXT,
    dosage_form TEXT,
    pack_size TEXT,
    max_price REAL,
    currency TEXT DEFAULT 'MKD',
    effective_date TEXT,
    registration_number TEXT,
    atc_code TEXT,
    marketing_authorisation_holder TEXT,
    source_type TEXT CHECK(source_type IN ('drug_register', 'max_prices', 'merged')),
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_number, product_name, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_nm_final_output_run ON nm_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_final_output_pcid ON nm_final_output(pcid);
CREATE INDEX IF NOT EXISTS idx_nm_final_output_reg ON nm_final_output(registration_number);

-- PCID Mappings
CREATE TABLE IF NOT EXISTS nm_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT NOT NULL,
    local_pack_code TEXT NOT NULL,
    presentation TEXT,
    product_name TEXT,
    generic_name TEXT,
    manufacturer TEXT,
    country TEXT DEFAULT 'NORTH MACEDONIA',
    region TEXT DEFAULT 'EUROPE',
    currency TEXT DEFAULT 'MKD',
    max_price REAL,
    effective_date TEXT,
    source TEXT DEFAULT 'PRICENTRIC',
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, pcid, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_nm_pcid_run ON nm_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_pcid_code ON nm_pcid_mappings(pcid);
CREATE INDEX IF NOT EXISTS idx_nm_pcid_local ON nm_pcid_mappings(local_pack_code);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS nm_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_nm_progress_run_step ON nm_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_nm_progress_status ON nm_step_progress(status);

-- Export reports
CREATE TABLE IF NOT EXISTS nm_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nm_export_reports_run ON nm_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_export_reports_type ON nm_export_reports(report_type);

-- Errors
CREATE TABLE IF NOT EXISTS nm_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nm_errors_run ON nm_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_nm_errors_step ON nm_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_nm_errors_type ON nm_errors(error_type);
