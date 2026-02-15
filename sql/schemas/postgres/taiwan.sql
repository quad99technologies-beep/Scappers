-- Taiwan scraper: PostgreSQL schema with tw_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Drug codes: Drug code URLs and basic info from NHI (Step 1)
CREATE TABLE IF NOT EXISTS tw_drug_codes (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    drug_code TEXT NOT NULL,
    drug_code_url TEXT,
    lic_id TEXT,
    name_en TEXT,
    name_zh TEXT,
    ingredient_content TEXT,
    gauge_quantity TEXT,
    single_compound TEXT,
    price TEXT,
    effective_date TEXT,
    effective_start_date TEXT,
    effective_end_date TEXT,
    pharmacists TEXT,
    dosage_form TEXT,
    classification TEXT,
    taxonomy_group TEXT,
    atc_code TEXT,
    page_number INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, drug_code)
);
CREATE INDEX IF NOT EXISTS idx_tw_drug_codes_run ON tw_drug_codes(run_id);
CREATE INDEX IF NOT EXISTS idx_tw_drug_codes_code ON tw_drug_codes(drug_code);
CREATE INDEX IF NOT EXISTS idx_tw_drug_codes_atc ON tw_drug_codes(atc_code);

-- Drug details: Detailed certificate/manufacturer info from FDA (Step 2)
CREATE TABLE IF NOT EXISTS tw_drug_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    drug_code TEXT NOT NULL,
    lic_id_url TEXT,
    valid_date_roc TEXT,
    valid_date_ad TEXT,
    original_certificate_date TEXT,
    license_type TEXT,
    customs_doc_number TEXT,
    chinese_product_name TEXT,
    english_product_name TEXT,
    indications TEXT,
    dosage_form TEXT,
    package TEXT,
    drug_category TEXT,
    atc_code TEXT,
    principal_components TEXT,
    restricted_items TEXT,
    drug_company_name TEXT,
    drugstore_address TEXT,
    manufacturer_code TEXT,
    factory TEXT,
    manufacturer_name TEXT,
    manufacturing_plant_address TEXT,
    manufacturing_plant_company_address TEXT,
    country_of_manufacture TEXT,
    process_description TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, drug_code)
);
CREATE INDEX IF NOT EXISTS idx_tw_drug_details_run ON tw_drug_details(run_id);
CREATE INDEX IF NOT EXISTS idx_tw_drug_details_code ON tw_drug_details(drug_code);
CREATE INDEX IF NOT EXISTS idx_tw_drug_details_atc ON tw_drug_details(atc_code);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS tw_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_tw_progress_run_step ON tw_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_tw_progress_status ON tw_step_progress(status);

-- Export reports
CREATE TABLE IF NOT EXISTS tw_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tw_export_reports_run ON tw_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_tw_export_reports_type ON tw_export_reports(report_type);

-- Errors
CREATE TABLE IF NOT EXISTS tw_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tw_errors_run ON tw_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_tw_errors_step ON tw_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_tw_errors_type ON tw_errors(error_type);
