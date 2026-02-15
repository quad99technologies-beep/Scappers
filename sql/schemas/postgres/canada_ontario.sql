-- Canada Ontario scraper: PostgreSQL schema with co_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Products: Product details from Ontario formulary (Step 1)
CREATE TABLE IF NOT EXISTS co_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_name TEXT,
    generic_name TEXT,
    manufacturer TEXT,
    manufacturer_code TEXT,
    din TEXT,
    strength TEXT,
    dosage_form TEXT,
    pack_size TEXT,
    unit_price REAL,
    interchangeability TEXT,
    benefit_status TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_products_run ON co_products(run_id);
CREATE INDEX IF NOT EXISTS idx_co_products_din ON co_products(din);
CREATE INDEX IF NOT EXISTS idx_co_products_manufacturer ON co_products(manufacturer_code);

-- Manufacturers: Manufacturer master data (Step 1)
CREATE TABLE IF NOT EXISTS co_manufacturers (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    manufacturer_code TEXT NOT NULL,
    manufacturer_name TEXT,
    address TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, manufacturer_code)
);
CREATE INDEX IF NOT EXISTS idx_co_manufacturers_run ON co_manufacturers(run_id);
CREATE INDEX IF NOT EXISTS idx_co_manufacturers_code ON co_manufacturers(manufacturer_code);

-- EAP Prices: EAP prices from Ontario.ca (Step 2)
CREATE TABLE IF NOT EXISTS co_eap_prices (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    din TEXT,
    product_name TEXT,
    generic_name TEXT,
    strength TEXT,
    dosage_form TEXT,
    eap_price REAL,
    currency TEXT DEFAULT 'CAD',
    effective_date TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_eap_run ON co_eap_prices(run_id);
CREATE INDEX IF NOT EXISTS idx_co_eap_din ON co_eap_prices(din);

-- Final output: EVERSANA format
CREATE TABLE IF NOT EXISTS co_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    country TEXT DEFAULT 'CANADA',
    region TEXT DEFAULT 'NORTH AMERICA',
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    unit_price REAL,
    public_with_vat_price REAL,
    public_without_vat_price REAL,
    eap_price REAL,
    currency TEXT DEFAULT 'CAD',
    reimbursement_category TEXT,
    reimbursement_amount REAL,
    copay_amount REAL,
    benefit_status TEXT,
    interchangeability TEXT,
    din TEXT,
    strength TEXT,
    dosage_form TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    local_pack_code TEXT,
    effective_start_date TEXT,
    effective_end_date TEXT,
    source TEXT DEFAULT 'PRICENTRIC',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_co_final_output_run ON co_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_co_final_output_pcid ON co_final_output(pcid);
CREATE INDEX IF NOT EXISTS idx_co_final_output_din ON co_final_output(din);

-- PCID Mappings: PCID mapped data for export
CREATE TABLE IF NOT EXISTS co_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT NOT NULL,
    local_pack_code TEXT NOT NULL,
    presentation TEXT,
    product_name TEXT,
    generic_name TEXT,
    manufacturer TEXT,
    country TEXT DEFAULT 'CANADA',
    region TEXT DEFAULT 'NORTH AMERICA',
    currency TEXT DEFAULT 'CAD',
    unit_price REAL,
    public_with_vat_price REAL,
    eap_price REAL,
    reimbursement_category TEXT,
    effective_date TEXT,
    source TEXT DEFAULT 'PRICENTRIC',
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, pcid, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_co_pcid_run ON co_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_co_pcid_code ON co_pcid_mappings(pcid);
CREATE INDEX IF NOT EXISTS idx_co_pcid_local ON co_pcid_mappings(local_pack_code);

-- Step progress: Sub-step resume tracking
CREATE TABLE IF NOT EXISTS co_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_co_progress_run_step ON co_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_co_progress_status ON co_step_progress(status);

-- Export reports: Generated export/report tracking
CREATE TABLE IF NOT EXISTS co_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_export_reports_run ON co_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_co_export_reports_type ON co_export_reports(report_type);

-- Errors: Error tracking
CREATE TABLE IF NOT EXISTS co_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_errors_run ON co_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_co_errors_step ON co_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_co_errors_type ON co_errors(error_type);
