-- Malaysia scraper: PostgreSQL schema with my_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Products: Registration numbers and prices from MyPriMe (Step 1)
CREATE TABLE IF NOT EXISTS my_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    registration_no TEXT NOT NULL,
    product_name TEXT,
    generic_name TEXT,
    dosage_form TEXT,
    strength TEXT,
    pack_size TEXT,
    pack_unit TEXT,
    manufacturer TEXT,
    unit_price REAL,
    retail_price REAL,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_my_products_regno ON my_products(registration_no);
CREATE INDEX IF NOT EXISTS idx_my_products_run ON my_products(run_id);

-- Product details: Product name/holder from Quest3Plus (Step 2)
CREATE TABLE IF NOT EXISTS my_product_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    registration_no TEXT NOT NULL,
    product_name TEXT,
    holder TEXT,
    holder_address TEXT,
    search_method TEXT CHECK(search_method IN ('bulk', 'individual')),
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_no)
);
CREATE INDEX IF NOT EXISTS idx_my_details_regno ON my_product_details(registration_no);
CREATE INDEX IF NOT EXISTS idx_my_details_run ON my_product_details(run_id);

-- Consolidated products: Deduplicated product master (Step 3)
CREATE TABLE IF NOT EXISTS my_consolidated_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    registration_no TEXT NOT NULL,
    product_name TEXT,
    holder TEXT,
    search_method TEXT CHECK(search_method IN ('bulk', 'individual')),
    consolidated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_no)
);
CREATE INDEX IF NOT EXISTS idx_my_consol_regno ON my_consolidated_products(registration_no);
CREATE INDEX IF NOT EXISTS idx_my_consol_run ON my_consolidated_products(run_id);

-- Reimbursable drugs: FUKKM fully reimbursable drugs (Step 4)
CREATE TABLE IF NOT EXISTS my_reimbursable_drugs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    drug_name TEXT,
    registration_no TEXT,
    dosage_form TEXT,
    strength TEXT,
    pack_size TEXT,
    manufacturer TEXT,
    source_page INTEGER,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, drug_name, dosage_form, strength)
);
CREATE INDEX IF NOT EXISTS idx_my_reimb_name ON my_reimbursable_drugs(drug_name);
CREATE INDEX IF NOT EXISTS idx_my_reimb_run ON my_reimbursable_drugs(run_id);

-- PCID mappings: Final PCID-mapped output (Step 5)
CREATE TABLE IF NOT EXISTS my_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    local_pack_code TEXT NOT NULL,
    package_number TEXT,
    country TEXT DEFAULT 'MALAYSIA',
    company TEXT,
    product_group TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    description TEXT,
    indication TEXT,
    pack_size TEXT,
    effective_start_date TEXT,
    effective_end_date TEXT,
    currency TEXT DEFAULT 'MYR',
    public_without_vat_price REAL,
    public_with_vat_price REAL,
    vat_percent REAL DEFAULT 0.0,
    reimbursable_status TEXT,
    reimbursable_price REAL,
    reimbursable_rate TEXT,
    reimbursable_notes TEXT,
    region TEXT DEFAULT 'MALAYSIA',
    marketing_authority TEXT,
    local_pack_description TEXT,
    formulation TEXT,
    strength TEXT,
    strength_unit TEXT,
    brand_type TEXT,
    source TEXT DEFAULT 'PRICENTRIC',
    unit_price REAL,
    search_method TEXT CHECK(search_method IN ('bulk', 'individual')),
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_my_pcid_code ON my_pcid_mappings(local_pack_code);
CREATE INDEX IF NOT EXISTS idx_my_pcid_run ON my_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_my_pcid_pcid ON my_pcid_mappings(pcid);

-- Step progress: Sub-step resume tracking (all steps)
CREATE TABLE IF NOT EXISTS my_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_my_progress_run_step ON my_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_my_progress_status ON my_step_progress(status);

-- Bulk search counts: per-keyword Quest3+ CSV/page row tracking (Step 2)
CREATE TABLE IF NOT EXISTS my_bulk_search_counts (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    keyword TEXT NOT NULL,
    page_rows INTEGER,
    csv_rows INTEGER,
    difference INTEGER,
    status TEXT,
    reason TEXT,
    csv_file TEXT,
    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, keyword)
);
CREATE INDEX IF NOT EXISTS idx_my_bulk_counts_run ON my_bulk_search_counts(run_id);
CREATE INDEX IF NOT EXISTS idx_my_bulk_counts_keyword ON my_bulk_search_counts(keyword);

-- Export reports: track generated files per run
CREATE TABLE IF NOT EXISTS my_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_my_export_reports_run ON my_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_my_export_reports_type ON my_export_reports(report_type);

-- PCID reference: Temporary table for PCID reference CSV loading
CREATE TABLE IF NOT EXISTS my_pcid_reference (
    id SERIAL PRIMARY KEY,
    pcid TEXT,
    local_pack_code TEXT NOT NULL,
    package_number TEXT,
    product_group TEXT,
    generic_name TEXT,
    description TEXT,
    UNIQUE(local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_my_pcidref_code ON my_pcid_reference(local_pack_code);

-- Input products (Malaysia-specific input)
CREATE TABLE IF NOT EXISTS my_input_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    registration_no TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Note: Migration from UNIQUE(run_id, registration_no) to UNIQUE(run_id, registration_no, dosage_form)
-- runs from Python apply_malaysia_schema() when the pipeline step 0 executes (single execute(), not executescript).