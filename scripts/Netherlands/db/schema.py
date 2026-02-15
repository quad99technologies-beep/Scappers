#!/usr/bin/env python3
"""
Netherlands-specific database schema (PostgreSQL).

Tables:
- nl_collected_urls: URLs collected from medicijnkosten.nl (Step 1a)
- nl_packs: Pack/pricing data from medicijnkosten.nl (Step 1b)
- nl_details: Product details from farmacotherapeutischkompas.nl (Step 2a)
- nl_costs: Cost/pricing data from farmacotherapeutischkompas.nl (Step 2b)
- nl_consolidated: Merged data from details + costs (Step 3)
- nl_chrome_instances: Chrome/browser instance tracking
- nl_products: Product data (legacy, kept for compatibility)
- nl_reimbursement: Reimbursement pricing data (legacy)
- nl_step_progress: Sub-step resume tracking (all steps)
- nl_export_reports: Generated export/report tracking
- nl_errors: Error tracking with extended fields
"""

# PostgreSQL uses SERIAL instead of AUTOINCREMENT
# PostgreSQL uses CURRENT_TIMESTAMP instead of datetime('now')
# PostgreSQL uses ON CONFLICT DO UPDATE/NOTHING instead of ON CONFLICT REPLACE

# =============================================================================
# NEW TABLES - DB-First Architecture (replaces CSV files)
# =============================================================================

COLLECTED_URLS_DDL = """
-- nl_collected_urls: Replaces collected_urls.csv (Step 1a)
CREATE TABLE IF NOT EXISTS nl_collected_urls (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    prefix TEXT NOT NULL,
    title TEXT,
    active_substance TEXT,
    manufacturer TEXT,
    document_type TEXT,
    price_text TEXT,
    reimbursement TEXT,
    url TEXT NOT NULL,
    url_with_id TEXT,
    packs_scraped TEXT DEFAULT 'pending' CHECK(packs_scraped IN ('pending', 'success', 'failed', 'skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    scraped_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, url)
);
CREATE INDEX IF NOT EXISTS idx_nl_collected_urls_run ON nl_collected_urls(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_collected_urls_prefix ON nl_collected_urls(run_id, prefix);
CREATE INDEX IF NOT EXISTS idx_nl_collected_urls_status ON nl_collected_urls(run_id, packs_scraped);
"""

PACKS_DDL = """
-- nl_packs: Replaces packs.csv (Step 1b - medicijnkosten.nl data)
CREATE TABLE IF NOT EXISTS nl_packs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    collected_url_id INTEGER REFERENCES nl_collected_urls(id),
    start_date DATE,
    end_date DATE,
    currency TEXT DEFAULT 'EUR',
    unit_price NUMERIC(12,4),
    ppp_ex_vat NUMERIC(12,4),
    ppp_vat NUMERIC(12,4),
    vat_percent NUMERIC(5,2) DEFAULT 9.0,
    reimbursable_status TEXT,
    reimbursable_rate TEXT,
    copay_price NUMERIC(12,4),
    copay_percent TEXT,
    deductible NUMERIC(12,4),
    ri_with_vat NUMERIC(12,4),  -- RI (Reimbursement Indicator) with VAT = deductible
    margin_rule TEXT,
    product_group TEXT,
    local_pack_description TEXT,
    active_substance TEXT,
    manufacturer TEXT,
    formulation TEXT,
    strength_size TEXT,
    local_pack_code TEXT,
    reimbursement_message TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, source_url, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_nl_packs_run ON nl_packs(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_packs_code ON nl_packs(local_pack_code);
CREATE INDEX IF NOT EXISTS idx_nl_packs_url ON nl_packs(source_url);
"""

DETAILS_DDL = """
-- nl_details: Replaces details.csv (Step 2a - farmacotherapeutischkompas.nl)
CREATE TABLE IF NOT EXISTS nl_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    detail_url TEXT NOT NULL,
    product_name TEXT,
    product_type TEXT,
    manufacturer TEXT,
    administration_form TEXT,
    strengths_raw TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, detail_url)
);
CREATE INDEX IF NOT EXISTS idx_nl_details_run ON nl_details(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_details_url ON nl_details(detail_url);
"""

COSTS_DDL = """
-- nl_costs: Replaces costs.csv (Step 2b - price data from kompas)
CREATE TABLE IF NOT EXISTS nl_costs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    detail_id INTEGER REFERENCES nl_details(id),
    detail_url TEXT NOT NULL,
    brand_full TEXT,
    brand_name TEXT,
    pack_presentation TEXT,
    ddd_text TEXT,
    currency TEXT DEFAULT 'EUR',
    price_per_day NUMERIC(12,4),
    price_per_week NUMERIC(12,4),
    price_per_month NUMERIC(12,4),
    price_per_six_months NUMERIC(12,4),
    reimbursed_per_day NUMERIC(12,4),
    extra_payment_per_day NUMERIC(12,4),
    table_type TEXT,
    unit_type TEXT,
    unit_amount TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nl_costs_run ON nl_costs(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_costs_detail ON nl_costs(detail_id);
CREATE INDEX IF NOT EXISTS idx_nl_costs_url ON nl_costs(detail_url);
"""

CONSOLIDATED_DDL = """
-- nl_consolidated: Merged data from details + costs (Step 3 output)
CREATE TABLE IF NOT EXISTS nl_consolidated (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    detail_url TEXT,
    product_name TEXT,
    brand_name TEXT,
    manufacturer TEXT,
    administration_form TEXT,
    strengths_raw TEXT,
    pack_presentation TEXT,
    currency TEXT DEFAULT 'EUR',
    price_per_day NUMERIC(12,4),
    reimbursed_per_day NUMERIC(12,4),
    extra_payment_per_day NUMERIC(12,4),
    ddd_text TEXT,
    table_type TEXT,
    unit_type TEXT,
    unit_amount TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, detail_url, brand_name)
);
CREATE INDEX IF NOT EXISTS idx_nl_consolidated_run ON nl_consolidated(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_consolidated_url ON nl_consolidated(detail_url);
"""

CHROME_INSTANCES_DDL = """
-- nl_chrome_instances: Track Chrome/browser instances for cleanup
CREATE TABLE IF NOT EXISTS nl_chrome_instances (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    thread_id INTEGER,
    browser_type TEXT DEFAULT 'chrome' CHECK(browser_type IN ('chrome', 'chromium', 'firefox')),
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    user_data_dir TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    terminated_at TIMESTAMP,
    termination_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_nl_chrome_run ON nl_chrome_instances(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_chrome_active ON nl_chrome_instances(run_id, terminated_at) WHERE terminated_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_nl_chrome_step ON nl_chrome_instances(run_id, step_number);
"""

# =============================================================================
# LEGACY TABLES (kept for compatibility)
# =============================================================================

PRODUCTS_DDL = """
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
CREATE INDEX IF NOT EXISTS idx_nl_products_atc ON nl_products(atc_code);
CREATE INDEX IF NOT EXISTS idx_nl_products_generic ON nl_products(generic_name);
CREATE INDEX IF NOT EXISTS idx_nl_products_url ON nl_products(product_url);
"""

REIMBURSEMENT_DDL = """
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
CREATE INDEX IF NOT EXISTS idx_nl_reimbursement_run ON nl_reimbursement(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_reimbursement_url ON nl_reimbursement(product_url);
CREATE INDEX IF NOT EXISTS idx_nl_reimbursement_status ON nl_reimbursement(reimbursement_status);
"""

STEP_PROGRESS_DDL = """
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
"""

EXPORT_REPORTS_DDL = """
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
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS nl_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    stack_trace TEXT,
    url TEXT,
    thread_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_nl_errors_run ON nl_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_errors_step ON nl_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_nl_errors_type ON nl_errors(error_type);
"""

# Migration DDL for existing nl_errors tables (add new columns)
ERRORS_MIGRATION_DDL = """
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS stack_trace TEXT;
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS url TEXT;
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS thread_id INTEGER;
"""

SEARCH_COMBINATIONS_DDL = """
-- nl_search_combinations: Track vorm/sterkte combinations for systematic collection
CREATE TABLE IF NOT EXISTS nl_search_combinations (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    
    -- Dropdown values
    vorm TEXT NOT NULL,           -- Form (e.g., "TABLETTEN EN CAPSULES")
    sterkte TEXT NOT NULL,        -- Strength (e.g., "10/80MG")
    
    -- Generated URL
    search_url TEXT NOT NULL,     -- Full search URL with vorm/sterkte
    
    -- Tracking
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'collecting', 'completed', 'failed', 'skipped')),
    products_found INTEGER DEFAULT 0,
    urls_discovered INTEGER DEFAULT 0,
    urls_fetched INTEGER DEFAULT 0,
    urls_inserted INTEGER DEFAULT 0,
    urls_duplicate INTEGER DEFAULT 0,
    urls_collected INTEGER DEFAULT 0,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Constraints
    UNIQUE(run_id, vorm, sterkte)
);
CREATE INDEX IF NOT EXISTS idx_nl_combinations_run ON nl_search_combinations(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_combinations_status ON nl_search_combinations(run_id, status);
CREATE INDEX IF NOT EXISTS idx_nl_combinations_vorm ON nl_search_combinations(vorm);
CREATE INDEX IF NOT EXISTS idx_nl_combinations_sterkte ON nl_search_combinations(sterkte);
"""

# All DDL statements in order (new tables first, then legacy, then migrations)
NETHERLANDS_SCHEMA_DDL = [
    # New DB-first tables
    COLLECTED_URLS_DDL,
    PACKS_DDL,
    DETAILS_DDL,
    COSTS_DDL,
    CONSOLIDATED_DDL,
    CHROME_INSTANCES_DDL,
    SEARCH_COMBINATIONS_DDL,  # NEW: vorm/sterkte combinations tracking
    # Legacy tables (kept for compatibility)
    PRODUCTS_DDL,
    REIMBURSEMENT_DDL,
    # Common tracking tables
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,
]

# Migration DDL for existing databases
NETHERLANDS_MIGRATION_DDL = [
    ERRORS_MIGRATION_DDL,
]


def apply_netherlands_schema(db) -> None:
    """Apply all Netherlands-specific DDL to a CountryDB instance."""
    from core.db.models import apply_common_schema
    apply_common_schema(db)

    # Apply main schema DDL
    for ddl in NETHERLANDS_SCHEMA_DDL:
        db.executescript(ddl)

    # Apply migrations (ALTER TABLE statements for existing tables)
    for ddl in NETHERLANDS_MIGRATION_DDL:
        try:
            db.executescript(ddl)
        except Exception:
            # Migrations may fail if columns already exist, that's OK
            pass
