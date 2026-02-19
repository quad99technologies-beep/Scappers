#!/usr/bin/env python3
"""
Netherlands-specific database schema (PostgreSQL).

Tables:
- nl_collected_urls: URLs collected from medicijnkosten.nl (Step 1a)
- nl_packs: Pack/pricing data from medicijnkosten.nl (Step 1b)
- nl_chrome_instances: Chrome/browser instance tracking (Infrastructure)
- nl_search_combinations: Search combinations tracking (Step 1a helper)
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
# TRACKING TABLES
# =============================================================================

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

# Migration DDL for existing nl_errors tables (add new columns if missing)
ERRORS_MIGRATION_DDL = """
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS stack_trace TEXT;
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS url TEXT;
ALTER TABLE nl_errors ADD COLUMN IF NOT EXISTS thread_id INTEGER;
"""

# =============================================================================
# FK REIMBURSEMENT TABLES (Steps 2-5) - Farmacotherapeutisch Kompas
# =============================================================================

FK_URLS_DDL = """
-- nl_fk_urls: Detail URLs from farmacotherapeutischkompas.nl (Step 2)
CREATE TABLE IF NOT EXISTS nl_fk_urls (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    url TEXT NOT NULL,
    generic_slug TEXT,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'success', 'failed', 'skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    scraped_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, url)
);
CREATE INDEX IF NOT EXISTS idx_nl_fk_urls_run ON nl_fk_urls(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_fk_urls_status ON nl_fk_urls(run_id, status);
"""

FK_REIMBURSEMENT_DDL = """
-- nl_fk_reimbursement: Parsed reimbursement rows from FK detail pages (Step 3)
CREATE TABLE IF NOT EXISTS nl_fk_reimbursement (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    fk_url_id INTEGER REFERENCES nl_fk_urls(id),
    generic_name TEXT,
    brand_name TEXT,
    manufacturer TEXT,
    dosage_form TEXT,
    strength TEXT,
    patient_population TEXT,
    indication_nl TEXT,
    indication_en TEXT,
    reimbursement_status TEXT,
    reimbursable_text TEXT,
    route_of_administration TEXT,
    pack_details TEXT,
    binding TEXT DEFAULT 'NO',
    reimbursement_body TEXT DEFAULT 'MINISTRY OF HEALTH',
    reimbursement_date TEXT,
    source_url TEXT NOT NULL,
    translation_status TEXT DEFAULT 'pending' CHECK(translation_status IN ('pending', 'translated', 'no_dutch', 'failed')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, source_url, COALESCE(brand_name,''), COALESCE(strength,''), COALESCE(patient_population,''))
);
CREATE INDEX IF NOT EXISTS idx_nl_fk_reimb_run ON nl_fk_reimbursement(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_fk_reimb_url ON nl_fk_reimbursement(fk_url_id);
CREATE INDEX IF NOT EXISTS idx_nl_fk_reimb_translation ON nl_fk_reimbursement(run_id, translation_status);
"""

FK_DICTIONARY_DDL = """
-- nl_fk_dictionary: Dutch->English translation dictionary (Step 4)
CREATE TABLE IF NOT EXISTS nl_fk_dictionary (
    id SERIAL PRIMARY KEY,
    source_term TEXT NOT NULL,
    translated_term TEXT NOT NULL,
    source_lang TEXT DEFAULT 'nl',
    target_lang TEXT DEFAULT 'en',
    category TEXT DEFAULT 'manual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_term, source_lang, target_lang)
);
CREATE INDEX IF NOT EXISTS idx_nl_fk_dict_term ON nl_fk_dictionary(source_term);
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

# All DDL statements in order
NETHERLANDS_SCHEMA_DDL = [
    # Main tables (medicijnkosten.nl pricing)
    COLLECTED_URLS_DDL,
    PACKS_DDL,

    # Infrastructure & Helper tables
    CHROME_INSTANCES_DDL,
    SEARCH_COMBINATIONS_DDL,
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,

    # FK Reimbursement tables (farmacotherapeutischkompas.nl)
    FK_URLS_DDL,
    FK_REIMBURSEMENT_DDL,
    FK_DICTIONARY_DDL,
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
