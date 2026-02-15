#!/usr/bin/env python3
"""
Malaysia-specific database schema (PostgreSQL).

Tables:
- products: Registration numbers and prices from MyPriMe (Step 1)
- product_details: Product name/holder from Quest3Plus (Step 2)
- consolidated_products: Deduplicated product master (Step 3)
- reimbursable_drugs: FUKKM fully reimbursable drugs (Step 4)
- pcid_mappings: Final PCID-mapped output (Step 5)
- step_progress: Sub-step resume tracking (all steps)
"""

# PostgreSQL uses SERIAL instead of AUTOINCREMENT
# PostgreSQL uses CURRENT_TIMESTAMP instead of datetime('now')
# PostgreSQL uses ON CONFLICT DO UPDATE/NOTHING instead of ON CONFLICT REPLACE

PRODUCTS_DDL = """
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
"""

PRODUCT_DETAILS_DDL = """
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
"""

CONSOLIDATED_PRODUCTS_DDL = """
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
"""

REIMBURSABLE_DRUGS_DDL = """
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
"""

PCID_MAPPINGS_DDL = """
CREATE TABLE IF NOT EXISTS my_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    local_pack_code TEXT NOT NULL,
    presentation TEXT,           -- Pack size/presentation from PCID reference
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
    UNIQUE(run_id, local_pack_code, presentation)
);
CREATE INDEX IF NOT EXISTS idx_my_pcid_code ON my_pcid_mappings(local_pack_code);
CREATE INDEX IF NOT EXISTS idx_my_pcid_code_presentation ON my_pcid_mappings(local_pack_code, presentation);
CREATE INDEX IF NOT EXISTS idx_my_pcid_run ON my_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_my_pcid_pcid ON my_pcid_mappings(pcid);
"""

STEP_PROGRESS_DDL = """
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
"""

BULK_SEARCH_COUNTS_DDL = """
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
"""

EXPORT_REPORTS_DDL = """
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
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS my_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_my_errors_run ON my_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_my_errors_step ON my_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_my_errors_type ON my_errors(error_type);
"""

# Temporary table for PCID reference CSV loading
PCID_REFERENCE_DDL = """
CREATE TABLE IF NOT EXISTS my_pcid_reference (
    id SERIAL PRIMARY KEY,
    pcid TEXT,
    local_pack_code TEXT NOT NULL,
    presentation TEXT,           -- Pack size/presentation for composite key (e.g., "a pack of 1 tube of 20gm")
    package_number TEXT,
    product_group TEXT,
    generic_name TEXT,
    description TEXT,
    UNIQUE(local_pack_code, presentation)
);
CREATE INDEX IF NOT EXISTS idx_my_pcidref_code ON my_pcid_reference(local_pack_code);
CREATE INDEX IF NOT EXISTS idx_my_pcidref_code_presentation ON my_pcid_reference(local_pack_code, presentation);
"""

# Migration: drop uniqueness on my_products to allow duplicates
MIGRATE_MY_PRODUCTS_DROP_UNIQUE = """
DO $$
DECLARE
    cname text;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'my_products') THEN
        SELECT conname INTO cname FROM pg_constraint
        WHERE conrelid = 'my_products'::regclass AND contype = 'u'
        LIMIT 1;
        IF cname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE my_products DROP CONSTRAINT %I', cname);
        END IF;
    END IF;
END $$;
"""

# Migration: update unique constraint on my_pcid_reference to include presentation
MIGRATE_MY_PCID_REFERENCE_CONSTRAINT = """
DO $$
DECLARE
    cname text;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'my_pcid_reference') THEN
        -- Drop old unique constraint if it exists (single column)
        SELECT conname INTO cname FROM pg_constraint
        WHERE conrelid = 'my_pcid_reference'::regclass 
        AND contype = 'u'
        AND array_length(conkey, 1) = 1
        LIMIT 1;
        IF cname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE my_pcid_reference DROP CONSTRAINT %I', cname);
        END IF;
        
        -- Create new composite unique constraint if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'my_pcid_reference'::regclass
            AND contype = 'u'
            AND array_length(conkey, 1) = 2
        ) THEN
            ALTER TABLE my_pcid_reference 
            ADD CONSTRAINT my_pcid_reference_local_pack_code_presentation_key 
            UNIQUE (local_pack_code, presentation);
        END IF;
    END IF;
END $$;
"""

MALAYSIA_SCHEMA_DDL = [
    PRODUCTS_DDL,
    PRODUCT_DETAILS_DDL,
    CONSOLIDATED_PRODUCTS_DDL,
    REIMBURSABLE_DRUGS_DDL,
    PCID_MAPPINGS_DDL,
    STEP_PROGRESS_DDL,
    BULK_SEARCH_COUNTS_DDL,
    EXPORT_REPORTS_DDL,
    PCID_REFERENCE_DDL,
    ERRORS_DDL,
]


def apply_malaysia_schema(db) -> None:
    """Apply all Malaysia-specific DDL to a CountryDB instance."""
    from core.db.models import apply_common_schema
    apply_common_schema(db)
    
    # Migrations for new columns must run BEFORE DDL that creates indexes on them
    # These are safe no-ops if columns already exist
    try:
        db.execute("ALTER TABLE my_pcid_mappings ADD COLUMN IF NOT EXISTS presentation TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE my_pcid_reference ADD COLUMN IF NOT EXISTS presentation TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE my_consolidated_products ADD COLUMN IF NOT EXISTS search_method TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE my_pcid_mappings ADD COLUMN IF NOT EXISTS search_method TEXT")
    except Exception:
        pass
    
    for ddl in MALAYSIA_SCHEMA_DDL:
        db.executescript(ddl)
    
    # Drop any UNIQUE constraint on my_products to allow duplicates
    # Use execute() not executescript() so the DO $$ ... END $$ block is not split on semicolons
    try:
        db.execute(MIGRATE_MY_PRODUCTS_DROP_UNIQUE)
    except Exception:
        pass  # Ignore if already migrated or table doesn't exist
    
    # Update unique constraint on my_pcid_reference to include presentation
    try:
        db.execute(MIGRATE_MY_PCID_REFERENCE_CONSTRAINT)
    except Exception:
        pass  # Ignore if already migrated or table doesn't exist