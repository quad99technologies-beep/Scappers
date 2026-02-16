#!/usr/bin/env python3
"""
Canada Ontario-specific database schema (PostgreSQL).

Tables:
- co_products: Product details from Ontario formulary (Step 1)
- co_manufacturers: Manufacturer master data (Step 1)
- co_eap_prices: EAP prices from Ontario.ca (Step 2)
- co_step_progress: Sub-step resume tracking (all steps)
- co_export_reports: Generated export/report tracking
- co_final_output: Final merged output (EVERSANA format)
- co_pcid_mappings: PCID mapped data for export
"""

# PostgreSQL uses SERIAL instead of AUTOINCREMENT
# PostgreSQL uses CURRENT_TIMESTAMP instead of datetime('now')
# PostgreSQL uses ON CONFLICT DO UPDATE/NOTHING instead of ON CONFLICT REPLACE

PRODUCTS_DDL = """
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
    reimbursable_price REAL,
    public_with_vat REAL,
    copay REAL,
    interchangeability TEXT,
    benefit_status TEXT,
    price_type TEXT,
    limited_use TEXT,
    therapeutic_notes TEXT,
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_products_run ON co_products(run_id);
CREATE INDEX IF NOT EXISTS idx_co_products_din ON co_products(din);
CREATE INDEX IF NOT EXISTS idx_co_products_manufacturer ON co_products(manufacturer_code);
"""

MANUFACTURERS_DDL = """
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
"""

EAP_PRICES_DDL = """
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
"""

# Final output table - EVERSANA format, no CSV reference
FINAL_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS co_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    -- EVERSANA standard fields
    pcid TEXT,
    country TEXT DEFAULT 'CANADA',
    region TEXT DEFAULT 'NORTH AMERICA',
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    -- Pricing fields
    unit_price REAL,
    public_with_vat_price REAL,
    public_without_vat_price REAL,
    eap_price REAL,
    currency TEXT DEFAULT 'CAD',
    -- Reimbursement
    reimbursement_category TEXT,
    reimbursement_amount REAL,
    copay_amount REAL,
    benefit_status TEXT,
    interchangeability TEXT,
    -- Product details
    din TEXT,
    strength TEXT,
    dosage_form TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    local_pack_code TEXT,
    -- Dates
    effective_start_date TEXT,
    effective_end_date TEXT,
    -- Source tracking
    source TEXT DEFAULT 'PRICENTRIC',
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, din, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_co_final_output_run ON co_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_co_final_output_pcid ON co_final_output(pcid);
CREATE INDEX IF NOT EXISTS idx_co_final_output_din ON co_final_output(din);
"""

PCID_MAPPINGS_DDL = """
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
"""

STEP_PROGRESS_DDL = """
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
"""

EXPORT_REPORTS_DDL = """
CREATE TABLE IF NOT EXISTS co_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',  -- 'db' means database-only, no file
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_co_export_reports_run ON co_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_co_export_reports_type ON co_export_reports(report_type);
"""

ERRORS_DDL = """
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
"""

CANADA_ONTARIO_SCHEMA_DDL = [
    PRODUCTS_DDL,
    MANUFACTURERS_DDL,
    EAP_PRICES_DDL,
    FINAL_OUTPUT_DDL,
    PCID_MAPPINGS_DDL,
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,
]


def _migrate_co_products_columns(db) -> None:
    """Add new columns to co_products if they don't exist (for existing deployments)."""
    alters = [
        "ALTER TABLE co_products ADD COLUMN reimbursable_price REAL",
        "ALTER TABLE co_products ADD COLUMN public_with_vat REAL",
        "ALTER TABLE co_products ADD COLUMN copay REAL",
        "ALTER TABLE co_products ADD COLUMN price_type TEXT",
        "ALTER TABLE co_products ADD COLUMN limited_use TEXT",
        "ALTER TABLE co_products ADD COLUMN therapeutic_notes TEXT",
    ]
    for sql in alters:
        try:
            db.execute(sql)
        except Exception:
            pass  # Column may already exist


def apply_canada_ontario_schema(db) -> None:
    """Apply all Canada Ontario-specific DDL to a CountryDB instance."""
    from core.db.models import apply_common_schema
    apply_common_schema(db)
    for ddl in CANADA_ONTARIO_SCHEMA_DDL:
        db.executescript(ddl)
    _migrate_co_products_columns(db)
