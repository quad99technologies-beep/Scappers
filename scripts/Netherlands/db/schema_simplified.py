#!/usr/bin/env python3
"""
Netherlands Database Schema - Simplified
Only includes tables actually used in the pipeline
"""

# =============================================================================
# CORE TABLES (Used in Pipeline)
# =============================================================================

COLLECTED_URLS_DDL = """
-- nl_collected_urls: Product URLs from single search
CREATE TABLE IF NOT EXISTS nl_collected_urls (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    prefix TEXT NOT NULL DEFAULT 'medicijnkosten',
    title TEXT,
    active_substance TEXT,
    manufacturer TEXT,
    document_type TEXT DEFAULT 'medicine',
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
CREATE INDEX IF NOT EXISTS idx_nl_collected_urls_status ON nl_collected_urls(run_id, packs_scraped);
"""

PACKS_DDL = """
-- nl_packs: Product pricing data from medicijnkosten.nl
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
    margin_rule TEXT,
    local_pack_description TEXT,
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
"""

CONSOLIDATED_DDL = """
-- nl_consolidated: Final merged and exported data
CREATE TABLE IF NOT EXISTS nl_consolidated (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_url TEXT,
    product_name TEXT,
    brand_name TEXT,
    manufacturer TEXT,
    formulation TEXT,
    strength_size TEXT,
    pack_description TEXT,
    currency TEXT DEFAULT 'EUR',
    unit_price NUMERIC(12,4),
    ppp_ex_vat NUMERIC(12,4),
    reimbursable_status TEXT,
    copay_price NUMERIC(12,4),
    local_pack_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, product_url, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_nl_consolidated_run ON nl_consolidated(run_id);
"""

CHROME_INSTANCES_DDL = """
-- nl_chrome_instances: Browser instance tracking for cleanup
CREATE TABLE IF NOT EXISTS nl_chrome_instances (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    thread_id INTEGER,
    browser_type TEXT DEFAULT 'chrome',
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    user_data_dir TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    terminated_at TIMESTAMP,
    termination_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_nl_chrome_run ON nl_chrome_instances(run_id);
CREATE INDEX IF NOT EXISTS idx_nl_chrome_active ON nl_chrome_instances(run_id) WHERE terminated_at IS NULL;
"""

ERRORS_DDL = """
-- nl_errors: Error tracking
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
CREATE INDEX IF NOT EXISTS idx_nl_errors_type ON nl_errors(error_type);
"""

# All DDL statements in order
NETHERLANDS_SCHEMA_DDL = [
    COLLECTED_URLS_DDL,
    PACKS_DDL,
    CONSOLIDATED_DDL,
    CHROME_INSTANCES_DDL,
    ERRORS_DDL,
]


def apply_netherlands_schema(db) -> None:
    """Apply Netherlands schema to database"""
    from core.db.models import apply_common_schema
    apply_common_schema(db)
    
    for ddl in NETHERLANDS_SCHEMA_DDL:
        db.executescript(ddl)


def drop_unused_tables(db) -> None:
    """
    Drop unused tables from old schema
    WARNING: This will delete data! Only run if you're sure.
    """
    unused_tables = [
        'nl_search_combinations',  # No longer needed (single URL)
        'nl_details',              # Not used
        'nl_costs',                # Not used
        'nl_products',             # Legacy
        'nl_reimbursement',        # Legacy
        'nl_step_progress',        # Not used
        'nl_export_reports',       # Not used
    ]
    
    for table in unused_tables:
        try:
            db.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            print(f"[CLEANUP] Dropped table: {table}")
        except Exception as e:
            print(f"[WARN] Could not drop {table}: {e}")
    
    db.commit()
    print("[CLEANUP] Unused tables dropped")


if __name__ == "__main__":
    # Test schema application
    from core.db.postgres_connection import get_db
    
    db = get_db("Netherlands")
    print("Applying Netherlands schema...")
    apply_netherlands_schema(db)
    print("Schema applied successfully!")
    
    # Uncomment to drop unused tables:
    # print("\nDropping unused tables...")
    # drop_unused_tables(db)
