#!/usr/bin/env python3
"""
Russia-specific database schema (PostgreSQL).

Tables:
- ru_ved_products: VED pricing data from farmcom.info (Step 1)
- ru_excluded_products: Excluded drugs list (Step 2)
- ru_translated_products: Translated/processed data (Step 3)
- ru_export_ready: Final formatted export data (Step 4)
- ru_step_progress: Sub-step resume tracking (all steps)
- ru_failed_pages: Track failed pages for retry (Step 3)
"""

# VED Products from farmcom.info (Step 1)
RU_VED_PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS ru_ved_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn TEXT,
    inn TEXT,
    manufacturer_country TEXT,
    release_form TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    page_number INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_ved_item ON ru_ved_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_run ON ru_ved_products(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_page ON ru_ved_products(page_number);
"""

# Excluded products (Step 2)
RU_EXCLUDED_PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS ru_excluded_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn TEXT,
    inn TEXT,
    manufacturer_country TEXT,
    release_form TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    page_number INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_excl_item ON ru_excluded_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_excl_run ON ru_excluded_products(run_id);
"""

# Translated/processed products (Step 3)
RU_TRANSLATED_PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS ru_translated_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    tn_ru TEXT,
    tn_en TEXT,
    inn_ru TEXT,
    inn_en TEXT,
    manufacturer_country_ru TEXT,
    manufacturer_country_en TEXT,
    release_form_ru TEXT,
    release_form_en TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_text TEXT,
    start_date_iso DATE,
    translation_method TEXT CHECK(translation_method IN ('dictionary', 'ai', 'none')),
    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_trans_item ON ru_translated_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_trans_run ON ru_translated_products(run_id);
"""

# Export-ready formatted data (Step 4)
RU_EXPORT_READY_DDL = """
CREATE TABLE IF NOT EXISTS ru_export_ready (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    item_id TEXT NOT NULL,
    trade_name_en TEXT,
    inn_en TEXT,
    manufacturer_country_en TEXT,
    dosage_form_en TEXT,
    ean TEXT,
    registered_price_rub TEXT,
    start_date_iso DATE,
    source_type TEXT CHECK(source_type IN ('ved', 'excluded')),
    formatted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_ru_export_run ON ru_export_ready(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_export_type ON ru_export_ready(source_type);
"""

# Step progress tracking (sub-step resume)
RU_STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS ru_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_ru_prog_run ON ru_step_progress(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_prog_step ON ru_step_progress(step_number, status);
"""

# Failed pages tracking (for retry mechanism)
RU_FAILED_PAGES_DDL = """
CREATE TABLE IF NOT EXISTS ru_failed_pages (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    page_number INTEGER NOT NULL,
    source_type TEXT CHECK(source_type IN ('ved', 'excluded')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    last_retry_at TIMESTAMP,
    status TEXT CHECK(status IN ('pending', 'retrying', 'failed_permanently')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, page_number, source_type)
);
CREATE INDEX IF NOT EXISTS idx_ru_failed_run ON ru_failed_pages(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_failed_status ON ru_failed_pages(status);
"""


def apply_russia_schema(db):
    """
    Apply all Russia schema DDL to the database.
    
    Args:
        db: CountryDB or PostgresDB instance
    """
    ddl_statements = [
        RU_VED_PRODUCTS_DDL,
        RU_EXCLUDED_PRODUCTS_DDL,
        RU_TRANSLATED_PRODUCTS_DDL,
        RU_EXPORT_READY_DDL,
        RU_STEP_PROGRESS_DDL,
        RU_FAILED_PAGES_DDL,
    ]
    
    for ddl in ddl_statements:
        # Split by semicolon and execute each statement
        for statement in ddl.split(';'):
            statement = statement.strip()
            if statement:
                db.execute(statement)
    
    try:
        db.commit()
    except Exception:
        pass  # May autocommit
