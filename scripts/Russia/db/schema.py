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
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_ved_item ON ru_ved_products(item_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_run ON ru_ved_products(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_ved_page ON ru_ved_products(page_number);
"""

# Excluded products (Step 2) - no UNIQUE on (run_id, item_id); data as on website (no dedup)
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
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
    status TEXT CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped', 'ean_missing')),
    error_message TEXT,
    log_details TEXT,
    -- URL for verification
    url TEXT,
    -- Detailed metrics for verification
    rows_found INTEGER DEFAULT 0,
    ean_found INTEGER DEFAULT 0,
    rows_scraped INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    ean_missing INTEGER DEFAULT 0,
    db_count_before INTEGER DEFAULT 0,
    db_count_after INTEGER DEFAULT 0,
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
    status TEXT CHECK(status IN ('pending', 'retrying', 'failed_permanently', 'resolved')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, page_number, source_type)
);
CREATE INDEX IF NOT EXISTS idx_ru_failed_run ON ru_failed_pages(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_failed_status ON ru_failed_pages(status);
"""

# Error tracking
RU_ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS ru_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_errors_run ON ru_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_errors_step ON ru_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_ru_errors_type ON ru_errors(error_type);
"""

# Validation results tracking
RU_VALIDATION_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS ru_validation_results (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    validation_type TEXT NOT NULL,
    table_name TEXT NOT NULL,
    record_id INTEGER,
    field_name TEXT,
    validation_rule TEXT,
    status TEXT CHECK(status IN ('pass', 'fail', 'warning')),
    message TEXT,
    severity TEXT CHECK(severity IN ('info', 'low', 'medium', 'high', 'critical')),
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_val_run ON ru_validation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_val_status ON ru_validation_results(status);
CREATE INDEX IF NOT EXISTS idx_ru_val_severity ON ru_validation_results(severity);
CREATE INDEX IF NOT EXISTS idx_ru_val_table ON ru_validation_results(table_name);
"""

# Statistics tracking
RU_STATISTICS_DDL = """
CREATE TABLE IF NOT EXISTS ru_statistics (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC,
    metric_type TEXT CHECK(metric_type IN ('count', 'duration', 'percentage', 'rate')),
    category TEXT,
    description TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_stats_run ON ru_statistics(run_id);
CREATE INDEX IF NOT EXISTS idx_ru_stats_step ON ru_statistics(step_number);
CREATE INDEX IF NOT EXISTS idx_ru_stats_metric ON ru_statistics(metric_name);
"""

# AI Translation Cache (replaces JSON file cache)
RU_TRANSLATION_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS ru_translation_cache (
    id SERIAL PRIMARY KEY,
    source_text TEXT NOT NULL UNIQUE,
    translated_text TEXT NOT NULL,
    source_language TEXT DEFAULT 'ru',
    target_language TEXT DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ru_trans_cache_source ON ru_translation_cache(source_text);
CREATE INDEX IF NOT EXISTS idx_ru_trans_cache_lookup ON ru_translation_cache(source_language, target_language, source_text);
"""



def apply_russia_schema(db):
    """
    Apply all Russia schema DDL to the database.
    Also applies inputs.sql so ru_input_dictionary exists (no CSV input).
    Args:
        db: CountryDB or PostgresDB instance
    """
    from pathlib import Path
    from core.db.schema_registry import SchemaRegistry

    ddl_statements = [
        RU_VED_PRODUCTS_DDL,
        RU_EXCLUDED_PRODUCTS_DDL,
        RU_TRANSLATED_PRODUCTS_DDL,
        RU_EXPORT_READY_DDL,
        RU_STEP_PROGRESS_DDL,
        RU_FAILED_PAGES_DDL,
        RU_ERRORS_DDL,
        RU_VALIDATION_RESULTS_DDL,
        RU_STATISTICS_DDL,
        RU_TRANSLATION_CACHE_DDL,
    ]

    for ddl in ddl_statements:
        for statement in ddl.split(';'):
            statement = statement.strip()
            if statement:
                db.execute(statement)

    # Ensure input table ru_input_dictionary exists (Russia uses input table, not CSV)
    repo_root = Path(__file__).resolve().parents[3]
    inputs_sql = repo_root / "sql" / "schemas" / "postgres" / "inputs.sql"
    if inputs_sql.exists():
        try:
            SchemaRegistry(db).apply_schema(inputs_sql)
        except Exception:
            pass

    try:
        db.commit()
    except Exception:
        pass  # May autocommit
