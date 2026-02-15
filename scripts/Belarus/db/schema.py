#!/usr/bin/env python3
"""
Belarus-specific database schema (PostgreSQL).

Tables:
- by_rceth_data: Raw RCETH drug price registry data (Step 1)
- by_pcid_mappings: PCID mapped data for export (Step 2)
- by_step_progress: Sub-step resume tracking (all steps)
- by_export_reports: Generated export/report tracking
- by_final_output: Final merged output (EVERSANA format)
"""

# PostgreSQL uses SERIAL instead of AUTOINCREMENT
# PostgreSQL uses CURRENT_TIMESTAMP instead of datetime('now')
# PostgreSQL uses ON CONFLICT DO UPDATE/NOTHING instead of ON CONFLICT REPLACE

RCETH_DATA_DDL = """
CREATE TABLE IF NOT EXISTS by_rceth_data (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    -- Drug identification
    inn TEXT,
    inn_en TEXT,
    trade_name TEXT,
    trade_name_en TEXT,
    manufacturer TEXT,
    manufacturer_country TEXT,
    -- Drug details
    dosage_form TEXT,
    dosage_form_en TEXT,
    strength TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    -- Registration info
    registration_number TEXT,
    registration_date TEXT,
    registration_valid_to TEXT,
    -- Pricing (in BYN - Belarusian Ruble)
    producer_price REAL,
    producer_price_vat REAL,
    wholesale_price REAL,
    wholesale_price_vat REAL,
    retail_price REAL,
    retail_price_vat REAL,
    import_price REAL,
    import_price_currency TEXT,
    currency TEXT DEFAULT 'BYN',
    -- Classification
    atc_code TEXT,
    who_atc_code TEXT,
    pharmacotherapeutic_group TEXT,
    -- Source tracking
    source_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_number, trade_name, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_by_rceth_run ON by_rceth_data(run_id);
CREATE INDEX IF NOT EXISTS idx_by_rceth_atc ON by_rceth_data(atc_code);
CREATE INDEX IF NOT EXISTS idx_by_rceth_inn ON by_rceth_data(inn);
CREATE INDEX IF NOT EXISTS idx_by_rceth_reg ON by_rceth_data(registration_number);
"""

PCID_MAPPINGS_DDL = """
CREATE TABLE IF NOT EXISTS by_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT NOT NULL,
    local_pack_code TEXT,
    presentation TEXT,
    -- Drug info
    inn TEXT,
    inn_en TEXT,
    trade_name TEXT,
    trade_name_en TEXT,
    manufacturer TEXT,
    manufacturer_country TEXT,
    -- Classification
    atc_code TEXT,
    who_atc_code TEXT,
    -- Pricing
    retail_price REAL,
    retail_price_vat REAL,
    currency TEXT DEFAULT 'BYN',
    -- Location
    country TEXT DEFAULT 'BELARUS',
    region TEXT DEFAULT 'EUROPE',
    -- Source
    source TEXT DEFAULT 'PRICENTRIC',
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, pcid, trade_name, local_pack_code)
);
CREATE INDEX IF NOT EXISTS idx_by_pcid_run ON by_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_by_pcid_code ON by_pcid_mappings(pcid);
CREATE INDEX IF NOT EXISTS idx_by_pcid_atc ON by_pcid_mappings(atc_code);
"""

FINAL_OUTPUT_DDL = """
CREATE TABLE IF NOT EXISTS by_final_output (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    -- EVERSANA standard fields
    pcid TEXT,
    country TEXT DEFAULT 'BELARUS',
    region TEXT DEFAULT 'EUROPE',
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    generic_name_en TEXT,
    -- Drug details
    dosage_form TEXT,
    dosage_form_en TEXT,
    strength TEXT,
    pack_size TEXT,
    local_pack_description TEXT,
    -- Pricing (BYN = Belarusian Ruble)
    producer_price REAL,
    producer_price_vat REAL,
    wholesale_price REAL,
    wholesale_price_vat REAL,
    retail_price REAL,
    retail_price_vat REAL,
    currency TEXT DEFAULT 'BYN',
    -- Classification
    atc_code TEXT,
    who_atc_code TEXT,
    pharmacotherapeutic_group TEXT,
    -- Registration
    registration_number TEXT,
    registration_date TEXT,
    registration_valid_to TEXT,
    -- Source tracking
    source_type TEXT CHECK(source_type IN ('rceth', 'pcid_mapped')),
    source_url TEXT,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, registration_number, local_product_name, pack_size)
);
CREATE INDEX IF NOT EXISTS idx_by_final_run ON by_final_output(run_id);
CREATE INDEX IF NOT EXISTS idx_by_final_pcid ON by_final_output(pcid);
CREATE INDEX IF NOT EXISTS idx_by_final_atc ON by_final_output(atc_code);
CREATE INDEX IF NOT EXISTS idx_by_final_reg ON by_final_output(registration_number);
"""

STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS by_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_by_progress_run_step ON by_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_by_progress_status ON by_step_progress(status);
"""

EXPORT_REPORTS_DDL = """
CREATE TABLE IF NOT EXISTS by_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT,
    row_count INTEGER,
    export_format TEXT DEFAULT 'db',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_export_reports_run ON by_export_reports(run_id);
CREATE INDEX IF NOT EXISTS idx_by_export_reports_type ON by_export_reports(report_type);
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS by_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_errors_run ON by_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_by_errors_step ON by_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_by_errors_type ON by_errors(error_type);
"""

# AI Translation Cache (replaces JSON file cache)
BY_TRANSLATION_CACHE_DDL = """
CREATE TABLE IF NOT EXISTS by_translation_cache (
    id SERIAL PRIMARY KEY,
    source_text TEXT NOT NULL UNIQUE,
    translated_text TEXT NOT NULL,
    source_language TEXT DEFAULT 'ru',
    target_language TEXT DEFAULT 'en',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_by_trans_cache_source ON by_translation_cache(source_text);
CREATE INDEX IF NOT EXISTS idx_by_trans_cache_lookup ON by_translation_cache(source_language, target_language, source_text);
"""

# Translated data (Step 3 - after scraping, before export)
TRANSLATED_DATA_DDL = """
CREATE TABLE IF NOT EXISTS by_translated_data (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    rceth_data_id INTEGER REFERENCES by_rceth_data(id),
    -- Original Russian fields
    inn_ru TEXT,
    trade_name_ru TEXT,
    dosage_form_ru TEXT,
    manufacturer_ru TEXT,
    manufacturer_country_ru TEXT,
    pharmacotherapeutic_group_ru TEXT,
    -- Translated English fields
    inn_en TEXT,
    trade_name_en TEXT,
    dosage_form_en TEXT,
    manufacturer_en TEXT,
    manufacturer_country_en TEXT,
    pharmacotherapeutic_group_en TEXT,
    -- Translation method tracking
    translation_method TEXT CHECK(translation_method IN ('dictionary', 'ai', 'cache', 'none')),
    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, rceth_data_id)
);
CREATE INDEX IF NOT EXISTS idx_by_translated_run ON by_translated_data(run_id);
CREATE INDEX IF NOT EXISTS idx_by_translated_rceth ON by_translated_data(rceth_data_id);
"""

BELARUS_SCHEMA_DDL = [
    RCETH_DATA_DDL,
    PCID_MAPPINGS_DDL,
    FINAL_OUTPUT_DDL,
    STEP_PROGRESS_DDL,
    EXPORT_REPORTS_DDL,
    ERRORS_DDL,
    TRANSLATED_DATA_DDL,
    BY_TRANSLATION_CACHE_DDL,
]


def _migrate_rceth_data_columns(db) -> None:
    """Add import_price columns to by_rceth_data if they don't exist (for existing DBs)."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'by_rceth_data' AND column_name = 'import_price'
        """)
        if cur.fetchone() is None:
            cur.execute("ALTER TABLE by_rceth_data ADD COLUMN import_price REAL")
            cur.execute("ALTER TABLE by_rceth_data ADD COLUMN import_price_currency TEXT")
            db.commit()


def _migrate_translated_data_manufacturer_columns(db) -> None:
    """Add manufacturer_ru and manufacturer_en columns to by_translated_data if they don't exist (for existing DBs)."""
    with db.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'by_translated_data' AND column_name = 'manufacturer_ru'
        """)
        if cur.fetchone() is None:
            cur.execute("ALTER TABLE by_translated_data ADD COLUMN manufacturer_ru TEXT")
            cur.execute("ALTER TABLE by_translated_data ADD COLUMN manufacturer_en TEXT")
            db.commit()


def apply_belarus_schema(db) -> None:
    """
    Apply all Belarus-specific DDL to a CountryDB instance.
    Also applies inputs.sql so by_input_dictionary exists (no CSV input).
    """
    from core.db.models import apply_common_schema
    from pathlib import Path
    from core.db.schema_registry import SchemaRegistry
    
    apply_common_schema(db)
    for ddl in BELARUS_SCHEMA_DDL:
        db.executescript(ddl)
    _migrate_rceth_data_columns(db)
    _migrate_translated_data_manufacturer_columns(db)
    
    # Ensure input table by_input_dictionary exists (Belarus uses input table, not CSV)
    repo_root = Path(__file__).resolve().parents[3]
    inputs_sql = repo_root / "sql" / "schemas" / "postgres" / "inputs.sql"
    if inputs_sql.exists():
        try:
            SchemaRegistry(db).apply_schema(inputs_sql)
        except Exception as e:
            print(f"[WARN] Could not apply inputs.sql schema: {e}")
    
    try:
        db.commit()
    except Exception as e:
        print(f"[WARN] Schema commit note (may autocommit): {e}")