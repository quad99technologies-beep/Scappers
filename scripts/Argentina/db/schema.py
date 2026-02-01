#!/usr/bin/env python3
"""
Argentina-specific PostgreSQL schema (ar_ prefix).

Tables:
- ar_product_index       : Product + company pairs sourced from AlfaBeta (prep/queue)
- ar_products            : Scraped product details (selenium/api)
- ar_products_translated : English-normalised view after dictionary translation
- ar_errors              : Per-product error log
- ar_step_progress       : Sub-step resume tracking
- ar_dictionary          : ES->EN dictionary entries (authoritative, replaces Dictionary.csv)
- ar_pcid_reference      : PCID mapping reference (replaces PCID Mapping - Argentina.csv)
- ar_pcid_mappings       : Final PCID mapped rows (export source)
- ar_export_reports      : Generated export/report tracking
"""

PRODUCT_INDEX_DDL = """
CREATE TABLE IF NOT EXISTS ar_product_index (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product TEXT NOT NULL,
    company TEXT NOT NULL,
    url TEXT,
    loop_count INTEGER DEFAULT 0,
    total_records INTEGER DEFAULT 0,
    scraped_by_selenium BOOLEAN DEFAULT FALSE,
    scraped_by_api BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending','in_progress','completed','failed','skipped')),
    last_attempt_at TIMESTAMP,
    last_attempt_source TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, company, product)
);
CREATE INDEX IF NOT EXISTS idx_ar_product_index_run ON ar_product_index(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_product_index_status ON ar_product_index(status);
"""

PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS ar_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    record_hash TEXT,
    input_company TEXT,
    input_product_name TEXT,
    company TEXT,
    product_name TEXT,
    active_ingredient TEXT,
    therapeutic_class TEXT,
    description TEXT,
    price_ars REAL,
    price_raw TEXT,
    date TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sifar_detail TEXT,
    pami_af TEXT,
    pami_os TEXT,
    ioma_detail TEXT,
    ioma_af TEXT,
    ioma_os TEXT,
    import_status TEXT,
    coverage_json TEXT,
    source TEXT CHECK(source IN ('selenium','api','manual')) DEFAULT 'selenium',
    UNIQUE(run_id, record_hash)
);
CREATE INDEX IF NOT EXISTS idx_ar_products_run ON ar_products(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_products_company_prod ON ar_products(input_company, input_product_name);
CREATE INDEX IF NOT EXISTS idx_ar_products_run_input ON ar_products(run_id, input_company, input_product_name);
"""

PRODUCTS_TRANSLATED_DDL = """
CREATE TABLE IF NOT EXISTS ar_products_translated (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    product_id INTEGER REFERENCES ar_products(id) ON DELETE CASCADE,
    company TEXT,
    product_name TEXT,
    active_ingredient TEXT,
    therapeutic_class TEXT,
    description TEXT,
    price_ars REAL,
    date TEXT,
    sifar_detail TEXT,
    pami_af TEXT,
    pami_os TEXT,
    ioma_detail TEXT,
    ioma_af TEXT,
    ioma_os TEXT,
    import_status TEXT,
    coverage_json TEXT,
    translated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    translation_source TEXT,
    UNIQUE(run_id, product_id)
);
CREATE INDEX IF NOT EXISTS idx_ar_products_translated_run ON ar_products_translated(run_id);
"""

ERRORS_DDL = """
CREATE TABLE IF NOT EXISTS ar_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    input_company TEXT,
    input_product_name TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_errors_run ON ar_errors(run_id);
"""

STEP_PROGRESS_DDL = """
CREATE TABLE IF NOT EXISTS ar_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending','in_progress','completed','failed','skipped')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_ar_progress_run_step ON ar_step_progress(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_ar_progress_status ON ar_step_progress(status);
"""

DICTIONARY_DDL = """
CREATE TABLE IF NOT EXISTS ar_dictionary (
    id SERIAL PRIMARY KEY,
    es TEXT NOT NULL,
    en TEXT NOT NULL,
    source TEXT DEFAULT 'file',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(es)
);
CREATE INDEX IF NOT EXISTS idx_ar_dictionary_es ON ar_dictionary(es);
"""

PCID_REFERENCE_DDL = """
CREATE TABLE IF NOT EXISTS ar_pcid_reference (
    id SERIAL PRIMARY KEY,
    pcid TEXT,
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    local_pack_description TEXT,
    UNIQUE(company, local_product_name, generic_name, local_pack_description)
);
"""

IGNORE_LIST_DDL = """
CREATE TABLE IF NOT EXISTS ar_ignore_list (
    id SERIAL PRIMARY KEY,
    company TEXT NOT NULL,
    product TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company, product)
);
CREATE INDEX IF NOT EXISTS idx_ar_ignore_company_product ON ar_ignore_list(company, product);
"""

PCID_MAPPINGS_DDL = """
CREATE TABLE IF NOT EXISTS ar_pcid_mappings (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    pcid TEXT,
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    local_pack_description TEXT,
    price_ars REAL,
    source TEXT DEFAULT 'PRICENTRIC',
    mapped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, company, local_product_name, generic_name, local_pack_description)
);
CREATE INDEX IF NOT EXISTS idx_ar_pcid_mappings_run ON ar_pcid_mappings(run_id);
CREATE INDEX IF NOT EXISTS idx_ar_pcid_mappings_pcid ON ar_pcid_mappings(pcid);
"""

EXPORT_REPORTS_DDL = """
CREATE TABLE IF NOT EXISTS ar_export_reports (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    report_type TEXT NOT NULL,
    file_path TEXT NOT NULL,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ar_export_reports_run ON ar_export_reports(run_id);
"""

ARGENTINA_SCHEMA_DDL = [
    PRODUCT_INDEX_DDL,
    PRODUCTS_DDL,
    PRODUCTS_TRANSLATED_DDL,
    ERRORS_DDL,
    STEP_PROGRESS_DDL,
    DICTIONARY_DDL,
    PCID_REFERENCE_DDL,
    IGNORE_LIST_DDL,
    PCID_MAPPINGS_DDL,
    EXPORT_REPORTS_DDL,
]


def apply_argentina_schema(db) -> None:
    """Apply all Argentina-specific DDL to a CountryDB/PostgresDB instance."""
    from core.db.models import apply_common_schema

    apply_common_schema(db)
    for ddl in ARGENTINA_SCHEMA_DDL:
        db.executescript(ddl)

    # Lightweight migrations (must use execute(), not executescript()) because executescript
    # naively splits on semicolons and cannot safely run procedural blocks.
    try:
        db.execute("ALTER TABLE ar_products ADD COLUMN IF NOT EXISTS record_hash TEXT")
    except Exception:
        pass

    # Drop any legacy UNIQUE constraints (previously UNIQUE(run_id, input_company, input_product_name, source))
    # so we can store multiple presentation rows per product.
    try:
        db.execute(
            """
            DO $$
            DECLARE c record;
            BEGIN
              FOR c IN
                SELECT conname
                FROM pg_constraint
                WHERE conrelid = 'ar_products'::regclass
                  AND contype = 'u'
              LOOP
                EXECUTE format('ALTER TABLE ar_products DROP CONSTRAINT IF EXISTS %I', c.conname);
              END LOOP;
            END $$
            """
        )
    except Exception:
        pass

    # Backfill hash for existing rows (best-effort).
    try:
        db.execute(
            """
            UPDATE ar_products
               SET record_hash = md5(
                   COALESCE(source,'') || '|' ||
                   COALESCE(input_company,'') || '|' ||
                   COALESCE(input_product_name,'') || '|' ||
                   COALESCE(company,'') || '|' ||
                   COALESCE(product_name,'') || '|' ||
                   COALESCE(active_ingredient,'') || '|' ||
                   COALESCE(therapeutic_class,'') || '|' ||
                   COALESCE(description,'') || '|' ||
                   COALESCE(price_raw,'') || '|' ||
                   COALESCE(date,'') || '|' ||
                   COALESCE(import_status,'') || '|' ||
                   COALESCE(coverage_json,'')
               )
             WHERE record_hash IS NULL OR record_hash = ''
            """
        )
    except Exception:
        pass

    # If legacy runs produced duplicates, remove exact duplicates before enforcing uniqueness.
    try:
        db.execute(
            """
            DELETE FROM ar_products a
            USING ar_products b
            WHERE a.id < b.id
              AND a.run_id = b.run_id
              AND COALESCE(a.record_hash,'') <> ''
              AND a.record_hash = b.record_hash
            """
        )
    except Exception:
        pass

    # Enforce uniqueness per scraped row (run_id + record_hash).
    try:
        db.execute("ALTER TABLE ar_products ALTER COLUMN record_hash SET NOT NULL")
    except Exception:
        pass

    try:
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ar_products_run_hash ON ar_products(run_id, record_hash)")
    except Exception:
        # If the index creation fails due to duplicates, one more dedupe pass and retry.
        db.execute(
            """
            DELETE FROM ar_products a
            USING ar_products b
            WHERE a.id < b.id
              AND a.run_id = b.run_id
              AND COALESCE(a.record_hash,'') <> ''
              AND a.record_hash = b.record_hash
            """
        )
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ar_products_run_hash ON ar_products(run_id, record_hash)")
