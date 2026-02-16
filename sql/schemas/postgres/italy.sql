
-- Italy scraper: PostgreSQL schema with it_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Determinas: Document metadata (Step 1)
CREATE TABLE IF NOT EXISTS it_determinas (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    determina_id TEXT NOT NULL, -- UUID from source
    title TEXT,
    publish_date TIMESTAMP,
    typology TEXT,
    metadata JSONB, -- Store full raw JSON
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, determina_id)
);
CREATE INDEX IF NOT EXISTS idx_it_determinas_run ON it_determinas(run_id);
CREATE INDEX IF NOT EXISTS idx_it_determinas_did ON it_determinas(determina_id);

-- Products: Extracted product data from PDFs (Step 3)
CREATE TABLE IF NOT EXISTS it_products (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    determina_id TEXT NOT NULL,
    aic_code TEXT,
    product_name TEXT,
    pack_description TEXT,
    price_ex_factory REAL,
    price_public REAL,
    source_pdf TEXT,
    company TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_it_products_run ON it_products(run_id);
CREATE INDEX IF NOT EXISTS idx_it_products_aic ON it_products(aic_code);
CREATE INDEX IF NOT EXISTS idx_it_products_did ON it_products(determina_id);

-- Step progress: Standard tracking
CREATE TABLE IF NOT EXISTS it_step_progress (
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
CREATE INDEX IF NOT EXISTS idx_it_progress_run_step ON it_step_progress(run_id, step_number);
