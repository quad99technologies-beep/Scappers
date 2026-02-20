
-- Italy scraper: PostgreSQL schema with it_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Determinas: Document metadata (Step 1)
CREATE TABLE IF NOT EXISTS it_determinas (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    determina_id TEXT NOT NULL, -- UUID from source
    source_keyword TEXT, -- "DET" vs "Riduzione" (search keyword used in Step 1)
    title TEXT,
    publish_date TIMESTAMP,
    typology TEXT,
    metadata JSONB, -- Store full raw JSON
    detail JSONB,   -- Step 2 detail payload (TNF/MSF), stored in DB (no JSON files)
    detail_scraped_at TIMESTAMP,
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

-- Step progress: Standard tracking (with per-window record count)
CREATE TABLE IF NOT EXISTS it_step_progress (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    progress_key TEXT NOT NULL,
    status TEXT DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    error_message TEXT,
    records_fetched INTEGER DEFAULT 0,
    api_total_count INTEGER DEFAULT 0, -- Total reported by API (elementAvailableNum)
    retry_count INTEGER DEFAULT 0,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    UNIQUE(run_id, step_number, progress_key)
);
CREATE INDEX IF NOT EXISTS idx_it_progress_run_step ON it_step_progress(run_id, step_number);

-- Run statistics (keyword + step scoped metrics)
CREATE TABLE IF NOT EXISTS it_run_stats (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    keyword TEXT NOT NULL, -- e.g. DET, Riduzione, or '*' for global
    step_number INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, keyword, step_number, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_it_run_stats_run ON it_run_stats(run_id);
CREATE INDEX IF NOT EXISTS idx_it_run_stats_key ON it_run_stats(run_id, keyword, step_number);

-- Idempotent: add columns if missing
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'it_determinas' AND column_name = 'source_keyword'
    ) THEN
        ALTER TABLE it_determinas ADD COLUMN source_keyword TEXT;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'it_determinas' AND column_name = 'detail'
    ) THEN
        ALTER TABLE it_determinas ADD COLUMN detail JSONB;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'it_determinas' AND column_name = 'detail_scraped_at'
    ) THEN
        ALTER TABLE it_determinas ADD COLUMN detail_scraped_at TIMESTAMP;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'it_step_progress' AND column_name = 'records_fetched'
    ) THEN
        ALTER TABLE it_step_progress ADD COLUMN records_fetched INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'it_step_progress' AND column_name = 'api_total_count'
    ) THEN
        ALTER TABLE it_step_progress ADD COLUMN api_total_count INTEGER DEFAULT 0;
    END IF;
END$$;

-- Indexes for newly-added columns (must come after idempotent ALTERs above)
CREATE INDEX IF NOT EXISTS idx_it_determinas_keyword ON it_determinas(run_id, source_keyword);

-- Convenience views: "table name with keyword" without duplicating storage.
CREATE OR REPLACE VIEW it_det_determinas AS
SELECT * FROM it_determinas WHERE UPPER(COALESCE(source_keyword, '')) = 'DET';

CREATE OR REPLACE VIEW it_riduzione_determinas AS
SELECT * FROM it_determinas WHERE LOWER(COALESCE(source_keyword, '')) = 'riduzione';

CREATE OR REPLACE VIEW it_det_products AS
SELECT p.*, d.source_keyword
FROM it_products p
JOIN it_determinas d
  ON d.run_id = p.run_id AND d.determina_id = p.determina_id
WHERE UPPER(COALESCE(d.source_keyword, '')) = 'DET';

CREATE OR REPLACE VIEW it_riduzione_products AS
SELECT p.*, d.source_keyword
FROM it_products p
JOIN it_determinas d
  ON d.run_id = p.run_id AND d.determina_id = p.determina_id
WHERE LOWER(COALESCE(d.source_keyword, '')) = 'riduzione';

CREATE OR REPLACE VIEW it_det_step1_progress AS
SELECT *
FROM it_step_progress
WHERE step_number = 1 AND progress_key LIKE 'DET:%';

CREATE OR REPLACE VIEW it_riduzione_step1_progress AS
SELECT *
FROM it_step_progress
WHERE step_number = 1 AND progress_key LIKE 'Riduzione:%';

CREATE OR REPLACE VIEW it_det_step2_progress AS
SELECT sp.*
FROM it_step_progress sp
JOIN it_determinas d
  ON d.run_id = sp.run_id AND d.determina_id = sp.progress_key
WHERE sp.step_number = 2 AND UPPER(COALESCE(d.source_keyword, '')) = 'DET';

CREATE OR REPLACE VIEW it_riduzione_step2_progress AS
SELECT sp.*
FROM it_step_progress sp
JOIN it_determinas d
  ON d.run_id = sp.run_id AND d.determina_id = sp.progress_key
WHERE sp.step_number = 2 AND LOWER(COALESCE(d.source_keyword, '')) = 'riduzione';
