-- India NPPA scraper: PostgreSQL schema with in_ prefix
-- Applied via SchemaRegistry.apply_schema()

-- Formulation lookup cache (from formulationListNew API)
CREATE TABLE IF NOT EXISTS in_formulation_map (
    formulation_id TEXT PRIMARY KEY,
    formulation_name TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_in_fmap_name ON in_formulation_map(formulation_name);

-- Main SKU rows from formulationDataTableNew API
CREATE TABLE IF NOT EXISTS in_sku_main (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    formulation TEXT NOT NULL,
    hidden_id TEXT NOT NULL,
    sku_name TEXT,
    company TEXT,
    composition TEXT,
    pack_size TEXT,
    dosage_form TEXT,
    schedule_status TEXT,
    ceiling_price TEXT,
    mrp TEXT,
    mrp_per_unit TEXT,
    year_month TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_sku_hid ON in_sku_main(hidden_id, run_id);
CREATE INDEX IF NOT EXISTS idx_in_sku_run ON in_sku_main(run_id);
CREATE INDEX IF NOT EXISTS idx_in_sku_form ON in_sku_main(formulation);

-- SKU MRP detail (from skuMrpNew API)
CREATE TABLE IF NOT EXISTS in_sku_mrp (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    hidden_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_skumrp_unique ON in_sku_mrp(hidden_id, run_id);
CREATE INDEX IF NOT EXISTS idx_in_skumrp_hid ON in_sku_mrp(hidden_id);

-- Other brand alternatives (from otherBrandPriceNew API)
CREATE TABLE IF NOT EXISTS in_brand_alternatives (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    hidden_id TEXT NOT NULL,
    formulation TEXT,  -- Denormalized for fast queries without JOIN
    brand_name TEXT,
    company TEXT,
    pack_size TEXT,
    brand_mrp TEXT,
    mrp_per_unit TEXT,
    year_month TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Unique constraint to prevent duplicate brand alternatives per SKU per run
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_brand_unique ON in_brand_alternatives(hidden_id, brand_name, pack_size, run_id);
CREATE INDEX IF NOT EXISTS idx_in_brand_hid ON in_brand_alternatives(hidden_id);
CREATE INDEX IF NOT EXISTS idx_in_brand_run ON in_brand_alternatives(run_id);
CREATE INDEX IF NOT EXISTS idx_in_brand_form ON in_brand_alternatives(formulation);

-- Medicine details (from medDtlsNew API)
CREATE TABLE IF NOT EXISTS in_med_details (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    hidden_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_in_med_unique ON in_med_details(hidden_id, run_id);
CREATE INDEX IF NOT EXISTS idx_in_med_hid ON in_med_details(hidden_id);

-- Formulation processing status (work queue for parallel workers)
-- PRIMARY KEY is (formulation, run_id) so each run has its own queue
CREATE TABLE IF NOT EXISTS in_formulation_status (
    formulation TEXT NOT NULL,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK(status IN ('pending', 'in_progress', 'completed', 'zero_records', 'failed', 'blocked', 'blocked_captcha')),
    worker_id INTEGER,
    claimed_by INTEGER,
    claimed_at TIMESTAMP,
    medicines_count INTEGER DEFAULT 0,
    substitutes_count INTEGER DEFAULT 0,
    error_message TEXT,
    attempts INTEGER DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (formulation, run_id)
);
CREATE INDEX IF NOT EXISTS idx_in_fstatus_run ON in_formulation_status(run_id);
CREATE INDEX IF NOT EXISTS idx_in_fstatus_status ON in_formulation_status(status);
CREATE INDEX IF NOT EXISTS idx_in_fstatus_worker ON in_formulation_status(worker_id);
CREATE INDEX IF NOT EXISTS idx_in_fstatus_claimed ON in_formulation_status(claimed_by, claimed_at);

-- Progress snapshots for tracking run progress over time
CREATE TABLE IF NOT EXISTS in_progress_snapshots (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    snapshot_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    pending INTEGER DEFAULT 0,
    in_progress INTEGER DEFAULT 0,
    completed INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    zero_records INTEGER DEFAULT 0,
    items_scraped INTEGER DEFAULT 0,
    rate_per_min REAL DEFAULT 0.0
);
CREATE INDEX IF NOT EXISTS idx_in_snap_run ON in_progress_snapshots(run_id);

-- in_input_formulations is now defined in inputs.sql (shared input tables)

-- Error tracking
CREATE TABLE IF NOT EXISTS in_errors (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    error_type TEXT,
    error_message TEXT NOT NULL,
    context JSONB,
    step_number INTEGER,
    step_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_in_errors_run ON in_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_in_errors_step ON in_errors(step_number);
CREATE INDEX IF NOT EXISTS idx_in_errors_type ON in_errors(error_type);
