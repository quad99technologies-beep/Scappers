-- Database Schema for Canada Quebec RAMQ Scraper
-- All tables include mandatory metadata: created_at, updated_at, run_id, scraper_id

-- Run tracking table
CREATE TABLE IF NOT EXISTS scraper_runs (
    run_id UUID PRIMARY KEY,
    scraper_id TEXT NOT NULL,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL DEFAULT 'running',
    total_pages INTEGER,
    total_records INTEGER,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Extracted data table (from step_04_extract_din_data.py)
CREATE TABLE IF NOT EXISTS extracted_data (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    scraper_id TEXT NOT NULL,
    generic TEXT,
    flags TEXT,
    form TEXT,
    strength TEXT,
    strength_value TEXT,
    strength_unit TEXT,
    ppb BOOLEAN,
    din TEXT NOT NULL,
    brand TEXT,
    manufacturer TEXT,
    pack TEXT,
    pack_price NUMERIC(10, 2),
    unit_price NUMERIC(10, 4),
    unit_price_source TEXT,
    page INTEGER,
    confidence NUMERIC(3, 2),
    confidence_label TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_extracted_data_run FOREIGN KEY (run_id) REFERENCES scraper_runs(run_id) ON DELETE CASCADE
);

-- Standard format data table (from step_07_transform_to_standard_format.py)
CREATE TABLE IF NOT EXISTS standard_format_data (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    scraper_id TEXT NOT NULL,
    country TEXT NOT NULL,
    company TEXT,
    local_product_name TEXT,
    generic_name TEXT,
    currency TEXT NOT NULL,
    ex_factory_wholesale_price NUMERIC(10, 2),
    region TEXT NOT NULL,
    marketing_authority TEXT NOT NULL,
    local_pack_description TEXT,
    formulation TEXT,
    fill_size TEXT,
    strength TEXT,
    strength_unit TEXT,
    local_pack_code TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_standard_format_data_run FOREIGN KEY (run_id) REFERENCES scraper_runs(run_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_extracted_data_run_id ON extracted_data(run_id);
CREATE INDEX IF NOT EXISTS idx_extracted_data_scraper_id ON extracted_data(scraper_id);
CREATE INDEX IF NOT EXISTS idx_extracted_data_din ON extracted_data(din);
CREATE INDEX IF NOT EXISTS idx_extracted_data_created_at ON extracted_data(created_at);

CREATE INDEX IF NOT EXISTS idx_standard_format_data_run_id ON standard_format_data(run_id);
CREATE INDEX IF NOT EXISTS idx_standard_format_data_scraper_id ON standard_format_data(scraper_id);
CREATE INDEX IF NOT EXISTS idx_standard_format_data_local_pack_code ON standard_format_data(local_pack_code);
CREATE INDEX IF NOT EXISTS idx_standard_format_data_created_at ON standard_format_data(created_at);

CREATE INDEX IF NOT EXISTS idx_scraper_runs_scraper_id ON scraper_runs(scraper_id);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_started_at ON scraper_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_status ON scraper_runs(status);

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers to auto-update updated_at
DROP TRIGGER IF EXISTS update_extracted_data_updated_at ON extracted_data;
CREATE TRIGGER update_extracted_data_updated_at
    BEFORE UPDATE ON extracted_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_standard_format_data_updated_at ON standard_format_data;
CREATE TRIGGER update_standard_format_data_updated_at
    BEFORE UPDATE ON standard_format_data
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_scraper_runs_updated_at ON scraper_runs;
CREATE TRIGGER update_scraper_runs_updated_at
    BEFORE UPDATE ON scraper_runs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

