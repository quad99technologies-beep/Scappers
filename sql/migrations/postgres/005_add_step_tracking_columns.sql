-- Migration 005: Add Enhanced Step Tracking Columns
-- Adds duration, row metrics, log paths, and run-level aggregation
-- Applied to all country-specific step_progress tables and run_ledger

-- =============================================================================
-- Enhanced step_progress tables (all countries)
-- =============================================================================

-- Add enhanced columns to all step_progress tables
-- Note: This migration uses DO blocks to handle all country prefixes dynamically

DO $$
DECLARE
    tbl_name TEXT;
    prefix TEXT;
    prefixes TEXT[] := ARRAY['ar', 'my', 'ru', 'by', 'nm', 'co', 'cq', 'tc', 'in', 'nl', 'tw', 'mk', 'ca_on', 'ca_qc'];
BEGIN
    FOREACH prefix IN ARRAY prefixes
    LOOP
        tbl_name := prefix || '_step_progress';
        
        -- Check if table exists before adding columns
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = tbl_name
        ) THEN
            -- Add duration tracking
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS duration_seconds REAL', tbl_name);
            
            -- Add row metrics
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rows_read INTEGER DEFAULT 0', tbl_name);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rows_processed INTEGER DEFAULT 0', tbl_name);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rows_inserted INTEGER DEFAULT 0', tbl_name);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rows_updated INTEGER DEFAULT 0', tbl_name);
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS rows_rejected INTEGER DEFAULT 0', tbl_name);
            
            -- Add browser instance tracking
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS browser_instances_spawned INTEGER DEFAULT 0', tbl_name);
            
            -- Add log file path
            EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS log_file_path TEXT', tbl_name);
            
            RAISE NOTICE 'Added enhanced columns to %', tbl_name;
        END IF;
    END LOOP;
END $$;

-- =============================================================================
-- Enhanced run_ledger table
-- =============================================================================

-- Add run-level aggregation columns
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS total_runtime_seconds REAL;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS slowest_step_number INTEGER;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS slowest_step_name TEXT;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS failure_step_number INTEGER;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS failure_step_name TEXT;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS recovery_step_number INTEGER;

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_run_ledger_slowest_step ON run_ledger(slowest_step_number) WHERE slowest_step_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_run_ledger_failure_step ON run_ledger(failure_step_number) WHERE failure_step_number IS NOT NULL;

-- =============================================================================
-- Step retries table (shared across all countries)
-- =============================================================================

CREATE TABLE IF NOT EXISTS step_retries (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    scraper_name TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    retry_number INTEGER NOT NULL, -- 1-based (first retry = 1)
    retry_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_reason TEXT, -- Why this retry was needed
    previous_status TEXT, -- Status before retry
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, step_number, retry_number)
);

CREATE INDEX IF NOT EXISTS idx_step_retries_run_step ON step_retries(run_id, step_number);
CREATE INDEX IF NOT EXISTS idx_step_retries_scraper ON step_retries(scraper_name, retry_at);
CREATE INDEX IF NOT EXISTS idx_step_retries_created ON step_retries(created_at DESC);

-- =============================================================================
-- Update schema version
-- =============================================================================

INSERT INTO _schema_versions (version, filename) 
VALUES (5, '005_add_step_tracking_columns.sql')
ON CONFLICT (version) DO NOTHING;
