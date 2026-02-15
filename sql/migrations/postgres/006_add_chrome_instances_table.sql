-- Migration 006: Add Chrome Instance Tracking Table (Standardized)
-- Creates shared chrome_instances table for all scrapers
-- Replaces country-specific tables (e.g., nl_chrome_instances) with shared table

-- =============================================================================
-- Shared Chrome Instances Table
-- =============================================================================

CREATE TABLE IF NOT EXISTS chrome_instances (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id) ON DELETE CASCADE,
    scraper_name TEXT NOT NULL,
    step_number INTEGER NOT NULL,
    thread_id INTEGER,
    browser_type TEXT DEFAULT 'chrome' CHECK(browser_type IN ('chrome', 'chromium', 'firefox')),
    pid INTEGER NOT NULL,
    parent_pid INTEGER,
    user_data_dir TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    terminated_at TIMESTAMP,
    termination_reason TEXT,
    UNIQUE(run_id, scraper_name, step_number, thread_id, pid)
);

CREATE INDEX IF NOT EXISTS idx_chrome_instances_run ON chrome_instances(run_id, scraper_name);
CREATE INDEX IF NOT EXISTS idx_chrome_instances_active ON chrome_instances(run_id, scraper_name, terminated_at) WHERE terminated_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_chrome_instances_step ON chrome_instances(run_id, scraper_name, step_number);
CREATE INDEX IF NOT EXISTS idx_chrome_instances_pid ON chrome_instances(pid);

-- =============================================================================
-- Migrate existing nl_chrome_instances data (if exists)
-- =============================================================================

DO $$
BEGIN
    -- Check if nl_chrome_instances exists and has data
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'nl_chrome_instances'
    ) THEN
        -- Migrate data to shared table
        INSERT INTO chrome_instances (
            run_id, scraper_name, step_number, thread_id, browser_type,
            pid, parent_pid, user_data_dir, started_at, terminated_at, termination_reason
        )
        SELECT 
            run_id, 'Netherlands', step_number, thread_id, browser_type,
            pid, parent_pid, user_data_dir, started_at, terminated_at, termination_reason
        FROM nl_chrome_instances
        ON CONFLICT (run_id, scraper_name, step_number, thread_id, pid) DO NOTHING;
        
        RAISE NOTICE 'Migrated data from nl_chrome_instances to chrome_instances';
    END IF;
END $$;

-- =============================================================================
-- Update schema version
-- =============================================================================

INSERT INTO _schema_versions (version, filename) 
VALUES (6, '006_add_chrome_instances_table.sql')
ON CONFLICT (version) DO NOTHING;
