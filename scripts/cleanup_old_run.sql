-- SQL Script to clean up an old run from the database
-- Usage: Update the run_id value below and execute

-- Set the run_id to clean up
-- \set run_id 'Malaysia_20260216_051417'

-- Or use directly:
DO $$
DECLARE
    v_run_id TEXT := 'Malaysia_20260216_051417';
    v_scraper_name TEXT := 'Malaysia';
BEGIN
    -- Delete from run_ledger (this will cascade to related tables if foreign keys exist)
    DELETE FROM run_ledger WHERE run_id = v_run_id;
    
    -- Clean up scraper-specific tables for Malaysia
    -- Product index
    DELETE FROM malaysia_product_index WHERE run_id = v_run_id;
    
    -- Products data
    DELETE FROM malaysia_products WHERE run_id = v_run_id;
    
    -- Progress tracking
    DELETE FROM malaysia_progress WHERE run_id = v_run_id;
    
    -- Errors
    DELETE FROM malaysia_errors WHERE run_id = v_run_id;
    
    -- Artifacts
    DELETE FROM malaysia_artifacts WHERE run_id = v_run_id;
    
    -- Snapshots
    DELETE FROM scrape_stats_snapshots WHERE run_id = v_run_id;
    
    RAISE NOTICE 'Cleaned up run_id: %', v_run_id;
END $$;
