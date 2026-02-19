-- Cleanup script for Malaysia run
-- Run this in your PostgreSQL database

-- Replace with your run_id
-- \set run_id 'Malaysia_20260216_051417'

-- Delete from tables with foreign key references first
DELETE FROM http_requests WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM scrape_stats_snapshots WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM pipeline_checkpoints WHERE run_id = 'Malaysia_20260216_051417';

-- Delete from Malaysia-specific tables
DELETE FROM malaysia_product_index WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM malaysia_products WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM malaysia_product_details WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM malaysia_progress WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM malaysia_errors WHERE run_id = 'Malaysia_20260216_051417';
DELETE FROM malaysia_artifacts WHERE run_id = 'Malaysia_20260216_051417';

-- Finally delete from run_ledger
DELETE FROM run_ledger WHERE run_id = 'Malaysia_20260216_051417';

-- Verify deletion
SELECT run_id, scraper_name, status FROM run_ledger WHERE run_id = 'Malaysia_20260216_051417';
