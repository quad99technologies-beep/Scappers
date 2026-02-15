-- Cleanup products added/modified by Step 8 (No-Data Retry)
-- Run this in your PostgreSQL database for Argentina

-- Option 1: Delete products marked as scraped by Step 8 (scrape_source='step7')
-- These are products that were successfully scraped by the no-data retry
DELETE FROM ar_product_index
WHERE run_id = (SELECT run_id FROM run_ledger WHERE country = 'Argentina' ORDER BY started_at DESC LIMIT 1)
  AND scrape_source = 'step7';

-- Option 2: Reset products to their pre-Step-8 state (if you want to keep them but reset status)
-- This sets them back to pending with loop_count=0
UPDATE ar_product_index
SET status = 'pending',
    loop_count = 0,
    total_records = 0,
    scraped_by_selenium = FALSE,
    scraped_by_api = FALSE,
    scrape_source = NULL,
    error_message = NULL,
    updated_at = CURRENT_TIMESTAMP
WHERE run_id = (SELECT run_id FROM run_ledger WHERE country = 'Argentina' ORDER BY started_at DESC LIMIT 1)
  AND scrape_source = 'step7';

-- Option 3: Delete ALL products data scraped by Step 8 (cascade to ar_products)
-- WARNING: This removes actual scraped product data
DELETE FROM ar_products
WHERE run_id = (SELECT run_id FROM run_ledger WHERE country = 'Argentina' ORDER BY started_at DESC LIMIT 1)
  AND id IN (
    SELECT id FROM ar_product_index
    WHERE run_id = (SELECT run_id FROM run_ledger WHERE country = 'Argentina' ORDER BY started_at DESC LIMIT 1)
      AND scrape_source = 'step7'
  );

-- Check how many products would be affected before running
SELECT COUNT(*) as step8_products,
       COUNT(CASE WHEN total_records > 0 THEN 1 END) as successfully_scraped
FROM ar_product_index
WHERE run_id = (SELECT run_id FROM run_ledger WHERE country = 'Argentina' ORDER BY started_at DESC LIMIT 1)
  AND scrape_source = 'step7';
