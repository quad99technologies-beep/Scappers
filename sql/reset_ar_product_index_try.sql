-- Reset ar_product_index table: set all try > 0 back to try = 1
-- This is useful when starting a new scraping session with round-robin retry mode

UPDATE ar_product_index 
SET "try" = 1 
WHERE "try" > 0;

-- Verify the update
SELECT 
    COUNT(*) as total_products,
    SUM(CASE WHEN "try" = 0 THEN 1 ELSE 0 END) as try_0_count,
    SUM(CASE WHEN "try" = 1 THEN 1 ELSE 0 END) as try_1_count,
    SUM(CASE WHEN "try" > 1 THEN 1 ELSE 0 END) as try_above_1_count,
    MAX("try") as max_try_value
FROM ar_product_index;
