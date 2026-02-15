-- India: Clean duplicate data and ensure unique indexes exist
-- Run this ONCE to fix existing duplicate data before the scraper can work properly

-- 1. Remove duplicate rows from in_sku_mrp (keep oldest by id)
DELETE FROM in_sku_mrp a
USING in_sku_mrp b
WHERE a.id > b.id
  AND a.hidden_id = b.hidden_id
  AND a.run_id = b.run_id;

-- 2. Remove duplicate rows from in_med_details (keep oldest by id)
DELETE FROM in_med_details a
USING in_med_details b
WHERE a.id > b.id
  AND a.hidden_id = b.hidden_id
  AND a.run_id = b.run_id;

-- 3. Remove duplicate rows from in_brand_alternatives (keep oldest by id)
DELETE FROM in_brand_alternatives a
USING in_brand_alternatives b
WHERE a.id > b.id
  AND a.hidden_id = b.hidden_id
  AND a.brand_name = b.brand_name
  AND a.pack_size = b.pack_size
  AND a.run_id = b.run_id;

-- 4. Remove duplicate rows from in_sku_main (keep oldest by id)
DELETE FROM in_sku_main a
USING in_sku_main b
WHERE a.id > b.id
  AND a.hidden_id = b.hidden_id
  AND a.run_id = b.run_id;

-- 5. Now create/recreate the unique indexes (will succeed after dedup)
DROP INDEX IF EXISTS idx_in_skumrp_unique;
CREATE UNIQUE INDEX idx_in_skumrp_unique ON in_sku_mrp(hidden_id, run_id);

DROP INDEX IF EXISTS idx_in_med_unique;
CREATE UNIQUE INDEX idx_in_med_unique ON in_med_details(hidden_id, run_id);

DROP INDEX IF EXISTS idx_in_brand_unique;
CREATE UNIQUE INDEX idx_in_brand_unique ON in_brand_alternatives(hidden_id, brand_name, pack_size, run_id);

DROP INDEX IF EXISTS idx_in_sku_hid;
CREATE UNIQUE INDEX idx_in_sku_hid ON in_sku_main(hidden_id, run_id);

-- 6. Verify no duplicates remain
DO $$
DECLARE
    dup_count INTEGER;
BEGIN
    -- Check in_sku_mrp
    SELECT COUNT(*) - COUNT(DISTINCT (hidden_id, run_id)) INTO dup_count FROM in_sku_mrp;
    IF dup_count > 0 THEN
        RAISE EXCEPTION 'in_sku_mrp still has % duplicates!', dup_count;
    END IF;

    -- Check in_med_details
    SELECT COUNT(*) - COUNT(DISTINCT (hidden_id, run_id)) INTO dup_count FROM in_med_details;
    IF dup_count > 0 THEN
        RAISE EXCEPTION 'in_med_details still has % duplicates!', dup_count;
    END IF;

    -- Check in_brand_alternatives
    SELECT COUNT(*) - COUNT(DISTINCT (hidden_id, brand_name, pack_size, run_id)) INTO dup_count FROM in_brand_alternatives;
    IF dup_count > 0 THEN
        RAISE EXCEPTION 'in_brand_alternatives still has % duplicates!', dup_count;
    END IF;

    -- Check in_sku_main
    SELECT COUNT(*) - COUNT(DISTINCT (hidden_id, run_id)) INTO dup_count FROM in_sku_main;
    IF dup_count > 0 THEN
        RAISE EXCEPTION 'in_sku_main still has % duplicates!', dup_count;
    END IF;

    RAISE NOTICE 'All India tables are now duplicate-free ✓';
END $$;
