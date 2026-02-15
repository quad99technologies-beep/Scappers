-- Migration 004: Remove UNIQUE(run_id, item_id) from ru_excluded_products
-- Data as on website: allow same item to appear multiple times (no dedup).

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ru_excluded_products'::regclass
          AND conname = 'ru_excluded_products_run_id_item_id_key'
    ) THEN
        ALTER TABLE ru_excluded_products DROP CONSTRAINT ru_excluded_products_run_id_item_id_key;
    END IF;
END $$;
