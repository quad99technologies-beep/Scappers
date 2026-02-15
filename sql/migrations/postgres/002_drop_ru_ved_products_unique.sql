-- Migration 002: Remove UNIQUE(run_id, item_id) from ru_ved_products
-- Allows same item_id to appear on multiple pages (no dedup).

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ru_ved_products'::regclass
          AND conname = 'ru_ved_products_run_id_item_id_key'
    ) THEN
        ALTER TABLE ru_ved_products DROP CONSTRAINT ru_ved_products_run_id_item_id_key;
    END IF;
END $$;
