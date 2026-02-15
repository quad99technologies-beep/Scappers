-- Alter script to update existing tc_tender_details and tc_tender_awards tables
-- Run this to add missing columns for complete data storage

-- Add province column to tender_details if not exists
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_details' AND column_name = 'province'
    ) THEN
        ALTER TABLE tc_tender_details ADD COLUMN province TEXT;
    END IF;
END $$;

-- Add new columns to tender_awards if not exists
DO $$
BEGIN
    -- un_classification_code (Unique Lot ID)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'un_classification_code'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN un_classification_code TEXT;
    END IF;

    -- buyer_specifications (Lot Title from award page)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'buyer_specifications'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN buyer_specifications TEXT;
    END IF;

    -- lot_quantity
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'lot_quantity'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN lot_quantity TEXT;
    END IF;

    -- supplier_specifications (AWARDED LOT TITLE)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'supplier_specifications'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN supplier_specifications TEXT;
    END IF;

    -- unit_price_offer (bidder's unit price)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'unit_price_offer'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN unit_price_offer REAL;
    END IF;

    -- awarded_quantity
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'awarded_quantity'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN awarded_quantity TEXT;
    END IF;

    -- total_net_awarded
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'total_net_awarded'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN total_net_awarded REAL;
    END IF;

    -- is_awarded (YES/NO flag)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'is_awarded'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN is_awarded TEXT;
    END IF;

    -- awarded_unit_price
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'awarded_unit_price'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN awarded_unit_price REAL;
    END IF;

    -- source_tender_url
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tc_tender_awards' AND column_name = 'source_tender_url'
    ) THEN
        ALTER TABLE tc_tender_awards ADD COLUMN source_tender_url TEXT;
    END IF;
END $$;
