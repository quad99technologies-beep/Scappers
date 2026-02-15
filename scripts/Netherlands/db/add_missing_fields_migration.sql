-- Migration to add missing fields to nl_packs table
-- Run this on the Netherlands database before running the updated scraper

-- Add active_substance column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'nl_packs' AND column_name = 'active_substance'
    ) THEN
        ALTER TABLE nl_packs ADD COLUMN active_substance TEXT;
        RAISE NOTICE 'Added column active_substance to nl_packs';
    ELSE
        RAISE NOTICE 'Column active_substance already exists in nl_packs';
    END IF;
END $$;

-- Add manufacturer column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'nl_packs' AND column_name = 'manufacturer'
    ) THEN
        ALTER TABLE nl_packs ADD COLUMN manufacturer TEXT;
        RAISE NOTICE 'Added column manufacturer to nl_packs';
    ELSE
        RAISE NOTICE 'Column manufacturer already exists in nl_packs';
    END IF;
END $$;

-- Add deductible column if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'nl_packs' AND column_name = 'deductible'
    ) THEN
        ALTER TABLE nl_packs ADD COLUMN deductible NUMERIC(12,4);
        RAISE NOTICE 'Added column deductible to nl_packs';
    ELSE
        RAISE NOTICE 'Column deductible already exists in nl_packs';
    END IF;
END $$;

SELECT 'Migration complete!' AS status;
