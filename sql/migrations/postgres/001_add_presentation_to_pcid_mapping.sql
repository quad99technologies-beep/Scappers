-- Migration 001: Add presentation column to pcid_mapping table
-- This column was added to support composite unique key (local_pack_code, presentation, source_country)

DO $$
BEGIN
    -- Add presentation column if it doesn't exist
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'pcid_mapping' 
        AND column_name = 'presentation'
    ) THEN
        ALTER TABLE pcid_mapping ADD COLUMN presentation TEXT;
        
        -- Add comment explaining the column
        COMMENT ON COLUMN pcid_mapping.presentation IS 'Pack size/presentation for composite key (e.g., "a pack of 1 tube of 20gm")';
    END IF;
END $$;

-- Drop and recreate the unique constraint to include presentation (if it exists with different columns)
DO $$
BEGIN
    -- Check if the old unique constraint exists (without presentation)
    IF EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'pcid_mapping_local_pack_code_source_country_key'
    ) THEN
        ALTER TABLE pcid_mapping DROP CONSTRAINT pcid_mapping_local_pack_code_source_country_key;
    END IF;
    
    -- Check if the new unique constraint exists (with presentation)
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_constraint 
        WHERE conname = 'pcid_mapping_local_pack_code_presentation_source_country_key'
    ) THEN
        -- Only add constraint if presentation column exists and is not null for existing rows
        -- For existing rows with NULL presentation, we'll allow duplicates temporarily
        ALTER TABLE pcid_mapping 
        ADD CONSTRAINT pcid_mapping_local_pack_code_presentation_source_country_key 
        UNIQUE (local_pack_code, presentation, source_country);
    END IF;
END $$;

-- Create index on (local_pack_code, presentation) if it doesn't exist
CREATE INDEX IF NOT EXISTS idx_pcid_code_presentation ON pcid_mapping(local_pack_code, presentation);
