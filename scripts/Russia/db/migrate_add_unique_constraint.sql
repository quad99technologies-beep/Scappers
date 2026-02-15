-- Migration: Add unique constraint to ru_input_dictionary
-- This fixes the "ON CONFLICT" error when saving translations

-- First, remove any duplicates (keep the one with smallest id)
DELETE FROM ru_input_dictionary a
USING ru_input_dictionary b
WHERE a.id > b.id 
  AND a.source_term = b.source_term 
  AND a.language_from = b.language_from 
  AND a.language_to = b.language_to;

-- Add the unique constraint if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'ru_input_dictionary_source_from_to_unique'
    ) THEN
        ALTER TABLE ru_input_dictionary 
        ADD CONSTRAINT ru_input_dictionary_source_from_to_unique 
        UNIQUE (source_term, language_from, language_to);
    END IF;
END $$;
