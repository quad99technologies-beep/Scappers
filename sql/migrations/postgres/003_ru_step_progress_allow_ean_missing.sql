-- Migration 003: Allow status 'ean_missing' in ru_step_progress
-- Fixes: "violates check constraint ru_step_progress_status_check" when marking pages with missing EAN.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ru_step_progress'::regclass
          AND conname = 'ru_step_progress_status_check'
    ) THEN
        ALTER TABLE ru_step_progress DROP CONSTRAINT ru_step_progress_status_check;
    END IF;
END $$;

ALTER TABLE ru_step_progress
ADD CONSTRAINT ru_step_progress_status_check
CHECK (status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped', 'ean_missing'));
