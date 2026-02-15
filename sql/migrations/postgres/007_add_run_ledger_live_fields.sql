-- Migration 007: Add Live Fields to run_ledger (Optional)
-- Adds current_step and current_step_name for live UI tracking

-- =============================================================================
-- Add live tracking columns to run_ledger
-- =============================================================================

ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS current_step INTEGER;
ALTER TABLE run_ledger ADD COLUMN IF NOT EXISTS current_step_name TEXT;

-- Add index for active runs query
CREATE INDEX IF NOT EXISTS idx_run_ledger_current_step ON run_ledger(current_step) WHERE current_step IS NOT NULL;

-- =============================================================================
-- Update schema version
-- =============================================================================

INSERT INTO _schema_versions (version, filename)
VALUES (7, '007_add_run_ledger_live_fields.sql')
ON CONFLICT (version) DO NOTHING;
