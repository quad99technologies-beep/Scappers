-- Migration 008: Add all_pids to chrome_instances for DB-based PID termination
-- Replaces JSON file tracking with single source of truth in DB
-- all_pids: JSONB array of all PIDs to kill (driver + children) for pipeline stop cleanup

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'chrome_instances' AND column_name = 'all_pids'
    ) THEN
        ALTER TABLE chrome_instances ADD COLUMN all_pids JSONB DEFAULT '[]'::jsonb;
    END IF;
END $$;

COMMENT ON COLUMN chrome_instances.all_pids IS 'Array of all PIDs to terminate (driver + browser children) for pipeline stop cleanup';

INSERT INTO _schema_versions (version, filename)
VALUES (8, '008_add_chrome_instances_all_pids.sql')
ON CONFLICT (version) DO NOTHING;
