#!/usr/bin/env python3
"""
Quick migration script to update my_pcid_reference unique constraint.
Run this if you get: "there is no unique or exclusion constraint matching the ON CONFLICT specification"
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

_malaysia_dir = Path(__file__).resolve().parents[1]
if str(_malaysia_dir) not in sys.path:
    sys.path.insert(0, str(_malaysia_dir))

_script_dir = Path(__file__).resolve().parents[1]
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from config_loader import load_env_file
load_env_file()

from core.db.connection import CountryDB

def migrate():
    """Apply migration to update my_pcid_reference constraint."""
    db = CountryDB("Malaysia")
    
    print("[MIGRATION] Updating my_pcid_reference unique constraint...")
    
    migration_sql = """
DO $$
DECLARE
    cname text;
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'my_pcid_reference') THEN
        -- Drop old unique constraint if it exists (single column)
        SELECT conname INTO cname FROM pg_constraint
        WHERE conrelid = 'my_pcid_reference'::regclass 
        AND contype = 'u'
        AND array_length(conkey, 1) = 1
        LIMIT 1;
        IF cname IS NOT NULL THEN
            EXECUTE format('ALTER TABLE my_pcid_reference DROP CONSTRAINT %I', cname);
            RAISE NOTICE 'Dropped old constraint: %', cname;
        END IF;
        
        -- Create new composite unique constraint if it doesn't exist
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = 'my_pcid_reference'::regclass
            AND contype = 'u'
            AND array_length(conkey, 1) = 2
        ) THEN
            ALTER TABLE my_pcid_reference 
            ADD CONSTRAINT my_pcid_reference_local_pack_code_presentation_key 
            UNIQUE (local_pack_code, presentation);
            RAISE NOTICE 'Created new composite constraint';
        ELSE
            RAISE NOTICE 'Composite constraint already exists';
        END IF;
    ELSE
        RAISE NOTICE 'Table my_pcid_reference does not exist';
    END IF;
END $$;
"""
    
    try:
        db.execute(migration_sql)
        print("[MIGRATION] Complete!")
        print("\nYou can now re-run Step 5.")
    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        raise

if __name__ == "__main__":
    migrate()
