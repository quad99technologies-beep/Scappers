#!/usr/bin/env python3
"""
Migration script: Add presentation column support to Malaysia PCID mapping.

This script:
1. Adds 'presentation' column to my_pcid_reference and my_pcid_mappings tables
2. Updates the unique constraint to include (local_pack_code, presentation)
3. Provides a template for updating the PCID mapping CSV

Run this before using the new composite key feature.
"""

import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.connection import CountryDB


def migrate():
    """Apply migration to Malaysia tables."""
    db = CountryDB("Malaysia")
    
    print("[MIGRATION] Starting Malaysia PCID presentation migration...")
    
    with db.cursor() as cur:
        # 1. Add presentation column to my_pcid_reference
        print("  -> Adding 'presentation' column to my_pcid_reference...")
        cur.execute("""
            ALTER TABLE my_pcid_reference 
            ADD COLUMN IF NOT EXISTS presentation TEXT
        """)
        
        # 2. Drop old unique constraint and create new composite unique constraint
        print("  -> Updating unique constraint on my_pcid_reference...")
        cur.execute("""
            DO $$
            BEGIN
                -- Drop old unique constraint if exists
                IF EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conrelid = 'my_pcid_reference'::regclass 
                    AND contype = 'u' 
                    AND conname LIKE '%local_pack_code%'
                ) THEN
                    ALTER TABLE my_pcid_reference DROP CONSTRAINT my_pcid_reference_local_pack_code_key;
                END IF;
                
                -- Create new composite unique constraint
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conrelid = 'my_pcid_reference'::regclass 
                    AND contype = 'u' 
                    AND conname = 'my_pcid_reference_local_pack_code_presentation_key'
                ) THEN
                    ALTER TABLE my_pcid_reference 
                    ADD CONSTRAINT my_pcid_reference_local_pack_code_presentation_key 
                    UNIQUE (local_pack_code, presentation);
                END IF;
            END $$;
        """)
        
        # 3. Add index for composite lookup
        print("  -> Creating index on (local_pack_code, presentation)...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_my_pcidref_code_presentation 
            ON my_pcid_reference(local_pack_code, presentation)
        """)
        
        # 4. Add presentation column to my_pcid_mappings
        print("  -> Adding 'presentation' column to my_pcid_mappings...")
        cur.execute("""
            ALTER TABLE my_pcid_mappings 
            ADD COLUMN IF NOT EXISTS presentation TEXT
        """)
        
        # 5. Update unique constraint on my_pcid_mappings
        print("  -> Updating unique constraint on my_pcid_mappings...")
        cur.execute("""
            DO $$
            BEGIN
                -- Drop old unique constraint if exists
                IF EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conrelid = 'my_pcid_mappings'::regclass 
                    AND contype = 'u' 
                    AND conname LIKE '%local_pack_code%'
                ) THEN
                    ALTER TABLE my_pcid_mappings DROP CONSTRAINT my_pcid_mappings_run_id_local_pack_code_key;
                END IF;
                
                -- Create new composite unique constraint
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint 
                    WHERE conrelid = 'my_pcid_mappings'::regclass 
                    AND contype = 'u' 
                    AND conname = 'my_pcid_mappings_run_id_local_pack_code_presentation_key'
                ) THEN
                    ALTER TABLE my_pcid_mappings 
                    ADD CONSTRAINT my_pcid_mappings_run_id_local_pack_code_presentation_key 
                    UNIQUE (run_id, local_pack_code, presentation);
                END IF;
            END $$;
        """)
        
        # 6. Add index for composite lookup
        print("  -> Creating index on my_pcid_mappings (local_pack_code, presentation)...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_my_pcid_code_presentation 
            ON my_pcid_mappings(local_pack_code, presentation)
        """)
    
    print("[MIGRATION] Complete!")
    print("\nNext steps:")
    print("1. Update your PCID Mapping CSV to include 'Presentation' column")
    print("2. Re-run Step 5 to generate PCID mappings with presentation support")
    print("\nExample CSV format:")
    print("  LOCAL_PACK_CODE,Presentation,PCID Mapping")
    print("  MAL19930608AZ,a pack of 1 tube of 10gm,1692095")
    print("  MAL19930608AZ,a pack of 1 tube of 20gm,1692096")


if __name__ == "__main__":
    migrate()
