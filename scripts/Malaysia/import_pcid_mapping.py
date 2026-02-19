#!/usr/bin/env python3
"""
Import PCID Mapping CSV/Excel into my_pcid_reference table
==========================================================
Loads PCID mapping reference data from CSV or Excel into PostgreSQL.

Usage:
    python import_pcid_mapping.py [path_to_file]

If no path provided, searches in:
  1. input/Malaysia/*.csv or *.xlsx
  (Strict Single Source of Truth: 'input' folder only)
"""

import sys
import pandas as pd
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB


from config_loader import load_env_file, require_env

load_env_file()

def find_default_file():
    """Find the PCID mapping file based on strict configuration."""
    filename = require_env("SCRIPT_05_PCID_MAPPING")
    input_dir = _repo_root / "input" / "Malaysia"
    target_file = input_dir / filename
    
    if not target_file.exists():
        # strict check - no fallbacks
        print(f"[ERROR] Configured PCID file not found: {target_file}")
        print(f"Please ensure '{filename}' exists in 'input/Malaysia/'")
        sys.exit(1)
        
    return target_file


def read_data(file_path: Path):
    """Read CSV or Excel using pandas and normalize columns."""
    print(f"[IMPORT] Reading file: {file_path}")
    
    if file_path.suffix.lower() == '.xlsx':
        df = pd.read_excel(file_path, dtype=str)
    else:
        df = pd.read_csv(file_path, dtype=str)
        
    df = df.fillna("")
    
    # Normalize columns
    # We need: pcid, local_pack_code, presentation, package_number, product_group, generic_name, description
    
    # Map for 'pcid'
    if 'PCID Mapping' in df.columns:
        df['pcid'] = df['PCID Mapping']
    elif 'PCID' in df.columns:
        df['pcid'] = df['PCID']
    elif 'Pcid' in df.columns:
        df['pcid'] = df['Pcid']
    else:
        print("[WARNING] Could not find 'PCID' column. Columns found:", df.columns.tolist())
        df['pcid'] = ""

    # Map for 'local_pack_code'
    if 'LOCAL_PACK_CODE' in df.columns:
        df['local_pack_code'] = df['LOCAL_PACK_CODE']
    elif 'Local Pack Code' in df.columns:
        df['local_pack_code'] = df['Local Pack Code']
    else:
        print("[WARNING] Could not find 'Local Pack Code' column. Columns found:", df.columns.tolist())
        df['local_pack_code'] = ""

    # Map for 'presentation'
    if 'Presentation' in df.columns:
        df['presentation'] = df['Presentation']
    elif 'Pack Size' in df.columns:
        df['presentation'] = df['Pack Size']
    elif 'PACK_SIZE' in df.columns:
        df['presentation'] = df['PACK_SIZE']
    else:
        df['presentation'] = ""
        
    # Map others
    col_map = {
        "Package Number": "package_number",
        "Product Group": "product_group",
        "Generic Name": "generic_name",
        "Description": "description"
    }
    
    for source, target in col_map.items():
        if source in df.columns:
            df[target] = df[source]
        else:
            df[target] = ""
            
    # Filter to needed columns
    required_cols = ['pcid', 'local_pack_code', 'presentation', 'package_number', 'product_group', 'generic_name', 'description']
    
    # Ensure they exist
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""
            
    records = df[required_cols].to_dict('records')
    return records


def import_pcid_mapping(file_path: Path, append: bool = False):
    """Import PCID mapping into global 'pcid_mapping' table."""

    if not file_path.exists():
        raise FileNotFoundError(f"PCID mapping file not found: {file_path}")

    records = read_data(file_path)

    if not records:
        print("[WARNING] No records found in file")
        return

    print(f"[IMPORT] Found {len(records)} records (Mode: {'APPEND' if append else 'REPLACE'})")

    # Connect to DB
    with CountryDB("Malaysia") as db:
        # Clear existing data for Malaysia in GLOBAL table ONLY if not appending
        if not append:
            with db.cursor() as cur:
                cur.execute("DELETE FROM pcid_mapping WHERE source_country = 'Malaysia'")
                print(f"[DB] Cleared existing Malaysia entries in global 'pcid_mapping'")
        else:
             print(f"[DB] Appending to existing Malaysia entries")

        # Insert new data
        with db.cursor() as cur:
            sql = """
                INSERT INTO pcid_mapping
                (pcid, local_pack_code, presentation, generic_name, local_pack_description, source_country, uploaded_at)
                VALUES (%s, %s, %s, %s, %s, 'Malaysia', NOW())
            """
            
            # Map fields to global schema
            # Global schema: pcid, local_pack_code, presentation, generic_name, local_pack_description
            batch_data = [
                (
                    r["pcid"].strip(),
                    r["local_pack_code"].strip(),
                    r["presentation"].strip(),
                    r["generic_name"].strip(),
                    r["description"].strip()
                )
                for r in records
                if r["local_pack_code"] and str(r["local_pack_code"]).strip()
            ]
            
            if not batch_data:
                print("[WARNING] No valid records with Local Pack Code found.")
                return

            print(f"[DB] Inserting {len(batch_data)} valid records into 'pcid_mapping'...")
            
            # Execute in batches
            batch_size = 1000
            for i in range(0, len(batch_data), batch_size):
                cur.executemany(sql, batch_data[i:i + batch_size])

        # Verify count
        with db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pcid_mapping WHERE source_country = 'Malaysia'")
            count = cur.fetchone()[0]
            print(f"[DB] Total Malaysia records in global table: {count}")


def main():
    """Main entry point."""
    import argparse
    parser = argparse.ArgumentParser(description="Import PCID Mapping for Malaysia")
    parser.add_argument("file", nargs="?", help="Path to CSV/Excel file")
    parser.add_argument("--append", action="store_true", help="Append to existing data instead of replacing")
    args = parser.parse_args()

    # Determine CSV path
    if args.file:
        target_path = Path(args.file)
    else:
        target_path = find_default_file()
            
    print(f"\n{'='*60}")
    print(f"[STARTUP] Malaysia PCID Mapping Import")
    print(f"Mode: {'APPEND' if args.append else 'REPLACE'}")
    print(f"{'='*60}")
    print(f"Target file: {target_path}")
    print(f"{'='*60}\n")

    try:
        import_pcid_mapping(target_path, append=args.append)
        print(f"\n[SUCCESS] PCID mapping import completed")
        return 0
    except Exception as e:
        print(f"\n[ERROR] Import failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
