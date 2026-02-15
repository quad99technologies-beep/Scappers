#!/usr/bin/env python3
"""
Reload PCID Mapping from CSV to Database
"""

import sys
from pathlib import Path

# Add repo root to path
repo_root = Path(__file__).resolve().parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from core.db.csv_importer import CSVImporter, PCID_MAPPING_CONFIG
from core.db.connection import CountryDB

def reload_pcid_mapping(scraper_name: str = "Argentina"):
    """Reload PCID mapping from CSV file to database"""
    
    # Find the CSV file
    input_dir = repo_root / "Input" / scraper_name
    csv_file = input_dir / "PCID Mapping - Argentina.csv"
    
    if not csv_file.exists():
        print(f"ERROR: CSV file not found: {csv_file}")
        return False
    
    print(f"Loading PCID mapping from: {csv_file}")
    
    # Connect to database
    db = CountryDB(scraper_name)
    
    # Clear existing PCID mapping for this country
    print(f"Clearing existing PCID mapping for {scraper_name}...")
    with db.cursor() as cur:
        cur.execute("DELETE FROM pcid_mapping WHERE source_country = %s", (scraper_name,))
    
    # Import new data
    importer = CSVImporter(db)
    result = importer.import_csv(
        csv_path=csv_file,
        table=PCID_MAPPING_CONFIG["table"],
        column_map=PCID_MAPPING_CONFIG["column_map"],
        mode="append",
        country=scraper_name,
    )
    
    print(f"\nImport Result:")
    print(f"  Status: {result.status}")
    print(f"  Rows imported: {result.rows_imported}")
    print(f"  Rows skipped: {result.rows_skipped}")
    print(f"  Message: {result.message}")
    
    # Verify the update
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pcid_mapping WHERE source_country = %s", (scraper_name,))
        count = cur.fetchone()[0]
        print(f"\nTotal PCID mappings in database for {scraper_name}: {count}")
        
        # Check for ACALIX AP specifically
        cur.execute("""
            SELECT pcid, company, local_product_name, generic_name, local_pack_description
            FROM pcid_mapping 
            WHERE source_country = %s 
            AND local_product_name ILIKE '%%ACALIX AP%%'
        """, (scraper_name,))
        rows = cur.fetchall()
        print(f"\nACALIX AP entries:")
        for row in rows:
            print(f"  PCID {row[0]}: {row[1]} | {row[2]} | {row[3]} | {row[4]}")
    
    db.close()
    return result.status == "ok"

if __name__ == "__main__":
    success = reload_pcid_mapping("Argentina")
    sys.exit(0 if success else 1)
