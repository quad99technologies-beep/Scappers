#!/usr/bin/env python3
"""
Convert Malaysia PCID Mapping CSV to new format with Presentation column.

This script:
1. Reads the existing PCID Mapping CSV
2. Adds a 'Presentation' column
3. For products with known multiple pack sizes (from email), creates separate rows
4. Outputs the new CSV format

Usage:
    python convert_malaysia_pcid_csv.py
    
Output:
    input/Malaysia/PCID Mapping - Malaysia_with_presentation.csv
"""

import csv
import sys
from pathlib import Path
from collections import defaultdict

repo_root = Path(__file__).resolve().parent
input_dir = repo_root / "input" / "Malaysia"
input_file = input_dir / "PCID Mapping - Malaysia.csv"
output_file = input_dir / "PCID Mapping - Malaysia_new.csv"

# Known presentation mappings from the email
# These are the products that have multiple presentations for the same local pack code
KNOWN_PRESENTATIONS = {
    # MAL19930608AZ - YSP Antifungal Cream (Miconazole)
    "MAL19930608AZ": [
        {"presentation": "a pack of 1 tube of 10gm", "pcid": "1692095"},  # Original
        {"presentation": "a pack of 1 tube of 20gm", "pcid": "1692095"},  # Same PCID or new one?
        {"presentation": "a pack of 50 tube of 10gm", "pcid": "1692095"},
        {"presentation": "a pack of 50 tube of 20gm", "pcid": "1692095"},
    ],
    # MAL07050226XZ - Avadol suspension (Paracetamol)
    "MAL07050226XZ": [
        {"presentation": "1 PET bottle with 90ml", "pcid": "1692130"},  # Original
        {"presentation": "a box with 30 PET bottle with 90ml", "pcid": "1692130"},  # Same PCID or new?
    ],
}


def convert_csv():
    """Convert the CSV to new format with presentation column."""
    
    if not input_file.exists():
        print(f"ERROR: Input file not found: {input_file}")
        sys.exit(1)
    
    print(f"[CONVERT] Reading: {input_file}")
    
    rows_by_code = defaultdict(list)
    
    with open(input_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            local_code = row.get("LOCAL_PACK_CODE", "").strip()
            pcid = row.get("PCID Mapping", "").strip()
            if local_code and pcid:
                rows_by_code[local_code].append({
                    "local_pack_code": local_code,
                    "pcid": pcid,
                    "presentation": "",  # Will be filled
                })
    
    print(f"[CONVERT] Found {len(rows_by_code)} unique local pack codes")
    
    # Generate output rows
    output_rows = []
    
    for local_code, rows in rows_by_code.items():
        if local_code in KNOWN_PRESENTATIONS:
            # This product has multiple known presentations
            for pres_info in KNOWN_PRESENTATIONS[local_code]:
                output_rows.append({
                    "LOCAL_PACK_CODE": local_code,
                    "Presentation": pres_info["presentation"],
                    "PCID Mapping": pres_info["pcid"],
                })
            print(f"  -> Expanded {local_code} to {len(KNOWN_PRESENTATIONS[local_code])} presentation rows")
        else:
            # Single presentation (default)
            for row in rows:
                output_rows.append({
                    "LOCAL_PACK_CODE": row["local_pack_code"],
                    "Presentation": "",  # Empty = default presentation
                    "PCID Mapping": row["pcid"],
                })
    
    # Write output CSV
    print(f"[CONVERT] Writing: {output_file}")
    with open(output_file, "w", encoding="utf-8-sig", newline="") as f:
        fieldnames = ["LOCAL_PACK_CODE", "Presentation", "PCID Mapping"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)
    
    print(f"[CONVERT] Complete! Wrote {len(output_rows)} rows")
    print(f"\nNext steps:")
    print(f"1. Review the new CSV: {output_file}")
    print(f"2. Fill in Presentation values for products that need them")
    print(f"3. Replace the old CSV with the new one")
    print(f"4. Run the migration script: python migrate_malaysia_pcid_presentation.py")
    print(f"5. Re-run Step 5 to generate PCID mappings")


if __name__ == "__main__":
    convert_csv()
