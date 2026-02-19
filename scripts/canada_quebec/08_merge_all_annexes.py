import csv
import sys
import os
import json
from pathlib import Path

# Path wiring
script_path = Path(__file__).resolve().parent
sys.path.insert(0, str(script_path))

from config_loader import (
    get_csv_output_dir, get_base_dir, DB_ENABLED,
    ANNEXE_V_CSV_NAME
)
from db_handler import DBHandler

def main():
    print("Merging all annexes into final output...")
    output_dir = get_csv_output_dir()
    db_enabled = DB_ENABLED
    run_id = os.getenv("PIPELINE_RUN_ID")
    
    annexes = [
        {"name": "III", "csv": "annexe_iii.csv", "table": "annexe_iii"},
        {"name": "IV", "csv": "annexe_iv.csv", "table": "annexe_iv"}, # Likely empty
        {"name": "IV.1", "csv": "annexe_iv1.csv", "table": "annexe_iv1"},
        {"name": "IV.2", "csv": "annexe_iv2.csv", "table": "annexe_iv2"},
        {"name": "V", "csv": "annexe_v.csv", "table": "annexe_v"},
    ]
    
    all_rows = []
    stats = {}
    
    if db_enabled and run_id:
        db = DBHandler()
        for ann in annexes:
            # For simplicity, we'll try to read from CSVs first as they are consistent
            csv_path = output_dir / ann["csv"]
            if csv_path.exists():
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    for r in rows:
                        r["Annexe"] = ann["name"]
                    all_rows.extend(rows)
                    stats[ann["name"]] = len(rows)
                    print(f"  -> {len(rows)} rows from Annexe {ann['name']}")
    else:
        for ann in annexes:
            csv_path = output_dir / ann["csv"]
            if csv_path.exists():
                with open(csv_path, 'r', encoding='utf-8-sig') as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    for r in rows:
                        r["Annexe"] = ann["name"]
                    all_rows.extend(rows)
                    stats[ann["name"]] = len(rows)
                    print(f"  -> {len(rows)} rows from Annexe {ann['name']}")

    final_csv = output_dir / "canada_quebec_final.csv"
    if all_rows:
        with open(final_csv, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_rows[0].keys())
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"Merged {len(all_rows)} rows into {final_csv}")
    else:
        print("No data found to merge.")

if __name__ == "__main__":
    main()
