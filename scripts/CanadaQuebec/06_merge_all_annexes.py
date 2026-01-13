#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 06: Merge All Annexes

Merges CSV outputs from all 3 annexes (IV.1, IV.2, V) into a single final CSV file.
Ensures consistent column structure and handles missing columns.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import csv
import logging
from typing import List, Dict, Any
from datetime import datetime

try:
    from step_00_utils_encoding import csv_reader_utf8, csv_writer_utf8
except ImportError:
    import io
    import sys
    # Try importing from doc directory
    # Docs are now in repo root doc/ folder
    doc_path = Path(__file__).resolve().parents[2] / "doc"
    if doc_path.exists():
        sys.path.insert(0, str(doc_path))
        try:
            from step_00_utils_encoding import csv_reader_utf8, csv_writer_utf8
        except ImportError:
            def csv_reader_utf8(file_path):
                return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')
            def csv_writer_utf8(file_path, add_bom=True):
                encoding = 'utf-8-sig' if add_bom else 'utf-8'
                return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')
    else:
        def csv_reader_utf8(file_path):
            return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')
        def csv_writer_utf8(file_path, add_bom=True):
            encoding = 'utf-8-sig' if add_bom else 'utf-8'
            return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')

# Configuration
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
from config_loader import (
    get_base_dir, get_csv_output_dir, get_central_output_dir,
    ANNEXE_IV1_CSV_NAME, ANNEXE_IV2_CSV_NAME, ANNEXE_V_CSV_NAME,
    FINAL_REPORT_NAME_PREFIX, FINAL_REPORT_DATE_FORMAT,
    LOG_FILE_MERGE,
    STATIC_CURRENCY, STATIC_REGION,
    FINAL_COLUMNS
)
BASE_DIR = get_base_dir()
OUTPUT_DIR = get_csv_output_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()
STATIC_VALUES = {
    "Currency": STATIC_CURRENCY,
    "Region": STATIC_REGION,
}

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CENTRAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_FILES = {
    "IV.1": OUTPUT_DIR / ANNEXE_IV1_CSV_NAME,
    "IV.2": OUTPUT_DIR / ANNEXE_IV2_CSV_NAME,
    "V": OUTPUT_DIR / ANNEXE_V_CSV_NAME,
}

# Generate date-based filename: canadaquebecreport_ddmmyyyy.csv
date_str = datetime.now().strftime(FINAL_REPORT_DATE_FORMAT)
OUTPUT_CSV = OUTPUT_DIR / f"{FINAL_REPORT_NAME_PREFIX}{date_str}.csv"
LOG_FILE = OUTPUT_DIR / LOG_FILE_MERGE

# Standard column order (loaded from config)
STANDARD_COLUMNS = FINAL_COLUMNS


def read_csv_with_columns(file_path: Path, annexe_name: str) -> List[Dict[str, Any]]:
    """
    Read CSV file and normalize columns to standard format.
    Returns list of rows as dictionaries.
    """
    if not file_path.exists():
        logging.warning(f"{annexe_name} CSV not found: {file_path}")
        return []
    
    rows = []
    try:
        with csv_reader_utf8(file_path) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            
            for row in reader:
                # Normalize row to standard columns
                normalized_row = {}
                for col in STANDARD_COLUMNS:
                    # Map various column name variations
                    value = None
                    if col in row:
                        value = row[col]
                    elif col == "Ex Factory Wholesale Price" and "PackPrice" in row:
                        value = row["PackPrice"]
                    elif col == "Unit Price" and "UnitPrice" in row:
                        value = row["UnitPrice"]
                    elif col == "Marketing Authority" and "Manufacturer" in row:
                        value = row["Manufacturer"]
                    elif col == "Product Group":
                        # Product Group should already be in the row from IV.1, IV.2, and V
                        # But fallback to Brand if Product Group is missing
                        if "Product Group" in row:
                            value = row["Product Group"]
                        elif "Brand" in row:
                            value = row["Brand"]
                    elif col == "LOCAL_PACK_CODE" and "DIN" in row:
                        value = row["DIN"]
                    
                    normalized_row[col] = value
                
                # Apply static values where needed
                for static_col, static_val in STATIC_VALUES.items():
                    if not normalized_row.get(static_col):
                        normalized_row[static_col] = static_val
                
                rows.append(normalized_row)
        
        logging.info(f"Read {len(rows)} rows from {annexe_name}")
        return rows
        
    except Exception as e:
        logging.error(f"Error reading {annexe_name} CSV: {e}", exc_info=True)
        return []


def merge_all_annexes() -> dict:
    """Merge all annexe CSV files into one final output."""
    logging.basicConfig(
        filename=str(LOG_FILE),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("MERGING ALL ANNEXES")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    all_rows = []
    annexe_stats = {}
    total_annexes = len(INPUT_FILES)
    
    # Read each annexe
    for idx, (annexe_name, file_path) in enumerate(INPUT_FILES.items(), 1):
        logger.info(f"Reading {annexe_name}: {file_path}")
        print(f"[PROGRESS] Merging annexes: Reading {annexe_name} ({idx}/{total_annexes})", flush=True)
        rows = read_csv_with_columns(file_path, annexe_name)
        all_rows.extend(rows)
        annexe_stats[annexe_name] = len(rows)
        logger.info(f"  → {len(rows)} rows from {annexe_name}")
    
    # Write merged output
    print(f"[PROGRESS] Merging annexes: Writing output ({len(all_rows)} rows)", flush=True)
    try:
        with csv_writer_utf8(OUTPUT_CSV, add_bom=True) as f:
            writer = csv.DictWriter(f, fieldnames=STANDARD_COLUMNS)
            writer.writeheader()
            
            for idx, row in enumerate(all_rows, 1):
                writer.writerow(row)
                # Update progress every 100 rows
                if idx % 100 == 0 or idx == len(all_rows):
                    percent = round((idx / len(all_rows)) * 100, 1) if len(all_rows) > 0 else 0
                    print(f"[PROGRESS] Merging annexes: Writing rows {idx}/{len(all_rows)} ({percent}%)", flush=True)
        
        # Also copy final report to central output directory
        import shutil
        central_final_report = CENTRAL_OUTPUT_DIR / OUTPUT_CSV.name
        shutil.copy2(OUTPUT_CSV, central_final_report)
        logger.info(f"Final report also saved to central location: {central_final_report}")
        
        logger.info("=" * 80)
        logger.info("MERGE COMPLETED")
        logger.info(f"Total rows: {len(all_rows)}")
        for annexe, count in annexe_stats.items():
            logger.info(f"  {annexe}: {count} rows")
        logger.info(f"Output: {OUTPUT_CSV}")
        logger.info(f"Central Output: {central_final_report}")
        logger.info("=" * 80)
        
        return {
            "status": "ok",
            "output": str(OUTPUT_CSV),
            "central_output": str(central_final_report),
            "total_rows": len(all_rows),
            "annexe_stats": annexe_stats
        }
        
    except Exception as e:
        logger.error(f"Error writing merged CSV: {e}", exc_info=True)
        return {
            "status": "error",
            "message": str(e)
        }


def main():
    """Main entry point."""
    print()
    print("=" * 80)
    print("MERGE ALL ANNEXES")
    print("=" * 80)
    print()
    
    result = merge_all_annexes()
    
    if result.get("status") == "error":
        print(f"[ERROR] {result.get('message')}")
        return
    
    print(f"[OK] Merge complete!")
    print(f"     Output: {result['output']}")
    if 'central_output' in result:
        print(f"     Central Output: {result['central_output']}")
    print(f"     Total rows: {result['total_rows']:,}")
    print()
    print("Rows by annexe:")
    for annexe, count in result['annexe_stats'].items():
        print(f"  {annexe}: {count:,} rows")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()

