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
    FINAL_COLUMNS,
    DB_ENABLED
)
from db_handler import DBHandler
import os

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
                    elif col == "Ex Factory Wholesale Price":
                        if "Price" in row:
                            value = row["Price"]
                        elif "PackPrice" in row:
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
    """Merge all annexe CSV files into one final output (or verify in DB)."""
    
def merge_all_annexes() -> dict:
    """Merge all annexe CSV files into one final output (or verify in DB)."""
    
    if DB_ENABLED:
        try:
            db = DBHandler()
            run_id = os.getenv("PIPELINE_RUN_ID")
            
            if not run_id:
                # Try to find latest COMPLETED run if not passed
                with db.db.cursor() as cur:
                     cur.execute(f"SELECT run_id FROM {db.prefix}pipeline_runs WHERE status = 'COMPLETED' ORDER BY start_time DESC LIMIT 1")
                     row = cur.fetchone()
                     if row:
                         run_id = row[0]
                         print(f"No PIPELINE_RUN_ID provided, using latest run: {run_id}")
            
            if run_id:
                print(f"Exporting data for Run ID: {run_id}")
                
                all_rows = []
                annexe_stats = {}
                
                # Fetch data for each annexe
                for suffix in ["annexe_iv1", "annexe_iv2", "annexe_v"]:
                    table = f"{db.prefix}{suffix}"
                    print(f"Fetching from {table}...")
                    with db.db.cursor() as cur:
                        # Select columns to match standard output
                        # We select specific columns to ensure order and presence
                        # Note: DB columns are lowercase with underscores
                        query = f"SELECT generic_name, formulation, din, brand, manufacturer, format_str, price, unit_price, page_num, annexe FROM {table} WHERE run_id = %s"
                        cur.execute(query, (run_id,))
                        rows = cur.fetchall()
                        
                        # Convert DB rows (tuples) to our standard list of dicts format
                        for r in rows:
                            # Map DB columns to Standard Columns (as per read_csv_with_columns logic)
                            # Standard Columns: Generic Name, Formulation, DIN, Brand, Manufacturer, Format, Price, Unit Price, Page, Annexe
                            # Plus static vals: Currency, Region, etc.
                            
                            row_dict = {
                                "Generic Name": r[0],
                                "Formulation": r[1],
                                "DIN": r[2],
                                "Brand": r[3],
                                "Manufacturer": r[4],
                                "Format": r[5],
                                "Ex Factory Wholesale Price": r[6], # Map price to Ex Factory Wholesale Price
                                "Unit Price": r[7],
                                "Page": r[8],
                                "Annexe": r[9],
                                "Currency": STATIC_CURRENCY,
                                "Region": STATIC_REGION,
                                "Product Group": r[3], # Fallback to Brand
                                "Marketing Authority": r[4], # Fallback to Manufacturer
                                "LOCAL_PACK_CODE": r[2] # Fallback to DIN
                            }
                            
                            # Additional logic for columns normally handled in read_csv_with_columns
                            # e.g., "Product Group" logic:
                            # If "Product Group" in row (it's not in DB schema yet, maybe add later?), fallback to Brand.
                            # Standard columns also include: "Local Pack Description" (Format), "Strength" (part of Form?), "Strength Unit"
                            # Let's fill what we can.
                            
                            row_dict["Local Pack Description"] = r[5] # Format
                            
                            # --- Additional Column Extractions ---
                            
                            # 1. Fill Size (from Format)
                            # Format typically looks like "100 2 ml" or "30 Co."
                            # We want the numeric part if it looks like a size
                            row_dict["Fill Size"] = r[5] 

                            # 2. Strength & Strength Unit from Formulation
                            # Formulation examples: "Co. 10 mg", "Sol. Inj. 50 mg/mL", "Caps. 200 mg"
                            # We need to extract the last numeric+unit part
                            
                            formulation = r[1] or ""
                            strength_val = ""
                            strength_unit = ""
                            
                            if formulation:
                                # Regex to find Strength + Unit at the end or embedded
                                # Matches: number (+ decimals/comma) + whitespace? + unit
                                # Units: mg, g, mcg, mL, %, U, UI, etc.
                                # Example: "10 mg" -> 10, mg
                                # Example: "50 mg/mL" -> 50, mg/mL
                                
                                import re
                                # Pattern looking for typical strength pattern
                                # (digits possibly with , or . ) (space) (unit)
                                re_strength = re.search(r"(\d+[.,]?\d*)\s*(mg|g|mcg|µg|ml|mL|%|U|UI|U\.I\.|mg/mL|UI/mL|U/mL)\b", formulation, re.IGNORECASE)
                                if re_strength:
                                    strength_val = re_strength.group(1).replace(',', '.') # Normalize decimal
                                    strength_unit = re_strength.group(2)
                                else:
                                    # Fallback: maybe just number at end?
                                    pass

                            row_dict["Strength"] = strength_val
                            row_dict["Strength Unit"] = strength_unit
                            
                            all_rows.append(row_dict)
                        
                        annexe_stats[suffix] = len(rows)
                        print(f"  -> {len(rows)} rows from {suffix}")

                # Proceed to write CSV (reuse existing logic if possible, or duplicate for clarity)
                # We'll use the existing CSV writing block below, but skip the file reading part.
                # So we just construct result manually here.
                
                print(f"Writing {len(all_rows)} rows to {OUTPUT_CSV}")
                # Ensure output dir
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                CENTRAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                
                with csv_writer_utf8(OUTPUT_CSV, add_bom=True) as f:
                    writer = csv.DictWriter(f, fieldnames=STANDARD_COLUMNS, extrasaction='ignore') # Ignore extra DB fields if any
                    writer.writeheader()
                    writer.writerows(all_rows)
                
                # Copy to central
                import shutil
                central_final_report = CENTRAL_OUTPUT_DIR / OUTPUT_CSV.name
                shutil.copy2(OUTPUT_CSV, central_final_report)
                
                # Log completion step
                db.log_step(run_id, "Merge All Annexes", "COMPLETED", len(all_rows), 0.0, annexe_stats)
                
                return {
                    "status": "ok",
                    "output": str(OUTPUT_CSV),
                    "central_output": str(central_final_report),
                    "total_rows": len(all_rows),
                    "annexe_stats": annexe_stats
                }
            else:
                 print("No PIPELINE_RUN_ID found for export.")
                 return {"status": "skipped", "message": "No run ID"}
                 
        except Exception as e:
            return {
                "status": "error",
                "message": f"DB Export failed: {e}"
            }

    # CSV MERGE LOGIC (Fallback - Only if DB Disabled)
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

