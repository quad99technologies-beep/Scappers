#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Transform to Standard Format

Transforms the extracted CSV data to match the standard pharmaceutical data format
with columns: Country, Company, Local Product Name, Generic Name, Currency,
Ex Factory Wholesale Price, Region, Marketing Authority, Local Pack Description,
Formulation, Fill Size, Strength, Strength Unit, LOCAL_PACK_CODE

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import csv
import json
import logging
import re
from typing import List, Optional

try:
    from step_00_utils_encoding import csv_reader_utf8, csv_writer_utf8
except ImportError:
    import io
    def csv_reader_utf8(file_path):
        return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')
    def csv_writer_utf8(file_path, add_bom=True):
        encoding = 'utf-8-sig' if add_bom else 'utf-8'
        return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')

try:
    from step_00_db_utils import get_db_manager, RunContext, is_db_enabled, safe_db_operation
except ImportError:
    # Database support is optional
    def get_db_manager():
        return None
    def RunContext(*args, **kwargs):
        return None
    def is_db_enabled():
        return False
    def safe_db_operation(name):
        def decorator(func):
            return func
        return decorator

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = OUTPUT_DIR / "legend_to_end_extracted_cleaned.csv"
OUTPUT_CSV = OUTPUT_DIR / "legend_to_end_standard_format.csv"

# Standard column mapping
STANDARD_COLUMNS = [
    "Country",
    "Company",
    "Local Product Name",
    "Generic Name",
    "Currency",
    "Ex Factory Wholesale Price",
    "Region",
    "Marketing Authority",
    "Local Pack Description",
    "Formulation",
    "Fill Size",
    "Strength",
    "Strength Unit",
    "LOCAL_PACK_CODE"
]

# Static values for Canada Quebec
STATIC_VALUES = {
    "Country": "CANADA-QUEBEC",
    "Currency": "CAD",
    "Region": "NORTH AMERICA",
    "Marketing Authority": "RAMQ (Régie de l'assurance maladie du Québec)"
}


def extract_numeric_strength(strength_value: str) -> str:
    """Extract only numeric values (including decimals) from strength field."""
    if not strength_value:
        return ''
    
    # Remove all non-numeric characters except decimal point
    # This will extract numbers like "5", "1.5", "100", etc. from "5 mg", "1.5 g", "100 mg"
    numeric_match = re.search(r'\d+\.?\d*', str(strength_value))
    if numeric_match:
        return numeric_match.group(0)
    return ''


def create_pack_description(row: dict) -> str:
    """Create a detailed pack description from available data."""
    parts = []

    # Add form if available
    if row.get('Form'):
        parts.append(row['Form'])

    # Add strength if available
    if row.get('Strength'):
        parts.append(row['Strength'])

    # Add pack size if available
    if row.get('Pack'):
        parts.append(f"Pack of {row['Pack']}")

    # Add flags if available
    if row.get('Flags'):
        flags = row['Flags'].strip()
        if flags and flags != 'X':
            parts.append(f"({flags})")

    return " - ".join(parts) if parts else ""


def transform_row(row: dict) -> dict:
    """Transform a row from internal format to standard format."""
    # Create pack description
    pack_description = create_pack_description(row)
    
    # Get StrengthValue - this column contains only numeric values (no units)
    # Priority: Use StrengthValue if available, otherwise extract from Strength
    strength_numeric = ''
    strength_value_col = row.get('StrengthValue', '').strip()
    if strength_value_col:
        # Use StrengthValue directly if it exists and is not empty
        strength_numeric = strength_value_col
    else:
        # Fallback: extract numeric value from Strength column
        strength_value = row.get('Strength', '').strip()
        if strength_value:
            strength_numeric = extract_numeric_strength(strength_value)
    strength_numeric = str(strength_numeric).strip() if strength_numeric else ''

    # Build standard row
    standard_row = {
        "Country": STATIC_VALUES["Country"],
        "Company": row.get('Manufacturer', ''),
        "Local Product Name": row.get('Brand', ''),
        "Generic Name": row.get('Generic', ''),
        "Currency": STATIC_VALUES["Currency"],
        "Ex Factory Wholesale Price": row.get('PackPrice', ''),
        "Region": STATIC_VALUES["Region"],
        "Marketing Authority": STATIC_VALUES["Marketing Authority"],
        "Local Pack Description": pack_description,
        "Formulation": row.get('Form', ''),
        "Fill Size": row.get('Pack', ''),
        "Strength": strength_numeric,
        "Strength Unit": row.get('StrengthUnit', ''),
        "LOCAL_PACK_CODE": row.get('DIN', '')
    }

    return standard_row


# ----------------------------- Database Helpers -----------------------------
@safe_db_operation("insert_standard_format_batch")
def _insert_standard_format_batch(db_manager, run_ctx: RunContext, rows_data: List[tuple]) -> Optional[int]:
    """Insert batch of standard format data records."""
    if not is_db_enabled() or not db_manager or not run_ctx:
        return None
    
    query = """INSERT INTO standard_format_data (
        run_id, scraper_id, country, company, local_product_name, generic_name, currency,
        ex_factory_wholesale_price, region, marketing_authority, local_pack_description,
        formulation, fill_size, strength, strength_unit, local_pack_code
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )"""
    
    return db_manager.execute_batch_insert(query, rows_data, page_size=100)


def transform_csv(input_path: Path, output_path: Path) -> dict:
    """Transform CSV from internal format to standard format."""
    if not input_path.exists():
        return {
            "status": "error",
            "message": f"Input CSV not found: {input_path}"
        }

    rows_processed = 0
    
    # Initialize database support
    db_manager = get_db_manager()
    run_ctx = RunContext() if is_db_enabled() else None
    
    # Batch buffer for database inserts
    db_batch_buffer: List[tuple] = []
    BATCH_SIZE = 100

    try:
        with csv_reader_utf8(input_path) as infile, \
             csv_writer_utf8(output_path, add_bom=True) as outfile:

            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=STANDARD_COLUMNS)
            writer.writeheader()

            for row in reader:
                # Debug: Check first row only
                if rows_processed == 0:
                    print(f"DEBUG: First row keys: {list(row.keys())}")
                    print(f"DEBUG: StrengthValue = {repr(row.get('StrengthValue'))}")
                    print(f"DEBUG: Strength = {repr(row.get('Strength'))}")
                standard_row = transform_row(row)
                if rows_processed == 0:
                    print(f"DEBUG: Result Strength = {repr(standard_row.get('Strength'))}")
                writer.writerow(standard_row)
                rows_processed += 1
                
                # Add to database batch buffer
                if run_ctx:
                    try:
                        db_row = (
                            str(run_ctx.run_id),
                            run_ctx.scraper_id,
                            standard_row.get("Country", ""),
                            standard_row.get("Company") or None,
                            standard_row.get("Local Product Name") or None,
                            standard_row.get("Generic Name") or None,
                            standard_row.get("Currency", ""),
                            standard_row.get("Ex Factory Wholesale Price") or None,
                            standard_row.get("Region", ""),
                            standard_row.get("Marketing Authority", ""),
                            standard_row.get("Local Pack Description") or None,
                            standard_row.get("Formulation") or None,
                            standard_row.get("Fill Size") or None,
                            standard_row.get("Strength") or None,
                            standard_row.get("Strength Unit") or None,
                            standard_row.get("LOCAL_PACK_CODE") or None
                        )
                        db_batch_buffer.append(db_row)
                        
                        # Flush batch when buffer is full
                        if len(db_batch_buffer) >= BATCH_SIZE:
                            _insert_standard_format_batch(db_manager, run_ctx, db_batch_buffer)
                            db_batch_buffer.clear()
                    except Exception as e:
                        logging.getLogger(__name__).warning(f"Failed to buffer row for database: {e}")

        # Flush remaining database batch
        if run_ctx and db_batch_buffer:
            try:
                _insert_standard_format_batch(db_manager, run_ctx, db_batch_buffer)
                db_batch_buffer.clear()
            except Exception as e:
                logging.getLogger(__name__).warning(f"Failed to flush final database batch: {e}")

        return {
            "status": "ok",
            "input": str(input_path),
            "output": str(output_path),
            "rows_processed": rows_processed,
            "columns": len(STANDARD_COLUMNS)
        }

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error transforming CSV: {str(e)}"
        }


def main() -> None:
    """Main entry point."""
    print()
    print("=" * 80)
    print("TRANSFORMING TO STANDARD FORMAT")
    print("=" * 80)
    print()

    result = transform_csv(INPUT_CSV, OUTPUT_CSV)

    if result.get("status") == "error":
        print(f"[ERROR] {result.get('message')}")
        return

    print(f"[OK] Transformation complete!")
    print(f"     Input:  {result['input']}")
    print(f"     Output: {result['output']}")
    print(f"     Rows:   {result['rows_processed']}")
    print(f"     Columns: {result['columns']}")
    print()

    # Display column mapping
    print("Column Mapping:")
    print("-" * 80)
    print(f"{'Standard Column':<35} {'Source':<30}")
    print("-" * 80)
    print(f"{'Country':<35} {'Static: Canada':<30}")
    print(f"{'Company':<35} {'Manufacturer':<30}")
    print(f"{'Local Product Name':<35} {'Brand':<30}")
    print(f"{'Generic Name':<35} {'Generic':<30}")
    print(f"{'Currency':<35} {'Static: CAD':<30}")
    print(f"{'Ex Factory Wholesale Price':<35} {'PackPrice':<30}")
    print(f"{'Region':<35} {'Static: Quebec':<30}")
    print(f"{'Marketing Authority':<35} {'Static: RAMQ':<30}")
    print(f"{'Local Pack Description':<35} {'Generated from Form+Pack+Flags':<30}")
    print(f"{'Formulation':<35} {'Form':<30}")
    print(f"{'Fill Size':<35} {'Pack':<30}")
    print(f"{'Strength':<35} {'Strength':<30}")
    print(f"{'Strength Unit':<35} {'StrengthUnit':<30}")
    print(f"{'LOCAL_PACK_CODE':<35} {'DIN':<30}")
    print("-" * 80)
    print()

    # Display sample row
    print("Sample Output (first row):")
    print("-" * 80)
    try:
        with csv_reader_utf8(OUTPUT_CSV) as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            for col in STANDARD_COLUMNS:
                value = first_row.get(col, '')
                print(f"{col:<35} {value}")
    except Exception as e:
        print(f"Could not read sample: {e}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    db_manager = get_db_manager()
    try:
        main()
    finally:
        # Clean up database connections
        if db_manager:
            try:
                db_manager.close()
            except Exception:
                pass
