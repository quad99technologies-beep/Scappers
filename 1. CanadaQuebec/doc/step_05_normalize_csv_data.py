#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV Data Normalization

Cleans and normalizes all text columns in extracted CSV data to ensure
encoding correctness and data quality.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import csv
import re
import json
import unicodedata

try:
    from step_00_utils_encoding import (
        clean_extracted_text, normalize_unicode_nfc, fix_mojibake,
        csv_reader_utf8, csv_writer_utf8
    )
except ImportError:
    import io
    def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
        return unicodedata.normalize('NFC', str(text)) if text else ""
    def normalize_unicode_nfc(text: str) -> str:
        return unicodedata.normalize('NFC', text) if text else text
    def fix_mojibake(text: str) -> str:
        return text
    def csv_reader_utf8(file_path):
        return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')
    def csv_writer_utf8(file_path, add_bom=True):
        encoding = 'utf-8-sig' if add_bom else 'utf-8'
        return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = OUTPUT_DIR / "legend_to_end_extracted.csv"
OUTPUT_CSV = OUTPUT_DIR / "legend_to_end_extracted_cleaned.csv"

# Text columns to clean
TEXT_COLUMNS = [
    'Generic', 'Flags', 'Form', 'Strength', 'StrengthValue', 
    'StrengthUnit', 'DIN', 'Brand', 'Manufacturer', 'Pack', 
    'UnitPriceSource', 'confidence_label'
]


def clean_whitespace(text: str) -> str:
    """Normalize whitespace."""
    if not text:
        return text
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def clean_text_field(text: str) -> str:
    """Clean a single text field using encoding utilities."""
    if not text:
        return text
    
    # Fix mojibake patterns
    cleaned = fix_mojibake(text)
    
    # Normalize Unicode to NFC
    cleaned = normalize_unicode_nfc(cleaned)
    
    # Clean whitespace
    cleaned = clean_whitespace(cleaned)
    
    # Final pass for safety
    cleaned = clean_extracted_text(cleaned, enforce_utf8=True)
    
    return cleaned


def normalize_csv(input_path: Path, output_path: Path) -> dict:
    """Normalize CSV file and return statistics."""
    if not input_path.exists():
        return {
            "status": "error",
            "message": f"Input CSV not found: {input_path}"
        }
    
    rows_processed = 0
    rows_cleaned = 0
    changes_made = []
    
    try:
        with csv_reader_utf8(input_path) as infile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames

            if not fieldnames:
                return {
                    "status": "error",
                    "message": "CSV file has no headers"
                }

            with csv_writer_utf8(output_path, add_bom=True) as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for row in reader:
                    rows_processed += 1
                    row_changed = False
                    row_changes = {}
                    
                    # Clean all text columns
                    for col in fieldnames:
                        if col in TEXT_COLUMNS or any(col.startswith(prefix) 
                                                      for prefix in ['Generic', 'Brand', 'Manufacturer', 'Form']):
                            original = row.get(col, '')
                            if original:
                                cleaned = clean_text_field(str(original))
                                if original != cleaned:
                                    row_changed = True
                                    row_changes[col] = {
                                        "original": original,
                                        "cleaned": cleaned
                                    }
                                    row[col] = cleaned
                    
                    if row_changed:
                        rows_cleaned += 1
                        if len(changes_made) < 10:
                            changes_made.append(row_changes)
                    
                    writer.writerow(row)
        
        return {
            "status": "ok",
            "input": str(input_path),
            "output": str(output_path),
            "rows_processed": rows_processed,
            "rows_cleaned": rows_cleaned,
            "changes_percentage": round((rows_cleaned / rows_processed * 100) 
                                      if rows_processed > 0 else 0, 2),
            "sample_changes": changes_made
        }
    
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error processing CSV: {str(e)}"
        }


def main() -> None:
    """Main entry point."""
    result = normalize_csv(INPUT_CSV, OUTPUT_CSV)
    
    if result.get("status") == "error":
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    
    print(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"\n✅ Normalization complete!")
    print(f"   Processed: {result['rows_processed']} rows")
    print(f"   Cleaned: {result['rows_cleaned']} rows ({result['changes_percentage']}%)")
    print(f"   Output: {result['output']}")


if __name__ == "__main__":
    main()

