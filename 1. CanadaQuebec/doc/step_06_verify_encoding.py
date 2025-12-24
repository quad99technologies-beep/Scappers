#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV Encoding Verification Tool

Verifies that CSV files have proper UTF-8 encoding and displays sample data
to check for mojibake issues.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import csv
import sys

try:
    from step_00_utils_encoding import csv_reader_utf8, detect_mojibake, fix_mojibake
except ImportError:
    import io
    import re

    def csv_reader_utf8(file_path):
        return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')

    def detect_mojibake(text):
        pattern = re.compile(r'Ã[^\s]{0,2}')
        return bool(pattern.search(text)) if text else False

    def fix_mojibake(text):
        fixes = {'Ã‰': 'É', 'Ã©': 'é', 'Ã¨': 'è', 'Ãª': 'ê', 'Ã§': 'ç'}
        for wrong, correct in fixes.items():
            text = text.replace(wrong, correct)
        return text

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR = BASE_DIR / "output" / "csv"


def check_file_encoding(file_path: Path) -> dict:
    """Check a CSV file for encoding issues."""
    if not file_path.exists():
        return {
            "status": "error",
            "message": f"File not found: {file_path}"
        }

    # Check for BOM
    with open(file_path, 'rb') as f:
        first_bytes = f.read(3)
        has_bom = (first_bytes == b'\xef\xbb\xbf')

    # Read and analyze content
    rows_checked = 0
    mojibake_rows = []
    sample_data = []
    french_chars_found = []

    try:
        with csv_reader_utf8(file_path) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames

            for i, row in enumerate(reader):
                rows_checked += 1

                # Check for mojibake in all fields
                has_mojibake = False
                for field, value in row.items():
                    if value and detect_mojibake(value):
                        has_mojibake = True
                        mojibake_rows.append({
                            "row": i + 1,
                            "field": field,
                            "value": value[:100],
                            "fixed": fix_mojibake(value)[:100]
                        })

                # Collect sample data from first 10 rows
                if i < 10:
                    generic = row.get('Generic', '')
                    brand = row.get('Brand', '')
                    if generic:
                        sample_data.append({
                            "row": i + 1,
                            "Generic": generic[:80],
                            "Brand": brand[:50]
                        })

                    # Check for French characters
                    for char in 'ÉéèêëÀàâçÎîïÔôÛûü':
                        if char in generic or char in brand:
                            french_chars_found.append({
                                "row": i + 1,
                                "char": char,
                                "context": generic[:50] if char in generic else brand[:50]
                            })

                if rows_checked >= 100:
                    break

        return {
            "status": "ok",
            "file": str(file_path),
            "has_bom": has_bom,
            "encoding": "UTF-8-SIG (with BOM)" if has_bom else "UTF-8 (no BOM)",
            "rows_checked": rows_checked,
            "mojibake_found": len(mojibake_rows),
            "mojibake_examples": mojibake_rows[:5],
            "french_chars_found": len(french_chars_found),
            "french_chars_examples": french_chars_found[:5],
            "sample_data": sample_data[:5]
        }

    except Exception as e:
        return {
            "status": "error",
            "file": str(file_path),
            "message": f"Error reading file: {str(e)}"
        }


def print_report(report: dict) -> None:
    """Print encoding verification report."""
    print("=" * 80)
    print("CSV ENCODING VERIFICATION REPORT")
    print("=" * 80)
    print()

    if report.get("status") == "error":
        print(f"[ERROR] {report.get('message')}")
        return

    print(f"File: {report['file']}")
    print(f"Encoding: {report['encoding']}")
    print(f"Rows checked: {report['rows_checked']}")
    print()

    # BOM status
    if report['has_bom']:
        print("[OK] UTF-8 BOM present (Excel will recognize encoding)")
    else:
        print("[WARN] No UTF-8 BOM (Excel may have issues)")
    print()

    # Mojibake status
    if report['mojibake_found'] > 0:
        print(f"[FAIL] MOJIBAKE DETECTED: {report['mojibake_found']} instances found!")
        print()
        print("Examples of mojibake:")
        for ex in report['mojibake_examples']:
            print(f"  Row {ex['row']}, Field '{ex['field']}':")
            print(f"    Original: {ex['value']}")
            print(f"    Fixed:    {ex['fixed']}")
            print()
    else:
        print("[OK] No mojibake detected")
        print()

    # French character verification
    if report['french_chars_found'] > 0:
        print(f"[OK] French characters found: {report['french_chars_found']} instances")
        print()
        print("Examples:")
        for ex in report['french_chars_examples']:
            print(f"  Row {ex['row']}: '{ex['char']}' in '{ex['context']}'")
        print()
    else:
        print("[WARN] No French accented characters found (may indicate encoding issue)")
        print()

    # Sample data
    if report['sample_data']:
        print("Sample data (first 5 rows):")
        print("-" * 80)
        for sample in report['sample_data']:
            print(f"Row {sample['row']}:")
            print(f"  Generic: {sample['Generic']}")
            print(f"  Brand:   {sample['Brand']}")
        print()


def main() -> None:
    """Main entry point."""
    files_to_check = [
        OUTPUT_DIR / "legend_to_end_extracted.csv",
        OUTPUT_DIR / "legend_to_end_extracted_cleaned.csv"
    ]

    print()
    print("Checking CSV files for encoding issues...")
    print()

    for csv_file in files_to_check:
        report = check_file_encoding(csv_file)
        print_report(report)
        print()

    print("=" * 80)
    print("Verification complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
