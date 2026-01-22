#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Output Formatter - Convert to Standardized Templates

Transforms the scraped and translated Russia data into standardized export formats:
1. Pricing Data Template (from VED list)
2. Discontinued List Template (from Excluded list)

Input files:
- en_russia_farmcom_ved_moscow_region.csv (English translated)
- en_russia_farmcom_excluded_list.csv (English translated)

Output files:
- russia_pricing_data.csv (Pricing Data Template)
- russia_discontinued_list.csv (Discontinued List Template)
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config
try:
    from config_loader import load_env_file, get_output_dir, get_central_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def get_output_dir():
        return _repo_root / "output" / "Russia"
    def get_central_output_dir():
        return _repo_root / "exports" / "Russia"


def format_pricing_data(input_path: Path, output_path: Path) -> int:
    """
    Format VED data into Pricing Data Template.

    Template columns:
    PCID, Country, Company, Product Group, Generic Name, Start Date, Currency,
    Ex-Factory Wholesale Price, Local Pack Description, LOCAL_PACK_CODE

    Args:
        input_path: Path to en_russia_farmcom_ved_moscow_region.csv
        output_path: Path to output russia_pricing_data.csv

    Returns:
        Number of rows written
    """
    if not input_path.exists():
        print(f"[WARNING] Input file not found: {input_path}")
        return 0

    rows_written = 0

    with input_path.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)

        # Output fieldnames matching the template
        output_fieldnames = [
            "PCID",
            "Country",
            "Company",
            "Product Group",
            "Generic Name",
            "Start Date",
            "Currency",
            "Ex-Factory Wholesale Price",
            "Local Pack Description",
            "LOCAL_PACK_CODE"
        ]

        with output_path.open("w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=output_fieldnames)
            writer.writeheader()

            for row in reader:
                # Map fields from scraped data to template
                output_row = {
                    "PCID": "",  # Empty - to be mapped later by PCID system
                    "Country": "Russia",
                    "Company": row.get("Manufacturer_Country", "").strip(),
                    "Product Group": row.get("TN", "").strip(),  # Trade Name
                    "Generic Name": row.get("INN", "").strip(),  # International Nonproprietary Name
                    "Start Date": row.get("Start_Date_Text", "").strip(),
                    "Currency": "RUB",
                    "Ex-Factory Wholesale Price": row.get("Registered_Price_RUB", "").strip(),
                    "Local Pack Description": row.get("Release_Form", "").strip(),
                    "LOCAL_PACK_CODE": row.get("EAN", "").strip()
                }

                writer.writerow(output_row)
                rows_written += 1

    return rows_written


def format_discontinued_list(input_path: Path, output_path: Path) -> int:
    """
    Format Excluded data into Discontinued List Template.

    Template columns:
    PCID, Country, Product Group, Generic Name, Start Date, End Date, Currency,
    Ex-Factory Wholesale Price, Local Pack Description, LOCAL_PACK_CODE

    Args:
        input_path: Path to en_russia_farmcom_excluded_list.csv
        output_path: Path to output russia_discontinued_list.csv

    Returns:
        Number of rows written
    """
    if not input_path.exists():
        print(f"[WARNING] Input file not found: {input_path}")
        return 0

    rows_written = 0

    with input_path.open("r", encoding="utf-8", newline="") as f_in:
        reader = csv.DictReader(f_in)

        # Output fieldnames matching the template
        output_fieldnames = [
            "PCID",
            "Country",
            "Product Group",
            "Generic Name",
            "Start Date",
            "End Date",
            "Currency",
            "Ex-Factory Wholesale Price",
            "Local Pack Description",
            "LOCAL_PACK_CODE"
        ]

        with output_path.open("w", encoding="utf-8", newline="") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=output_fieldnames)
            writer.writeheader()

            for row in reader:
                # Map fields from scraped data to template
                # Note: End Date is typically empty for Russia data - would need to be extracted
                # from Start_Date_Text if available (e.g., "15.03.2010 - 24.12.2025")
                output_row = {
                    "PCID": "",  # Empty - to be mapped later by PCID system
                    "Country": "Russia",
                    "Product Group": row.get("TN", "").strip(),  # Trade Name
                    "Generic Name": row.get("INN", "").strip(),  # International Nonproprietary Name
                    "Start Date": row.get("Start_Date_Text", "").strip(),
                    "End Date": "",  # Empty - end date not typically available in excluded list
                    "Currency": "RUB",
                    "Ex-Factory Wholesale Price": row.get("Registered_Price_RUB", "").strip(),
                    "Local Pack Description": row.get("Release_Form", "").strip(),
                    "LOCAL_PACK_CODE": row.get("EAN", "").strip()
                }

                writer.writerow(output_row)
                rows_written += 1

    return rows_written


def main():
    print()
    print("=" * 80)
    print("RUSSIA OUTPUT FORMATTER")
    print("=" * 80)
    print()

    # Paths
    output_dir = get_output_dir()
    central_dir = get_central_output_dir()

    # Input files (English translated)
    ved_input = output_dir / "en_russia_farmcom_ved_moscow_region.csv"
    excluded_input = output_dir / "en_russia_farmcom_excluded_list.csv"

    # Output files (standardized templates)
    pricing_output = output_dir / "russia_pricing_data.csv"
    discontinued_output = output_dir / "russia_discontinued_list.csv"

    # Format VED data into Pricing Data Template
    print("[1/2] Formatting VED data into Pricing Data Template...")
    ved_rows = format_pricing_data(ved_input, pricing_output)

    if ved_rows > 0:
        print(f"  Written: {pricing_output.name} ({ved_rows} rows)")

        # Copy to central exports
        central_pricing = central_dir / "Russia_Pricing_Data.csv"
        central_pricing.write_bytes(pricing_output.read_bytes())
        print(f"  Exported: {central_pricing.name}")
    else:
        print(f"  No data to format (input file missing or empty)")

    # Format Excluded data into Discontinued List Template
    print()
    print("[2/2] Formatting Excluded data into Discontinued List Template...")
    excluded_rows = format_discontinued_list(excluded_input, discontinued_output)

    if excluded_rows > 0:
        print(f"  Written: {discontinued_output.name} ({excluded_rows} rows)")

        # Copy to central exports
        central_discontinued = central_dir / "Russia_Discontinued_List.csv"
        central_discontinued.write_bytes(discontinued_output.read_bytes())
        print(f"  Exported: {central_discontinued.name}")
    else:
        print(f"  No data to format (input file missing or empty)")

    # Summary
    print()
    print("=" * 80)
    print("FORMATTING COMPLETE!")
    print("=" * 80)
    print(f"  Pricing Data: {ved_rows} rows")
    print(f"  Discontinued List: {excluded_rows} rows")
    print()
    print("Output files (output/Russia/):")
    print("  - russia_pricing_data.csv")
    print("  - russia_discontinued_list.csv")
    print()
    print("Central exports (exports/Russia/):")
    print("  - Russia_Pricing_Data.csv")
    print("  - Russia_Discontinued_List.csv")
    print("=" * 80)
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
