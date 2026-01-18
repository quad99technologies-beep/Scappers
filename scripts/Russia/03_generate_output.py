#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Output Generator

Processes the raw scraped data from both VED Registry and Excluded List,
generates clean output reports.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import sys
import csv
import re
from pathlib import Path
from datetime import datetime

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Russia to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config
try:
    from config_loader import load_env_file, getenv, get_output_dir, get_central_output_dir
    from translation_utils import format_date_ddmmyyyy
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def getenv(key, default=""):
        return os.getenv(key, default)
    def get_output_dir():
        return Path(__file__).parent
    def get_central_output_dir():
        return Path(__file__).parent
    def format_date_ddmmyyyy(value: str) -> str:
        return value

# File paths
if USE_CONFIG:
    OUT_DIR = get_output_dir()
    CENTRAL_OUTPUT_DIR = get_central_output_dir()
    # VED Registry input
    VED_INPUT_CSV = OUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "russia_farmcom_ved_moscow_region.csv")
    # Excluded List input
    EXCLUDED_INPUT_CSV = OUT_DIR / getenv("SCRIPT_02_OUTPUT_CSV", "russia_farmcom_excluded_list.csv")
    # Output files
    VED_OUTPUT_CSV = OUT_DIR / getenv("SCRIPT_03_VED_OUTPUT_CSV", "russia_ved_report.csv")
    EXCLUDED_OUTPUT_CSV = OUT_DIR / getenv("SCRIPT_03_EXCLUDED_OUTPUT_CSV", "russia_excluded_report.csv")
    # Final reports for exports
    VED_FINAL_REPORT = CENTRAL_OUTPUT_DIR / getenv("SCRIPT_03_VED_FINAL_REPORT", "Russia_VED_Report.csv")
    EXCLUDED_FINAL_REPORT = CENTRAL_OUTPUT_DIR / getenv("SCRIPT_03_EXCLUDED_FINAL_REPORT", "Russia_Excluded_Report.csv")
else:
    OUT_DIR = Path(__file__).parent
    CENTRAL_OUTPUT_DIR = Path(__file__).parent
    VED_INPUT_CSV = OUT_DIR / "russia_farmcom_ved_moscow_region.csv"
    EXCLUDED_INPUT_CSV = OUT_DIR / "russia_farmcom_excluded_list.csv"
    VED_OUTPUT_CSV = OUT_DIR / "russia_ved_report.csv"
    EXCLUDED_OUTPUT_CSV = OUT_DIR / "russia_excluded_report.csv"
    VED_FINAL_REPORT = OUT_DIR / "Russia_VED_Report.csv"
    EXCLUDED_FINAL_REPORT = OUT_DIR / "Russia_Excluded_Report.csv"


def clean_price(price_str: str) -> str:
    """Clean and format price string."""
    if not price_str:
        return ""
    # Remove any non-numeric characters except decimal point
    cleaned = re.sub(r'[^\d.,]', '', str(price_str))
    # Normalize decimal separator
    cleaned = cleaned.replace(',', '.')
    try:
        return f"{float(cleaned):.2f}"
    except (ValueError, TypeError):
        return price_str


def parse_date(date_str: str) -> str:
    """Parse and format date string."""
    if not date_str:
        return ""
    return format_date_ddmmyyyy(date_str)


def process_csv(input_csv: Path, source_type: str) -> list:
    """Process a CSV file and return cleaned rows."""
    if not input_csv.exists():
        print(f"  [SKIP] Input file not found: {input_csv}")
        return []
    
    rows = []
    try:
        with open(input_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        print(f"  [ERROR] Failed to read {input_csv}: {e}")
        return []
    
    print(f"  Found {len(rows)} records in {input_csv.name}")
    
    # Process rows
    processed_rows = []
    for row in rows:
        processed = {
            "Item_ID": row.get("item_id", ""),
            "Trade_Name": row.get("TN", ""),
            "INN": row.get("INN", ""),
            "Manufacturer_Country": row.get("Manufacturer_Country", ""),
            "Release_Form": row.get("Release_Form", ""),
            "EAN": row.get("EAN", ""),
            "Registered_Price_RUB": clean_price(row.get("Registered_Price_RUB", "")),
            "Price_Start_Date": parse_date(row.get("Start_Date_Text", "")),
            "Raw_Date_Text": row.get("Start_Date_Text", ""),
            "Source": source_type,
        }
        processed_rows.append(processed)
    
    # Sort by Trade Name
    processed_rows.sort(key=lambda x: (x.get("Trade_Name", "").lower(), x.get("INN", "").lower()))
    
    return processed_rows


def write_csv(rows: list, output_path: Path, include_source: bool = True) -> bool:
    """Write rows to CSV file."""
    if not rows:
        print(f"  [SKIP] No rows to write to {output_path.name}")
        return False
    
    fieldnames = [
        "Item_ID",
        "Trade_Name",
        "INN",
        "Manufacturer_Country",
        "Release_Form",
        "EAN",
        "Registered_Price_RUB",
        "Price_Start_Date",
        "Raw_Date_Text",
    ]
    if include_source:
        fieldnames.append("Source")
    
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Written: {output_path} ({len(rows)} rows)")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed to write {output_path}: {e}")
        return False


def process_data():
    """Process raw data and generate clean output."""
    print()
    print("=" * 80)
    print("RUSSIA OUTPUT GENERATOR")
    print("=" * 80)
    print()
    
    total_steps = 3
    current_step = 0
    
    # Step 1: Process VED Registry data
    current_step += 1
    print(f"[{current_step}/{total_steps}] Processing VED Registry data...")
    print(f"[PROGRESS] Generating output: {current_step}/{total_steps} ({round(current_step / total_steps * 100)}%)", flush=True)
    ved_rows = process_csv(VED_INPUT_CSV, "VED_Registry")
    
    # Step 2: Process Excluded List data
    current_step += 1
    print(f"\n[{current_step}/{total_steps}] Processing Excluded List data...")
    print(f"[PROGRESS] Generating output: {current_step}/{total_steps} ({round(current_step / total_steps * 100)}%)", flush=True)
    excluded_rows = process_csv(EXCLUDED_INPUT_CSV, "Excluded_List")
    
    # Step 3: Write individual reports
    current_step += 1
    print(f"\n[{current_step}/{total_steps}] Writing output files...")
    print(f"[PROGRESS] Generating output: {current_step}/{total_steps} ({round(current_step / total_steps * 100)}%)", flush=True)
    
    # Write VED report (without Source column for individual report)
    if ved_rows:
        write_csv(ved_rows, VED_OUTPUT_CSV, include_source=False)
        write_csv(ved_rows, VED_FINAL_REPORT, include_source=False)
    
    # Write Excluded report (without Source column for individual report)
    if excluded_rows:
        write_csv(excluded_rows, EXCLUDED_OUTPUT_CSV, include_source=False)
        write_csv(excluded_rows, EXCLUDED_FINAL_REPORT, include_source=False)
    
    # Summary
    print()
    print("=" * 80)
    print("OUTPUT GENERATION COMPLETE!")
    print("=" * 80)
    print(f"  VED Registry records: {len(ved_rows)}")
    print(f"  Excluded List records: {len(excluded_rows)}")
    print()
    print("Output files:")
    if ved_rows:
        print(f"  - {VED_OUTPUT_CSV}")
        print(f"  - {VED_FINAL_REPORT}")
    if excluded_rows:
        print(f"  - {EXCLUDED_OUTPUT_CSV}")
        print(f"  - {EXCLUDED_FINAL_REPORT}")
    print("=" * 80)
    print()
    print(f"[PROGRESS] Generating output: {total_steps}/{total_steps} (100%) - Completed", flush=True)
    
    return True


def main():
    success = process_data()
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
