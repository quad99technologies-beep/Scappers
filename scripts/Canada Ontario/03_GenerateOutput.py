#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Canada Ontario Final Output Generator

Reads products.csv and generates final output report with standardized columns.
Saves to exports/CanadaOntario/ with timestamp.

Output format:
- PCID (blank, can be populated later)
- Country
- Company (manufacturer_name)
- Local Product Name (blank or derived)
- Generic Name
- Effective Start Date (blank)
- Public With VAT Price
- Reimbursement Category
- Reimbursement Amount
- Co-Pay Amount
- Local Pack Description (brand_name_strength_dosage)
- LOCAL_PACK_CODE
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir, get_central_output_dir,
    FINAL_REPORT_NAME_PREFIX, FINAL_REPORT_DATE_FORMAT,
    PRODUCTS_CSV_NAME, STATIC_CURRENCY, STATIC_REGION,
    get_run_id, get_run_dir,
)
from core.logger import setup_standard_logger
from core.progress_tracker import StandardProgress

import pandas as pd

# Paths
OUTPUT_DIR = get_output_dir()
CENTRAL_OUTPUT_DIR = get_central_output_dir()  # This is exports/CanadaOntario/
INPUT_CSV = OUTPUT_DIR / PRODUCTS_CSV_NAME

# Generate date-based filename: canadaontarioreport_ddmmyyyy.csv
date_str = datetime.now().strftime(FINAL_REPORT_DATE_FORMAT)
# Save directly to exports folder (final output folder)
OUTPUT_CSV = CENTRAL_OUTPUT_DIR / f"{FINAL_REPORT_NAME_PREFIX}{date_str}.csv"

# Ensure directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CENTRAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def parse_float(value: Optional[object]) -> Optional[float]:
    """Parse float value, handling None and empty strings."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s.lower() in {"n/a", "na", ""}:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def determine_reimbursement_category(row: pd.Series) -> tuple:
    """
    Determine reimbursement category and amounts based on Ontario rules.
    
    Returns: (category, reimbursement_amount, copay_amount)
    """
    reimbursable_price = parse_float(row.get("reimbursable_price"))
    copay = parse_float(row.get("copay"))
    public_with_vat = parse_float(row.get("public_with_vat"))
    
    # If reimbursable_price exists and is > 0, product is reimbursable
    if reimbursable_price is not None and reimbursable_price > 0:
        # Determine category based on copay
        if copay is not None and copay > 0:
            category = "REIMBURSABLE WITH COPAY"
        else:
            category = "FULLY REIMBURSABLE"
        return category, reimbursable_price, copay if copay is not None else 0.0
    else:
        # No reimbursement
        return "NON REIMBURSABLE", None, copay if copay is not None else None


def main():
    """Main entry point."""
    run_id = get_run_id()
    run_dir = get_run_dir(run_id)
    logger = setup_standard_logger(
        "canada_ontario_output",
        scraper_name="CanadaOntario",
        log_file=run_dir / "logs" / "final_output.log",
    )
    progress = StandardProgress(
        "canada_ontario_output",
        total=3,
        unit="steps",
        logger=logger,
        state_path=CENTRAL_OUTPUT_DIR / "output_progress.json",
        log_every=1,
    )
    logger.info("Canada Ontario final output generator")
    
    if not INPUT_CSV.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_CSV}\n"
            f"Please run script 01 (extract_product_details.py) first to generate this file."
        )
    
    # Read input CSV
    progress.update(0, message="load data", force=True)
    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    logger.info("Loaded %s rows from %s", len(df), INPUT_CSV.name)
    
    # Build final output dataframe
    progress.update(1, message="process data", force=True)
    
    # Initialize final dataframe
    df_final = pd.DataFrame()
    
    # Standard columns (matching other scrapers' format)
    df_final["PCID"] = ""  # Blank, can be populated later via mapping
    
    # Country
    df_final["Country"] = "CANADA"
    
    # Company (from manufacturer_name)
    df_final["Company"] = df["manufacturer_name"].fillna("").astype(str)
    
    # Local Product Name (blank for now, can be derived from brand_name_strength_dosage if needed)
    df_final["Local Product Name"] = ""
    
    # Generic Name
    df_final["Generic Name"] = df["generic_name"].fillna("").astype(str)
    
    # Effective Start Date (blank for now)
    df_final["Effective Start Date"] = ""
    
    # Public With VAT Price
    df_final["Public With VAT Price"] = df["public_with_vat"].apply(parse_float)
    
    # Reimbursement fields
    reimbursement_data = df.apply(determine_reimbursement_category, axis=1)
    df_final["Reimbursement Category"] = [x[0] for x in reimbursement_data]
    df_final["Reimbursement Amount"] = [x[1] for x in reimbursement_data]
    df_final["Co-Pay Amount"] = [x[2] for x in reimbursement_data]
    
    # Local Pack Description (from brand_name_strength_dosage)
    df_final["Local Pack Description"] = df["brand_name_strength_dosage"].fillna("").astype(str)
    
    # LOCAL_PACK_CODE
    df_final["LOCAL_PACK_CODE"] = df["local_pack_code"].fillna("").astype(str)
    
    # Additional Ontario-specific fields (optional, for reference)
    df_final["Price Type"] = df["price_type"].fillna("").astype(str)  # UNIT/PACK
    df_final["Interchangeable"] = df["interchangeable"].fillna("").astype(str)
    df_final["Limited Use"] = df["limited_use"].fillna("").astype(str)
    df_final["Therapeutic Notes"] = df["therapeutic_notes_requirements"].fillna("").astype(str)
    
    # Static values
    df_final["Currency"] = STATIC_CURRENCY
    df_final["Region"] = STATIC_REGION
    
    # Reorder columns to match standard format (PCID first, then standard columns)
    final_cols = [
        "PCID",
        "Country",
        "Company",
        "Local Product Name",
        "Generic Name",
        "Effective Start Date",
        "Public With VAT Price",
        "Reimbursement Category",
        "Reimbursement Amount",
        "Co-Pay Amount",
        "Local Pack Description",
        "LOCAL_PACK_CODE",
        "Price Type",
        "Interchangeable",
        "Limited Use",
        "Therapeutic Notes",
        "Currency",
        "Region",
    ]
    
    # Ensure all columns exist
    for col in final_cols:
        if col not in df_final.columns:
            df_final[col] = ""
    
    df_final = df_final[final_cols].copy()
    
    # Write output CSV
    progress.update(2, message="write output", force=True)
    df_final.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig", float_format="%.2f")
    progress.update(3, message="complete", force=True)
    
    logger.info("Wrote output: %s", OUTPUT_CSV)
    logger.info("Total rows: %s", len(df_final))
    logger.info("Final output: %s", OUTPUT_CSV)


if __name__ == "__main__":
    main()
