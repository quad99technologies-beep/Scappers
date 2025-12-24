"""
Consolidate product details into a single file.

This script:
- Reads quest3_product_details.csv (from Script 02)
- Standardizes column names
- Saves as consolidated_products.csv

This replaces the old consolidation of multiple search result files.
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd

# Paths
OUTPUT_BASE_DIR = Path("../Output")
QUEST3_DETAILS = OUTPUT_BASE_DIR / "quest3_product_details.csv"
CONSOLIDATED_FILE = OUTPUT_BASE_DIR / "consolidated_products.csv"

OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)


def consolidate_product_details() -> None:
    """Process quest3_product_details.csv into standardized consolidated_products.csv"""

    if not QUEST3_DETAILS.exists():
        print(f"ERROR: Product details file not found: {QUEST3_DETAILS}")
        print("Please run Script 02 first to generate quest3_product_details.csv")
        return

    print(f"Reading product details from: {QUEST3_DETAILS}")

    try:
        df = pd.read_csv(QUEST3_DETAILS, dtype=str, keep_default_na=False)

        # Check required columns exist
        required_cols = ["Registration No", "Product Name", "Holder"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in input file: {', '.join(missing_cols)}")

        # Rename columns to match expected format for Script 05
        # Expected columns: Registration No / Notification No, Product Name, Holder
        df_consolidated = pd.DataFrame()
        df_consolidated["Registration No / Notification No"] = df["Registration No"]
        df_consolidated["Product Name"] = df["Product Name"]
        df_consolidated["Holder"] = df["Holder"]

        # Remove rows with missing Product Name or Holder
        initial_count = len(df_consolidated)
        df_consolidated = df_consolidated[
            (df_consolidated["Product Name"] != "") &
            (df_consolidated["Holder"] != "")
        ]
        final_count = len(df_consolidated)

        if initial_count != final_count:
            print(f"  Removed {initial_count - final_count} rows with missing Product Name or Holder")

        # Remove duplicates
        df_consolidated = df_consolidated.drop_duplicates(subset=["Registration No / Notification No"])

        # Save consolidated file
        df_consolidated.to_csv(CONSOLIDATED_FILE, index=False, encoding="utf-8")

        print(f"\n[SUCCESS] Created {CONSOLIDATED_FILE}")
        print(f"  Total rows: {len(df_consolidated)}")
        print(f"  Columns: {', '.join(df_consolidated.columns.tolist())}")

    except Exception as e:
        print(f"\n[ERROR] Failed to process product details: {e}")
        raise


if __name__ == "__main__":
    consolidate_product_details()

