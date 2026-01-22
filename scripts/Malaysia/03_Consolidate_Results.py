"""
Consolidate product details into a single file.

This script:
- Reads quest3_product_details.csv (from Script 02)
- Standardizes column names
- Saves as consolidated_products.csv

This replaces the old consolidation of multiple search result files.
"""

from __future__ import annotations

import sys
import os

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from pathlib import Path
import pandas as pd
from config_loader import load_env_file, require_env, getenv, getenv_list, get_output_dir

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.standalone_checkpoint import run_with_checkpoint

# Load environment variables from .env file
load_env_file()

# Paths - Use ConfigManager output directory instead of local output folder
output_base_dir_path = getenv("SCRIPT_03_OUTPUT_BASE_DIR", "")
if output_base_dir_path and Path(output_base_dir_path).is_absolute():
    OUTPUT_BASE_DIR = Path(output_base_dir_path)
else:
    OUTPUT_BASE_DIR = get_output_dir()
QUEST3_DETAILS = OUTPUT_BASE_DIR / require_env("SCRIPT_03_QUEST3_DETAILS")
CONSOLIDATED_FILE = OUTPUT_BASE_DIR / require_env("SCRIPT_03_CONSOLIDATED_FILE")

OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)


def consolidate_product_details() -> None:
    """Process quest3_product_details.csv into standardized consolidated_products.csv"""

    if not QUEST3_DETAILS.exists():
        print(f"ERROR: Product details file not found: {QUEST3_DETAILS}", flush=True)
        print("Please run Script 02 first to generate quest3_product_details.csv", flush=True)
        return

    print(f"Reading product details from: {QUEST3_DETAILS}", flush=True)

    try:
        print(f"  -> Loading CSV file...", flush=True)
        df = pd.read_csv(QUEST3_DETAILS, dtype=str, keep_default_na=False)
        print(f"  -> Loaded {len(df):,} rows, {len(df.columns)} columns", flush=True)
        print(f"[PROGRESS] Consolidating: Loading data ({len(df):,} rows)", flush=True)

        # Check required columns exist
        from config_loader import getenv_list
        print(f"  -> Checking required columns...", flush=True)
        required_cols = getenv_list("SCRIPT_03_REQUIRED_COLUMNS", ["Registration No", "Product Name", "Holder"])
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in input file: {', '.join(missing_cols)}")
        print(f"  -> All required columns found: {', '.join(required_cols)}", flush=True)

        # Rename columns to match expected format for Script 05
        # Expected columns: Registration No / Notification No, Product Name, Holder
        print(f"  -> Renaming columns for output format...", flush=True)
        df_consolidated = pd.DataFrame()
        output_reg_col = require_env("SCRIPT_03_OUTPUT_COLUMN_REGISTRATION")
        df_consolidated[output_reg_col] = df["Registration No"]
        df_consolidated["Product Name"] = df["Product Name"]
        df_consolidated["Holder"] = df["Holder"]

        # Remove rows with missing Product Name or Holder
        print(f"  -> Removing rows with missing data...", flush=True)
        initial_count = len(df_consolidated)
        df_consolidated = df_consolidated[
            (df_consolidated["Product Name"] != "") &
            (df_consolidated["Holder"] != "")
        ]
        final_count = len(df_consolidated)

        if initial_count != final_count:
            print(f"  -> Removed {initial_count - final_count} rows with missing Product Name or Holder", flush=True)

        # Remove duplicates
        print(f"  -> Removing duplicates...", flush=True)
        output_reg_col = require_env("SCRIPT_03_OUTPUT_COLUMN_REGISTRATION")
        before_dedup = len(df_consolidated)
        df_consolidated = df_consolidated.drop_duplicates(subset=[output_reg_col])
        after_dedup = len(df_consolidated)
        if before_dedup != after_dedup:
            print(f"  -> Removed {before_dedup - after_dedup} duplicate rows", flush=True)

        # Save consolidated file
        print(f"  -> Saving consolidated file...", flush=True)
        df_consolidated.to_csv(CONSOLIDATED_FILE, index=False, encoding="utf-8")
        
        print(f"[PROGRESS] Consolidating: {len(df_consolidated)}/{len(df_consolidated)} (100%)", flush=True)

        print(f"\n[SUCCESS] Created {CONSOLIDATED_FILE}", flush=True)
        print(f"  Total rows: {len(df_consolidated):,}", flush=True)
        print(f"  Columns: {', '.join(df_consolidated.columns.tolist())}", flush=True)

    except Exception as e:
        print(f"\n[ERROR] Failed to process product details: {e}", flush=True)
        raise


if __name__ == "__main__":
    run_with_checkpoint(
        consolidate_product_details,
        "Malaysia",
        3,
        "Consolidate Results",
        output_files=[CONSOLIDATED_FILE]
    )

