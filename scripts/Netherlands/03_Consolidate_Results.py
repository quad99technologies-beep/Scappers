"""
Consolidate product details and costs into a single file.

This script:
- Reads details.csv and costs.csv (from Script 02)
- Joins them on detail_url
- Standardizes column names
- Removes duplicates
- Saves as consolidated_products.csv
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

DETAILS_CSV = OUTPUT_BASE_DIR / require_env("SCRIPT_03_DETAILS_CSV", "details.csv")
COSTS_CSV = OUTPUT_BASE_DIR / require_env("SCRIPT_03_COSTS_CSV", "costs.csv")
CONSOLIDATED_FILE = OUTPUT_BASE_DIR / require_env("SCRIPT_03_CONSOLIDATED_FILE", "consolidated_products.csv")

OUTPUT_BASE_DIR.mkdir(parents=True, exist_ok=True)


def consolidate_product_details() -> None:
    """Process details.csv and costs.csv into standardized consolidated_products.csv"""

    if not DETAILS_CSV.exists():
        print(f"ERROR: Details file not found: {DETAILS_CSV}", flush=True)
        print("Please run Script 02 first to generate details.csv", flush=True)
        return

    if not COSTS_CSV.exists():
        print(f"ERROR: Costs file not found: {COSTS_CSV}", flush=True)
        print("Please run Script 02 first to generate costs.csv", flush=True)
        return

    print(f"Reading details from: {DETAILS_CSV}", flush=True)
    print(f"Reading costs from: {COSTS_CSV}", flush=True)

    try:
        print(f"  -> Loading details CSV file...", flush=True)
        df_details = pd.read_csv(DETAILS_CSV, dtype=str, keep_default_na=False)
        print(f"  -> Loaded {len(df_details):,} detail rows, {len(df_details.columns)} columns", flush=True)

        print(f"  -> Loading costs CSV file...", flush=True)
        df_costs = pd.read_csv(COSTS_CSV, dtype=str, keep_default_na=False)
        print(f"  -> Loaded {len(df_costs):,} cost rows, {len(df_costs.columns)} columns", flush=True)

        print(f"[PROGRESS] Consolidating: Loading data ({len(df_details):,} details, {len(df_costs):,} costs)", flush=True)

        # Check required columns exist
        required_detail_cols = getenv_list("SCRIPT_03_REQUIRED_DETAIL_COLUMNS", ["detail_url", "product_name"])
        missing_detail_cols = [col for col in required_detail_cols if col not in df_details.columns]
        if missing_detail_cols:
            raise ValueError(f"Missing required columns in details file: {', '.join(missing_detail_cols)}")
        print(f"  -> All required detail columns found: {', '.join(required_detail_cols)}", flush=True)

        required_cost_cols = getenv_list("SCRIPT_03_REQUIRED_COST_COLUMNS", ["detail_url", "brand_name"])
        missing_cost_cols = [col for col in required_cost_cols if col not in df_costs.columns]
        if missing_cost_cols:
            raise ValueError(f"Missing required columns in costs file: {', '.join(missing_cost_cols)}")
        print(f"  -> All required cost columns found: {', '.join(required_cost_cols)}", flush=True)

        # Join details and costs on detail_url
        print(f"  -> Joining details and costs on detail_url...", flush=True)
        df_consolidated = df_details.merge(
            df_costs,
            on="detail_url",
            how="left",
            suffixes=("", "_cost")
        )
        print(f"  -> Joined result: {len(df_consolidated):,} rows", flush=True)

        # Remove rows with missing essential data
        print(f"  -> Removing rows with missing data...", flush=True)
        initial_count = len(df_consolidated)
        
        # Get columns to check for missing data
        check_cols = getenv_list("SCRIPT_03_CHECK_COLUMNS", ["product_name", "brand_name"])
        existing_check_cols = [col for col in check_cols if col in df_consolidated.columns]
        
        if existing_check_cols:
            # Keep rows where at least one check column has data
            mask = df_consolidated[existing_check_cols[0]].str.strip() != ""
            for col in existing_check_cols[1:]:
                mask = mask | (df_consolidated[col].str.strip() != "")
            df_consolidated = df_consolidated[mask]
        else:
            # If no check columns specified, just remove completely empty rows
            df_consolidated = df_consolidated.dropna(how="all")

        final_count = len(df_consolidated)
        if initial_count != final_count:
            print(f"  -> Removed {initial_count - final_count} rows with missing essential data", flush=True)

        # Remove duplicates based on detail_url (keep first occurrence)
        print(f"  -> Removing duplicates...", flush=True)
        before_dedup = len(df_consolidated)
        
        # If brand_name exists, deduplicate on detail_url + brand_name, otherwise just detail_url
        dedup_cols = getenv_list("SCRIPT_03_DEDUP_COLUMNS", ["detail_url", "brand_name"])
        existing_dedup_cols = [col for col in dedup_cols if col in df_consolidated.columns]
        
        if existing_dedup_cols:
            df_consolidated = df_consolidated.drop_duplicates(subset=existing_dedup_cols, keep="first")
        else:
            df_consolidated = df_consolidated.drop_duplicates(subset=["detail_url"], keep="first")
        
        after_dedup = len(df_consolidated)
        if before_dedup != after_dedup:
            print(f"  -> Removed {before_dedup - after_dedup} duplicate rows", flush=True)

        # Standardize column order (put key columns first)
        key_cols = getenv_list("SCRIPT_03_KEY_COLUMNS", [
            "detail_url", "product_name", "brand_name", "manufacturer", 
            "administration_form", "strengths_raw", "pack_presentation",
            "currency", "price_per_day", "reimbursed_per_day"
        ])
        existing_key_cols = [col for col in key_cols if col in df_consolidated.columns]
        remaining_cols = [col for col in df_consolidated.columns if col not in existing_key_cols]
        df_consolidated = df_consolidated[existing_key_cols + remaining_cols]

        # Save consolidated file
        print(f"  -> Saving consolidated file...", flush=True)
        df_consolidated.to_csv(CONSOLIDATED_FILE, index=False, encoding="utf-8")
        
        print(f"[PROGRESS] Consolidating: {len(df_consolidated)}/{len(df_consolidated)} (100%)", flush=True)

        print(f"\n[SUCCESS] Created {CONSOLIDATED_FILE}", flush=True)
        print(f"  Total rows: {len(df_consolidated):,}", flush=True)
        print(f"  Columns: {len(df_consolidated.columns)}", flush=True)
        print(f"  Key columns: {', '.join(existing_key_cols[:10])}{'...' if len(existing_key_cols) > 10 else ''}", flush=True)

    except Exception as e:
        print(f"\n[ERROR] Failed to process product details: {e}", flush=True)
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_with_checkpoint(
        consolidate_product_details,
        "Netherlands",
        3,
        "Consolidate Results",
        output_files=[str(CONSOLIDATED_FILE)]
    )
