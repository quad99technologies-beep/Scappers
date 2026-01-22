#!/usr/bin/env python3
"""
Generate Netherlands PCID Mapped report from:
  - consolidated_products.csv (from Script 03)

Output columns align to standard PCID format.

Usage:
  python 05_Generate_PCID_Mapped.py
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

import argparse
import re

import numpy as np
import pandas as pd
from config_loader import (
    load_env_file, require_env, getenv, getenv_float, getenv_int, 
    getenv_list, get_output_dir, get_central_output_dir, get_input_dir
)

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.standalone_checkpoint import run_with_checkpoint

# Load environment variables from .env file
load_env_file()

# FINAL_COLUMNS will be loaded from config
FINAL_COLUMNS = getenv_list("SCRIPT_05_FINAL_COLUMNS", [
    "Country", "Currency", "Source", "Region", "Company", "Product Group",
    "Local Product Name", "Generic Name", "Local Pack Description", "Pack Unit",
    "Pack Size", "LOCAL_PACK_CODE", "Unit Price", "PCID Mapping", "VAT Percent",
    "Public without VAT Price", "Public with VAT Price", "Reimbursable Status",
    "Reimbursable Rate", "Copayment Percent"
])


def _file_date(path: Path) -> datetime.date:
    """Return the date part of the file's last modification time."""
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def _is_same_day(path: Path) -> bool:
    """Return True if the file was modified today (local date)."""
    return _file_date(path) == datetime.now().date()


def norm_key(x: object) -> str:
    """Normalize join key: strip, uppercase, collapse spaces."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    return s


def load_pcid_mapping(pcid_path: Path) -> pd.DataFrame:
    """Load PCID mapping from CSV file."""
    pcid_df = pd.read_csv(pcid_path, dtype=str, keep_default_na=False)
    
    # Determine join column - check common column names
    join_col = getenv("SCRIPT_05_PCID_JOIN_COLUMN", "LOCAL_PACK_CODE")
    if join_col not in pcid_df.columns:
        # Try common alternatives
        for alt_col in ["detail_url", "product_name", "brand_name"]:
            if alt_col in pcid_df.columns:
                join_col = alt_col
                break
    
    # Normalize join column for consistent joining
    if join_col in pcid_df.columns:
        pcid_df[join_col] = pcid_df[join_col].map(norm_key)
    
    return pcid_df


def load_consolidated(consolidated_path: Path) -> pd.DataFrame:
    """Load consolidated products data."""
    cons = pd.read_csv(consolidated_path, dtype=str, keep_default_na=False)
    return cons


def to_float(series: pd.Series) -> pd.Series:
    """Convert string to float safely (empty -> NaN)."""
    s = series.replace("", np.nan)
    # Remove commas, currency symbols if any
    s = s.astype(str).str.replace(",", "", regex=False)
    s = s.str.replace("€", "", regex=False).str.replace("EUR", "", regex=False)
    s = s.str.replace("$", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def build_report(cons: pd.DataFrame, pcid_mapping: pd.DataFrame) -> pd.DataFrame:
    """Build final report from consolidated products and PCID mapping."""
    
    # Determine join key
    join_key = getenv("SCRIPT_05_JOIN_KEY", "detail_url")
    
    # If join_key doesn't exist, try to create one from product_name + brand_name
    if join_key not in cons.columns:
        if "product_name" in cons.columns and "brand_name" in cons.columns:
            cons["_join_key"] = (cons["product_name"].fillna("") + "|" + cons["brand_name"].fillna("")).map(norm_key)
            join_key = "_join_key"
        elif "detail_url" in cons.columns:
            join_key = "detail_url"
        else:
            raise ValueError("Cannot determine join key. Need detail_url or (product_name + brand_name)")
    
    # Normalize join key in consolidated
    if join_key == "detail_url":
        cons["_normalized_key"] = cons[join_key].str.strip()
    else:
        cons["_normalized_key"] = cons[join_key].map(norm_key)
    
    # Determine PCID join column
    pcid_join_col = getenv("SCRIPT_05_PCID_JOIN_COLUMN", "LOCAL_PACK_CODE")
    if pcid_join_col not in pcid_mapping.columns:
        # Try to find matching column
        for col in pcid_mapping.columns:
            if col.lower() in ["detail_url", "local_pack_code", "product_name", "brand_name"]:
                pcid_join_col = col
                break
    
    if pcid_join_col in pcid_mapping.columns:
        # Normalize PCID join column
        if pcid_join_col == "detail_url":
            pcid_mapping["_normalized_key"] = pcid_mapping[pcid_join_col].str.strip()
        else:
            pcid_mapping["_normalized_key"] = pcid_mapping[pcid_join_col].map(norm_key)
    else:
        # If no join column found, create dummy mapping
        pcid_mapping["_normalized_key"] = ""
        pcid_mapping["PCID Mapping"] = ""
    
    # Join with PCID mapping
    df = cons.merge(
        pcid_mapping[["_normalized_key", "PCID Mapping"]].drop_duplicates(subset=["_normalized_key"]),
        on="_normalized_key",
        how="left",
    )
    
    # Convert numeric columns
    price_col = getenv("SCRIPT_05_PRICE_COLUMN", "price_per_day")
    if price_col in df.columns:
        df["PRICE_NUM"] = to_float(df[price_col])
    else:
        # Try to find price column
        for col in ["price_per_day", "price_per_week", "price_per_month", "unit_price_vat", "pack_price_vat"]:
            if col in df.columns:
                df["PRICE_NUM"] = to_float(df[col])
                break
        else:
            df["PRICE_NUM"] = np.nan
    
    # Extract pack size if available
    if "unit_amount" in df.columns:
        df["PACK_SIZE_NUM"] = to_float(df["unit_amount"])
    elif "pack_presentation" in df.columns:
        # Try to extract number from pack_presentation
        df["PACK_SIZE_NUM"] = df["pack_presentation"].str.extract(r"(\d+)", expand=False).astype(float)
    else:
        df["PACK_SIZE_NUM"] = np.nan
    
    # Product group: prefer brand_name, else product_name
    if "brand_name" in df.columns and "product_name" in df.columns:
        df["PRODUCT_GROUP"] = df["brand_name"].where(df["brand_name"].str.strip() != "", df["product_name"])
    elif "product_name" in df.columns:
        df["PRODUCT_GROUP"] = df["product_name"]
    elif "brand_name" in df.columns:
        df["PRODUCT_GROUP"] = df["brand_name"]
    else:
        df["PRODUCT_GROUP"] = ""
    
    # Build final frame with required columns
    out = pd.DataFrame(index=df.index)
    for col in FINAL_COLUMNS:
        out[col] = np.nan
    
    # Set static values
    country_value = getenv("SCRIPT_05_COUNTRY_VALUE", "Netherlands")
    out["Country"] = country_value
    currency_value = getenv("SCRIPT_05_CURRENCY_VALUE", "EUR")
    out["Currency"] = currency_value
    source_value = getenv("SCRIPT_05_SOURCE_VALUE", "Farmacotherapeutisch Kompas")
    out["Source"] = source_value
    region_value = getenv("SCRIPT_05_REGION_VALUE", "EUROPE")
    out["Region"] = region_value
    
    # Map data columns
    if "manufacturer" in df.columns:
        out["Company"] = df["manufacturer"].replace("", np.nan)
    else:
        out["Company"] = np.nan
    
    out["Product Group"] = df["PRODUCT_GROUP"].replace("", np.nan)
    out["Local Product Name"] = np.nan  # Not available in Netherlands data
    out["Generic Name"] = df.get("product_name", pd.Series(index=df.index)).replace("", np.nan)
    
    if "pack_presentation" in df.columns:
        out["Local Pack Description"] = df["pack_presentation"].replace("", np.nan)
    else:
        out["Local Pack Description"] = np.nan
    
    if "unit_type" in df.columns:
        out["Pack Unit"] = df["unit_type"].replace("", np.nan)
    else:
        out["Pack Unit"] = np.nan
    
    out["Pack Size"] = df["PACK_SIZE_NUM"]
    
    # Use detail_url as LOCAL_PACK_CODE if no dedicated code exists
    if "local_pack_code" in df.columns:
        out["LOCAL_PACK_CODE"] = df["local_pack_code"].replace("", np.nan)
    elif "detail_url" in df.columns:
        out["LOCAL_PACK_CODE"] = df["detail_url"].replace("", np.nan)
    else:
        out["LOCAL_PACK_CODE"] = np.nan
    
    out["Unit Price"] = df["PRICE_NUM"]
    
    # Use PCID Mapping from join
    if "PCID Mapping" in df.columns:
        out["PCID Mapping"] = df["PCID Mapping"].replace("", np.nan)
    else:
        out["PCID Mapping"] = np.nan
    
    # VAT Handling for Netherlands
    vat_rate = getenv_float("SCRIPT_05_DEFAULT_VAT_PERCENT", 0.09)  # 9% VAT in Netherlands
    out["VAT Percent"] = vat_rate
    
    # Calculate prices with/without VAT
    price_without_vat = df["PRICE_NUM"] / (1.0 + vat_rate) if "PRICE_NUM" in df.columns else np.nan
    out["Public without VAT Price"] = price_without_vat
    out["Public with VAT Price"] = df["PRICE_NUM"]
    
    # Reimbursable Status Logic
    if "reimbursed_per_day" in df.columns:
        # If reimbursed_per_day has a value, it's reimbursed
        is_reimbursable = df["reimbursed_per_day"].notna() & (df["reimbursed_per_day"].str.strip() != "")
        out["Reimbursable Status"] = np.where(is_reimbursable, "REIMBURSABLE", "NON REIMBURSABLE")
        out["Reimbursable Rate"] = np.where(is_reimbursable, "100.00%", "0.00%")
        out["Copayment Percent"] = np.where(is_reimbursable, "0.00%", "100.00%")
    else:
        out["Reimbursable Status"] = "UNKNOWN"
        out["Reimbursable Rate"] = np.nan
        out["Copayment Percent"] = np.nan
    
    # Reorder exactly
    out = out[FINAL_COLUMNS]
    
    return out


def main() -> None:
    # Get input directory using ConfigManager
    input_dir_str = getenv("SCRIPT_05_INPUT_DIR", "")
    if input_dir_str and Path(input_dir_str).is_absolute():
        input_dir = Path(input_dir_str)
    else:
        input_dir = get_input_dir()
    
    # Get output directory using ConfigManager
    output_dir_str = getenv("SCRIPT_05_OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        output_dir = Path(output_dir_str)
    else:
        output_dir = get_output_dir()
    
    pcid_mapping_path = (input_dir / require_env("SCRIPT_05_PCID_MAPPING")).resolve()
    consolidated_path = (output_dir / require_env("SCRIPT_05_CONSOLIDATED")).resolve()
    
    # Use exports directory for final output files
    exports_dir = get_central_output_dir()
    exports_dir.mkdir(parents=True, exist_ok=True)
    
    # Get date in ddmmyyyy format
    date_str = datetime.now().strftime("%d%m%Y")
    
    # Add date to filenames
    mapped_filename = require_env("SCRIPT_05_OUT_MAPPED")
    not_mapped_filename = require_env("SCRIPT_05_OUT_NOT_MAPPED")
    
    # Insert date before file extension
    mapped_name_parts = Path(mapped_filename).stem, Path(mapped_filename).suffix
    not_mapped_name_parts = Path(not_mapped_filename).stem, Path(not_mapped_filename).suffix
    
    mapped_filename_with_date = f"{mapped_name_parts[0]}_{date_str}{mapped_name_parts[1]}"
    not_mapped_filename_with_date = f"{not_mapped_name_parts[0]}_{date_str}{not_mapped_name_parts[1]}"
    
    out_path_mapped = (exports_dir / mapped_filename_with_date).resolve()
    out_path_not_mapped = (exports_dir / not_mapped_filename_with_date).resolve()
    
    # Allow command line override
    ap = argparse.ArgumentParser(description="Generate Netherlands PCID Mapped report")
    ap.add_argument("--pcid", default=str(pcid_mapping_path), help="Path to PCID Mapping file")
    ap.add_argument("--consolidated", default=str(consolidated_path), help="Path to consolidated_products.csv")
    ap.add_argument("--out-mapped", default=str(out_path_mapped), help="Output path for mapped records")
    ap.add_argument("--out-not-mapped", default=str(out_path_not_mapped), help="Output path for not mapped records")
    args = ap.parse_args()
    
    pcid_path = Path(args.pcid).resolve()
    cons_path = Path(args.consolidated).resolve()
    
    print(f"\n[SCRIPT 05] Loading input files...", flush=True)
    if not pcid_path.exists():
        # Check for alternative filename
        alt_names = ["PCID Mapping - Netherlands.csv", "Netherlands_PCID.csv"]
        found_alt = None
        for alt_name in alt_names:
            alt_path = input_dir / alt_name
            if alt_path.exists():
                found_alt = alt_path
                break
        
        if found_alt:
            print(f"  -> WARNING: Config specifies '{pcid_path.name}', but found '{found_alt.name}' in input directory.", flush=True)
            print(f"  -> Using found file: {found_alt}", flush=True)
            pcid_path = found_alt
        else:
            error_msg = (
                f"PCID mapping file not found: {pcid_path}\n"
                f"  Expected file name (from config SCRIPT_05_PCID_MAPPING): {pcid_path.name}\n"
                f"  Checked in: {input_dir}\n"
                f"  Also checked for: {', '.join(alt_names)}\n"
                f"  Please ensure the PCID mapping file exists in the input directory."
            )
            raise FileNotFoundError(error_msg)
    print(f"  -> PCID mapping: {pcid_path}", flush=True)
    
    if not cons_path.exists():
        raise FileNotFoundError(f"Consolidated products file not found: {cons_path}")
    print(f"  -> Consolidated products: {cons_path}", flush=True)
    
    print(f"\n[SCRIPT 05] Loading data...", flush=True)
    print(f"  -> Loading PCID mapping...", flush=True)
    pcid_mapping = load_pcid_mapping(pcid_path)
    print(f"  -> Loaded {len(pcid_mapping):,} PCID mappings", flush=True)
    print(f"[PROGRESS] Loading data: PCID mapping loaded (1/2)", flush=True)
    
    print(f"  -> Loading consolidated products...", flush=True)
    cons = load_consolidated(cons_path)
    print(f"  -> Loaded {len(cons):,} consolidated products", flush=True)
    print(f"[PROGRESS] Loading data: Consolidated products loaded (2/2) (100%)", flush=True)
    
    print(f"\n[SCRIPT 05] Building report...", flush=True)
    report = build_report(cons, pcid_mapping)
    print(f"  -> Report built: {len(report):,} total rows", flush=True)
    print(f"[PROGRESS] Building report: {len(report)}/{len(report)} (100%)", flush=True)
    
    # Split into mapped and not mapped
    print(f"\n[SCRIPT 05] Splitting into mapped and not mapped...", flush=True)
    mapped_report = report[report["PCID Mapping"].notna() & (report["PCID Mapping"].str.strip() != "")].copy()
    not_mapped_report = report[report["PCID Mapping"].isna() | (report["PCID Mapping"].str.strip() == "")].copy()
    print(f"  -> Mapped products: {len(mapped_report):,}", flush=True)
    print(f"  -> Not mapped products: {len(not_mapped_report):,}", flush=True)
    
    # Save mapped records
    print(f"\n[SCRIPT 05] Saving mapped records...", flush=True)
    out_path_mapped = Path(args.out_mapped)
    out_path_mapped.parent.mkdir(parents=True, exist_ok=True)
    mapped_report.to_csv(out_path_mapped, index=False, encoding="utf-8-sig")
    print(f"[OK] Wrote {len(mapped_report):,} MAPPED rows to: {out_path_mapped}", flush=True)
    
    # Save not mapped records
    print(f"\n[SCRIPT 05] Saving not mapped records...", flush=True)
    out_path_not_mapped = Path(args.out_not_mapped)
    out_path_not_mapped.parent.mkdir(parents=True, exist_ok=True)
    not_mapped_report.to_csv(out_path_not_mapped, index=False, encoding="utf-8-sig")
    print(f"[OK] Wrote {len(not_mapped_report):,} NOT MAPPED rows to: {out_path_not_mapped}", flush=True)
    
    print(f"\n[SUMMARY] Summary:")
    print(f"   Total records: {len(report):,}")
    print(f"   Mapped: {len(mapped_report):,}")
    print(f"   Not Mapped: {len(not_mapped_report):,}")


if __name__ == "__main__":
    run_with_checkpoint(
        main,
        "Netherlands",
        5,
        "Generate PCID Mapped",
        output_files=[]  # Output files are in exports directory, tracked separately
    )
