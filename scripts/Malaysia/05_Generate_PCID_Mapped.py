#!/usr/bin/env python3
"""
Generate "Malaysia_PCID Mapped" report from:
  - consolidated_products.csv  (NPRA product master)
  - malaysia_drug_prices_view_all.csv (price + pack + SKU)

Output columns are aligned to "Malaysia_PCID Mapped_ 02122025.xlsx" structure.

Usage:
  python generate_malaysia_pcid_mapped.py \
    --consolidated consolidated_products.csv \
    --prices malaysia_drug_prices_view_all.csv \
    --out Malaysia_PCID_Mapped_generated.xlsx

Notes:
- Join key = Registration Number (MAL....)
- Many PCID pricing fields are not present in the two inputs; they are left blank (NaN) to match the mapped format.
- PCID Mapping is generated deterministically from LOCAL_PACK_CODE + pack description + pack unit + pack size.
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
import csv
import re

import numpy as np
import pandas as pd
from config_loader import load_env_file, require_env, getenv, getenv_float, getenv_int, getenv_list, get_output_dir, get_central_output_dir

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.standalone_checkpoint import run_with_checkpoint
from core.standalone_checkpoint import run_with_checkpoint

# Load environment variables from .env file
load_env_file()

# FINAL_COLUMNS will be loaded from config
FINAL_COLUMNS = getenv_list("SCRIPT_05_FINAL_COLUMNS", [])


def _file_date(path: Path) -> datetime.date:
    """Return the date part of the file's last modification time."""
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def _is_same_day(path: Path) -> bool:
    """Return True if the file was modified today (local date)."""
    return _file_date(path) == datetime.now().date()


def norm_regno(x: object) -> str:
    """Normalize Registration Number: strip, uppercase, collapse spaces."""
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    return s


def load_pcid_mapping(pcid_path: Path) -> pd.DataFrame:
    """Load PCID mapping from CSV file."""
    pcid_df = pd.read_csv(pcid_path, dtype=str, keep_default_na=False)
    # Normalize LOCAL_PACK_CODE for consistent joining
    pcid_df["LOCAL_PACK_CODE"] = pcid_df["LOCAL_PACK_CODE"].map(norm_regno)
    return pcid_df


def load_inputs(consolidated_path: Path, prices_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    cons = pd.read_csv(consolidated_path, dtype=str, keep_default_na=False)
    prices = pd.read_csv(prices_path, dtype=str, keep_default_na=False)

    # consolidated_products expected cols
    # ['Source File','#','Registration No / Notification No','Product Name','Holder']
    cons = cons.rename(
        columns={
            "Registration No / Notification No": "LOCAL_PACK_CODE",
            "Product Name": "CONS_PRODUCT_NAME",
            "Holder": "COMPANY",
        }
    )
    cons["LOCAL_PACK_CODE"] = cons["LOCAL_PACK_CODE"].map(norm_regno)

    # malaysia_drug_prices_view_all expected cols (long bilingual headers)
    col_reg = "Nombor Pendaftaran Produk/ Product Registration Number"
    col_gen = "Nama Generik/ Generic Name"
    col_brand = "Nama Dagangan/ Brand Name"
    col_packdesc = "Deskripsi Pembungkusan (Per Pek)/ Packaging Description (Per Pack)"
    col_unit = "Unit (SKU)"
    col_qty = "Kuantiti/ Quantity (SKU)"
    col_price_sku = "Harga Runcit Per Unit SKU yang Disyorkan oleh Pemegang Pendaftaran Produk/ Retail Price Per SKU Suggested by Product Registration Holder"
    col_price_pack = "Harga Runcit Per Pek yang Disyorkan oleh Pemegang Pendaftaran Produk / Retail Price per Pack Suggested by Product Registration Holder"
    col_year = "Tahun Kemaskini Harga/ Price Updated Year"

    # Some exports occasionally have slightly different spacing; do a fallback lookup if needed.
    def must_find(df: pd.DataFrame, name: str) -> str:
        if name in df.columns:
            return name
        # relaxed match - normalize spaces, case, and punctuation
        def normalize(s: str) -> str:
            # Remove all spaces, convert to lowercase, remove special chars except slashes
            s = re.sub(r"\s+", "", s.lower())
            s = re.sub(r"[^\w/]", "", s)
            return s
        target = normalize(name)
        for c in df.columns:
            if normalize(c) == target:
                return c
        # Last resort: try partial match
        target_words = set(re.findall(r"\w+", name.lower()))
        best_match = None
        best_score = 0
        for c in df.columns:
            col_words = set(re.findall(r"\w+", c.lower()))
            common = len(target_words & col_words)
            if common > best_score and common >= len(target_words) * 0.7:  # 70% word match
                best_score = common
                best_match = c
        if best_match:
            return best_match
        raise KeyError(f"Missing expected column in prices file: {name}\nAvailable columns: {list(df.columns)}")

    col_reg = must_find(prices, col_reg)
    col_gen = must_find(prices, col_gen)
    col_brand = must_find(prices, col_brand)
    col_packdesc = must_find(prices, col_packdesc)
    col_unit = must_find(prices, col_unit)
    col_qty = must_find(prices, col_qty)
    col_price_sku = must_find(prices, col_price_sku)
    col_price_pack = must_find(prices, col_price_pack)
    col_year = must_find(prices, col_year)

    prices = prices.rename(
        columns={
            col_reg: "LOCAL_PACK_CODE",
            col_gen: "GENERIC_NAME",
            col_brand: "BRAND_NAME",
            col_packdesc: "LOCAL_PACK_DESC",
            col_unit: "PACK_UNIT",
            col_qty: "PACK_SIZE",
            col_price_sku: "UNIT_PRICE",
            col_price_pack: "PACK_PRICE",
            col_year: "PRICE_UPDATED_YEAR",
        }
    )
    prices["LOCAL_PACK_CODE"] = prices["LOCAL_PACK_CODE"].map(norm_regno)

    return cons, prices


def to_float(series: pd.Series) -> pd.Series:
    """Convert string to float safely (empty -> NaN)."""
    s = series.replace("", np.nan)
    # Remove commas, currency symbols if any
    s = s.astype(str).str.replace(",", "", regex=False).str.replace("RM", "", regex=False).str.strip()
    return pd.to_numeric(s, errors="coerce")


def load_fully_reimbursable(reimbursable_path: Path) -> pd.DataFrame:
    """Load fully reimbursable drugs data."""
    reimb_df = pd.read_csv(reimbursable_path, dtype=str, keep_default_na=False)
    # Extract just the Generic Name column and normalize
    if "Generic Name" in reimb_df.columns:
        reimb_df = reimb_df[["Generic Name"]].copy()
        reimb_df["Generic Name"] = reimb_df["Generic Name"].str.strip().str.upper()
        reimb_df = reimb_df.drop_duplicates()
        return reimb_df
    return pd.DataFrame(columns=["Generic Name"])


def build_report(cons: pd.DataFrame, prices: pd.DataFrame, pcid_mapping: pd.DataFrame, fully_reimbursable: pd.DataFrame) -> pd.DataFrame:
    # Handle duplicates in consolidated file - keep first occurrence
    # (same registration number might appear in multiple product type searches)
    cons_unique = cons[["LOCAL_PACK_CODE", "COMPANY", "CONS_PRODUCT_NAME"]].drop_duplicates(
        subset=["LOCAL_PACK_CODE"], keep="first"
    )

    # Join: price rows (SKU-level) enriched with company + product name
    df = prices.merge(
        cons_unique,
        on="LOCAL_PACK_CODE",
        how="left",
        validate="m:1",
    )

    # Diagnostic: Count records without company match
    missing_company_count = df["COMPANY"].isna().sum()
    total_count = len(df)
    if missing_company_count > 0:
        print(f"\n[WARNING] WARNING: {missing_company_count:,} out of {total_count:,} records are missing company information")
        print(f"   This occurs when products in MyPriMe were not found in QUEST3+ product search.")
        print(f"   These records will still be included in the output.\n")

    # Join with PCID mapping
    df = df.merge(
        pcid_mapping,
        on="LOCAL_PACK_CODE",
        how="left",
    )

    # Convert numeric columns
    df["PACK_SIZE_NUM"] = to_float(df["PACK_SIZE"])
    df["UNIT_PRICE_NUM"] = to_float(df["UNIT_PRICE"])
    df["PACK_PRICE_NUM"] = to_float(df["PACK_PRICE"])

    # Product group: prefer brand name, else consolidated product name
    df["PRODUCT_GROUP"] = df["BRAND_NAME"].where(df["BRAND_NAME"].str.strip() != "", df["CONS_PRODUCT_NAME"])

    # Build final frame with required columns (same number of rows as merged df)
    out = pd.DataFrame(index=df.index)
    for col in FINAL_COLUMNS:
        out[col] = np.nan

    country_value = require_env("SCRIPT_05_COUNTRY_VALUE")
    out["Country"] = country_value
    currency_value = require_env("SCRIPT_05_CURRENCY_VALUE")
    out["Currency"] = currency_value
    source_value = require_env("SCRIPT_05_SOURCE_VALUE")
    out["Source"] = source_value
    region_value = require_env("SCRIPT_05_REGION_VALUE")
    out["Region"] = region_value

    out["Company"] = df["COMPANY"].replace("", np.nan)
    out["Product Group"] = df["PRODUCT_GROUP"].replace("", np.nan)

    # In your provided mapped file, Local Product Name is blank; keep blank to match.
    out["Local Product Name"] = np.nan

    out["Generic Name"] = df["GENERIC_NAME"].replace("", np.nan)
    out["Local Pack Description"] = df["LOCAL_PACK_DESC"].replace("", np.nan)
    out["Pack Unit"] = df["PACK_UNIT"].replace("", np.nan)
    out["Pack Size"] = df["PACK_SIZE_NUM"]

    out["LOCAL_PACK_CODE"] = df["LOCAL_PACK_CODE"].replace("", np.nan)
    out["Unit Price"] = df["UNIT_PRICE_NUM"]

    # Use PCID Mapping from CSV file
    out["PCID Mapping"] = df["PCID Mapping"].replace("", np.nan)

    # VAT Handling for Malaysia
    # In Malaysia, medicines are generally zero-rated for GST/VAT
    # Retail prices from MyPriMe are final retail prices
    DEFAULT_VAT_PERCENT = getenv_float("SCRIPT_05_DEFAULT_VAT_PERCENT", 0.0)  # Optional, defaults to 0.0 if not set
    out["VAT Percent"] = DEFAULT_VAT_PERCENT
    out["Public without VAT Price"] = df["PACK_PRICE_NUM"]  # Same as with VAT (zero-rated)
    out["Public with VAT Price"] = df["PACK_PRICE_NUM"]     # Retail price per pack from MyPriMe

    # Reimbursable Status Logic
    # Normalize generic names for matching
    df["GENERIC_NAME_NORM"] = df["GENERIC_NAME"].str.strip().str.upper()

    # Create a set of fully reimbursable generic names for fast lookup
    reimbursable_set = set(fully_reimbursable["Generic Name"].values)

    # Check if generic name matches
    is_reimbursable = df["GENERIC_NAME_NORM"].isin(reimbursable_set)

    # Set reimbursable status fields
    out["Reimbursable Status"] = np.where(is_reimbursable, "FULLY REIMBURSABLE", "NON REIMBURSABLE")
    out["Reimbursable Rate"] = np.where(is_reimbursable, "100.00%", "0.00%")
    out["Copayment Percent"] = np.where(is_reimbursable, "0.00%", "100.00%")

    # Keep others as NaN (not available in the two input files)
    # "Package Number" remains blank like your sample file

    # Reorder exactly
    out = out[FINAL_COLUMNS]

    return out


def main() -> None:
    # Use ConfigManager for paths instead of local directories
    script_dir = Path(__file__).parent
    
    # Get input directory using ConfigManager
    from config_loader import get_input_dir
    input_dir_str = getenv("SCRIPT_05_INPUT_DIR", "")
    if input_dir_str and Path(input_dir_str).is_absolute():
        input_dir = Path(input_dir_str)
    else:
        # Use scraper-specific input directory
        input_dir = get_input_dir()
    
    # Get output directory using ConfigManager (not local output folder)
    output_dir_str = getenv("SCRIPT_05_OUTPUT_DIR", "")
    if output_dir_str and Path(output_dir_str).is_absolute():
        output_dir = Path(output_dir_str)
    else:
        # Use scraper-specific output directory
        output_dir = get_output_dir()

    pcid_mapping_path = (input_dir / require_env("SCRIPT_05_PCID_MAPPING")).resolve()
    consolidated_path = (output_dir / require_env("SCRIPT_05_CONSOLIDATED")).resolve()
    prices_path = (output_dir / require_env("SCRIPT_05_PRICES")).resolve()
    reimbursable_path = (output_dir / require_env("SCRIPT_05_REIMBURSABLE")).resolve()
    
    # Use exports directory for final output files (mapped and unmapped)
    exports_dir = get_central_output_dir()  # This returns the exports directory
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
    ap = argparse.ArgumentParser(description="Generate Malaysia_PCID_Mapped report")
    ap.add_argument("--pcid", default=str(pcid_mapping_path), help="Path to PCID Mapping - Malaysia.csv")
    ap.add_argument("--consolidated", default=str(consolidated_path), help="Path to consolidated_products.csv")
    ap.add_argument("--prices", default=str(prices_path), help="Path to malaysia_drug_prices_view_all.csv")
    ap.add_argument("--reimbursable", default=str(reimbursable_path), help="Path to malaysia_fully_reimbursable_drugs.csv")
    ap.add_argument("--out-mapped", default=str(out_path_mapped), help="Output path for mapped records")
    ap.add_argument("--out-not-mapped", default=str(out_path_not_mapped), help="Output path for not mapped records")
    args = ap.parse_args()

    pcid_path = Path(args.pcid).resolve()
    cons_path = Path(args.consolidated).resolve()
    prices_path = Path(args.prices).resolve()
    reimb_path = Path(args.reimbursable).resolve()

    print(f"\n[SCRIPT 05] Loading input files...", flush=True)
    if not pcid_path.exists():
        # Check for alternative filename in case config hasn't been updated
        alt_names = ["PCID Mapping - Malaysia.csv", "Malaysia_PCID.csv"]
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
    if not _is_same_day(pcid_path):
        file_date = _file_date(pcid_path)
        today = datetime.now().date()
        print(f"\n[SKIP] PCID mapping file {pcid_path.name} is from {file_date}, not today's run ({today}).", flush=True)
        print("[SKIP] Skipping report generation because the PCID mapping file was not replaced today.", flush=True)
        sys.exit(1)
    if not cons_path.exists():
        raise FileNotFoundError(f"Consolidated products file not found: {cons_path}")
    print(f"  -> Consolidated products: {cons_path}", flush=True)
    if not prices_path.exists():
        raise FileNotFoundError(f"Prices file not found: {prices_path}")
    print(f"  -> Prices: {prices_path}", flush=True)
    if not reimb_path.exists():
        print(f"[WARNING] Fully reimbursable drugs file not found: {reimb_path}", flush=True)
        print(f"[WARNING] Creating empty file to allow pipeline to continue...", flush=True)
        # Create empty CSV with Generic Name header
        reimb_path.parent.mkdir(parents=True, exist_ok=True)
        with open(reimb_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Generic Name", "_source_page"])
        print(f"[WARNING] Created empty file: {reimb_path}", flush=True)
    print(f"  -> Reimbursable drugs: {reimb_path}", flush=True)

    print(f"\n[SCRIPT 05] Loading data...", flush=True)
    print(f"  -> Loading PCID mapping...", flush=True)
    pcid_mapping = load_pcid_mapping(pcid_path)
    print(f"  -> Loaded {len(pcid_mapping):,} PCID mappings", flush=True)
    print(f"[PROGRESS] Loading data: PCID mapping loaded (1/3)", flush=True)
    print(f"  -> Loading fully reimbursable drugs...", flush=True)
    fully_reimbursable = load_fully_reimbursable(reimb_path)
    print(f"  -> Loaded {len(fully_reimbursable):,} reimbursable products", flush=True)
    print(f"[PROGRESS] Loading data: Reimbursable drugs loaded (2/3)", flush=True)
    print(f"  -> Loading consolidated products and prices...", flush=True)
    cons, prices = load_inputs(cons_path, prices_path)
    print(f"  -> Loaded {len(cons):,} consolidated products, {len(prices):,} price records", flush=True)
    print(f"[PROGRESS] Loading data: Products and prices loaded (3/3) (100%)", flush=True)
    print(f"\n[SCRIPT 05] Building report...", flush=True)
    report = build_report(cons, prices, pcid_mapping, fully_reimbursable)
    print(f"  -> Report built: {len(report):,} total rows", flush=True)
    print(f"[PROGRESS] Building report: {len(report)}/{len(report)} (100%)", flush=True)

    # Split into mapped and not mapped
    print(f"\n[SCRIPT 05] Splitting into mapped and not mapped...", flush=True)
    mapped_report = report[report["PCID Mapping"].notna()].copy()
    not_mapped_report = report[report["PCID Mapping"].isna()].copy()
    print(f"  -> Mapped products: {len(mapped_report):,}", flush=True)
    print(f"  -> Not mapped products: {len(not_mapped_report):,}", flush=True)

    # Save mapped records
    print(f"\n[SCRIPT 05] Saving mapped records...", flush=True)
    out_path_mapped = Path(args.out_mapped)
    out_path_mapped.parent.mkdir(parents=True, exist_ok=True)

    if out_path_mapped.suffix.lower() in [".xlsx", ".xls"]:
        print(f"  -> Writing to Excel: {out_path_mapped}", flush=True)
        with pd.ExcelWriter(out_path_mapped, engine="openpyxl") as w:
            sheet_name_mapped = require_env("SCRIPT_05_SHEET_NAME_MAPPED")
            mapped_report.to_excel(w, index=False, sheet_name=sheet_name_mapped)
    else:
        print(f"  -> Writing to CSV: {out_path_mapped}", flush=True)
        mapped_report.to_csv(out_path_mapped, index=False, encoding="utf-8-sig")

    print(f"[OK] Wrote {len(mapped_report):,} MAPPED rows to: {out_path_mapped}", flush=True)
    
    # Save not mapped records
    print(f"\n[SCRIPT 05] Saving not mapped records...", flush=True)
    out_path_not_mapped = Path(args.out_not_mapped)
    out_path_not_mapped.parent.mkdir(parents=True, exist_ok=True)

    if out_path_not_mapped.suffix.lower() in [".xlsx", ".xls"]:
        print(f"  -> Writing to Excel: {out_path_not_mapped}", flush=True)
        with pd.ExcelWriter(out_path_not_mapped, engine="openpyxl") as w:
            sheet_name_not_mapped = require_env("SCRIPT_05_SHEET_NAME_NOT_MAPPED")
            not_mapped_report.to_excel(w, index=False, sheet_name=sheet_name_not_mapped)
    else:
        print(f"  -> Writing to CSV: {out_path_not_mapped}", flush=True)
        not_mapped_report.to_csv(out_path_not_mapped, index=False, encoding="utf-8-sig")

    print(f"[OK] Wrote {len(not_mapped_report):,} NOT MAPPED rows to: {out_path_not_mapped}", flush=True)
    _write_diff_summary(
        country="Malaysia",
        exports_dir=exports_dir,
        new_path=out_path_mapped,
        glob_pattern="malaysia_pcid_mapped_*.csv",
        key_column="LOCAL_PACK_CODE",
        date_str=date_str,
    )
    print(f"\n[SUMMARY] Summary:")
    print(f"   Total records: {len(report):,}")
    print(f"   Mapped: {len(mapped_report):,}")
    print(f"   Not Mapped: {len(not_mapped_report):,}")
    
    # Generate comprehensive final report
    generate_final_report(report, mapped_report, not_mapped_report, cons, prices, pcid_mapping, fully_reimbursable, output_dir)


def generate_final_report(
    report: pd.DataFrame,
    mapped_report: pd.DataFrame,
    not_mapped_report: pd.DataFrame,
    cons: pd.DataFrame,
    prices: pd.DataFrame,
    pcid_mapping: pd.DataFrame,
    fully_reimbursable: pd.DataFrame,
    output_dir: Path
) -> None:
    """Generate comprehensive human-readable final report."""
    report_filename = require_env("SCRIPT_05_COVERAGE_REPORT")
    report_path = output_dir / report_filename
    
    # Calculate statistics
    total_products = len(report)
    mapped_count = len(mapped_report)
    not_mapped_count = len(not_mapped_report)
    pcid_coverage = (mapped_count / total_products * 100) if total_products > 0 else 0
    
    # Missing company information
    missing_company = report[report["Company"].isna() | (report["Company"].str.strip() == "")]
    missing_company_count = len(missing_company)
    
    # Missing product names
    missing_product_name = report[report["Product Group"].isna() | (report["Product Group"].str.strip() == "")]
    missing_product_name_count = len(missing_product_name)
    
    # Missing generic names
    missing_generic = report[report["Generic Name"].isna() | (report["Generic Name"].str.strip() == "")]
    missing_generic_count = len(missing_generic)
    
    # Missing prices
    missing_price = report[report["Public with VAT Price"].isna()]
    missing_price_count = len(missing_price)
    
    # Reimbursable statistics
    reimbursable_count = len(report[report["Reimbursable Status"] == "FULLY REIMBURSABLE"])
    non_reimbursable_count = len(report[report["Reimbursable Status"] == "NON REIMBURSABLE"])
    
    # Expected vs actual
    expected_from_prices = len(prices)
    expected_from_cons = len(cons)
    expected_pcid = len(pcid_mapping)
    expected_reimbursable = len(fully_reimbursable)
    
    # Products not found in consolidated (missing company)
    missing_in_cons = prices[~prices["LOCAL_PACK_CODE"].isin(cons["LOCAL_PACK_CODE"])]
    missing_in_cons_count = len(missing_in_cons)
    
    # Products in consolidated but not in prices
    missing_in_prices = cons[~cons["LOCAL_PACK_CODE"].isin(prices["LOCAL_PACK_CODE"])]
    missing_in_prices_count = len(missing_in_prices)
    
    # Products without PCID mapping
    products_without_pcid = sorted(not_mapped_report["LOCAL_PACK_CODE"].dropna().unique().tolist())
    
    # Generate report
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("MALAYSIA MEDICINE PRICE SCRAPER - FINAL DATA COVERAGE REPORT\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("\n")
        
        # Executive Summary
        f.write("=" * 80 + "\n")
        f.write("EXECUTIVE SUMMARY\n")
        f.write("=" * 80 + "\n")
        f.write(f"Total Products Processed:       {total_products:,}\n")
        f.write(f"Products with PCID Mapping:     {mapped_count:,} ({pcid_coverage:.2f}%)\n")
        f.write(f"Products without PCID Mapping:  {not_mapped_count:,} ({100-pcid_coverage:.2f}%)\n")
        f.write(f"Reimbursable Products:          {reimbursable_count:,}\n")
        f.write(f"Non-Reimbursable Products:      {non_reimbursable_count:,}\n")
        f.write("\n")
        
        # Data Source Statistics
        f.write("=" * 80 + "\n")
        f.write("DATA SOURCE STATISTICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Products from MyPriMe (Prices): {expected_from_prices:,}\n")
        f.write(f"Products from QUEST3+ (Details): {expected_from_cons:,}\n")
        f.write(f"PCID Mappings Available:        {expected_pcid:,}\n")
        f.write(f"Reimbursable Drugs in FUKKM:    {expected_reimbursable:,}\n")
        f.write("\n")
        
        # Data Quality Metrics
        f.write("=" * 80 + "\n")
        f.write("DATA QUALITY METRICS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Products with Company Info:     {total_products - missing_company_count:,} ({((total_products - missing_company_count) / total_products * 100):.2f}%)\n")
        f.write(f"Products missing Company Info:   {missing_company_count:,} ({((missing_company_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write(f"Products with Product Name:      {total_products - missing_product_name_count:,} ({((total_products - missing_product_name_count) / total_products * 100):.2f}%)\n")
        f.write(f"Products missing Product Name:  {missing_product_name_count:,} ({((missing_product_name_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write(f"Products with Generic Name:     {total_products - missing_generic_count:,} ({((total_products - missing_generic_count) / total_products * 100):.2f}%)\n")
        f.write(f"Products missing Generic Name:  {missing_generic_count:,} ({((missing_generic_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write(f"Products with Price:            {total_products - missing_price_count:,} ({((total_products - missing_price_count) / total_products * 100):.2f}%)\n")
        f.write(f"Products missing Price:         {missing_price_count:,} ({((missing_price_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write("\n")
        
        # Missing Data Analysis
        f.write("=" * 80 + "\n")
        f.write("MISSING DATA ANALYSIS\n")
        f.write("=" * 80 + "\n")
        
        # Missing in consolidated
        if missing_in_cons_count > 0:
            MISSING_PRODUCTS_REPORT_LIMIT = getenv_int("SCRIPT_05_MISSING_PRODUCTS_REPORT_LIMIT", 20)
            f.write(f"\n[WARNING] Products in MyPriMe but NOT in QUEST3+ ({missing_in_cons_count:,} products):\n")
            f.write("   These products have prices but no company/holder information.\n")
            f.write(f"   First {MISSING_PRODUCTS_REPORT_LIMIT} registration numbers:\n")
            for i, regno in enumerate(missing_in_cons["LOCAL_PACK_CODE"].head(MISSING_PRODUCTS_REPORT_LIMIT).tolist(), 1):
                f.write(f"     {i:3d}. {regno}\n")
            if missing_in_cons_count > MISSING_PRODUCTS_REPORT_LIMIT:
                f.write(f"     ... and {missing_in_cons_count - MISSING_PRODUCTS_REPORT_LIMIT} more\n")
        else:
            f.write("\n[OK] All products from MyPriMe have corresponding QUEST3+ entries.\n")
        
        # Missing in prices
        if missing_in_prices_count > 0:
            MISSING_PRODUCTS_REPORT_LIMIT = getenv_int("SCRIPT_05_MISSING_PRODUCTS_REPORT_LIMIT", 20)
            f.write(f"\n[WARNING] Products in QUEST3+ but NOT in MyPriMe ({missing_in_prices_count:,} products):\n")
            f.write("   These products have company info but no price information.\n")
            f.write(f"   First {MISSING_PRODUCTS_REPORT_LIMIT} registration numbers:\n")
            for i, regno in enumerate(missing_in_prices["LOCAL_PACK_CODE"].head(MISSING_PRODUCTS_REPORT_LIMIT).tolist(), 1):
                f.write(f"     {i:3d}. {regno}\n")
            if missing_in_prices_count > MISSING_PRODUCTS_REPORT_LIMIT:
                f.write(f"     ... and {missing_in_prices_count - MISSING_PRODUCTS_REPORT_LIMIT} more\n")
        else:
            f.write("\n[OK] All products from QUEST3+ have corresponding MyPriMe entries.\n")
        
        f.write("\n")
        
        # PCID Mapping Analysis
        f.write("=" * 80 + "\n")
        f.write("PCID MAPPING ANALYSIS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Total PCID Mappings Available:   {expected_pcid:,}\n")
        f.write(f"Products Successfully Mapped:    {mapped_count:,}\n")
        f.write(f"Products NOT Mapped:             {not_mapped_count:,}\n")
        f.write(f"Mapping Coverage:                {pcid_coverage:.2f}%\n")
        f.write("\n")
        
        if products_without_pcid:
            f.write(f"[WARNING] Products WITHOUT PCID Mapping ({len(products_without_pcid):,} unique products):\n")
            f.write("   These products need PCID mapping added to input/PCID Mapping - Malaysia.csv\n")
            f.write("   First 50 registration numbers:\n")
            for i, regno in enumerate(products_without_pcid[:50], 1):
                f.write(f"     {i:3d}. {regno}\n")
            if len(products_without_pcid) > 50:
                f.write(f"     ... and {len(products_without_pcid) - 50} more (see malaysia_pcid_not_mapped.csv)\n")
        else:
            f.write("[OK] All products have PCID mappings!\n")
        f.write("\n")
        
        # Reimbursable Analysis
        f.write("=" * 80 + "\n")
        f.write("REIMBURSABLE STATUS ANALYSIS\n")
        f.write("=" * 80 + "\n")
        f.write(f"Total Reimbursable Drugs:        {reimbursable_count:,} ({((reimbursable_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write(f"Total Non-Reimbursable Drugs:    {non_reimbursable_count:,} ({((non_reimbursable_count / total_products * 100) if total_products > 0 else 0):.2f}%)\n")
        f.write(f"Reimbursable Drugs in FUKKM:     {expected_reimbursable:,}\n")
        f.write("\n")
        
        # Recommendations
        f.write("=" * 80 + "\n")
        f.write("RECOMMENDATIONS\n")
        f.write("=" * 80 + "\n")
        
        if missing_company_count > 0:
            f.write(f"[WARNING] {missing_company_count:,} products are missing company information.\n")
            f.write("   Action: Re-run Script 02 to retry extraction from QUEST3+.\n")
            f.write("\n")
        
        if not_mapped_count > 0:
            f.write(f"[WARNING] {not_mapped_count:,} products are missing PCID mappings.\n")
            f.write("   Action: Add PCID mappings to input/PCID Mapping - Malaysia.csv for these products.\n")
            f.write("   See malaysia_pcid_not_mapped.csv for the complete list.\n")
            f.write("\n")
        
        if missing_in_cons_count > 0:
            f.write(f"[WARNING] {missing_in_cons_count:,} products from MyPriMe were not found in QUEST3+.\n")
            f.write("   These may be deprecated products or registration number mismatches.\n")
            f.write("\n")
        
        PCID_COVERAGE_HIGH_THRESHOLD = getenv_int("SCRIPT_05_PCID_COVERAGE_HIGH_THRESHOLD", 90)
        PCID_COVERAGE_MEDIUM_THRESHOLD = getenv_int("SCRIPT_05_PCID_COVERAGE_MEDIUM_THRESHOLD", 70)
        if pcid_coverage >= PCID_COVERAGE_HIGH_THRESHOLD:
            f.write(f"[OK] Excellent PCID mapping coverage! (>{PCID_COVERAGE_HIGH_THRESHOLD}%)\n")
        elif pcid_coverage >= PCID_COVERAGE_MEDIUM_THRESHOLD:
            f.write(f"[WARNING] Good PCID mapping coverage, but improvements possible.\n")
        else:
            f.write("[ERROR] Low PCID mapping coverage. Consider adding more mappings.\n")
        f.write("\n")
        
        # Output Files
        f.write("=" * 80 + "\n")
        f.write("OUTPUT FILES\n")
        f.write("=" * 80 + "\n")
        f.write("Generated Files:\n")
        f.write("  - malaysia_pcid_mapped.csv (products WITH PCID mapping)\n")
        f.write("  - malaysia_pcid_not_mapped.csv (products WITHOUT PCID mapping)\n")
        report_filename = require_env("SCRIPT_05_COVERAGE_REPORT")
        f.write(f"  - {report_filename} (this report)\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")
    
    print(f"\n[REPORT] Final report generated: {report_path}")


def _find_previous_export_file(exports_dir: Path, pattern: str, current_path: Path) -> Optional[Path]:
    """Return the most recent export file matching pattern, excluding current_path."""
    candidates = [
        p for p in exports_dir.glob(pattern)
        if p.is_file() and p.resolve() != current_path.resolve()
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _extract_key_set(df: pd.DataFrame, column: str) -> Optional[set[str]]:
    """Normalize the key column values for comparison."""
    if column not in df.columns:
        return None
    values = df[column].dropna().astype(str).str.strip()
    return {v for v in values if v}


def _write_diff_summary(
    country: str,
    exports_dir: Path,
    new_path: Path,
    glob_pattern: str,
    key_column: str,
    date_str: str
) -> None:
    """Compare the new export with the previous file and write a human-readable diff."""
    previous = _find_previous_export_file(exports_dir, glob_pattern, new_path)
    if not previous:
        print(f"[DIFF] No previous {country} mapped report to compare. Skipping diff summary.", flush=True)
        return

    try:
        new_df = pd.read_csv(new_path, dtype=str, keep_default_na=False)
        old_df = pd.read_csv(previous, dtype=str, keep_default_na=False)
    except Exception as exc:
        print(f"[DIFF] Could not read mapped reports for comparison: {exc}", flush=True)
        return

    new_keys = _extract_key_set(new_df, key_column)
    old_keys = _extract_key_set(old_df, key_column)
    if new_keys is None or old_keys is None:
        print(f"[DIFF] Key column '{key_column}' missing in one of the files; skipping diff.", flush=True)
        return

    new_only = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    shared = sorted(new_keys & old_keys)

    summary_lines = [
        "=" * 80,
        f"{country} PCID-Mapped Report Diff ({date_str})",
        "=" * 80,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"New report:      {new_path.name}",
        f"Compared to:     {previous.name}",
        f"Key column used: {key_column}",
        "",
        f"New entries: {len(new_only)}",
        f"Removed entries: {len(removed)}",
        f"Unchanged entries: {len(shared)}",
    ]

    if new_only:
        summary_lines.append(f"  Sample new keys:     {', '.join(new_only[:5])}")
    if removed:
        summary_lines.append(f"  Sample removed keys: {', '.join(removed[:5])}")

    summary_path = exports_dir / f"report_diff_{country.lower()}_{date_str}.txt"
    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            for line in summary_lines:
                f.write(line + "\n")
        print(f"[DIFF] Diff summary saved: {summary_path}", flush=True)
    except Exception as exc:
        print(f"[DIFF] Failed to write diff summary: {exc}", flush=True)


if __name__ == "__main__":
    run_with_checkpoint(
        main,
        "Malaysia",
        5,
        "Generate PCID Mapped"
    )
