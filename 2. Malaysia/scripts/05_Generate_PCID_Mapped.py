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

import argparse
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from config_loader import load_env_file, getenv

# Load environment variables from .env file
load_env_file()


FINAL_COLUMNS = [
    "PCID Mapping",
    "Package Number",
    "Country",
    "Company",
    "Product Group",
    "Local Product Name",
    "Generic Name",
    "Description",
    "Indication",
    "Pack Size",
    "Effective Start Date",
    "Effective End Date",
    "Currency",
    "Ex Factory Wholesale Price",
    "Ex Factory Wholesale Price Less Rebate",
    "Ex Factory Hospital Price",
    "Ex Factory Hospital Price Less Rebate",
    "Ex Factory to Pharmacy Price",
    "Pharmacy Purchase Price",
    "Pharmacy Purchase Price Less Rebate",
    "Public without VAT Price",
    "Public Without VAT Price Less Rebate",
    "Public with VAT Price",
    "Public With VAT Price Less Rebate",
    "VAT Percent",
    "Reimbursable Status",
    "Reimbursable Price",
    "Reimbursable Rate",
    "Reimbursable Notes",
    "Copayment Value",
    "Copayment Percent",
    "Margin Rule",
    "Package Notes",
    "Discontinued",
    "Region",
    "WHO ATC Code",
    "Therapeutic Areas",
    "Presentation",
    "Marketing Authority",
    "Local Pack Description",
    "Formulation",
    "Fill Unit",
    "Fill Size",
    "Pack Unit",
    "Strength",
    "Strength Unit",
    "Brand Type",
    "Import Type",
    "Combination Molecule",
    "Source",
    "LOCAL_PACK_CODE",
    "Unit Price",
]


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

    out["Country"] = "MALAYSIA"
    out["Currency"] = "MYR"
    out["Source"] = "PRICENTRIC"
    out["Region"] = "MALAYSIA"

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
    out["VAT Percent"] = 0.0
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
    # Default paths relative to script location
    script_dir = Path(__file__).parent
    base_dir_str = getenv("SCRIPT_05_BASE_DIR", None)
    if base_dir_str:
        if base_dir_str.startswith("/") or (len(base_dir_str) > 1 and base_dir_str[1] == ":"):
            base_dir = Path(base_dir_str)
        else:
            # If relative path, resolve from script's parent directory (Malaysia folder)
            base_dir = (script_dir.parent / base_dir_str).resolve()
    else:
        # Default: use script's parent directory (Malaysia folder)
        base_dir = script_dir.parent
    
    input_dir_str = getenv("SCRIPT_05_INPUT_DIR", "input")
    input_dir = base_dir / input_dir_str if not Path(input_dir_str).is_absolute() else Path(input_dir_str)
    
    output_dir_str = getenv("SCRIPT_05_OUTPUT_DIR", "output")
    output_dir = base_dir / output_dir_str if not Path(output_dir_str).is_absolute() else Path(output_dir_str)

    pcid_mapping_path = (input_dir / getenv("SCRIPT_05_PCID_MAPPING", "Malaysia_PCID.csv")).resolve()
    consolidated_path = (output_dir / getenv("SCRIPT_05_CONSOLIDATED", "consolidated_products.csv")).resolve()
    prices_path = (output_dir / getenv("SCRIPT_05_PRICES", "malaysia_drug_prices_view_all.csv")).resolve()
    reimbursable_path = (output_dir / getenv("SCRIPT_05_REIMBURSABLE", "malaysia_fully_reimbursable_drugs.csv")).resolve()
    out_path_mapped = (output_dir / getenv("SCRIPT_05_OUT_MAPPED", "malaysia_pcid_mapped.csv")).resolve()
    out_path_not_mapped = (output_dir / getenv("SCRIPT_05_OUT_NOT_MAPPED", "malaysia_pcid_not_mapped.csv")).resolve()

    # Allow command line override
    ap = argparse.ArgumentParser(description="Generate Malaysia_PCID_Mapped report")
    ap.add_argument("--pcid", default=str(pcid_mapping_path), help="Path to Malaysia_PCID.csv")
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

    if not pcid_path.exists():
        raise FileNotFoundError(f"PCID mapping file not found: {pcid_path}")
    if not cons_path.exists():
        raise FileNotFoundError(f"Consolidated products file not found: {cons_path}")
    if not prices_path.exists():
        raise FileNotFoundError(f"Prices file not found: {prices_path}")
    if not reimb_path.exists():
        raise FileNotFoundError(f"Fully reimbursable drugs file not found: {reimb_path}")

    pcid_mapping = load_pcid_mapping(pcid_path)
    fully_reimbursable = load_fully_reimbursable(reimb_path)
    cons, prices = load_inputs(cons_path, prices_path)
    report = build_report(cons, prices, pcid_mapping, fully_reimbursable)

    # Split into mapped and not mapped
    mapped_report = report[report["PCID Mapping"].notna()].copy()
    not_mapped_report = report[report["PCID Mapping"].isna()].copy()

    # Save mapped records
    out_path_mapped = Path(args.out_mapped)
    out_path_mapped.parent.mkdir(parents=True, exist_ok=True)

    if out_path_mapped.suffix.lower() in [".xlsx", ".xls"]:
        with pd.ExcelWriter(out_path_mapped, engine="openpyxl") as w:
            mapped_report.to_excel(w, index=False, sheet_name="Malaysia_PCID_Mapped")
    else:
        mapped_report.to_csv(out_path_mapped, index=False, encoding="utf-8-sig")

    print(f"[OK] Wrote {len(mapped_report):,} MAPPED rows to: {out_path_mapped}")
    
    # Copy final report (mapped) to central output directory
    try:
        from config_loader import get_central_output_dir
        import shutil
        central_output_dir = get_central_output_dir()
        central_final_report = central_output_dir / out_path_mapped.name
        shutil.copy2(out_path_mapped, central_final_report)
        print(f"[OK] Final report also saved to central location: {central_final_report}")
    except Exception as e:
        print(f"[WARNING] Could not copy to central output: {e}")
    
    # Save not mapped records
    out_path_not_mapped = Path(args.out_not_mapped)
    out_path_not_mapped.parent.mkdir(parents=True, exist_ok=True)

    if out_path_not_mapped.suffix.lower() in [".xlsx", ".xls"]:
        with pd.ExcelWriter(out_path_not_mapped, engine="openpyxl") as w:
            not_mapped_report.to_excel(w, index=False, sheet_name="Malaysia_PCID_Not_Mapped")
    else:
        not_mapped_report.to_csv(out_path_not_mapped, index=False, encoding="utf-8-sig")

    print(f"[OK] Wrote {len(not_mapped_report):,} NOT MAPPED rows to: {out_path_not_mapped}")
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
    report_path = output_dir / "final_data_coverage_report.txt"
    
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
            f.write(f"\n[WARNING] Products in MyPriMe but NOT in QUEST3+ ({missing_in_cons_count:,} products):\n")
            f.write("   These products have prices but no company/holder information.\n")
            f.write("   First 20 registration numbers:\n")
            for i, regno in enumerate(missing_in_cons["LOCAL_PACK_CODE"].head(20).tolist(), 1):
                f.write(f"     {i:3d}. {regno}\n")
            if missing_in_cons_count > 20:
                f.write(f"     ... and {missing_in_cons_count - 20} more\n")
        else:
            f.write("\n[OK] All products from MyPriMe have corresponding QUEST3+ entries.\n")
        
        # Missing in prices
        if missing_in_prices_count > 0:
            f.write(f"\n[WARNING] Products in QUEST3+ but NOT in MyPriMe ({missing_in_prices_count:,} products):\n")
            f.write("   These products have company info but no price information.\n")
            f.write("   First 20 registration numbers:\n")
            for i, regno in enumerate(missing_in_prices["LOCAL_PACK_CODE"].head(20).tolist(), 1):
                f.write(f"     {i:3d}. {regno}\n")
            if missing_in_prices_count > 20:
                f.write(f"     ... and {missing_in_prices_count - 20} more\n")
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
            f.write("   These products need PCID mapping added to input/Malaysia_PCID.csv\n")
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
            f.write("   Action: Add PCID mappings to input/Malaysia_PCID.csv for these products.\n")
            f.write("   See malaysia_pcid_not_mapped.csv for the complete list.\n")
            f.write("\n")
        
        if missing_in_cons_count > 0:
            f.write(f"[WARNING] {missing_in_cons_count:,} products from MyPriMe were not found in QUEST3+.\n")
            f.write("   These may be deprecated products or registration number mismatches.\n")
            f.write("\n")
        
        if pcid_coverage >= 90:
            f.write("[OK] Excellent PCID mapping coverage! (>90%)\n")
        elif pcid_coverage >= 70:
            f.write("[WARNING] Good PCID mapping coverage, but improvements possible.\n")
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
        f.write("  - final_data_coverage_report.txt (this report)\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")
    
    print(f"\n[REPORT] Final report generated: {report_path}")


if __name__ == "__main__":
    main()

