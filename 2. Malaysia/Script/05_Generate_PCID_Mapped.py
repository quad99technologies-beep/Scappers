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
        print(f"\n⚠️  WARNING: {missing_company_count:,} out of {total_count:,} records are missing company information")
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
    input_dir = script_dir.parent / "Input"
    output_dir = script_dir.parent / "Output"

    pcid_mapping_path = input_dir / "Malaysia_PCID.csv"
    consolidated_path = output_dir / "consolidated_products.csv"
    prices_path = output_dir / "malaysia_drug_prices_view_all.csv"
    reimbursable_path = output_dir / "malaysia_fully_reimbursable_drugs.csv"
    out_path_mapped = output_dir / "malaysia_pcid_mapped.csv"
    out_path_not_mapped = output_dir / "malaysia_pcid_not_mapped.csv"

    # Allow command line override
    ap = argparse.ArgumentParser(description="Generate Malaysia_PCID_Mapped report")
    ap.add_argument("--pcid", default=str(pcid_mapping_path), help="Path to Malaysia_PCID.csv")
    ap.add_argument("--consolidated", default=str(consolidated_path), help="Path to consolidated_products.csv")
    ap.add_argument("--prices", default=str(prices_path), help="Path to malaysia_drug_prices_view_all.csv")
    ap.add_argument("--reimbursable", default=str(reimbursable_path), help="Path to malaysia_fully_reimbursable_drugs.csv")
    ap.add_argument("--out-mapped", default=str(out_path_mapped), help="Output path for mapped records")
    ap.add_argument("--out-not-mapped", default=str(out_path_not_mapped), help="Output path for not mapped records")
    args = ap.parse_args()

    pcid_path = Path(args.pcid)
    cons_path = Path(args.consolidated)
    prices_path = Path(args.prices)
    reimb_path = Path(args.reimbursable)

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

    print(f"✅ Wrote {len(mapped_report):,} MAPPED rows to: {out_path_mapped}")

    # Save not mapped records
    out_path_not_mapped = Path(args.out_not_mapped)
    out_path_not_mapped.parent.mkdir(parents=True, exist_ok=True)

    if out_path_not_mapped.suffix.lower() in [".xlsx", ".xls"]:
        with pd.ExcelWriter(out_path_not_mapped, engine="openpyxl") as w:
            not_mapped_report.to_excel(w, index=False, sheet_name="Malaysia_PCID_Not_Mapped")
    else:
        not_mapped_report.to_csv(out_path_not_mapped, index=False, encoding="utf-8-sig")

    print(f"✅ Wrote {len(not_mapped_report):,} NOT MAPPED rows to: {out_path_not_mapped}")
    print(f"\n📊 Summary:")
    print(f"   Total records: {len(report):,}")
    print(f"   Mapped: {len(mapped_report):,}")
    print(f"   Not Mapped: {len(not_mapped_report):,}")


if __name__ == "__main__":
    main()

