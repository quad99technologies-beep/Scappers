#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 4: Merge Final CSV
=======================
Merges tender details, supplier data, and input metadata into the final EVERSANA-format CSV.

INPUTS:
  - input/Tender_Chile/tender_list.csv
  - output/Tender_Chile/tender_details.csv
  - output/Tender_Chile/mercadopublico_supplier_rows.csv

OUTPUTS:
  - output/Tender_Chile/final_tender_data.csv
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional, List

import pandas as pd

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Tender- Chile to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Import config_loader for platform integration
try:
    from config_loader import (
        load_env_file,
        get_input_dir as _get_input_dir,
        get_output_dir as _get_output_dir
    )
    load_env_file()
    _CONFIG_LOADER_AVAILABLE = True
except ImportError:
    _CONFIG_LOADER_AVAILABLE = False

# Path resolution
if _CONFIG_LOADER_AVAILABLE:
    INPUT_DIR = _get_input_dir()
    OUTPUT_DIR = _get_output_dir()
else:
    INPUT_DIR = _repo_root / "input" / "Tender_Chile"
    OUTPUT_DIR = _repo_root / "output" / "Tender_Chile"

# Try multiple filename variants for tender list
TENDER_LIST_FILE = INPUT_DIR / "tender_list.csv"
if not TENDER_LIST_FILE.exists():
    TENDER_LIST_FILE = INPUT_DIR / "TenderList.csv"

TENDER_DETAILS_FILE = OUTPUT_DIR / "tender_details.csv"
SUPPLIER_ROWS_FILE = OUTPUT_DIR / "mercadopublico_supplier_rows.csv"

FINAL_OUTPUT_FILE = OUTPUT_DIR / "final_tender_data.csv"


def require_file(path: Path) -> None:
    if not path.exists():
        print(f"[ERROR] Required file missing: {path}")
        sys.exit(1)


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()


def normalize_lot_number(x) -> str:
    s = norm_str(x)
    if not s:
        return ""
    if re.fullmatch(r"\d+\.0", s):
        return s.split(".")[0]
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def to_yes_no(x) -> str:
    s = norm_str(x).lower()
    if s in ("1", "true", "yes", "y", "yes ", "awarded"):
        return "YES"
    if s in ("0", "false", "no", "n", "not awarded", "not_awarded"):
        return "NO"
    return ""


def safe_float_str(x) -> str:
    s = norm_str(x)
    if not s:
        return ""
    try:
        v = float(s.replace(",", ""))
        if v.is_integer():
            return str(int(v))
        return str(v)
    except Exception:
        return s


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    require_file(TENDER_LIST_FILE)
    require_file(TENDER_DETAILS_FILE)
    require_file(SUPPLIER_ROWS_FILE)

    tender_list = pd.read_csv(TENDER_LIST_FILE, encoding="utf-8-sig")
    tender_details = pd.read_csv(TENDER_DETAILS_FILE, encoding="utf-8-sig")
    supplier_rows = pd.read_csv(SUPPLIER_ROWS_FILE, encoding="utf-8-sig")

    # -------------------------
    # Base Tender ID (CN code)
    # -------------------------
    if "Tender ID" not in tender_details.columns:
        print(f"[ERROR] tender_details.csv missing 'Tender ID'. Found: {list(tender_details.columns)}")
        sys.exit(1)

    tender_details["__tender_id"] = tender_details["Tender ID"].astype(str).str.strip()

    # tender_list: identify tender id column (CN code)
    tl_id_col = pick_col(tender_list, ["CN Document Number", "Source Tender Id", "Tender ID", "IDLicitacion", "idlicitacion"])
    if tl_id_col:
        tender_list["__tender_id"] = tender_list[tl_id_col].astype(str).str.strip()
    else:
        tender_list["__tender_id"] = ""

    # Optional fields from tender_list
    tl_currency_col = pick_col(tender_list, ["Local Currency", "Moneda", "Currency"])
    tl_proc_col = pick_col(tender_list, ["Tender Procedure Type", "Tipo", "Procedure"])
    tl_meat_col = pick_col(tender_list, ["MEAT"])
    tl_ceiling_col = pick_col(tender_list, ["Ceiling Unit Price", "Ceiling_Unit_Price", "CeilingUnitPrice"])

    keep_cols = ["__tender_id"]
    for c in [tl_currency_col, tl_proc_col, tl_meat_col, tl_ceiling_col]:
        if c and c in tender_list.columns:
            keep_cols.append(c)

    tender_list_small = tender_list[keep_cols].drop_duplicates("__tender_id")

    # -------------------------
    # Join tender_list fields into details
    # -------------------------
    td = tender_details.merge(tender_list_small, on="__tender_id", how="left", suffixes=("", "_tl"))

    # -------------------------
    # Join supplier rows by (Source URL + Lot Number)
    # -------------------------
    for col in ["source_tender_url", "lot_number"]:
        if col not in supplier_rows.columns:
            print(f"[ERROR] mercadopublico_supplier_rows.csv missing '{col}'. Found: {list(supplier_rows.columns)}")
            sys.exit(1)

    td["__source_url"] = td["Source URL"].astype(str).str.strip()
    td["__lot_number"] = td["Lot Number"].apply(normalize_lot_number)

    supplier_rows["__source_tender_url"] = supplier_rows["source_tender_url"].astype(str).str.strip()
    supplier_rows["__lot_number"] = supplier_rows["lot_number"].apply(normalize_lot_number)

    merged = td.merge(
        supplier_rows,
        left_on=["__source_url", "__lot_number"],
        right_on=["__source_tender_url", "__lot_number"],
        how="left",
        suffixes=("", "_sup"),
    )

    # -------------------------
    # Derive award status per tender (overall)
    # -------------------------
    if "is_awarded" in merged.columns:
        merged["__is_awarded_yes"] = merged["is_awarded"].apply(to_yes_no).eq("YES")
    else:
        merged["__is_awarded_yes"] = False

    tender_awarded = merged.groupby("__tender_id")["__is_awarded_yes"].any().reset_index(name="__tender_any_award")
    merged = merged.merge(tender_awarded, on="__tender_id", how="left")

    merged["Status"] = merged["__tender_any_award"].apply(lambda x: "AWARDED" if bool(x) else "PUBLISHED")

    # Bid Status Award only meaningful if tender is awarded
    merged["Bid Status Award"] = ""
    if "is_awarded" in merged.columns:
        mask_awarded_tender = merged["Status"].eq("AWARDED")
        merged.loc[mask_awarded_tender, "Bid Status Award"] = merged.loc[mask_awarded_tender, "is_awarded"].apply(to_yes_no)

    # -------------------------
    # Map final template fields
    # -------------------------
    merged["COUNTRY"] = "CHILE"
    merged["SOURCE"] = "MERCADOPUBLICO"

    merged["Source Tender Id"] = merged["__tender_id"]
    merged["CN Document Number"] = merged["__tender_id"]
    merged["CAN Document Number"] = merged["__tender_id"]

    merged["Tender Title"] = merged.get("Tender Title", "")
    merged["TENDERING AUTHORITY"] = merged.get("TENDERING AUTHORITY", "")
    merged["PROVINCE"] = merged.get("PROVINCE", "")

    merged["Deadline Date"] = merged.get("Closing Date", "")

    merged["Tendering Authority Type"] = ""
    merged["Tender Procedure Type"] = merged[tl_proc_col] if tl_proc_col and tl_proc_col in merged.columns else ""
    merged["Local Currency"] = merged[tl_currency_col] if tl_currency_col and tl_currency_col in merged.columns else ""
    merged["MEAT"] = merged[tl_meat_col] if tl_meat_col and tl_meat_col in merged.columns else ""
    merged["Ceiling Unit Price"] = merged[tl_ceiling_col] if tl_ceiling_col and tl_ceiling_col in merged.columns else ""

    merged["Price Evaluation ratio"] = merged.get("Price Evaluation ratio", "")
    merged["Quality Evaluation ratio"] = merged.get("Quality Evaluation ratio", "")
    merged["Other Evaluation ratio"] = merged.get("Other Evaluation ratio", "")

    merged["Original_Publication_Link_Notice"] = merged.get("Source URL", "")
    merged["Original_Publication_Link_Award"] = merged["source_url"] if "source_url" in merged.columns else ""

    merged["Lot Number"] = merged.get("Lot Number", "")
    merged["Sub Lot Number"] = ""
    merged["Unique Lot ID"] = merged.get("Unique Lot ID", "")
    merged["Lot Title"] = merged.get("Lot Title", merged.get("item_title", ""))

    merged["Est Lot Value Local"] = ""

    merged["Bidder"] = merged["supplier"] if "supplier" in merged.columns else ""
    merged["Award Date"] = merged["award_date"] if "award_date" in merged.columns else ""

    merged["Awarded Unit Price"] = merged["unit_price_offer"] if "unit_price_offer" in merged.columns else ""
    merged["Lot_Award_Value_Local"] = merged["lot_total_line"] if "lot_total_line" in merged.columns else ""

    merged["Awarded Unit Price"] = merged["Awarded Unit Price"].apply(safe_float_str)
    merged["Lot_Award_Value_Local"] = merged["Lot_Award_Value_Local"].apply(safe_float_str)
    merged["Ceiling Unit Price"] = merged["Ceiling Unit Price"].apply(safe_float_str)

    # -------------------------
    # EVERSANA RULE: If PUBLISHED, keep award-related fields BLANK
    # -------------------------
    published_mask = merged["Status"].eq("PUBLISHED")
    for c in ["Award Date", "Bidder", "Bid Status Award", "Lot_Award_Value_Local", "Awarded Unit Price", "Original_Publication_Link_Award"]:
        merged.loc[published_mask, c] = ""

    # -------------------------
    # Final columns (exact)
    # -------------------------
    final_columns = [
        "COUNTRY",
        "PROVINCE",
        "SOURCE",
        "Source Tender Id",
        "Tender Title",
        "Unique Lot ID",
        "Lot Number",
        "Sub Lot Number",
        "Lot Title",
        "Est Lot Value Local",
        "Local Currency",
        "Deadline Date",
        "TENDERING AUTHORITY",
        "Tendering Authority Type",
        "Tender Procedure Type",
        "CN Document Number",
        "Original_Publication_Link_Notice",
        "Ceiling Unit Price",
        "MEAT",
        "Price Evaluation ratio",
        "Quality Evaluation ratio",
        "Other Evaluation ratio",
        "CAN Document Number",
        "Award Date",
        "Bidder",
        "Bid Status Award",
        "Lot_Award_Value_Local",
        "Awarded Unit Price",
        "Original_Publication_Link_Award",
        "Status",
    ]

    for c in final_columns:
        if c not in merged.columns:
            merged[c] = ""

    final_df = merged[final_columns].copy()
    final_df.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"[OK] Final output created: {FINAL_OUTPUT_FILE}")
    print(f"   Rows: {len(final_df)}")


if __name__ == "__main__":
    main()

