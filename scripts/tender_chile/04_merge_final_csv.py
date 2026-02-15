#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 4: Merge Final CSV
=======================
Merges tender details, supplier data, and input metadata into the final EVERSANA-format CSV.

INPUTS:
  - PostgreSQL table tc_input_tender_list (input metadata)
  - PostgreSQL table tc_tender_details (tender details)
  - PostgreSQL table tc_tender_awards (award data)
  PostgreSQL is the ONLY source of truth.

OUTPUTS:
  - PostgreSQL table tc_final_output (source of truth)
  - output/Tender_Chile/final_tender_data.csv (export only)

UPDATED (per EVERSANA):
  - Adds new bidder-row field: "AWARDED LOT TITLE"
    Source: mercadopublico_supplier_rows.csv (expected column "AWARDED LOT TITLE")
    Fallback: supplier_specifications (if column not present)
  - Applies EVERSANA rule: If Status=PUBLISHED, award-related fields must be blank (includes AWARDED LOT TITLE)
"""

from __future__ import annotations

import os
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

FINAL_OUTPUT_FILE = OUTPUT_DIR / "final_tender_data.csv"  # CSV export only
# Fallback to new filename if original is locked
FINAL_OUTPUT_FILE_ALT = OUTPUT_DIR / "final_tender_data_v2.csv"


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


def safe_int_str(x) -> str:
    s = norm_str(x)
    if not s:
        return ""
    try:
        v = int(float(str(s).replace(",", "")))
        return str(v)
    except Exception:
        digits = re.sub(r"\D", "", str(s))
        return digits


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Read from PostgreSQL (PostgreSQL is the ONLY source of truth)
    run_id = os.getenv("TENDER_CHILE_RUN_ID", "")
    if not run_id:
        print("[ERROR] TENDER_CHILE_RUN_ID environment variable not set")
        sys.exit(1)

    try:
        from core.db.connection import CountryDB
        from db.repositories import ChileRepository
        
        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        
        # Read input tender list
        with db.cursor(dict_cursor=True) as cur:
            cur.execute("SELECT tender_id, description, url FROM tc_input_tender_list ORDER BY id")
            input_rows = cur.fetchall()
        tender_list = pd.DataFrame(input_rows)
        if not tender_list.empty:
            tender_list.rename(columns={"tender_id": "CN Document Number"}, inplace=True)
        
        # Read tender details from database (PostgreSQL is the ONLY source of truth)
        details_rows = repo.get_all_tender_details()
        tender_details = pd.DataFrame(details_rows)
        if not tender_details.empty:
            tender_details.rename(columns={
                "tender_id": "Tender ID",
                "tender_name": "Tender Title",
                "organization": "TENDERING AUTHORITY",
                "province": "PROVINCE",
                "closing_date": "Closing Date",
                "source_url": "Source URL"
            }, inplace=True)
        
        # Read tender awards (supplier rows) from database - includes ALL bidders
        with db.cursor(dict_cursor=True) as cur:
            cur.execute("""
                SELECT 
                    tender_id,
                    lot_number,
                    lot_title,
                    un_classification_code,
                    buyer_specifications,
                    lot_quantity,
                    supplier_name as supplier,
                    supplier_rut,
                    supplier_specifications,
                    unit_price_offer,
                    awarded_quantity,
                    total_net_awarded,
                    award_amount,
                    award_date,
                    award_status as state,
                    is_awarded,
                    awarded_unit_price,
                    source_url,
                    source_tender_url
                FROM tc_tender_awards
                WHERE run_id = %s
            """, (run_id,))
            award_rows = cur.fetchall()
        supplier_rows = pd.DataFrame(award_rows)
        print(f"Reading: PostgreSQL tables (source of truth)")
        
        if not supplier_rows.empty and "source_tender_url" in supplier_rows.columns:
            supplier_rows["source_tender_url"] = supplier_rows["source_tender_url"]
        
        print(f"   Input tenders: {len(tender_list)}")
        print(f"   Tender details: {len(tender_details)}")
        print(f"   Supplier rows: {len(supplier_rows)}")
        
    except Exception as e:
        print(f"[ERROR] Failed to read from PostgreSQL: {e}")
        print(f"[ERROR] Make sure all previous steps completed successfully")
        sys.exit(1)
    
    if tender_details.empty:
        print("[ERROR] No tender details found in database")
        print("[INFO] Run Step 2 first to populate tc_tender_details table")
        sys.exit(1)
    
    if supplier_rows.empty:
        print("[WARN] No tender awards found in database - final output will have no award data")
        # Create empty supplier_rows dataframe with required columns for merge
        supplier_rows = pd.DataFrame(columns=["tender_id", "lot_number", "supplier", "source_tender_url", "__lot_number", "__source_tender_url"])
        # Create empty supplier_rows dataframe with required columns
        supplier_rows = pd.DataFrame(columns=["tender_id", "lot_number", "supplier", "source_tender_url", "__lot_number", "__source_tender_url"])

    # -------------------------
    # Base Tender ID (CN code)
    # -------------------------
    if "Tender ID" not in tender_details.columns:
        print(f"[ERROR] Tender details missing 'Tender ID' column. Found: {list(tender_details.columns)}")
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
        if col not in supplier_rows.columns and not supplier_rows.empty:
            print(f"[ERROR] Tender awards missing '{col}' column. Found: {list(supplier_rows.columns)}")
            sys.exit(1)

    # Ensure Source URL exists in td
    if "Source URL" not in td.columns:
        print(f"[ERROR] Tender details missing 'Source URL' column. Found: {list(td.columns)}")
        sys.exit(1)
    
    td["__source_url"] = td["Source URL"].astype(str).str.strip()
    
    # Also create a normalized URL for matching (remove trailing slashes, normalize)
    def normalize_url(url):
        if pd.isna(url):
            return ""
        url = str(url).strip()
        # Remove trailing slash for matching
        if url.endswith('/'):
            url = url[:-1]
        return url
    
    td["__source_url_norm"] = td["Source URL"].apply(normalize_url)
    
    supplier_rows["__source_tender_url"] = supplier_rows["source_tender_url"].astype(str).str.strip()
    supplier_rows["__source_tender_url_norm"] = supplier_rows["source_tender_url"].apply(normalize_url)
    supplier_rows["__lot_number"] = supplier_rows["lot_number"].apply(normalize_lot_number)

    # Debug: Print sample URLs to help diagnose merge issues
    if not td.empty:
        print(f"[DEBUG] Sample tender_details URLs: {td['__source_url'].head(3).tolist()}")
    if not supplier_rows.empty:
        print(f"[DEBUG] Sample supplier_rows URLs: {supplier_rows['__source_tender_url'].head(3).tolist()}")

    # CRITICAL: Merge with supplier_rows on LEFT to get all bidder rows (one per supplier)
    # If we put td on left, we only get 1 row (one per tender)
    merged = supplier_rows.merge(
        td,
        left_on="__source_tender_url_norm",
        right_on="__source_url_norm",
        how="left",
        suffixes=("", "_td"),
    )
    
    # Check merge results
    merge_success = merged["__tender_id"].notna().sum()
    merge_total = len(merged)
    print(f"[DEBUG] Merge results: {merge_success}/{merge_total} rows matched with tender details")

    # -------------------------
    # Derive award status per tender (overall)
    # -------------------------
    # First create effective tender_id for grouping
    merged["__effective_tender_id"] = merged["__tender_id"].fillna(merged.get("tender_id", ""))
    
    if "is_awarded" in merged.columns:
        merged["__is_awarded_yes"] = merged["is_awarded"].apply(to_yes_no).eq("YES")
    else:
        merged["__is_awarded_yes"] = False

    # Group by effective tender_id (which includes fallback to supplier_rows.tender_id)
    tender_awarded = merged.groupby("__effective_tender_id")["__is_awarded_yes"].any().reset_index(name="__tender_any_award")
    merged = merged.merge(tender_awarded, on="__effective_tender_id", how="left")

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

    # Use __tender_id from tender_details if available, otherwise fallback to tender_id from supplier_rows
    merged["__effective_tender_id"] = merged["__tender_id"].fillna(merged.get("tender_id", ""))
    
    merged["Source Tender Id"] = merged["__effective_tender_id"]
    merged["CN Document Number"] = merged["__effective_tender_id"]
    merged["CAN Document Number"] = merged["__effective_tender_id"]

    # Use tender details if available, otherwise leave empty (we don't have this data in award page)
    merged["Tender Title"] = merged.get("Tender Title", "")
    merged["TENDERING AUTHORITY"] = merged.get("TENDERING AUTHORITY", "")
    merged["PROVINCE"] = merged.get("PROVINCE", "")
    merged["Deadline Date"] = merged.get("Closing Date", "")
    
    # Evaluation ratios from tender details
    merged["Price Evaluation ratio"] = merged.get("Price Evaluation ratio", "")
    merged["Quality Evaluation ratio"] = merged.get("Quality Evaluation ratio", "")
    merged["Other Evaluation ratio"] = merged.get("Other Evaluation ratio", "")

    merged["Tendering Authority Type"] = ""
    merged["Tender Procedure Type"] = merged[tl_proc_col] if tl_proc_col and tl_proc_col in merged.columns else ""
    merged["Local Currency"] = merged[tl_currency_col] if tl_currency_col and tl_currency_col in merged.columns else ""
    merged["MEAT"] = merged[tl_meat_col] if tl_meat_col and tl_meat_col in merged.columns else ""
    merged["Ceiling Unit Price"] = merged[tl_ceiling_col] if tl_ceiling_col and tl_ceiling_col in merged.columns else ""

    # URLs
    merged["Original_Publication_Link_Notice"] = merged.get("Source URL", merged.get("source_tender_url", ""))
    merged["Original_Publication_Link_Award"] = merged["source_url"] if "source_url" in merged.columns else ""

    # Lot information - prefer supplier_rows data (from award page)
    merged["Lot Number"] = merged["lot_number"] if "lot_number" in merged.columns else ""
    merged["Sub Lot Number"] = ""
    # Unique Lot ID from supplier_rows (un_classification_code) or tender_details
    merged["Unique Lot ID"] = merged.get("un_classification_code", merged.get("Unique Lot ID", ""))
    # Lot Title from award page (buyer_specifications or item_title)
    if "buyer_specifications" in merged.columns:
        merged["Lot Title"] = merged["buyer_specifications"]
    elif "item_title" in merged.columns:
        merged["Lot Title"] = merged["item_title"]
    else:
        merged["Lot Title"] = merged.get("lot_title", "")

    merged["Est Lot Value Local"] = ""

    merged["Bidder"] = merged["supplier"] if "supplier" in merged.columns else ""
    merged["Award Date"] = merged["award_date"] if "award_date" in merged.columns else ""

    # Awarded Unit Price: use awarded_unit_price for winners, unit_price_offer for all bidders
    # This matches expected format where all bidders have a price quoted
    if "unit_price_offer" in merged.columns:
        merged["Awarded Unit Price"] = merged["unit_price_offer"]
    elif "awarded_unit_price" in merged.columns:
        merged["Awarded Unit Price"] = merged["awarded_unit_price"]
    else:
        merged["Awarded Unit Price"] = ""

    # -------------------------
    # NEW FIELD: AWARDED LOT TITLE (bidder-row specific)
    # -------------------------
    # Use supplier_specifications from database (supplier's bid description)
    if "supplier_specifications" in merged.columns:
        merged["AWARDED LOT TITLE"] = merged["supplier_specifications"].fillna("").astype(str)
    else:
        merged["AWARDED LOT TITLE"] = ""

    # -------------------------
    # Bidder-row specific values
    # -------------------------
    # Lot_Award_Value_Local should be populated only for the winning bidder row.
    if "total_net_awarded" in merged.columns and "is_awarded" in merged.columns:
        merged["Lot_Award_Value_Local"] = merged.apply(
            lambda r: r.get("total_net_awarded") if to_yes_no(r.get("is_awarded")) == "YES" else 0,
            axis=1,
        )
    else:
        merged["Lot_Award_Value_Local"] = ""

    # Quantity from award page (lot_quantity) - this is the lot-level quantity
    # Fallback to Quantity from tender_details if available
    if "lot_quantity" in merged.columns:
        merged["QUANTITY"] = merged["lot_quantity"].apply(safe_int_str)
    else:
        merged["QUANTITY"] = merged.get("Quantity", "")

    # Awarded quantity is bidder-row specific; only the winner gets it.
    if "awarded_quantity" in merged.columns and "is_awarded" in merged.columns:
        merged["AWARDED QUANTITY"] = merged.apply(
            lambda r: r.get("awarded_quantity") if to_yes_no(r.get("is_awarded")) == "YES" else 0,
            axis=1,
        )
    else:
        merged["AWARDED QUANTITY"] = ""

    merged["Awarded Unit Price"] = merged["Awarded Unit Price"].apply(safe_float_str)
    merged["Lot_Award_Value_Local"] = merged["Lot_Award_Value_Local"].apply(safe_float_str)
    merged["Ceiling Unit Price"] = merged["Ceiling Unit Price"].apply(safe_float_str)

    merged["QUANTITY"] = merged["QUANTITY"].apply(safe_int_str)
    merged["AWARDED QUANTITY"] = merged["AWARDED QUANTITY"].apply(safe_int_str)

    # -------------------------
    # EVERSANA RULE: If PUBLISHED, keep award-related fields BLANK
    # -------------------------
    published_mask = merged["Status"].eq("PUBLISHED")
    for c in [
        "Award Date",
        "Bidder",
        "Bid Status Award",
        "Lot_Award_Value_Local",
        "Awarded Unit Price",
        "Original_Publication_Link_Award",
        "AWARDED QUANTITY",
        "AWARDED LOT TITLE",  # NEW field is award/bidder-row related
    ]:
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
        "AWARDED LOT TITLE",  # NEW column in final output
        "Bid Status Award",
        "Lot_Award_Value_Local",
        "Awarded Unit Price",
        "Original_Publication_Link_Award",
        "Status",
        "QUANTITY",
        "AWARDED QUANTITY",
    ]

    for c in final_columns:
        if c not in merged.columns:
            merged[c] = ""

    final_df = merged[final_columns].copy()
    
    # Sort by Lot Number and Bidder to match expected output order
    final_df = final_df.sort_values(by=["Lot Number", "Bidder"]).reset_index(drop=True)
    
    # Save to PostgreSQL (PostgreSQL is the ONLY source of truth)
    try:
        from core.db.connection import CountryDB
        from db.repositories import ChileRepository
        
        db = CountryDB("Tender_Chile")
        db.connect()
        repo = ChileRepository(db, run_id)
        
        # Prepare final output data for database
        final_outputs = []
        for _, row in final_df.iterrows():
            final_outputs.append({
                "tender_id": str(row.get("Tender ID", "")).strip(),
                "tender_name": str(row.get("Tender Title", "")).strip(),
                "tender_status": str(row.get("Status", "")).strip(),
                "organization": str(row.get("TENDERING AUTHORITY", "")).strip(),
                "contact_info": "",  # Not available in merged data
                "lot_number": str(row.get("Lot Number", "")).strip(),
                "lot_title": str(row.get("AWARDED LOT TITLE") or row.get("Lot Title", "")).strip(),
                "supplier_name": str(row.get("Supplier Name", "")).strip(),
                "supplier_rut": str(row.get("Supplier RUT", "")).strip(),
                "currency": str(row.get("Local Currency", "CLP")).strip(),
                "estimated_amount": None,  # Not available in merged data
                "award_amount": None,  # Can be extracted from Lot_Award_Value_Local if available
                "publication_date": "",  # Not available in merged data
                "closing_date": str(row.get("Closing Date", "")).strip(),
                "award_date": str(row.get("Award Date", "")).strip(),
                "description": str(row.get("Description", "")).strip(),
                "source_url": str(row.get("Source URL", "")).strip(),
            })
        
        # Bulk insert to database
        if final_outputs:
            count = repo.insert_final_output(final_outputs)
            print(f"[DB] Saved {count} final output rows to PostgreSQL table 'tc_final_output'")
        
        db.close()
    except Exception as e:
        print(f"[WARN] Could not save to PostgreSQL: {e}")
        print(f"[WARN] Continuing with CSV export only")
    
    # Also write CSV for export (PostgreSQL is source of truth, CSV is export only)
    # Try primary filename first, fallback to alternative if locked
    try:
        final_df.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8-sig")
        print(f"[OK] Final output CSV export created: {FINAL_OUTPUT_FILE}")
    except PermissionError:
        final_df.to_csv(FINAL_OUTPUT_FILE_ALT, index=False, encoding="utf-8-sig")
        print(f"[OK] Final output CSV export created: {FINAL_OUTPUT_FILE_ALT}")
    print(f"   Rows: {len(final_df)}")


if __name__ == "__main__":
    main()
