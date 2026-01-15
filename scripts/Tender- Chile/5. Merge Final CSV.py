from pathlib import Path
import pandas as pd
import sys
import re

_script_dir = Path(__file__).resolve().parent
_scraper_root = _script_dir
if str(_scraper_root) not in sys.path:
    sys.path.insert(0, str(_scraper_root))

from config_loader import load_env_file, getenv, get_output_dir


load_env_file()
OUTPUT_DIR = get_output_dir()

TENDER_DETAILS_FILE = OUTPUT_DIR / getenv("SCRIPT_03_OUTPUT_CSV", "tender_details.csv")
SUPPLIER_ROWS_FILE = OUTPUT_DIR / getenv("SCRIPT_04_SUPPLIER_OUTPUT_CSV", "mercadopublico_supplier_rows.csv")
TENDER_LIST_FILE = OUTPUT_DIR / getenv("SCRIPT_01_OUTPUT_CSV", "tender_list.csv")
FINAL_OUTPUT_FILE = OUTPUT_DIR / getenv("SCRIPT_05_FINAL_OUTPUT_CSV", "final_tender_data.csv")


def require_file(path: Path):
    if not path.exists():
        print(f"[ERROR] Required file missing: {path}")
        sys.exit(1)


def require_columns(df: pd.DataFrame, required: list[str], file_label: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[ERROR] Missing columns in {file_label}: {missing}")
        print(f"   Available columns: {list(df.columns)}")
        sys.exit(1)


def normalize_lot_number(x) -> str:
    """
    Normalizes lot numbers so '1', '1.0', 1, 1.0 all become '1'.
    Keeps non-numeric lot numbers as trimmed strings.
    """
    if pd.isna(x):
        return ""
    s = str(x).strip()
    # Convert "1.0" -> "1"
    if re.fullmatch(r"\d+\.0", s):
        return s.split(".")[0]
    # Convert numeric strings/floats -> int string when safe
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except Exception:
        pass
    return s


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    require_file(TENDER_DETAILS_FILE)
    require_file(SUPPLIER_ROWS_FILE)
    require_file(TENDER_LIST_FILE)

    tender_details = pd.read_csv(TENDER_DETAILS_FILE, encoding="utf-8-sig")
    supplier_rows = pd.read_csv(SUPPLIER_ROWS_FILE, encoding="utf-8-sig")
    tender_list = pd.read_csv(TENDER_LIST_FILE, encoding="utf-8-sig")

    require_columns(tender_details, ["Tender ID", "Lot Number", "Source URL"], "tender_details.csv")
    require_columns(supplier_rows, ["source_tender_url", "lot_number", "supplier"], "mercadopublico_supplier_rows.csv")

    if "IDLicitacion" not in tender_list.columns:
        if "CN Document Number" in tender_list.columns:
            tender_list["IDLicitacion"] = tender_list["CN Document Number"]
        elif "Source Tender Id" in tender_list.columns:
            tender_list["IDLicitacion"] = tender_list["Source Tender Id"]
        else:
            tender_list["IDLicitacion"] = ""
    if "Moneda" not in tender_list.columns:
        tender_list["Moneda"] = ""
    if "Tipo" not in tender_list.columns:
        tender_list["Tipo"] = ""

    # Normalize join keys
    tender_details["__source_url"] = tender_details["Source URL"].astype(str).str.strip()
    supplier_rows["__source_tender_url"] = supplier_rows["source_tender_url"].astype(str).str.strip()

    tender_details["__lot_number"] = tender_details["Lot Number"].apply(normalize_lot_number)
    supplier_rows["__lot_number"] = supplier_rows["lot_number"].apply(normalize_lot_number)

    # Merge tender_details + supplier_rows for bidder-level output
    merged = pd.merge(
        tender_details,
        supplier_rows,
        left_on=["__source_url", "__lot_number"],
        right_on=["__source_tender_url", "__lot_number"],
        how="left",
        suffixes=("", "_award"),
    )

    # Join tender_list for currency + procedure
    merged["Tender ID"] = merged["Tender ID"].astype(str).str.strip()
    tender_list["IDLicitacion"] = tender_list["IDLicitacion"].astype(str).str.strip()

    merged = pd.merge(
        merged,
        tender_list[["IDLicitacion", "Moneda", "Tipo"]],
        left_on="Tender ID",
        right_on="IDLicitacion",
        how="left",
    )

    # Add REF TEMPLATE columns (closest achievable)
    merged["COUNTRY"] = "CHILE"
    merged["SOURCE"] = "MERCADOPUBLICO"
    merged["Local Currency"] = merged["Moneda"]
    merged["Tender Procedure Type"] = merged["Tipo"]

    # Not available today -> blank
    merged["Tendering Authority Type"] = ""
    merged["CN Document Number"] = ""
    merged["CAN Document Number"] = ""
    merged["Ceiling Unit Price"] = ""
    merged["MEAT"] = ""
    merged["Sub Lot Number"] = ""
    merged["Est Lot Value Local"] = ""

    # Links
    merged["Original_Publication_Link_Notice"] = merged["Source URL"]
    merged["Original_Publication_Link_Award"] = merged["source_url"] if "source_url" in merged.columns else ""

    # Bidder-level fields (from supplier_rows)
    merged["Bidder"] = merged["supplier"] if "supplier" in merged.columns else ""
    merged["Bid Status Award"] = merged["is_awarded"] if "is_awarded" in merged.columns else ""
    if "unit_price_offer" in merged.columns:
        merged["Awarded Unit Price"] = merged["unit_price_offer"]
    elif "unit_price_offer_raw" in merged.columns:
        merged["Awarded Unit Price"] = merged["unit_price_offer_raw"]
    else:
        merged["Awarded Unit Price"] = ""
    if "total_net_awarded" in merged.columns:
        merged["Lot_Award_Value_Local"] = merged["total_net_awarded"]
    elif "total_net_awarded_raw" in merged.columns:
        merged["Lot_Award_Value_Local"] = merged["total_net_awarded_raw"]
    else:
        merged["Lot_Award_Value_Local"] = ""
    merged["Award Date"] = merged["award_date"] if "award_date" in merged.columns else ""

    # Overall Status per lot (AWARDED if any bidder awarded)
    if "Bid Status Award" in merged.columns:
        status_map = (
            merged.groupby(["__source_url", "__lot_number"])["Bid Status Award"]
            .transform(lambda s: "AWARDED" if any(str(v).upper() == "YES" for v in s) else "NO AWARD")
        )
        merged["Status"] = status_map
    else:
        merged["Status"] = ""

    # Rename to template headers
    merged.rename(
        columns={
            "Tender ID": "Source Tender Id",
            "Tender Title": "Tender Title",
            "Unique Lot ID": "Unique Lot ID",
            "Lot Number": "Lot Number",
            "Lot Title": "Lot Title",
            "Closing Date": "Deadline Date",
            "TENDERING AUTHORITY": "TENDERING AUTHORITY",
            "PROVINCE": "PROVINCE",
            "Price Evaluation ratio": "Price Evaluation ratio",
            "Quality Evaluation ratio": "Quality Evaluation ratio",
            "Other Evaluation ratio": "Other Evaluation ratio",
        },
        inplace=True,
    )

    final_columns = [
        "COUNTRY", "PROVINCE", "SOURCE",
        "Source Tender Id", "Tender Title",
        "Unique Lot ID", "Lot Number", "Sub Lot Number", "Lot Title",
        "Est Lot Value Local", "Local Currency", "Deadline Date",
        "TENDERING AUTHORITY", "Tendering Authority Type", "Tender Procedure Type",
        "CN Document Number", "Original_Publication_Link_Notice",
        "Ceiling Unit Price", "MEAT",
        "Price Evaluation ratio", "Quality Evaluation ratio", "Other Evaluation ratio",
        "CAN Document Number", "Award Date", "Bidder", "Bid Status Award",
        "Lot_Award_Value_Local", "Awarded Unit Price",
        "Original_Publication_Link_Award", "Status",
    ]

    final_df = merged[[c for c in final_columns if c in merged.columns]]
    final_df.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"[OK] Final report updated: {FINAL_OUTPUT_FILE}")


if __name__ == "__main__":
    main()
