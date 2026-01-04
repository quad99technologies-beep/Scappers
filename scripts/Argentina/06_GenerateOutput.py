#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Argentina Report Generator — STRICT 4-key PCID match (no fallbacks, no synthetic)

Reads:
  ./Output/alfabeta_products_all_dict_en.csv
  ./input/pcid_Mapping.csv   (required for PCIDs)

Writes:
  ./Output/alfabeta_Report_<ddmmyyyy>.csv
  ./Output/alfabeta_Report_<ddmmyyyy>.xlsx

PCID ATTACH (STRICT):
  Match ONLY when ALL FOUR keys match (case/space-insensitive):
    1) Company
    2) Local Product Name
    3) Generic Name
    4) Local Pack Description
  Then copy PCID from mapping. Otherwise pcid remains blank.
"""

from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime
import re
import pandas as pd
from charset_normalizer import from_path
from config_loader import (
    get_input_dir, get_output_dir,
    OUTPUT_TRANSLATED_CSV, PCID_MAPPING_FILE, OUTPUT_REPORT_PREFIX,
    DATE_FORMAT, EXCLUDE_PRICE
)

# ---------- Paths / Options ----------
INPUT_FILE     = get_output_dir() / OUTPUT_TRANSLATED_CSV
PCID_PATH      = get_input_dir() / PCID_MAPPING_FILE  # must contain the 5 columns listed below

# Auto-generate prefix with today's date (format from config)
today_str      = datetime.now().strftime(DATE_FORMAT)
OUT_PREFIX     = f"{OUTPUT_REPORT_PREFIX}{today_str}"

# EXCLUDE_PRICE is imported from config_loader

# ---------- Money parsing ----------
MONEY_RE = re.compile(
    r"""
    (?:^|[^\d])
    (?:ARS|\$)?\s*
    (
        (?:\d{1,3}(?:[.,]\d{3})+[.,]\d{2})
        | \d+[.,]\d{2}
        | \d+
    )
    (?:[^\d]|$)
    """,
    re.VERBOSE,
)

def parse_money(x: Optional[object]) -> Optional[float]:
    """Parse ARS money in ES/EN variants without losing decimals."""
    if x is None:
        return None
    s = str(x).strip().replace("\u00a0", "")
    if not s:
        return None

    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        try: return float(s)
        except: pass

    if re.fullmatch(r"\d{1,3}(?:,\d{3})+(?:\.\d+)?", s):
        try: return float(s.replace(",", ""))
        except: pass

    if re.fullmatch(r"\d{1,3}(?:\.\d{3})+,\d{2}", s):
        try: return float(s.replace(".", "").replace(",", "."))
        except: pass

    if re.fullmatch(r"\d+,\d{2}", s):
        try: return float(s.replace(",", "."))
        except: pass

    m = MONEY_RE.search(s)
    if not m:
        return None
    token = m.group(1)

    if "." in token and "," in token:
        if token.rfind(",") > token.rfind("."):
            token = token.replace(".", "").replace(",", ".")
        else:
            token = token.replace(",", "")
    else:
        if "," in token and "." not in token:
            token = token.replace(",", ".")
        elif "." in token and "," not in token and token.count(".") > 1:
            token = token.replace(".", "")

    try: return float(token)
    except: return None

# ---------- Small utilities ----------
def _norm(s: Optional[str]) -> str:
    """Lowercase + collapse whitespace; empty string for None."""
    return re.sub(r"\s+", " ", str(s).strip().lower()) if s is not None else ""

def contains(text, *needles: str) -> bool:
    try: t = str(text).lower()
    except: return False
    return any(n.lower() in t for n in needles)

def seriespick(df: pd.DataFrame, *candidates: str, default: str = "") -> pd.Series:
    """
    Return a Series for the first existing column among candidates.
    If none exists, return a Series filled with default.
    """
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series([default] * len(df), index=df.index, dtype="string")

def get(row, *names, default=None):
    for n in names:
        if n in row:
            val = row[n]
            if pd.notna(val) and str(val).strip():
                return val
    return default

# ---------- Encoding-safe readers ----------
def detect_encoding(path: Path) -> str:
    result = from_path(path).best()
    if result:
        enc = result.encoding
        print(f"[INFO] Detected encoding for {path.name}: {enc}")
        return enc
    return "latin1"

def safe_read_csv(path: Path, dtype=None) -> pd.DataFrame:
    enc = detect_encoding(path)
    try:
        return pd.read_csv(path, dtype=dtype, encoding=enc)
    except UnicodeDecodeError:
        print(f"[WARNING] Failed with {enc}, retrying with latin1...")
        return pd.read_csv(path, dtype=dtype, encoding="latin1")

# ---------- PCID mapping (STRICT 4-key match) ----------
def attach_pcid_strict(df: pd.DataFrame, pcid_path: Path) -> pd.DataFrame:
    """
    STRICT match: (Company, Local Product Name, Generic Name, Local Pack Description) -> PCID.
    - Case-insensitive, whitespace-normalized comparison.
    - No fallbacks. If any of the four keys don't match, PCID stays blank.
    Required mapping columns (exact logical names; aliases accepted):
      Company | Local Product Name | Generic Name | Local Pack Description | PCID
    """
    df = df.copy()

    # Pick report-side fields (use exact logical names produced later)
    s_company = seriespick(df, "company_term", "Company", "company")
    s_lprod   = seriespick(df, "product_name", "Local Product Name")
    s_generic = seriespick(df, "active_ingredient", "Generic Name")
    s_desc    = seriespick(df, "description", "Local Pack Description")

    df["n_company"] = s_company.map(_norm)
    df["n_lprod"]   = s_lprod.map(_norm)
    df["n_generic"] = s_generic.map(_norm)
    df["n_desc"]    = s_desc.map(_norm)

    # If mapping missing → leave blank
    if not pcid_path.exists():
        df["pcid"] = ""
        df.drop(columns=["n_company", "n_lprod", "n_generic", "n_desc"], inplace=True)
        return df

    # Load mapping and normalize headers to logical names
    m = safe_read_csv(pcid_path, dtype=str)

    rename = {}
    for c in m.columns:
        cl = c.strip().lower()
        if cl == "company": rename[c] = "Company"
        elif cl in ("local product name", "product name", "product", "product_name"):
            rename[c] = "Local Product Name"
        elif cl in ("generic name", "generic", "generic_name"):
            rename[c] = "Generic Name"
        elif cl in ("local pack description", "description", "local_pack_description"):
            rename[c] = "Local Pack Description"
        elif cl == "pcid": rename[c] = "PCID"
    m = m.rename(columns=rename)

    required = ["Company", "Local Product Name", "Generic Name", "Local Pack Description", "PCID"]
    for col in required:
        if col not in m.columns:
            raise ValueError(f"pcid_Mapping.csv missing required column: {col}")

    # Normalize mapping keys
    m["n_company"] = m["Company"].map(_norm)
    m["n_lprod"]   = m["Local Product Name"].map(_norm)
    m["n_generic"] = m["Generic Name"].map(_norm)
    m["n_desc"]    = m["Local Pack Description"].map(_norm)
    m["PCID"]      = m["PCID"].astype(str)

    # Build dict keyed by strict 4-tuple
    key_to_pcid = {
        (r.n_company, r.n_lprod, r.n_generic, r.n_desc): r.PCID
        for _, r in m.iterrows()
        if str(r.PCID).strip()
    }

    # Lookup (strict)
    df["pcid"] = df.apply(
        lambda r: key_to_pcid.get((r["n_company"], r["n_lprod"], r["n_generic"], r["n_desc"]), ""),
        axis=1,
    ).astype("string")

    # Clean helpers
    df.drop(columns=["n_company", "n_lprod", "n_generic", "n_desc"], inplace=True)
    return df

# ---------- RI logic ----------
def compute_ri_fields(row):
    ioma_os      = get(row, "IOMA_OS")
    ioma_af      = get(row, "IOMA_AF")
    pami_af      = get(row, "PAMI_AF")
    ioma_detail  = get(row, "IOMA_detail", default="")
    import_stat  = get(row, "import_status", default="")

    has_ioma     = bool(ioma_os) or bool(ioma_af) or contains(ioma_detail, "ioma")
    has_pami     = (not has_ioma) and bool(pami_af)
    is_imported  = contains(import_stat, "importado", "imported")

    if has_ioma:
        return "IOMA", parse_money(ioma_os), parse_money(ioma_af), "IOMA-preferred (OS→Reimb, AF→Copay)"
    if has_pami:
        return "PAMI-only", None, parse_money(pami_af), "PAMI-only-AF-as-Copay"
    if is_imported:
        return "IMPORTED", None, None, "Imported-fallback"
    return None, None, None, "No-scheme"

# ---------- Main ----------
def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}\n"
            f"Please run script 04 (TranslateUsingDictionary.py) first to generate this file."
        )

    # Read input as text (avoid implicit type casts)
    df = safe_read_csv(INPUT_FILE, dtype=str)

    # Compute RI fields
    ri = df.apply(
        lambda r: pd.Series(
            compute_ri_fields(r),
            index=["Reimbursement Category", "Reimbursement Amount", "Co-Pay Amount", "rule_label"],
        ),
        axis=1,
    )
    df = pd.concat([df, ri], axis=1)

    # Map input → final names (create the logical columns the matcher expects)
    df["Country"]                = "ARGENTINA"
    df["Company"]                = seriespick(df, "company_term", "Company", "company")
    df["Local Product Name"]     = seriespick(df, "product_name", "Local Product Name")
    df["Generic Name"]           = seriespick(df, "active_ingredient", "Generic Name")
    df["Effective Start Date"]   = seriespick(df, "date", "Effective Start Date")
    df["Local Pack Description"] = seriespick(df, "description", "Local Pack Description")

    # Price
    if not EXCLUDE_PRICE:
        df["Public With VAT Price"] = seriespick(df, "price_ars", "Public With VAT Price").apply(parse_money)

    # Ensure RI numeric fields are floats
    df["Reimbursement Amount"] = df["Reimbursement Amount"].apply(parse_money)
    df["Co-Pay Amount"]        = df["Co-Pay Amount"].apply(parse_money)

    # STRICT 4-key PCID attach
    df = attach_pcid_strict(df, PCID_PATH)

    # Final selection (pcid first)
    base_cols = [
        "pcid",
        "Country",
        "Company",
        "Local Product Name",
        "Generic Name",
        "Effective Start Date",
    ]
    tail_cols = [
        "Reimbursement Category",
        "Reimbursement Amount",
        "Co-Pay Amount",
        "Local Pack Description",
        "rule_label",
    ]
    final_cols = base_cols + (["Public With VAT Price"] if not EXCLUDE_PRICE else []) + tail_cols

    for c in final_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df_final = df[final_cols].copy()

    # Write outputs next to INPUT_FILE
    out_csv  = INPUT_FILE.parent / f"{OUT_PREFIX}.csv"
    out_xlsx = INPUT_FILE.parent / f"{OUT_PREFIX}.xlsx"

    df_final.to_csv(out_csv, index=False, encoding="utf-8-sig", float_format="%.2f")

    try:
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            df_final.to_excel(writer, index=False)
    except Exception as e:
        print("Could not write XLSX:", e)

    print("✔ Wrote:", out_csv)
    print("✔ Wrote:", out_xlsx)
    
    # Copy final report (CSV) to central output directory
    try:
        import sys
        from pathlib import Path
        # Add script directory to path for config_loader import
        script_dir = Path(__file__).resolve().parent
        if str(script_dir) not in sys.path:
            sys.path.insert(0, str(script_dir))
        from config_loader import get_central_output_dir
        import shutil
        central_output_dir = get_central_output_dir()
        central_final_report = central_output_dir / out_csv.name
        shutil.copy2(out_csv, central_final_report)
        print(f"✔ Central Output: {central_final_report}")
    except Exception as e:
        print(f"[WARNING] Could not copy to central output: {e}")
    if EXCLUDE_PRICE:
        print("Note: Pricing column was excluded (EXCLUDE_PRICE=True).")

if __name__ == "__main__":
    main()
