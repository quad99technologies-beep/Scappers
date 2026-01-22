#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Argentina Report Generator — STRICT 4-key PCID match (no fallbacks, no synthetic)

Reads:
  ./Output/alfabeta_products_all_dict_en.csv
  ./input/pcid_Mapping.csv   (required for PCIDs)

Writes:
  ./Output/alfabeta_Report_<ddmmyyyy>_pcid_mapping.csv
  ./Output/alfabeta_Report_<ddmmyyyy>_pcid_missing.csv
  ./Output/alfabeta_Report_<ddmmyyyy>_pcid_oos.csv
  ./Output/alfabeta_Report_<ddmmyyyy>_pcid_no_data.csv

PCID ATTACH (STRICT):
  Match ONLY when ALL FOUR keys match (case/space-insensitive):
    1) Company
    2) Local Product Name
    3) Generic Name
    4) Local Pack Description
  Then copy PCID from mapping. Otherwise pcid remains blank.
"""

from pathlib import Path
from typing import Optional, Tuple, List
from datetime import datetime
import re
import json
import sys
import pandas as pd
import unicodedata
import string
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


def _file_date(path: Path) -> datetime.date:
    """Return the date part of the file's modification timestamp."""
    return datetime.fromtimestamp(path.stat().st_mtime).date()


def _is_same_day(path: Path) -> bool:
    """Return True if the file was modified today."""
    return _file_date(path) == datetime.now().date()

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
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().replace("\u00a0", "")
    if not s:
        return None

    sl = s.lower()
    if sl in {"nan", "none", "null"}:
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

def _is_real_value(val) -> bool:
    """
    Treat '', None, NaN, 'nan', 'none', 'null' as empty.
    """
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    if pd.isna(val):
        return False
    s = str(val).strip()
    if not s:
        return False
    if s.lower() in {"nan", "none", "null"}:
        return False
    return True

def get(row, *names, default=None):
    """
    FIXED: never treat 'nan'/'none'/'null' as valid.
    """
    for n in names:
        if n in row:
            val = row[n]
            if _is_real_value(val):
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

def fix_mojibake(s: str) -> str:
    """Repair common mojibake where UTF-8 bytes were decoded as Latin-1/CP1252."""
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s

def normalize_cell(s: Optional[object]) -> Optional[str]:
    """
    FIXED: Preserve missing values.
    Previously NaN became 'nan' via str(NaN) and then triggered IOMA everywhere.
    """
    if s is None or pd.isna(s):
        return None
    if not isinstance(s, str):
        s = str(s)
    s = fix_mojibake(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = "".join(
        ch for ch in s
        if ord(ch) < 128 and (ch.isalnum() or ch in string.punctuation or ch.isspace())
    )
    # Drop replacement-like '?' inside words (e.g., "amino?c" -> "aminoc").
    s = re.sub(r"(?<=[A-Za-z])\?(?=[A-Za-z])", "", s)
    return s.strip()

def normalize_df_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype) == "string":
            df[col] = df[col].map(normalize_cell)
    return df

# ---------- PCID mapping (STRICT 4-key match) ----------
def load_pcid_mapping(pcid_path: Path) -> Optional[pd.DataFrame]:
    if not pcid_path.exists():
        return None

    m = safe_read_csv(pcid_path, dtype=str)
    m = normalize_df_strings(m)

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

    m["PCID"] = m["PCID"].astype(str)
    return m

def attach_pcid_strict(df: pd.DataFrame, mapping_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    STRICT match: (Company, Local Product Name, Generic Name, Local Pack Description) -> PCID.
    - Case-insensitive, whitespace-normalized comparison.
    - No fallbacks. If any of the four keys don't match, PCID stays blank.
    """
    df = df.copy()

    # Normalize whitespace/mojibake before matching
    df = normalize_df_strings(df)

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
    if mapping_df is None:
        df["PCID"] = ""
        df.drop(columns=["n_company", "n_lprod", "n_generic", "n_desc"], inplace=True)
        return df

    m = normalize_df_strings(mapping_df)

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
    df["PCID"] = df.apply(
        lambda r: key_to_pcid.get((r["n_company"], r["n_lprod"], r["n_generic"], r["n_desc"]), ""),
        axis=1,
    ).astype("string")

    # Clean helpers
    df.drop(columns=["n_company", "n_lprod", "n_generic", "n_desc"], inplace=True)
    return df

def detect_oos_column(columns: List[str]) -> Optional[str]:
    for c in columns:
        if "oos" in c.strip().lower():
            return c
    return None

def normalize_pcid_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()

def normalize_pcid_marker(value: Optional[object]) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def is_invalid_pcid_marker(marker: str) -> bool:
    if not marker:
        return False
    compact = marker.replace(" ", "")
    return marker in {"oos", "new full", "new pull"} or compact in {"newfull", "newpull"}

def mapping_rows_to_output(mapping_df: pd.DataFrame, country_value: str, output_cols: List[str]) -> pd.DataFrame:
    def col_or_blank(name: str) -> pd.Series:
        if name in mapping_df.columns:
            return mapping_df[name].astype("string").fillna("")
        return pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string")

    out = pd.DataFrame({
        "PCID": col_or_blank("PCID"),
        "Country": country_value,
        "Company": col_or_blank("Company"),
        "Local Product Name": col_or_blank("Local Product Name"),
        "Generic Name": col_or_blank("Generic Name"),
        "Effective Start Date": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
        "Public With VAT Price": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
        "Reimbursement Category": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
        "Reimbursement Amount": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
        "Co-Pay Amount": pd.Series([""] * len(mapping_df), index=mapping_df.index, dtype="string"),
        "Local Pack Description": col_or_blank("Local Pack Description"),
    })
    return normalize_df_strings(out.reindex(columns=output_cols))

# ---------- RI logic ----------
def compute_ri_fields(row):
    """Apply Argentina RI rules with strict priority:
    1) IOMA, 2) PAMI-only, 3) IMPORTED, 4) No-scheme.
    """
    ioma_os      = get(row, "IOMA_OS")
    ioma_af      = get(row, "IOMA_AF")
    pami_af      = get(row, "PAMI_AF")
    ioma_detail  = get(row, "IOMA_detail", default="")
    import_stat  = get(row, "import_status", default="")

    # FIXED: use real-value checks instead of bool("nan") / bool("None")
    has_ioma = _is_real_value(ioma_os) or _is_real_value(ioma_af) or contains(ioma_detail, "ioma")
    has_pami = (not has_ioma) and _is_real_value(pami_af)
    is_imported = contains(import_stat, "importado", "imported")

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

    if not PCID_PATH.exists():
        raise FileNotFoundError(f"PCID mapping file not found: {PCID_PATH}")
    if not _is_same_day(PCID_PATH):
        file_date = _file_date(PCID_PATH)
        today = datetime.now().date()
        print(f"\n[SKIP] PCID mapping file {PCID_PATH.name} is from {file_date}, not today's run ({today}).", flush=True)
        print("[SKIP] Skipping Argentina report generation because the PCID mapping file was not replaced today.", flush=True)
        sys.exit(1)

    # Read input as text (avoid implicit type casts)
    print(f"[PROGRESS] Generating output: Loading data (1/4)", flush=True)
    df = safe_read_csv(INPUT_FILE, dtype=str)
    df = normalize_df_strings(df)

    # Compute RI fields
    print(f"[PROGRESS] Generating output: Computing RI fields (2/4)", flush=True)
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
    print(f"[PROGRESS] Generating output: Attaching PCIDs (3/4)", flush=True)
    mapping_df = load_pcid_mapping(PCID_PATH)
    df = attach_pcid_strict(df, mapping_df)

    # Final selection (PCID first)
    print(f"[PROGRESS] Generating output: Writing output (4/4)", flush=True)
    output_cols = [
        "PCID",
        "Country",
        "Company",
        "Local Product Name",
        "Generic Name",
        "Effective Start Date",
        "Public With VAT Price",
        "Reimbursement Category",
        "Reimbursement Amount",
        "Co-Pay Amount",
        "Local Pack Description",
    ]
    if EXCLUDE_PRICE:
        df["Public With VAT Price"] = pd.NA

    for c in output_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df_final = normalize_df_strings(df[output_cols].copy())

    pcid_norm = normalize_pcid_series(df_final["PCID"])
    pcid_marker = pcid_norm.map(normalize_pcid_marker)
    keep_mask = ~pcid_marker.map(is_invalid_pcid_marker)
    df_final = df_final[keep_mask].copy()
    pcid_norm = pcid_norm[keep_mask]

    mapped_mask = pcid_norm.ne("")

    df_mapped = df_final[mapped_mask].copy()
    df_missing = df_final[~mapped_mask].copy()

    out_mapped = INPUT_FILE.parent / f"{OUT_PREFIX}_pcid_mapping.csv"
    out_missing = INPUT_FILE.parent / f"{OUT_PREFIX}_pcid_missing.csv"
    out_oos = INPUT_FILE.parent / f"{OUT_PREFIX}_pcid_oos.csv"
    out_no_data = INPUT_FILE.parent / f"{OUT_PREFIX}_pcid_no_data.csv"

    df_mapped.to_csv(out_mapped, index=False, encoding="utf-8-sig", float_format="%.2f")
    df_missing.to_csv(out_missing, index=False, encoding="utf-8-sig", float_format="%.2f")

    if mapping_df is None:
        empty = pd.DataFrame(columns=output_cols)
        empty.to_csv(out_oos, index=False, encoding="utf-8-sig")
        empty.to_csv(out_no_data, index=False, encoding="utf-8-sig")
        print("[WARNING] pcid_Mapping.csv not found; OOS and no-data files are empty.")
    else:
        oos_col = detect_oos_column(list(mapping_df.columns))
        oos_mask = pd.Series(False, index=mapping_df.index)
        if oos_col:
            oos_vals = mapping_df[oos_col].astype("string").fillna("").str.strip().str.lower()
            oos_mask = oos_mask | oos_vals.str.contains("oos") | oos_vals.isin(["yes", "y", "true", "1"])

        mapping_pcid = normalize_pcid_series(mapping_df["PCID"])
        mapping_marker = mapping_pcid.map(normalize_pcid_marker)
        oos_mask = oos_mask | (mapping_marker == "oos")
        oos_df = mapping_rows_to_output(mapping_df[oos_mask].copy(), "ARGENTINA", output_cols)
        if not oos_mask.any():
            print("[WARNING] No OOS markers found in pcid_Mapping.csv; OOS file is empty.")

        output_pcid_set = set(pcid_norm[mapped_mask].tolist())
        invalid_mapping = mapping_marker.map(is_invalid_pcid_marker)
        no_data_mask = ~mapping_pcid.isin(output_pcid_set) & mapping_pcid.ne("") & ~invalid_mapping
        no_data_df = mapping_rows_to_output(mapping_df[no_data_mask].copy(), "ARGENTINA", output_cols)

        oos_df.to_csv(out_oos, index=False, encoding="utf-8-sig", float_format="%.2f")
        no_data_df.to_csv(out_no_data, index=False, encoding="utf-8-sig", float_format="%.2f")

    print(f"[PROGRESS] Generating output: {len(df_final)}/{len(df_final)} (100%)", flush=True)
    print("[OK] Wrote:", out_mapped)
    print("[OK] Wrote:", out_missing)
    print("[OK] Wrote:", out_oos)
    print("[OK] Wrote:", out_no_data)

    # Copy final reports (CSV) to central output directory
    try:
        # Add script directory to path for config_loader import
        script_dir = Path(__file__).resolve().parent
        if str(script_dir) not in sys.path:
            sys.path.insert(0, str(script_dir))
        from config_loader import get_central_output_dir
        import shutil
        central_output_dir = get_central_output_dir()
        central_mapped: Optional[Path] = None
        for out_path in [out_mapped, out_missing, out_oos, out_no_data]:
            central_final_report = central_output_dir / out_path.name
            shutil.copy2(out_path, central_final_report)
            print(f"[OK] Central Output: {central_final_report}")
            if out_path == out_mapped:
                central_mapped = central_final_report
        if central_mapped is not None:
            _write_diff_summary(
                country="Argentina",
                exports_dir=central_output_dir,
                new_path=central_mapped,
                glob_pattern=f"{OUTPUT_REPORT_PREFIX}*_pcid_mapping.csv",
                key_column="PCID",
                date_str=today_str,
            )
    except Exception as e:
        print(f"[WARNING] Could not copy to central output: {e}")
    if EXCLUDE_PRICE:
        print("Note: Pricing column was excluded (EXCLUDE_PRICE=True).")

def _find_previous_export_file(exports_dir: Path, pattern: str, current_path: Path) -> Optional[Path]:
    """Return the most recent export file matching the pattern, excluding the current file."""
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
    """Compare the new export with the previous one and write a summary."""
    previous = _find_previous_export_file(exports_dir, glob_pattern, new_path)
    if not previous:
        print(f"[DIFF] No previous {country} report to compare against. Skipping diff summary.", flush=True)
        return

    try:
        new_df = pd.read_csv(new_path, dtype=str, keep_default_na=False)
        old_df = pd.read_csv(previous, dtype=str, keep_default_na=False)
    except Exception as exc:
        print(f"[DIFF] Could not read exports for comparison: {exc}", flush=True)
        return

    new_keys = _extract_key_set(new_df, key_column)
    old_keys = _extract_key_set(old_df, key_column)
    if new_keys is None or old_keys is None:
        print(f"[DIFF] Key column '{key_column}' missing; skipping diff.", flush=True)
        return

    new_only = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    shared = sorted(new_keys & old_keys)

    summary_lines = [
        "=" * 80,
        f"{country} PCID Mapping Diff ({date_str})",
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
    main()
