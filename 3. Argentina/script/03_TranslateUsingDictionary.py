#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Whole-cell ES→EN translation using input/Dictionary.csv (Spanish,English).

Rules (applied only to TARGET_COLUMNS):
  1) If the WHOLE cell (case-insensitive, NFC-normalized) matches a Spanish entry → replace with English mapping.
  2) Else if the WHOLE cell matches an English entry (from the dictionary) → keep as-is.
  3) Else if the cell is numeric-like (amounts/currency/percents) → keep as-is (don’t log missing).
  4) Else → log the entire cell value to output/missing_cells.csv (original value).

Also:
  • Auto-repairs mojibake where UTF-8 text was read as Latin-1/CP1252 (e.g., 'AntibiÃ³tico' → 'Antibiótico').
  • Robust CSV loading across utf-8, cp1252, latin1, etc.

Inputs:
  - input/Dictionary.csv
  - output/alfabeta_products_all.csv

Outputs:
  - output/alfabeta_products_all_dict_en.csv
  - output/missing_cells.csv  (value, count, example_columns)
"""

from pathlib import Path
from collections import Counter, defaultdict
import pandas as pd
import unicodedata

INPUT_DIR  = Path("Input")
OUTPUT_DIR = Path("Output")

SOURCE_FILE = OUTPUT_DIR / "alfabeta_products_by_product.csv"
DICT_FILE   = INPUT_DIR / "Dictionary.csv"

OUT_TRANSLATED = OUTPUT_DIR / "alfabeta_products_all_dict_en.csv"
OUT_MISSING    = OUTPUT_DIR / "missing_cells.csv"

# Only these columns are processed
TARGET_COLUMNS = [
    "active_ingredient", "therapeutic_class", "description",
    "SIFAR_detail", "IOMA_detail", "IOMA_AF", "IOMA_OS", "import_status",
]

# ---------- Robust CSV loader ----------
_PREFERRED_ENCODINGS = ["utf-8", "utf-8-sig", "cp1252", "latin1", "iso-8859-1"]

def read_csv_robust(path: Path, **kwargs) -> pd.DataFrame:
    last_err = None
    for enc in _PREFERRED_ENCODINGS:
        try:
            return pd.read_csv(path, encoding=enc, **kwargs)
        except Exception as e:
            last_err = e
            continue
    # final fallback: python engine + delimiter auto
    return pd.read_csv(path, encoding=_PREFERRED_ENCODINGS[-1], sep=None, engine="python", **kwargs)

# ---------- Text normalization & mojibake repair ----------
def fix_mojibake(s: str) -> str:
    """Repair common mojibake where UTF-8 bytes were decoded as Latin-1/CP1252."""
    if not isinstance(s, str):
        return s
    try:
        # If original text was UTF-8 but got decoded as cp1252, this reverses it.
        repaired = s.encode("latin1").decode("utf-8")
        return repaired
    except Exception:
        return s

def normalize_text(s: str) -> str:
    """NFC normalize, replace NBSP with space, strip, lower."""
    if not isinstance(s, str):
        return ""
    s = s.replace("\u00A0", " ")  # NBSP → space
    s = unicodedata.normalize("NFC", s)
    return s.strip().lower()

CURRENCY_CHARS = "₹$€£ARS "

def is_numeric_like(s: str) -> bool:
    """
    Detect amounts like '13,162.12', '13162.12', ' 701.49', '₹1,000', '10%', '18072.12'.
    Strips currency/space/percent and thousands separators, then tries float().
    """
    if not s:
        return False
    t = s.strip()
    # strip leading currency symbols and spaces
    t = t.strip(CURRENCY_CHARS)
    # handle percent at end
    if t.endswith("%"):
        t = t[:-1]
    # remove thousands separators/spaces
    t = t.replace(",", "").replace(" ", "")
    try:
        float(t)
        return True
    except ValueError:
        return False

# ---------- Dictionary loader ----------
def load_dictionary(dict_path: Path):
    if not dict_path.exists():
        raise FileNotFoundError(f"Dictionary file not found: {dict_path}")
    df = read_csv_robust(dict_path, dtype=str)
    if df.shape[1] < 2:
        df = read_csv_robust(dict_path, dtype=str, sep=None, engine="python")
        if df.shape[1] < 2:
            raise ValueError("Dictionary.csv must have at least two columns (Spanish, English)")
    es_col, en_col = df.columns[:2]
    df = df[[es_col, en_col]].fillna("")
    df.columns = ["es", "en"]

    es_to_en = {}
    english_set = set()

    for _, row in df.iterrows():
        es_raw = fix_mojibake(str(row["es"]))
        en_raw = fix_mojibake(str(row["en"]))
        es_key = normalize_text(es_raw)
        en_key = normalize_text(en_raw)
        if es_key:
            es_to_en[es_key] = en_raw.strip()  # keep original English casing
        if en_key:
            english_set.add(en_key)

    return es_to_en, english_set

# ---------- Cell translation ----------
def translate_cell_value(val, es_to_en, english_set, colname, miss_counter, miss_cols):
    # keep non-strings as-is
    if not isinstance(val, str):
        return val

    # Repair mojibake in the cell first
    raw = fix_mojibake(val)
    key = raw.strip()

    # Skip numeric-like values (amounts)
    if is_numeric_like(key):
        return raw

    nkey = normalize_text(key)

    # Whole-cell match only
    if nkey in es_to_en:
        return es_to_en[nkey]
    if nkey in english_set:
        return raw

    # Record as missing (non-empty)
    if key:
        miss_counter[raw] += 1
        miss_cols[raw].add(colname)
    return raw

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}\n"
            f"Please run script 03 (alfabeta_scraper_labs.py) first to generate this file."
        )
    
    if not DICT_FILE.exists():
        raise FileNotFoundError(
            f"Dictionary file not found: {DICT_FILE}\n"
            f"Please ensure Dictionary.csv exists in the Input/ directory."
        )

    es_to_en, english_set = load_dictionary(DICT_FILE)
    df = read_csv_robust(SOURCE_FILE, dtype=str, keep_default_na=False)

    # Only process target columns that exist
    cols = [c for c in TARGET_COLUMNS if c in df.columns]
    if not cols:
        df.to_csv(OUT_TRANSLATED, index=False)
        pd.DataFrame(columns=["value", "count", "example_columns"]).to_csv(OUT_MISSING, index=False)
        print("No target columns found; wrote pass-through file.")
        return

    missing_counter = Counter()
    missing_columns = defaultdict(set)

    for col in cols:
        df[col] = df[col].apply(lambda v: translate_cell_value(v, es_to_en, english_set,
                                                               col, missing_counter, missing_columns))

    df.to_csv(OUT_TRANSLATED, index=False)

    if missing_counter:
        rows = [{
            "value": v,
            "count": missing_counter[v],
            "example_columns": ",".join(sorted(missing_columns[v])),
        } for v in sorted(missing_counter, key=lambda k: (-missing_counter[k], k.lower()))]
        pd.DataFrame(rows).to_csv(OUT_MISSING, index=False)
    else:
        pd.DataFrame(columns=["value", "count", "example_columns"]).to_csv(OUT_MISSING, index=False)

    print(f"Done.\nTranslated → {OUT_TRANSLATED}\nMissing cells → {OUT_MISSING}")

if __name__ == "__main__":
    main()
