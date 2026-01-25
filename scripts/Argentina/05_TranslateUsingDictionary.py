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
import json
import pandas as pd
import unicodedata
import logging
from config_loader import (
    get_input_dir, get_output_dir,
    DICTIONARY_FILE, OUTPUT_PRODUCTS_CSV, OUTPUT_TRANSLATED_CSV, OUTPUT_MISSING_CSV,
    TARGET_COLUMNS, OPENAI_API_KEY, OPENAI_MODEL, OPENAI_TEMPERATURE
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("translate")

# Initialize Google translator (same approach as Russia pipeline).
_google_translator = None
try:
    from deep_translator import GoogleTranslator  # type: ignore
    _google_translator = GoogleTranslator(source="auto", target="en")
    log.info("GoogleTranslator initialized (deep_translator)")
except Exception:
    _google_translator = None
    log.warning("deep_translator not installed; Google translation disabled (pip install deep-translator)")

# Optional OpenAI client (only used as fallback)
_openai_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY.strip())
        log.info(f"OpenAI client initialized (model: {OPENAI_MODEL})")
    except ImportError:
        log.warning("openai package not installed. Install with: pip install openai")
    except Exception as e:
        log.warning(f"Failed to initialize OpenAI client: {e}")
else:
    log.info("OPENAI_API_KEY not set - OpenAI translation will be disabled")

INPUT_DIR  = get_input_dir()
OUTPUT_DIR = get_output_dir()

SOURCE_FILE = OUTPUT_DIR / OUTPUT_PRODUCTS_CSV
DICT_FILE   = INPUT_DIR / DICTIONARY_FILE

OUT_TRANSLATED = OUTPUT_DIR / OUTPUT_TRANSLATED_CSV
OUT_MISSING    = OUTPUT_DIR / OUTPUT_MISSING_CSV

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CACHE_DIR = _REPO_ROOT / "cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)
_CACHE_FILE = _CACHE_DIR / "argentina_translation_cache.json"
_TRANSLATION_CACHE = {}

# Only these columns are processed (from config)
# TARGET_COLUMNS is imported from config_loader

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

    return es_to_en, english_set, df  # Return original dataframe for updates

# ---------- OpenAI translation ----------
def translate_with_openai(text: str) -> str:
    """Translate Spanish text to English using OpenAI API."""
    if not _openai_client:
        return None
    
    try:
        response = _openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=OPENAI_TEMPERATURE,
            messages=[
                {
                    "role": "system",
                    "content": "You are a professional medical translator. Translate the following Spanish text to English. Return only the translation, no explanations. If the text is already in English, return it as-is."
                },
                {
                    "role": "user",
                    "content": f"Translate to English: {text}"
                }
            ],
            max_tokens=200
        )
        
        translation = response.choices[0].message.content.strip()
        log.info(f"[OpenAI] Translated '{text[:50]}...' → '{translation[:50]}...'")
        return translation
    except Exception as e:
        log.error(f"[OpenAI] Translation failed for '{text[:50]}...': {e}")
        return None

# ---------- Google translation ----------
def load_translation_cache(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def save_translation_cache(path: Path, cache: dict) -> None:
    try:
        # keep ASCII-only json on disk for portability
        path.write_text(json.dumps(cache, ensure_ascii=True, indent=2), encoding="utf-8")
    except Exception:
        # Best-effort cache; ignore failures.
        pass

def translate_with_google(text: str) -> str:
    """Translate Spanish text to English using Google (deep_translator)."""
    if not _google_translator:
        return None
    nkey = normalize_text(text)
    if not nkey:
        return None
    cached = _TRANSLATION_CACHE.get(nkey)
    if isinstance(cached, str) and cached.strip():
        return cached
    try:
        out = _google_translator.translate(text)
        if isinstance(out, str):
            out = out.strip()
        if out:
            _TRANSLATION_CACHE[nkey] = out
            log.info(f"[GOOGLE] Translated '{text[:50]}...' -> '{out[:50]}...'")
            return out
    except Exception as e:
        log.warning(f"[GOOGLE] Translation failed for '{text[:50]}...': {e}")
    return None

# ---------- Update dictionary ----------
def update_dictionary(dict_path: Path, df_dict: pd.DataFrame, spanish_text: str, english_text: str):
    """Add a new translation to the dictionary file."""
    try:
        # Check if entry already exists (normalized comparison)
        es_key_new = normalize_text(spanish_text)
        for idx, row in df_dict.iterrows():
            es_existing = normalize_text(str(row["es"]))
            if es_existing == es_key_new:
                # Update existing entry
                df_dict.at[idx, "en"] = english_text
                df_dict.to_csv(dict_path, index=False, encoding="utf-8-sig")
                log.info(f"[Dictionary] Updated entry: '{spanish_text}' → '{english_text}'")
                return
        
        # Add new entry - append to dataframe using loc
        new_idx = len(df_dict)
        df_dict.loc[new_idx] = [spanish_text, english_text]
        df_dict.to_csv(dict_path, index=False, encoding="utf-8-sig")
        log.info(f"[Dictionary] Added new entry: '{spanish_text}' → '{english_text}'")
    except Exception as e:
        log.error(f"[Dictionary] Failed to update dictionary: {e}")

# ---------- Cell translation ----------
def translate_cell_value(val, es_to_en, english_set, colname, miss_counter, miss_cols, df_dict, dict_path):
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

    # Translation not found - try Google first (preferred)
    if key:
        translation = translate_with_google(raw)
        if translation:
            # Update dictionary with new translation
            update_dictionary(dict_path, df_dict, raw, translation)
            # Update in-memory dictionary for subsequent use
            es_to_en[nkey] = translation
            english_set.add(normalize_text(translation))
            return translation

    # Optional fallback: OpenAI if configured
    if key and _openai_client:
        translation = translate_with_openai(raw)
        if translation:
            update_dictionary(dict_path, df_dict, raw, translation)
            es_to_en[nkey] = translation
            english_set.add(normalize_text(translation))
            return translation

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

    global _TRANSLATION_CACHE
    _TRANSLATION_CACHE = load_translation_cache(_CACHE_FILE)
    if _TRANSLATION_CACHE:
        log.info(f"[CACHE] Loaded {len(_TRANSLATION_CACHE)} cached Google translations")

    es_to_en, english_set, df_dict = load_dictionary(DICT_FILE)
    
    # Load source file
    df_source = read_csv_robust(SOURCE_FILE, dtype=str, keep_default_na=False)
    
    # Check if output file already exists and is complete (same row count)
    # If complete, we can skip processing (already done)
    if OUT_TRANSLATED.exists():
        try:
            df_existing = read_csv_robust(OUT_TRANSLATED, dtype=str, keep_default_na=False)
            if len(df_existing) == len(df_source):
                log.info(f"[RESUME] Output file already exists with {len(df_existing)} rows (complete). Skipping translation.")
                print(f"Translation already complete. Output file: {OUT_TRANSLATED}")
                return
            else:
                log.info(f"[RESUME] Output file exists but is incomplete ({len(df_existing)} vs {len(df_source)} rows). Reprocessing from scratch.")
        except Exception as e:
            log.warning(f"[RESUME] Error checking existing output file: {e}. Will process from scratch.")
    
    # Process from source file
    df = df_source.copy()

    # Only process target columns that exist
    cols = [c for c in TARGET_COLUMNS if c in df.columns]
    if not cols:
        df.to_csv(OUT_TRANSLATED, index=False)
        pd.DataFrame(columns=["value", "count", "example_columns"]).to_csv(OUT_MISSING, index=False)
        print("No target columns found; wrote pass-through file.")
        return

    log.info(f"Processing {len(df)} rows with {len(cols)} target columns...")
    log.info(f"Dictionary contains {len(es_to_en)} Spanish→English translations")

    # 1) Build unique list from target columns
    unique_values = {}
    for col in cols:
        series = df[col]
        for val in series:
            if not isinstance(val, str):
                continue
            raw = fix_mojibake(val)
            key = raw.strip()
            if not key:
                continue
            if is_numeric_like(key):
                continue
            nkey = normalize_text(key)
            if not nkey:
                continue
            entry = unique_values.setdefault(nkey, {"raw": raw, "count": 0, "columns": set()})
            entry["count"] += 1
            entry["columns"].add(col)

    log.info(f"[TRANSLATION] Unique non-numeric values to check: {len(unique_values)}")

    # 2) Lookup with dictionary / English set and collect missing entries
    missing_candidates = {}
    for nkey, meta in unique_values.items():
        if nkey in es_to_en:
            continue
        if nkey in english_set:
            continue
        missing_candidates[nkey] = meta

    log.info(f"[TRANSLATION] Missing dictionary entries: {len(missing_candidates)}")

    # Write initial missing list before translation (as requested)
    if missing_candidates:
        rows = [{
            "value": meta["raw"],
            "count": meta["count"],
            "example_columns": ",".join(sorted(meta["columns"])),
        } for meta in sorted(missing_candidates.values(), key=lambda m: (-m["count"], m["raw"].lower()))]
        pd.DataFrame(rows).to_csv(OUT_MISSING, index=False)
        log.info(f"[TRANSLATION] Initial missing list saved to: {OUT_MISSING}")

    # 3) Translate missing entries (cache first), update dictionary & cache
    new_entries = {}
    final_missing = {}
    for nkey, meta in missing_candidates.items():
        raw = meta["raw"]
        translation = translate_with_google(raw)
        if not translation and _openai_client:
            translation = translate_with_openai(raw)
        if translation:
            es_to_en[nkey] = translation
            english_set.add(normalize_text(translation))
            new_entries[raw] = translation
        else:
            final_missing[nkey] = meta

    # Persist dictionary updates once
    if new_entries:
        try:
            for raw, translation in new_entries.items():
                update_dictionary(DICT_FILE, df_dict, raw, translation)
        except Exception as e:
            log.warning(f"[Dictionary] Failed to persist new entries: {e}")

    # 4) Apply translations using updated dictionary (whole-cell match)
    total_cols = len(cols)
    print(f"[PROGRESS] Translating: Starting (0/{total_cols} columns)", flush=True)
    for col_idx, col in enumerate(cols, 1):
        def apply_translation(val):
            if not isinstance(val, str):
                return val
            raw = fix_mojibake(val)
            key = raw.strip()
            if not key:
                return raw
            if is_numeric_like(key):
                return raw
            nkey = normalize_text(key)
            if nkey in es_to_en:
                return es_to_en[nkey]
            if nkey in english_set:
                return raw
            return raw

        df[col] = df[col].apply(apply_translation)
        percent = round((col_idx / total_cols) * 100, 1) if total_cols > 0 else 0
        print(f"[PROGRESS] Translating: Column {col_idx}/{total_cols} ({percent}%) - {col}", flush=True)

    df.to_csv(OUT_TRANSLATED, index=False)
    save_translation_cache(_CACHE_FILE, _TRANSLATION_CACHE)

    if final_missing:
        rows = [{
            "value": meta["raw"],
            "count": meta["count"],
            "example_columns": ",".join(sorted(meta["columns"])),
        } for meta in sorted(final_missing.values(), key=lambda m: (-m["count"], m["raw"].lower()))]
        pd.DataFrame(rows).to_csv(OUT_MISSING, index=False)
        log.info(f"[TRANSLATION] Still missing: {len(final_missing)} unique values")
        log.info(f"[TRANSLATION] Missing translations saved to: {OUT_MISSING}")
    else:
        pd.DataFrame(columns=["value", "count", "example_columns"]).to_csv(OUT_MISSING, index=False)
        log.info("[TRANSLATION] All translations resolved.")

    print(f"Done.\nTranslated -> {OUT_TRANSLATED}\nMissing cells -> {OUT_MISSING}")

if __name__ == "__main__":
    main()
