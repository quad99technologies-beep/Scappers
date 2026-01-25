#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Post-Processing Script (Simplified)

Single step after scraping:
1. Fix Start_Date_Text column (extract date only)
2. Translate using Dictionary.csv (VLOOKUP style)
3. For missing translations only, use AI/Google translation
4. Output 4 CSVs:
   - russia_farmcom_ved_moscow_region.csv (Russian, date fixed)
   - russia_farmcom_excluded_list.csv (Russian, date fixed)
   - en_russia_farmcom_ved_moscow_region.csv (English translated)
   - en_russia_farmcom_excluded_list.csv (English translated)
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

# Try to load config
try:
    from config_loader import load_env_file, get_input_dir, get_output_dir, get_central_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def get_input_dir():
        return _repo_root / "input" / "Russia"
    def get_output_dir():
        return _repo_root / "output" / "Russia"
    def get_central_output_dir():
        return _repo_root / "exports" / "Russia"

# Constants
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
TRANSLATE_COLUMNS = ["TN", "INN", "Manufacturer_Country", "Release_Form"]

# Translation cache
_translator = None


def fix_date(date_text: str) -> str:
    """
    Extract just the date from Start_Date_Text.
    Examples:
        '15.03.2010 (1907-Пр/10)' -> '15.03.2010'
        '07.11.2019 20-4-4115809-изм' -> '07.11.2019'
        '2026.01.12' -> '12.01.2026'
    """
    if not date_text:
        return ""

    text = str(date_text).strip()

    # Pattern 1: DD.MM.YYYY at the start
    match = re.match(r'^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{day.zfill(2)}.{month.zfill(2)}.{year}"

    # Pattern 2: YYYY.MM.DD format (convert to DD.MM.YYYY)
    match = re.match(r'^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', text)
    if match:
        year, month, day = match.groups()
        return f"{day.zfill(2)}.{month.zfill(2)}.{year}"

    # Pattern 3: Search for date anywhere in text
    match = re.search(r'(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})', text)
    if match:
        day, month, year = match.groups()
        return f"{day.zfill(2)}.{month.zfill(2)}.{year}"

    return text


def normalize_text(text: str) -> str:
    """Normalize text for dictionary lookup."""
    if not isinstance(text, str):
        return ""
    text = text.replace("\u00A0", " ")
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def load_dictionary(dict_path: Path) -> Tuple[Dict[str, str], Set[str]]:
    """Load Dictionary.csv into a lookup dict."""
    if not dict_path.exists():
        print(f"[WARNING] Dictionary not found: {dict_path}")
        return {}, set()

    mapping = {}
    english_set = set()
    corrupted_count = 0

    encodings = ["utf-8-sig", "utf-8", "cp1251"]

    for enc in encodings:
        try:
            with dict_path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)

                # Check if first row is header
                is_header = False
                if header and len(header) >= 2:
                    h0 = normalize_text(header[0])
                    h1 = normalize_text(header[1])
                    if h0 in ("russian", "ru") and h1 in ("english", "en"):
                        is_header = True

                # Process rows
                rows_to_process = [] if is_header else [header]
                rows_to_process.extend(reader)

                for row in rows_to_process:
                    if not row or len(row) < 2:
                        continue
                    ru_text = str(row[0]).strip()
                    en_text = str(row[1]).strip()

                    # Skip corrupted entries (contains ? characters where Cyrillic should be)
                    if '???' in ru_text or not ru_text or not en_text:
                        corrupted_count += 1
                        continue

                    # Only add if Russian text has Cyrillic characters
                    if CYRILLIC_RE.search(ru_text):
                        mapping[normalize_text(ru_text)] = en_text
                        english_set.add(normalize_text(en_text))

                if corrupted_count > 0:
                    print(f"[WARNING] Dictionary has {corrupted_count} corrupted entries (skipped)")

                if mapping:
                    print(f"[INFO] Loaded {len(mapping)} valid dictionary entries")
                    return mapping, english_set
                else:
                    print(f"[WARNING] Dictionary appears corrupted - no valid Cyrillic entries found")
                    return {}, set()

        except Exception as e:
            continue

    print(f"[WARNING] Failed to load dictionary: {dict_path}")
    return {}, set()


def load_all_caches(cache_dir: Path) -> Dict[str, str]:
    """Load all available translation caches."""
    combined_cache = {}

    # List of cache files to try (in order of preference)
    cache_files = [
        cache_dir / "russia_translation_cache.json",
        cache_dir / "russia_ai_translation_cache.json",
        cache_dir / "russia_translation_cache_en.json",
    ]

    for cache_path in cache_files:
        if cache_path.exists():
            try:
                with cache_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        # Add entries that aren't already in combined cache
                        for k, v in data.items():
                            if k not in combined_cache and v:
                                combined_cache[k] = v
                        print(f"[INFO] Loaded {len(data)} entries from {cache_path.name}")
            except Exception as e:
                print(f"[WARNING] Failed to load {cache_path.name}: {e}")

    return combined_cache


def save_translation_cache(cache_path: Path, cache: Dict[str, str]) -> None:
    """Save AI translations to cache."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with cache_path.open("w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARNING] Failed to save cache: {e}")


def get_translator():
    """Get Google translator instance."""
    global _translator
    if _translator is not None:
        return _translator

    try:
        from deep_translator import GoogleTranslator
        _translator = GoogleTranslator(source="ru", target="en")
        return _translator
    except ImportError:
        print("[WARNING] deep_translator not installed. Missing entries won't be translated.")
        return None


def translate_with_ai(text: str, retries: int = 3) -> Optional[str]:
    """Translate using Google Translate with retries."""
    translator = get_translator()
    if not translator:
        return None

    for attempt in range(retries):
        try:
            result = translator.translate(text)
            if result and result.strip():
                return result.strip()
        except Exception as e:
            if attempt == retries - 1:
                pass  # Silent fail, will use original
        time.sleep(0.2 + attempt * 0.2)

    return None


def collect_unique_values(rows: List[dict], columns: List[str]) -> Set[str]:
    """Collect all unique Cyrillic values from specified columns."""
    unique = set()
    for row in rows:
        for col in columns:
            if col in row and row[col]:
                val = row[col].strip()
                if CYRILLIC_RE.search(val):
                    unique.add(val)
    return unique


def get_translate_columns(fieldnames: List[str]) -> List[str]:
    """Return translate columns present in the file and warn on missing."""
    available = [col for col in TRANSLATE_COLUMNS if col in fieldnames]
    missing = [col for col in TRANSLATE_COLUMNS if col not in fieldnames]
    if missing:
        print(f"[WARNING] Missing translate columns in file: {', '.join(missing)}")
    return available


def batch_translate_missing(missing_values: List[str], cache: Dict[str, str],
                           cache_path: Path, batch_size: int = 50) -> int:
    """Batch translate missing values using AI."""
    if not missing_values:
        return 0

    translator = get_translator()
    if not translator:
        print("[WARNING] No translator available - keeping original Russian text")
        return 0

    print(f"[INFO] Using Google Translator for {len(missing_values)} missing entries")
    translated_count = 0

    for i, text in enumerate(missing_values):
        if text in cache:
            continue

        result = translate_with_ai(text)
        if result and result != text:
            cache[text] = result
            translated_count += 1
        else:
            cache[text] = text  # Keep original if translation fails

        # Progress update
        if (i + 1) % batch_size == 0:
            print(f"[PROGRESS] Translated {i + 1}/{len(missing_values)} values")
            save_translation_cache(cache_path, cache)

    return translated_count


def process_files(ved_input: Path, excluded_input: Path,
                 output_dir: Path, central_dir: Path,
                 dictionary: Dict[str, str], english_set: Set[str],
                 cache: Dict[str, str], cache_path: Path) -> Tuple[int, int, int]:
    """
    Process both files:
    1. Fix dates
    2. Collect all unique values needing translation
    3. Batch translate missing
    4. Write outputs

    Returns: (ved_rows, excluded_rows, total_ai_translated)
    """
    # Read VED file
    ved_rows = []
    ved_translate_columns = []
    if ved_input.exists():
        with ved_input.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            ved_fieldnames = reader.fieldnames or []
            ved_translate_columns = get_translate_columns(ved_fieldnames)
            for row in reader:
                if "Start_Date_Text" in row:
                    row["Start_Date_Text"] = fix_date(row.get("Start_Date_Text", ""))
                ved_rows.append(row)
        print(f"[INFO] Read {len(ved_rows)} VED rows")
        print(f"[INFO] Translate columns (VED): {', '.join(ved_translate_columns)}")

    # Read Excluded file
    excluded_rows = []
    excluded_translate_columns = []
    if excluded_input.exists():
        with excluded_input.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            excluded_fieldnames = reader.fieldnames or []
            excluded_translate_columns = get_translate_columns(excluded_fieldnames)
            for row in reader:
                if "Start_Date_Text" in row:
                    row["Start_Date_Text"] = fix_date(row.get("Start_Date_Text", ""))
                excluded_rows.append(row)
        print(f"[INFO] Read {len(excluded_rows)} Excluded rows")
        print(f"[INFO] Translate columns (Excluded): {', '.join(excluded_translate_columns)}")

    # Collect unique Cyrillic values only from translate columns
    all_unique = set()
    all_unique.update(collect_unique_values(ved_rows, ved_translate_columns))
    all_unique.update(collect_unique_values(excluded_rows, excluded_translate_columns))
    print(f"[INFO] Total unique Cyrillic values: {len(all_unique)}")

    # Find values that need translation (not in dictionary or cache)
    missing_values = []
    for val in all_unique:
        key = normalize_text(val)
        if key in dictionary:
            continue  # Has dictionary translation
        if key in english_set:
            continue  # Already English
        if val in cache:
            continue  # Has cached translation
        missing_values.append(val)

    print(f"[INFO] Values already in dictionary: {len(all_unique) - len(missing_values) - sum(1 for v in all_unique if v in cache)}")
    print(f"[INFO] Values already in cache: {sum(1 for v in all_unique if v in cache)}")
    print(f"[INFO] Values needing AI translation: {len(missing_values)}")

    # Save missing values list before translation
    if missing_values:
        missing_csv = output_dir / "russia_missing_ai_translations.csv"
        with missing_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["value"])
            for val in sorted(missing_values):
                writer.writerow([val])
        print(f"[INFO] Missing translations saved to: {missing_csv.name}")

    # Batch translate missing values
    ai_translated = 0
    if missing_values:
        ai_translated = batch_translate_missing(missing_values, cache, cache_path)
        print(f"[INFO] AI translated {ai_translated} new values")
        save_translation_cache(cache_path, cache)

    # Helper function to translate a value
    def translate_value(val: str) -> str:
        if not val:
            return ""
        if not CYRILLIC_RE.search(val):
            return val

        key = normalize_text(val)

        # 1. Check dictionary
        if key in dictionary:
            return dictionary[key]

        # 2. Check cache
        if val in cache:
            return cache[val]

        # 3. Return original
        return val

    # Write outputs
    output_dir.mkdir(parents=True, exist_ok=True)
    central_dir.mkdir(parents=True, exist_ok=True)

    # VED outputs
    if ved_rows:
        ved_ru_output = output_dir / "russia_farmcom_ved_moscow_region.csv"
        ved_en_output = output_dir / "en_russia_farmcom_ved_moscow_region.csv"

        # Russian (date fixed)
        with ved_ru_output.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ved_fieldnames)
            writer.writeheader()
            writer.writerows(ved_rows)

        # English (translated)
        with ved_en_output.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=ved_fieldnames)
            writer.writeheader()
            for row in ved_rows:
                en_row = row.copy()
                for col in ved_translate_columns:
                    if col in en_row and en_row[col]:
                        en_row[col] = translate_value(en_row[col].strip())
                writer.writerow(en_row)

        print(f"  Written: {ved_ru_output.name} ({len(ved_rows)} rows)")
        print(f"  Written: {ved_en_output.name} ({len(ved_rows)} rows)")

        # Central exports
        (central_dir / "Russia_VED_Moscow_Region.csv").write_bytes(ved_ru_output.read_bytes())
        (central_dir / "EN_Russia_VED_Moscow_Region.csv").write_bytes(ved_en_output.read_bytes())
        print(f"  Exported: Russia_VED_Moscow_Region.csv")
        print(f"  Exported: EN_Russia_VED_Moscow_Region.csv")

    # Excluded outputs
    if excluded_rows:
        excluded_ru_output = output_dir / "russia_farmcom_excluded_list.csv"
        excluded_en_output = output_dir / "en_russia_farmcom_excluded_list.csv"

        # Russian (date fixed)
        with excluded_ru_output.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=excluded_fieldnames)
            writer.writeheader()
            writer.writerows(excluded_rows)

        # English (translated)
        with excluded_en_output.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=excluded_fieldnames)
            writer.writeheader()
            for row in excluded_rows:
                en_row = row.copy()
                for col in excluded_translate_columns:
                    if col in en_row and en_row[col]:
                        en_row[col] = translate_value(en_row[col].strip())
                writer.writerow(en_row)

        print(f"  Written: {excluded_ru_output.name} ({len(excluded_rows)} rows)")
        print(f"  Written: {excluded_en_output.name} ({len(excluded_rows)} rows)")

        # Central exports
        (central_dir / "Russia_Excluded_List.csv").write_bytes(excluded_ru_output.read_bytes())
        (central_dir / "EN_Russia_Excluded_List.csv").write_bytes(excluded_en_output.read_bytes())
        print(f"  Exported: Russia_Excluded_List.csv")
        print(f"  Exported: EN_Russia_Excluded_List.csv")

    return len(ved_rows), len(excluded_rows), ai_translated


def main():
    print()
    print("=" * 80)
    print("RUSSIA POST-PROCESSING (Simplified)")
    print("=" * 80)
    print()

    # Paths
    output_dir = get_output_dir()
    central_dir = get_central_output_dir()
    input_dir = get_input_dir()
    cache_dir = _repo_root / "cache"

    # Input files (from scraper)
    ved_input = output_dir / "russia_farmcom_ved_moscow_region.csv"
    excluded_input = output_dir / "russia_farmcom_excluded_list.csv"

    # Cache path
    cache_path = cache_dir / "russia_translation_cache.json"

    # Dictionary
    dict_path = input_dir / "Dictionary.csv"

    # Load dictionary
    print("[1/4] Loading dictionary and caches...")
    print(f"[PROGRESS] Step: 1/4 (25%)", flush=True)
    dictionary, english_set = load_dictionary(dict_path)

    # Load ALL translation caches
    cache = load_all_caches(cache_dir)
    print(f"[INFO] Total cached translations: {len(cache)}")

    # Process files
    print()
    print("[2/4] Processing files...")
    print(f"[PROGRESS] Step: 2/4 (50%)", flush=True)

    ved_rows, excluded_rows, ai_translated = process_files(
        ved_input, excluded_input,
        output_dir, central_dir,
        dictionary, english_set,
        cache, cache_path
    )

    # Save final cache
    print()
    print("[3/4] Saving translation cache...")
    print(f"[PROGRESS] Step: 3/4 (75%)", flush=True)
    save_translation_cache(cache_path, cache)

    # Summary
    print()
    print("[4/4] Complete!")
    print(f"[PROGRESS] Step: 4/4 (100%)", flush=True)

    print()
    print("=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print(f"  VED Moscow Region: {ved_rows} rows")
    print(f"  Excluded List: {excluded_rows} rows")
    print(f"  Dictionary entries: {len(dictionary)}")
    print(f"  Cached translations: {len(cache)}")
    print(f"  New AI translations: {ai_translated}")
    print()
    print("Output files (output/Russia/):")
    print("  - russia_farmcom_ved_moscow_region.csv (Russian)")
    print("  - en_russia_farmcom_ved_moscow_region.csv (English)")
    print("  - russia_farmcom_excluded_list.csv (Russian)")
    print("  - en_russia_farmcom_excluded_list.csv (English)")
    print()
    print("Central exports (exports/Russia/):")
    print("  - Russia_VED_Moscow_Region.csv")
    print("  - EN_Russia_VED_Moscow_Region.csv")
    print("  - Russia_Excluded_List.csv")
    print("  - EN_Russia_Excluded_List.csv")
    print("=" * 80)
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
