#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Russia Post-Processing Script - DB-Based (No CSV)

Single step after scraping:
1. Fix Start_Date_Text column (extract date only)
2. Translate using ru_input_dictionary table (VLOOKUP style)
3. For missing translations only, use AI/Google translation
4. Store translated data in database (ru_translated_products table)
"""

from __future__ import annotations

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
    from config_loader import load_env_file, get_output_dir
    load_env_file()
    USE_CONFIG = True
except ImportError:
    USE_CONFIG = False
    def get_output_dir():
        return _repo_root / "output" / "Russia"

# DB imports
from core.db.connection import CountryDB
from db.schema import apply_russia_schema
from db.repositories import RussiaRepository

# Constants
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
TRANSLATE_COLUMNS = ["tn", "inn", "manufacturer_country", "release_form"]

# Translation cache
_translator = None


def fix_date(date_text: str) -> str:
    """Extract just the date from Start_Date_Text."""
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
    text = text.strip().lower()
    text = unicodedata.normalize('NFKC', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def load_dictionary_from_db(repo: RussiaRepository) -> Tuple[Dict[str, str], Set[str]]:
    """Load translation dictionary from ru_input_dictionary table into a lookup dict."""
    dictionary = {}
    english_set = set()
    rows = repo.get_translation_dictionary_rows()
    if not rows:
        print("[WARNING] No dictionary rows in ru_input_dictionary (table empty or not applied)")
        return dictionary, english_set
    for source_term, translated_term in rows:
        ru_text = normalize_text(source_term or "")
        en_text = (translated_term or "").strip()
        if ru_text and en_text:
            dictionary[ru_text] = en_text
            english_set.add(en_text.lower())
    return dictionary, english_set


def load_all_caches(repo) -> Dict[str, str]:
    """Load all translation cache from DB (replaces JSON file cache)."""
    return repo.get_translation_cache('ru', 'en')


def save_translation_cache(repo, cache: Dict[str, str]) -> None:
    """Save translation cache to DB (replaces JSON file cache)."""
    repo.save_translation_cache(cache, 'ru', 'en')


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
        except Exception:
            if attempt == retries - 1:
                pass
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


def batch_translate_missing(missing_values: List[str], cache: Dict[str, str],
                           cache_path: Optional[Path], batch_size: int = 50, repo: RussiaRepository = None) -> int:
    """Batch translate missing values using AI and save to database."""
    if not missing_values:
        return 0

    translator = get_translator()
    if not translator:
        print("[WARNING] No translator available - keeping original Russian text")
        return 0

    print(f"[INFO] Using Google Translator for {len(missing_values)} missing entries")
    print(f"[INFO] Translations will be saved to ru_input_dictionary for future runs")
    translated_count = 0
    saved_to_db_count = 0

    for i, text in enumerate(missing_values):
        if text in cache:
            continue

        result = translate_with_ai(text)
        if result and result != text:
            cache[text] = result
            translated_count += 1

            # Save to database for future runs (avoid re-translation)
            if repo:
                try:
                    with repo.db.cursor() as cur:
                        cur.execute("""
                            INSERT INTO ru_input_dictionary (source_term, translated_term, language_from, language_to)
                            VALUES (%s, %s, 'ru', 'en')
                            ON CONFLICT (source_term, language_from, language_to) DO NOTHING
                        """, (text, result))
                    repo.db.commit()
                    saved_to_db_count += 1
                except Exception as e:
                    print(f"[WARNING] Failed to save Google translation to DB: {e}")
        else:
            cache[text] = text  # Keep original if translation fails

        # Progress update - save to DB cache periodically
        if (i + 1) % batch_size == 0:
            print(f"[PROGRESS] Translated {i + 1}/{len(missing_values)} values (Saved to DB: {saved_to_db_count})")
            if repo:
                repo.save_translation_cache(cache, 'ru', 'en')

    if saved_to_db_count > 0:
        print(f"[SUCCESS] Saved {saved_to_db_count} new translations to ru_input_dictionary (will be reused in next run)")

    return translated_count


def translate_value(value: str, dictionary: Dict[str, str], cache: Dict[str, str]) -> Tuple[str, str]:
    """
    Translate a single value using dictionary or cache.
    Returns: (translated_value, method)
    method is one of: 'dictionary', 'cache', 'ai', 'none'
    """
    if not value or not isinstance(value, str):
        return value, 'none'
    
    # If already English (no Cyrillic), no translation needed
    if not CYRILLIC_RE.search(value):
        return value, 'none'
    
    key = normalize_text(value)
    
    # Try dictionary first
    if key in dictionary:
        return dictionary[key], 'dictionary'
    
    # Try cache
    if value in cache:
        return cache[value], 'cache'
    
    # Will need AI translation - return original for now
    return value, 'ai'


def process_and_translate_data(repo: RussiaRepository, dictionary: Dict[str, str],
                               cache: Dict[str, str], cache_path: Optional[Path] = None) -> Tuple[int, int, int]:
    """
    Process data from database:
    1. Read VED and Excluded products from DB
    2. Fix dates
    3. Collect unique values needing translation
    4. Batch translate missing
    5. Store translated data in DB
    
    Returns: (ved_rows, excluded_rows, total_ai_translated)
    """
    # Resolve run_id(s) to read from (pipeline run may have 0 rows; use runs that have VED/Excluded data)
    ved_run_id = repo.run_id if repo.get_ved_product_count() > 0 else repo.get_best_ved_run_id()
    excluded_run_id = repo.run_id if repo.get_excluded_product_count() > 0 else repo.get_best_excluded_run_id()
    if ved_run_id and ved_run_id != repo.run_id:
        print(f"[INFO] Using run_id {ved_run_id} for VED (current run has no VED data)")
    if excluded_run_id and excluded_run_id != repo.run_id:
        print(f"[INFO] Using run_id {excluded_run_id} for Excluded (current run has no Excluded data)")

    # Read VED products from DB
    print("[INFO] Reading VED products from database...")
    ved_products = repo.get_ved_products_for_run(ved_run_id) if ved_run_id else []
    print(f"[INFO] Read {len(ved_products)} VED rows")
    
    # Read Excluded products from DB
    print("[INFO] Reading Excluded products from database...")
    excluded_products = repo.get_excluded_products_for_run(excluded_run_id) if excluded_run_id else []
    print(f"[INFO] Read {len(excluded_products)} Excluded rows")
    
    # Fix dates
    for product in ved_products:
        if product.get('start_date_text'):
            product['start_date_text'] = fix_date(product['start_date_text'])
    
    for product in excluded_products:
        if product.get('start_date_text'):
            product['start_date_text'] = fix_date(product['start_date_text'])
    
    # Collect unique Cyrillic values
    all_unique = set()
    all_unique.update(collect_unique_values(ved_products, TRANSLATE_COLUMNS))
    all_unique.update(collect_unique_values(excluded_products, TRANSLATE_COLUMNS))
    print(f"[INFO] Total unique Cyrillic values: {len(all_unique)}")
    
    # Find values that need AI translation
    missing_values = []
    for val in all_unique:
        key = normalize_text(val)
        if key in dictionary:
            continue
        if val in cache:
            continue
        missing_values.append(val)
    
    print(f"[INFO] Values needing AI translation: {len(missing_values)}")

    # Batch translate missing values and save to database
    ai_translated = batch_translate_missing(missing_values, cache, cache_path, repo=repo)
    
    # Process and store VED products
    print("[INFO] Processing and storing VED products...")
    ved_count = 0
    for product in ved_products:
        # Translate fields
        tn_ru = product.get('tn', '')
        inn_ru = product.get('inn', '')
        manufacturer_country_ru = product.get('manufacturer_country', '')
        release_form_ru = product.get('release_form', '')
        
        tn_en, tn_method = translate_value(tn_ru, dictionary, cache)
        inn_en, inn_method = translate_value(inn_ru, dictionary, cache)
        manufacturer_country_en, mc_method = translate_value(manufacturer_country_ru, dictionary, cache)
        release_form_en, rf_method = translate_value(release_form_ru, dictionary, cache)
        
        # Determine translation method (DB allows only dictionary/ai/none; map cache -> dictionary)
        methods = [m for m in [tn_method, inn_method, mc_method, rf_method] if m != 'none']
        raw_method = methods[0] if methods else 'none'
        translation_method = 'dictionary' if raw_method == 'cache' else raw_method

        # Parse date
        start_date_iso = None
        date_text = product.get('start_date_text', '')
        if date_text:
            try:
                # Try DD.MM.YYYY format
                match = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
                if match:
                    day, month, year = match.groups()
                    start_date_iso = f"{year}-{month}-{day}"
            except Exception:
                pass
        
        # Store translated product
        translated = {
            'item_id': product['item_id'],
            'tn_ru': tn_ru,
            'tn_en': tn_en,
            'inn_ru': inn_ru,
            'inn_en': inn_en,
            'manufacturer_country_ru': manufacturer_country_ru,
            'manufacturer_country_en': manufacturer_country_en,
            'release_form_ru': release_form_ru,
            'release_form_en': release_form_en,
            'ean': product.get('ean', ''),
            'registered_price_rub': product.get('registered_price_rub', ''),
            'start_date_text': date_text,
            'start_date_iso': start_date_iso,
            'translation_method': translation_method
        }
        
        repo.insert_translated_product(translated)
        ved_count += 1
    
    print(f"[INFO] Stored {ved_count} translated VED products")
    
    # Process and store Excluded products
    print("[INFO] Processing and storing Excluded products...")
    excluded_count = 0
    for product in excluded_products:
        # Translate fields
        tn_ru = product.get('tn', '')
        inn_ru = product.get('inn', '')
        manufacturer_country_ru = product.get('manufacturer_country', '')
        release_form_ru = product.get('release_form', '')
        
        tn_en, tn_method = translate_value(tn_ru, dictionary, cache)
        inn_en, inn_method = translate_value(inn_ru, dictionary, cache)
        manufacturer_country_en, mc_method = translate_value(manufacturer_country_ru, dictionary, cache)
        release_form_en, rf_method = translate_value(release_form_ru, dictionary, cache)
        
        # Determine translation method (DB allows only dictionary/ai/none; map cache -> dictionary)
        methods = [m for m in [tn_method, inn_method, mc_method, rf_method] if m != 'none']
        raw_method = methods[0] if methods else 'none'
        translation_method = 'dictionary' if raw_method == 'cache' else raw_method

        # Parse date
        start_date_iso = None
        date_text = product.get('start_date_text', '')
        if date_text:
            try:
                match = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
                if match:
                    day, month, year = match.groups()
                    start_date_iso = f"{year}-{month}-{day}"
            except Exception:
                pass

        # Store translated product
        translated = {
            'item_id': product['item_id'],
            'tn_ru': tn_ru,
            'tn_en': tn_en,
            'inn_ru': inn_ru,
            'inn_en': inn_en,
            'manufacturer_country_ru': manufacturer_country_ru,
            'manufacturer_country_en': manufacturer_country_en,
            'release_form_ru': release_form_ru,
            'release_form_en': release_form_en,
            'ean': product.get('ean', ''),
            'registered_price_rub': product.get('registered_price_rub', ''),
            'start_date_text': date_text,
            'start_date_iso': start_date_iso,
            'translation_method': translation_method
        }

        repo.insert_translated_product(translated)
        excluded_count += 1
    
    print(f"[INFO] Stored {excluded_count} translated Excluded products")
    
    return ved_count, excluded_count, ai_translated


def main():
    """Main entry point."""
    print()
    print("=" * 80)
    print("Russia Data Processing and Translation (DB-Based)")
    print("=" * 80)
    print()
    
    # Resolve run_id (from env or .current_run_id written by pipeline)
    run_id = os.environ.get("RUSSIA_RUN_ID", "").strip()
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    if not run_id:
        print("[ERROR] No run_id. Set RUSSIA_RUN_ID or run pipeline from step 0.")
        return 1

    # Initialize database
    try:
        db = CountryDB("Russia")
        apply_russia_schema(db)
        repo = RussiaRepository(db, run_id)
    except Exception as e:
        print(f"[ERROR] Could not initialize database: {e}")
        return 1

    cache_dir = _repo_root / "cache"
    cache_path = cache_dir / "russia_translation_cache.json"

    # Load dictionary from input table (ru_input_dictionary)
    print("[1/4] Loading dictionary and caches...")
    print(f"[PROGRESS] Step: 1/4 (25%)", flush=True)
    dictionary, english_set = load_dictionary_from_db(repo)
    
    # Load ALL translation caches from DB (replaces JSON file cache)
    cache = load_all_caches(repo)
    print(f"[INFO] Total cached translations from DB: {len(cache)}")
    
    # Process data
    print()
    print("[2/4] Processing and translating data from database...")
    print(f"[PROGRESS] Step: 2/4 (50%)", flush=True)
    
    ved_rows, excluded_rows, ai_translated = process_and_translate_data(
        repo, dictionary, cache, None  # cache_path no longer needed
    )
    
    # Save final cache to DB (replaces JSON file cache)
    print()
    print("[3/4] Saving translation cache to DB...")
    print(f"[PROGRESS] Step: 3/4 (75%)", flush=True)
    save_translation_cache(repo, cache)
    
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
    print("Data stored in database:")
    print("  - ru_translated_products table")
    
    # VALIDATION REPORT
    print()
    print("=" * 80)
    print("STEP 4 VALIDATION REPORT (Process & Translate)")
    print("=" * 80)
    
    # Get counts from DB
    ved_in_db = repo.get_ved_product_count()
    excluded_in_db = repo.get_excluded_product_count()
    translated_in_db = len(repo.get_translated_products())
    total_source = ved_in_db + excluded_in_db
    
    print(f"Source Records:")
    print(f"  VED Products (source):      {ved_in_db:,}")
    print(f"  Excluded Products (source): {excluded_in_db:,}")
    print(f"  TOTAL Source Records:       {total_source:,}")
    print()
    print(f"Translated Records:")
    print(f"  VED + Excluded (translated): {translated_in_db:,}")
    print()
    
    if total_source == translated_in_db:
        print(f"[VALIDATION PASSED] All {total_source:,} records translated and stored")
    else:
        print(f"[VALIDATION WARNING] Count mismatch:")
        print(f"  Source: {total_source:,}, Translated: {translated_in_db:,}")
        print(f"  Difference: {abs(total_source - translated_in_db):,}")
    
    print("=" * 80)
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
