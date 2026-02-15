#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Belarus Post-Processing Script - Translation with Dictionary + AI Fallback

Single step after scraping:
1. Collect unique Russian words from by_rceth_data
2. Generate missing words report (words not in dictionary)
3. Translate using by_input_dictionary table (VLOOKUP style)
4. For missing translations only, use AI/Google translation
5. Cache translations to DB (by_translated_data table) and file cache
6. Generate final report
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
        return _repo_root / "output" / "Belarus"

# DB imports
from core.db.connection import CountryDB
from db.schema import apply_belarus_schema
from db.repositories import BelarusRepository

# Constants
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
TRANSLATE_COLUMNS = ["inn", "trade_name", "dosage_form", "manufacturer", "manufacturer_country", "pharmacotherapeutic_group"]

# Translation cache
_translator = None


def load_translation_cache_from_db(repo) -> Dict[str, str]:
    """Load translation cache from database."""
    try:
        cache = repo.get_translation_cache(source_lang='ru', target_lang='en')
        print(f"[INFO] Loaded {len(cache)} translations from DB cache")
        return cache
    except Exception as e:
        print(f"[WARNING] Could not load translation cache from DB: {e}")
        return {}


def normalize_text(text: str) -> str:
    """Normalize text for dictionary lookup."""
    if not text:
        return ""
    # Lowercase and normalize unicode
    normalized = unicodedata.normalize("NFKD", text.lower().strip())
    # Remove extra whitespace
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def load_dictionary_from_db(repo: BelarusRepository) -> Tuple[Dict[str, str], Set[str]]:
    """Load translation dictionary from by_input_dictionary table into a lookup dict."""
    dictionary = {}
    english_set = set()
    rows = repo.get_translation_dictionary_rows()
    if not rows:
        print("[WARNING] No dictionary rows in by_input_dictionary (table empty or not applied)")
        return dictionary, english_set
    for source_term, translated_term in rows:
        ru_text = normalize_text(source_term or "")
        en_text = (translated_term or "").strip()
        if ru_text and en_text:
            dictionary[ru_text] = en_text
            english_set.add(en_text.lower())
    print(f"[INFO] Loaded {len(dictionary)} dictionary entries")
    return dictionary, english_set


def load_all_caches(repo) -> Dict[str, str]:
    """Load translation cache from database (replaces JSON file cache)."""
    return load_translation_cache_from_db(repo)


def save_translation_cache(repo, cache: Dict[str, str]) -> None:
    """Save translation cache to database (replaces JSON file cache)."""
    try:
        repo.save_translation_cache(cache, source_lang='ru', target_lang='en')
    except Exception as e:
        print(f"[WARNING] Failed to save cache to DB: {e}")


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
                print(f"  [TRANSLATE] AI translation failed after {retries} attempts for '{text[:60]}': {e}")
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


def generate_missing_words_report(missing_values: Set[str], output_dir: Path) -> Path:
    """Generate CSV report of missing words that need dictionary entries."""
    if not missing_values:
        return None
    
    report_path = output_dir / "belarus_missing_words_report.csv"
    try:
        with open(report_path, "w", encoding="utf-8", newline="") as f:
            f.write("source_term,translated_term,language_from,language_to\n")
            for word in sorted(missing_values):
                f.write(f'"{word}",,ru,en\n')
        print(f"[INFO] Generated missing words report: {report_path}")
        print(f"[INFO] Total missing words: {len(missing_values)}")
        return report_path
    except Exception as e:
        print(f"[WARNING] Failed to generate missing words report: {e}")
        return None


def batch_translate_missing(missing_values: List[str], cache: Dict[str, str],
                           batch_size: int = 50, repo: BelarusRepository = None) -> int:
    """Batch translate missing values using AI and save to database."""
    if not missing_values:
        return 0

    translator = get_translator()
    if not translator:
        print("[WARNING] No translator available - keeping original Russian text")
        return 0

    print(f"[INFO] Using Google Translator for {len(missing_values)} missing entries")
    print(f"[INFO] Translations will be saved to by_input_dictionary for future runs")
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
                            INSERT INTO by_input_dictionary (source_term, translated_term, language_from, language_to, category)
                            VALUES (%s, %s, 'ru', 'en', 'google_auto')
                            ON CONFLICT (source_term, language_from, language_to) DO NOTHING
                        """, (text, result))
                    repo.db.commit()
                    saved_to_db_count += 1
                except Exception as e:
                    print(f"[WARNING] Failed to save Google translation to DB: {e}")
        else:
            cache[text] = text  # Keep original if translation fails

        # Progress update
        if (i + 1) % batch_size == 0:
            print(f"[PROGRESS] Translated {i + 1}/{len(missing_values)} values (Saved to DB: {saved_to_db_count})")
            save_translation_cache(repo, cache)

    if saved_to_db_count > 0:
        print(f"[SUCCESS] Saved {saved_to_db_count} new translations to by_input_dictionary (will be reused in next run)")

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


def process_and_translate_data(repo: BelarusRepository, dictionary: Dict[str, str],
                               cache: Dict[str, str]) -> Tuple[int, int]:
    """
    Process RCETH data and translate Russian fields to English.
    
    Steps:
    1. Load all RCETH data
    2. Collect unique Russian values
    3. Find values that need AI translation
    4. Batch translate missing values
    5. Store translated data in DB
    
    Returns: (translated_count, ai_translated_count)
    """
    print("[INFO] Loading RCETH data...")
    rceth_products = repo.get_all_rceth_data()
    if not rceth_products:
        print("[WARNING] No RCETH data found for this run_id")
        return 0, 0
    
    print(f"[INFO] Found {len(rceth_products)} RCETH records")
    
    # Collect all unique Russian values
    print("[INFO] Collecting unique Russian values...")
    all_unique = set()
    all_unique.update(collect_unique_values(rceth_products, TRANSLATE_COLUMNS))
    
    print(f"[INFO] Found {len(all_unique)} unique Russian values")
    
    # Find values that need AI translation
    missing_values = []
    for val in all_unique:
        key = normalize_text(val)
        if key not in dictionary and val not in cache:
            missing_values.append(val)
    
    print(f"[INFO] Values needing AI translation: {len(missing_values)}")
    
    # Batch translate missing values and save to database
    ai_translated = batch_translate_missing(missing_values, cache, repo=repo)
    
    # Process and translate each record
    translated_count = 0
    for product in rceth_products:
        rceth_id = product.get("id")
        if not rceth_id:
            continue
        
        # Translate fields
        inn_ru = product.get("inn") or ""
        trade_name_ru = product.get("trade_name") or ""
        dosage_form_ru = product.get("dosage_form") or ""
        manufacturer_ru = product.get("manufacturer") or ""
        manufacturer_country_ru = product.get("manufacturer_country") or ""
        pharmacotherapeutic_group_ru = product.get("pharmacotherapeutic_group") or ""
        
        inn_en, inn_method = translate_value(inn_ru, dictionary, cache)
        trade_name_en, tn_method = translate_value(trade_name_ru, dictionary, cache)
        dosage_form_en, df_method = translate_value(dosage_form_ru, dictionary, cache)
        manufacturer_en, m_method = translate_value(manufacturer_ru, dictionary, cache)
        manufacturer_country_en, mc_method = translate_value(manufacturer_country_ru, dictionary, cache)
        pharmacotherapeutic_group_en, pg_method = translate_value(pharmacotherapeutic_group_ru, dictionary, cache)
        
        # Determine overall translation method (use most specific method)
        methods = [inn_method, tn_method, df_method, m_method, mc_method, pg_method]
        if 'dictionary' in methods:
            translation_method = 'dictionary'
        elif 'cache' in methods:
            translation_method = 'cache'
        elif 'ai' in methods:
            translation_method = 'ai'
        else:
            translation_method = 'none'
        
        # Store translated data
        translated = {
            "rceth_data_id": rceth_id,
            "inn_ru": inn_ru,
            "trade_name_ru": trade_name_ru,
            "dosage_form_ru": dosage_form_ru,
            "manufacturer_ru": manufacturer_ru,
            "manufacturer_country_ru": manufacturer_country_ru,
            "pharmacotherapeutic_group_ru": pharmacotherapeutic_group_ru,
            "inn_en": inn_en,
            "trade_name_en": trade_name_en,
            "dosage_form_en": dosage_form_en,
            "manufacturer_en": manufacturer_en,
            "manufacturer_country_en": manufacturer_country_en,
            "pharmacotherapeutic_group_en": pharmacotherapeutic_group_en,
            "translation_method": translation_method,
        }
        
        repo.insert_translated_data(translated)
        translated_count += 1
    
    print(f"[INFO] Stored {translated_count} translated records")

    # -- Verification: per-field translation coverage --
    method_counts = {"dictionary": 0, "cache": 0, "ai": 0, "none": 0}
    fields_still_cyrillic = {col: 0 for col in TRANSLATE_COLUMNS}
    for product in rceth_products:
        for col in TRANSLATE_COLUMNS:
            val = product.get(col) or ""
            if CYRILLIC_RE.search(val):
                key = normalize_text(val)
                if key in dictionary:
                    method_counts["dictionary"] += 1
                elif val in cache:
                    method_counts["cache"] += 1
                else:
                    method_counts["ai"] += 1
                    fields_still_cyrillic[col] += 1
            else:
                method_counts["none"] += 1

    print(f"[VERIFY] Translation method breakdown: dictionary={method_counts['dictionary']}, "
          f"cache={method_counts['cache']}, ai={method_counts['ai']}, "
          f"already_english={method_counts['none']}")
    for col, count in fields_still_cyrillic.items():
        if count > 0:
            print(f"[VERIFY] Field '{col}': {count} values still need AI translation")

    # -- Verification: DB count matches --
    try:
        db_translated = repo.get_translated_data_count()
        print(f"[VERIFY] DB verification: {db_translated} rows in by_translated_data (expected {translated_count})")
        if db_translated != translated_count:
            print(f"[VERIFY] NOTE: DB count differs (upsert from prior runs is normal)")
    except Exception as e:
        print(f"[VERIFY] Could not verify DB count: {e}")

    return translated_count, ai_translated


def main():
    """Main entry point."""
    print()
    print("=" * 80)
    print("Belarus Data Processing and Translation (DB-Based)")
    print("=" * 80)
    print()
    
    # Resolve run_id (from env or .current_run_id written by pipeline)
    run_id = os.environ.get("BELARUS_RUN_ID", "").strip()
    if not run_id:
        run_id_file = get_output_dir() / ".current_run_id"
        if run_id_file.exists():
            try:
                run_id = run_id_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
    if not run_id:
        print("[ERROR] No run_id. Set BELARUS_RUN_ID or run pipeline from step 0.")
        return 1

    # Initialize database
    try:
        db = CountryDB("Belarus")
        apply_belarus_schema(db)
        repo = BelarusRepository(db, run_id)
    except Exception as e:
        print(f"[ERROR] Could not initialize database: {e}")
        return 1

    # Load dictionary from input table (by_input_dictionary)
    print("[1/5] Loading dictionary and caches...")
    print(f"[PROGRESS] Step: 1/5 (20%)", flush=True)
    dictionary, english_set = load_dictionary_from_db(repo)
    
    # Load translation cache from DB (replaces JSON file cache)
    cache = load_all_caches(repo)
    print(f"[INFO] Total cached translations: {len(cache)}")
    
    # Process data
    print()
    print("[2/5] Processing and translating data from database...")
    print(f"[PROGRESS] Step: 2/5 (40%)", flush=True)
    
    translated_count, ai_translated = process_and_translate_data(
        repo, dictionary, cache
    )
    
    # Generate missing words report
    print()
    print("[3/5] Generating missing words report...")
    print(f"[PROGRESS] Step: 3/5 (60%)", flush=True)
    
    # Collect missing words (not in dictionary or cache)
    rceth_products = repo.get_all_rceth_data()
    all_unique = collect_unique_values(rceth_products, TRANSLATE_COLUMNS)
    missing_words = set()
    for val in all_unique:
        key = normalize_text(val)
        if key not in dictionary and val not in cache:
            missing_words.add(val)
    
    missing_report = generate_missing_words_report(missing_words, get_output_dir())
    
    # Save final cache
    print()
    print("[4/5] Saving translation cache...")
    print(f"[PROGRESS] Step: 4/5 (80%)", flush=True)
    save_translation_cache(repo, cache)
    
    # Summary
    print()
    print("[5/5] Complete!")
    print(f"[PROGRESS] Step: 5/5 (100%)", flush=True)
    
    print()
    print("=" * 80)
    print("PROCESSING COMPLETE!")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  Dictionary entries: {len(dictionary)}")
    print(f"  Cached translations: {len(cache)}")
    print(f"  New AI translations: {ai_translated}")
    print(f"  Total translated records: {translated_count}")
    print(f"  Missing words (need dictionary): {len(missing_words)}")
    print()
    print("Output:")
    print("  - by_translated_data table")
    if missing_report:
        print(f"  - Missing words report: {missing_report}")
    print()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
