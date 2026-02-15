#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
North Macedonia - Dictionary-based Translation Step

Reads:
  - nm_drug_register (current run_id)
  - nm_input_dictionary (MK -> EN) or nm_dictionary

Writes:
  - Updates nm_drug_register with improved translations from dictionary

Flow:
  1. Load dictionary from nm_input_dictionary
  2. For each product in nm_drug_register
  3. Translate Macedonian/Cyrillic fields using dictionary (whole-word match)
  4. Update the record with improved translations
  5. Optionally use Google Translate as fallback for unknown terms

Notes:
  - Case-insensitive matching
  - Preserves numeric values and codes
  - Dictionary translations take priority over existing translations
"""

import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Optional, Tuple

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = Path(__file__).resolve().parents[2]

if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from config_loader import load_env_file, get_output_dir
from core.db.connection import CountryDB
from db.repositories import NorthMacedoniaRepository

load_env_file()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("translate")

OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

# Optional Google Translator
_google_translator = None
try:
    from deep_translator import GoogleTranslator
    # Initialize with timeout to avoid hanging
    _google_translator = GoogleTranslator(source="auto", target="en")
    log.info("GoogleTranslator initialized")
except Exception as e:
    log.info(f"deep_translator not available; Google translation disabled: {e}")

# Cache for Google Translate results - now uses unified core.translation cache
# This persists across restarts unlike the old in-memory dict
_translation_cache = None

def _get_translation_cache():
    """Lazy initialization of unified translation cache."""
    global _translation_cache
    if _translation_cache is None:
        from core.translation import get_cache
        _translation_cache = get_cache("north_macedonia")
    return _translation_cache

# Configuration
SKIP_GOOGLE_TRANSLATE_DELAY = os.environ.get("SKIP_GOOGLE_TRANSLATE_DELAY", "").lower() in ("1", "true", "yes")
GOOGLE_TRANSLATE_TIMEOUT = int(os.environ.get("GOOGLE_TRANSLATE_TIMEOUT", "10"))  # seconds


def _get_run_id() -> str:
    """Get or generate run_id from environment or file."""
    rid = os.environ.get("NORTH_MACEDONIA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    from core.db.models import generate_run_id
    rid = generate_run_id()
    os.environ["NORTH_MACEDONIA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid


def normalize_text(s: str) -> str:
    """Normalize text for comparison (lowercase, NFC, trimmed)."""
    if not isinstance(s, str):
        return ""
    s = s.replace("\u00A0", " ")  # Non-breaking space
    s = unicodedata.normalize("NFC", s)
    return s.strip().lower()


_cyrillic_re = re.compile(r"[\u0400-\u04FF]")


def has_cyrillic(text: str) -> bool:
    """Check if text contains Cyrillic characters."""
    return bool(_cyrillic_re.search(text or ""))


def is_numeric_like(s: str) -> bool:
    """Check if string is numeric (price, code, etc)."""
    if not s:
        return False
    t = s.strip().replace(",", "").replace(" ", "")
    if t.endswith("%"):
        t = t[:-1]
    try:
        float(t)
        return True
    except ValueError:
        return False


def load_dictionary_from_db(db: CountryDB) -> Tuple[Dict[str, str], set]:
    """
    Load Macedonian->English dictionary from nm_input_dictionary or nm_dictionary.

    Returns:
        (mk_to_en dict, english_terms set)
    """
    mk_to_en: Dict[str, str] = {}
    english_set = set()

    # Try nm_input_dictionary first (from CSV import)
    table_name = "nm_input_dictionary"
    with db.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table_name,))
        if not cur.fetchone()[0]:
            # Fallback to nm_dictionary if nm_input_dictionary doesn't exist
            table_name = "nm_dictionary"
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = %s
                )
            """, (table_name,))
            if not cur.fetchone()[0]:
                log.warning("No dictionary table found (nm_input_dictionary or nm_dictionary)")
                return mk_to_en, english_set

        cur.execute(f'SELECT source_term, translated_term FROM {table_name}')
        rows = cur.fetchall()

        for row in rows:
            mk_term = row[0] if isinstance(row, tuple) else row.get("source_term", "")
            en_term = row[1] if isinstance(row, tuple) else row.get("translated_term", "")

            if not mk_term or not en_term:
                continue

            mk_norm = normalize_text(mk_term)
            en_norm = normalize_text(en_term)

            if mk_norm and en_norm:
                mk_to_en[mk_norm] = en_term  # Preserve original casing of EN
                english_set.add(en_norm)

    log.info(f"Loaded {len(mk_to_en)} dictionary entries from {table_name}")
    return mk_to_en, english_set


def translate_with_timeout(translator, text: str, timeout: int = GOOGLE_TRANSLATE_TIMEOUT) -> Optional[str]:
    """
    Translate text with a timeout to avoid hanging.
    
    Uses threading to enforce timeout since deep_translator doesn't support it natively.
    """
    import threading
    result = [None]
    exception = [None]
    
    def _translate():
        try:
            result[0] = translator.translate(text)
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=_translate)
    thread.daemon = True
    thread.start()
    thread.join(timeout)
    
    if thread.is_alive():
        log.warning(f"Google Translate timeout for '{text[:50]}...' after {timeout}s")
        return None
    
    if exception[0]:
        raise exception[0]
    
    return result[0]


def batch_translate_missing_terms(
    missing_terms: set,
    mk_to_en: Dict[str, str],
    use_google: bool = False,
    stats: dict = None
) -> Dict[str, str]:
    """
    Pre-translate all missing terms in batch using Google Translate.
    
    This is much more efficient than translating one by one during product processing.
    
    Args:
        missing_terms: Set of terms that need translation
        mk_to_en: Existing dictionary (will be updated with new translations)
        use_google: Whether to use Google Translate
        stats: Statistics dictionary
        
    Returns:
        Dictionary of new translations {normalized_term: translated_text}
    """
    new_translations = {}
    
    if not use_google or not _google_translator or not missing_terms:
        return new_translations
    
    total = len(missing_terms)
    log.info(f"Batch translating {total} missing terms with Google Translate...")
    
    for i, term in enumerate(sorted(missing_terms), 1):
        norm_term = normalize_text(term)
        
        # Skip if already in cache or dictionary
        cache = _get_translation_cache()
        cached = cache.get(term, "mk", "en")  # Use original term for cache lookup
        if cached:
            translated = cached
            mk_to_en[norm_term] = translated
            new_translations[norm_term] = translated
            if stats is not None:
                stats['google_fallback'] = stats.get('google_fallback', 0) + 1
            continue
        
        try:
            translated = translate_with_timeout(_google_translator, term)
            if translated and translated.strip():
                translated = translated.strip()
                
                # Cache the result (persist to DB)
                cache.set(term, translated, "mk", "en")
                
                # Add to dictionaries
                mk_to_en[norm_term] = translated
                new_translations[norm_term] = translated
                
                if stats is not None:
                    stats['google_fallback'] = stats.get('google_fallback', 0) + 1
                
                # Progress logging every 10 terms
                if i % 10 == 0 or i == total:
                    log.info(f"  Google Translate progress: {i}/{total} ({i*100/total:.1f}%)")
            else:
                log.debug(f"Google Translate returned empty for '{term}'")
        except Exception as e:
            log.debug(f"Google translate failed for '{term}': {e}")
    
    log.info(f"Batch translation complete: {len(new_translations)} new translations")
    return new_translations


def translate_text(
    text: str,
    mk_to_en: Dict[str, str],
    english_set: set,
    use_google: bool = False,
    stats: dict = None,
    db: CountryDB = None,
    new_dict_entries: list = None
) -> str:
    """
    Translate text using dictionary first, then Google Translate as fallback.

    Args:
        text: Text to translate
        mk_to_en: Macedonian->English dictionary
        english_set: Set of known English terms (skip translation)
        use_google: Enable Google Translate fallback
        stats: Dictionary to track translation statistics
        db: Database connection (not used directly, entries added to batch)
        new_dict_entries: List to collect new translations for batch insert

    Returns:
        Translated text or original if not translatable
    """
    if not text or not isinstance(text, str):
        return text

    text = text.strip()

    # Skip if already English (no Cyrillic)
    if not has_cyrillic(text):
        return text

    # Skip numeric values
    if is_numeric_like(text):
        return text

    # Try dictionary first (whole-cell match)
    norm_text = normalize_text(text)
    if norm_text in mk_to_en:
        if stats is not None:
            stats['dict_hits'] = stats.get('dict_hits', 0) + 1
        return mk_to_en[norm_text]

    # Try Google Translate ONLY for terms missing from dictionary
    # Note: In optimized mode, terms should already be pre-translated in batch
    if use_google and _google_translator:
        # Check cache first
        cache = _get_translation_cache()
        cached = cache.get(text, "mk", "en")
        if cached:
            translated = cached
            mk_to_en[norm_text] = translated
            if new_dict_entries is not None:
                new_dict_entries.append((text, translated))
            if stats is not None:
                stats['google_fallback'] = stats.get('google_fallback', 0) + 1
            return translated
        
        # Fallback to individual translation (should be rare after batch pre-translation)
        try:
            translated = translate_with_timeout(_google_translator, text)
            if translated and translated.strip():
                translated = translated.strip()

                # Add to cache (persist to DB) and in-memory dictionary for this run
                cache.set(text, translated, "mk", "en")
                mk_to_en[norm_text] = translated

                # Add to batch for DB insert later (avoids individual commits)
                if new_dict_entries is not None:
                    new_dict_entries.append((text, translated))

                if stats is not None:
                    stats['google_fallback'] = stats.get('google_fallback', 0) + 1
                log.debug(f"[GOOGLE FALLBACK] '{text}' -> '{translated}' (queued for batch save)")
                return translated
        except Exception as e:
            log.debug(f"Google translate failed for '{text}': {e}")

    # Return original if no translation found
    if stats is not None:
        stats['no_translation'] = stats.get('no_translation', 0) + 1
    return text


def translate_product_fields(
    product: dict,
    mk_to_en: Dict[str, str],
    english_set: set,
    use_google: bool = False,
    stats: dict = None,
    db: CountryDB = None,
    new_dict_entries: list = None
) -> dict:
    """
    Translate relevant fields in a product record.

    Returns:
        Dictionary with updated field values
    """
    updates = {}

    # Fields that may contain Macedonian text (only specific columns requested by user)
    translatable_fields = [
        "generic_name",           # Generic Name
        "formulation",            # Formulation
        "fill_size",              # Fill Size
        "customized_1",           # Customized 1
        "local_pack_description", # Local Pack Description
    ]

    for field in translatable_fields:
        original = product.get(field, "")
        if original and has_cyrillic(original):
            translated = translate_text(original, mk_to_en, english_set, use_google, stats, db, new_dict_entries)
            if translated != original:
                updates[field] = translated

    return updates


def main():
    """Main translation workflow."""
    log.info("="*60)
    log.info("North Macedonia Dictionary Translation")
    log.info("="*60)

    # Initialize database
    run_id = _get_run_id()
    db = CountryDB("NorthMacedonia")
    repo = NorthMacedoniaRepository(db, run_id)

    log.info(f"Run ID: {run_id}")

    # Load translation cache from DB
    global _google_translate_cache
    _google_translate_cache = repo.get_translation_cache(source_lang='mk', target_lang='en')
    log.info(f"Loaded {len(_google_translate_cache)} cached translations from DB")

    # Load dictionary
    log.info("Loading dictionary...")
    mk_to_en, english_set = load_dictionary_from_db(db)

    if not mk_to_en:
        log.warning("No dictionary entries found. Translation skipped.")
        log.info("To use dictionary translation:")
        log.info("1. Create a CSV with columns: Macedonian, English (or source_term, translated_term)")
        log.info("2. Import via GUI: Input tab -> Select CSV -> Import to 'Dictionary (MK→EN)'")
        return

    # Get products to translate
    log.info("Fetching products from nm_drug_register...")
    with db.cursor() as cur:
        cur.execute("""
            SELECT id, local_product_name, generic_name, formulation,
                   strength_size, fill_size, customized_1,
                   marketing_authority_company_name, local_pack_description
            FROM nm_drug_register
            WHERE run_id = %s
        """, (run_id,))

        products = []
        for row in cur.fetchall():
            products.append({
                "id": row[0],
                "local_product_name": row[1],
                "generic_name": row[2],
                "formulation": row[3],
                "strength_size": row[4],
                "fill_size": row[5],
                "customized_1": row[6],
                "marketing_authority_company_name": row[7],
                "local_pack_description": row[8],
            })

    total = len(products)
    log.info(f"Found {total} products to process")

    if total == 0:
        log.info("No products found. Run Step 2 (scrape details) first.")
        return

    # Collect unique terms that need translation (not in dictionary)
    log.info("Analyzing which terms need translation...")
    missing_terms = set()

    for product in products:
        for field in ["generic_name", "formulation", "fill_size", "customized_1", "local_pack_description"]:
            value = product.get(field, "")
            if value and has_cyrillic(value):
                norm_value = normalize_text(value)
                if norm_value not in mk_to_en:
                    missing_terms.add(value)  # Keep original text

    # Export missing terms report
    if missing_terms:
        missing_report_path = OUTPUT_DIR / "manual_translation_needed.csv"
        with open(missing_report_path, "w", encoding="utf-8", newline="") as f:
            f.write("source_term,translated_term,language_from,language_to,category\n")
            for term in sorted(missing_terms):
                f.write(f'"{term}",,mk,en,manual\n')

        log.info("="*60)
        log.info(f"⚠️  MISSING TERMS REPORT GENERATED")
        log.info(f"   File: {missing_report_path}")
        log.info(f"   Terms needing translation: {len(missing_terms)}")
        log.info(f"")
        log.info(f"   To AVOID SLOW Google Translate:")
        log.info(f"   1. Open the CSV file")
        log.info(f"   2. Fill in the 'translated_term' column manually")
        log.info(f"   3. Import via GUI (Input tab → Dictionary)")
        log.info(f"   4. Re-run this step (translations will be instant!)")
        log.info("="*60)

        # Ask user if they want to continue with Google Translate
        import time
        log.info("")
        
        if SKIP_GOOGLE_TRANSLATE_DELAY:
            log.info(f"SKIP_GOOGLE_TRANSLATE_DELAY is set - proceeding immediately with Google Translate")
        else:
            log.info(f"Proceeding with Google Translate in 10 seconds...")
            log.info(f"(Press Ctrl+C to cancel and add manual translations first)")
            log.info(f"(Set SKIP_GOOGLE_TRANSLATE_DELAY=1 to skip this delay)")
            time.sleep(10)
    else:
        log.info(f"✓ All terms found in dictionary! No Google Translate needed.")

    # Enable Google fallback if available
    use_google = _google_translator is not None

    # Translate and update
    updated_count = 0
    batch_size = 100

    # Translation statistics
    translation_stats = {
        'dict_hits': 0,        # Terms found in dictionary
        'google_fallback': 0,  # Terms translated by Google (dictionary miss)
        'no_translation': 0,   # Terms not translated (Cyrillic remained)
        'saved_to_dict': 0     # Google translations saved to dictionary
    }

    # Batch buffer for new dictionary entries (avoid individual commits)
    new_dict_entries = []

    # Pre-translate all missing terms in batch (much faster than one-by-one)
    if missing_terms and use_google:
        log.info(f"Pre-translating {len(missing_terms)} missing terms in batch...")
        batch_translate_missing_terms(missing_terms, mk_to_en, use_google, translation_stats)
        log.info(f"Pre-translation complete. Dictionary now has {len(mk_to_en)} entries.")

    log.info(f"Translating with dictionary (Google fallback ONLY for missing terms: {use_google})...")
    if use_google:
        log.info(f"Google translations will be saved to dictionary for future runs")

    for i, product in enumerate(products, 1):
        updates = translate_product_fields(product, mk_to_en, english_set, use_google, translation_stats, db, new_dict_entries)

        if updates:
            # Update database
            set_clauses = ", ".join([f"{k} = %s" for k in updates.keys()])
            values = list(updates.values()) + [product["id"], run_id]

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with db.cursor() as cur:
                        cur.execute(f"""
                            UPDATE nm_drug_register
                            SET {set_clauses}
                            WHERE id = %s AND run_id = %s
                        """, values)
                    # Commit handled by context manager
                    break
                except Exception as e:
                    import psycopg2
                    if isinstance(e, psycopg2.OperationalError) or isinstance(e, psycopg2.InterfaceError):
                        log.warning(f"DB Connection lost on update (attempt {attempt+1}/{max_retries}): {e}")
                        db.close() # Force fresh connection next time
                        if attempt == max_retries - 1:
                            raise
                    else:
                        log.error(f"Update failed: {e}")
                        raise

            updated_count += 1

        # Periodic Connection Refresh (every 500 items)
        if i % 500 == 0:
            db.close()

        # Batch save new dictionary entries every 50 products
        if len(new_dict_entries) >= 50:
            # Retry logic for dict entries too
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with db.cursor() as cur:
                        for source, translated in new_dict_entries:
                            cur.execute("""
                                INSERT INTO nm_input_dictionary (source_term, translated_term, language_from, language_to, category)
                                VALUES (%s, %s, 'mk', 'en', 'google_auto')
                                ON CONFLICT (source_term, language_from, language_to) DO NOTHING
                            """, (source, translated))
                    break
                except Exception as e:
                    import psycopg2
                    if isinstance(e, psycopg2.OperationalError) or isinstance(e, psycopg2.InterfaceError):
                        log.warning(f"DB Connection lost on dict save (attempt {attempt+1}/{max_retries}): {e}")
                        db.close()
                        if attempt == max_retries - 1:
                             raise
                    else:
                        raise

            translation_stats['saved_to_dict'] += len(new_dict_entries)
            new_dict_entries.clear()

        # Progress
        if i % batch_size == 0 or i == total:
            log.info(f"Progress: {i}/{total} ({i*100/total:.1f}%) - Updated: {updated_count} | Dict: {translation_stats['dict_hits']} | Google: {translation_stats['google_fallback']} | Saved: {translation_stats['saved_to_dict']} | Untranslated: {translation_stats['no_translation']}")

    # Save remaining dictionary entries
    if new_dict_entries:
        try:
            with db.cursor() as cur:
                for source, translated in new_dict_entries:
                    cur.execute("""
                        INSERT INTO nm_input_dictionary (source_term, translated_term, language_from, language_to, category)
                        VALUES (%s, %s, 'mk', 'en', 'google_auto')
                        ON CONFLICT (source_term, language_from, language_to) DO NOTHING
                    """, (source, translated))
        except Exception as e:
            log.error(f"Failed to save remaining dict entries: {e}")

        translation_stats['saved_to_dict'] += len(new_dict_entries)
        new_dict_entries.clear()

    # Save translation cache to DB for future runs
    if _google_translate_cache:
        repo.save_translation_cache(_google_translate_cache, source_lang='mk', target_lang='en')
        log.info(f"Saved {len(_google_translate_cache)} translations to DB cache")

    log.info("="*60)
    log.info(f"Translation complete!")
    log.info(f"  Total products: {total}")
    log.info(f"  Products updated: {updated_count}")
    log.info(f"  Dictionary entries (initial): {len(mk_to_en)}")
    log.info(f"  Dictionary hits: {translation_stats['dict_hits']}")
    log.info(f"  Google fallback: {translation_stats['google_fallback']} (only for missing terms)")
    log.info(f"  Saved to dictionary: {translation_stats['saved_to_dict']} (will be reused in next run)")
    log.info(f"  Untranslated: {translation_stats['no_translation']}")
    log.info("="*60)


if __name__ == "__main__":
    main()
