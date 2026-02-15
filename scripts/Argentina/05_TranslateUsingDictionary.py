#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - DB translation step (no CSV input/output).

Reads:
  - ar_products (current run_id)
  - ar_dictionary (ES -> EN)

Writes:
  - ar_products_translated (one row per ar_products.id)

Notes:
  - Whole-cell translation only (case-insensitive, NFC-normalized).
  - Numeric-like values are left unchanged.
  - Optional external translation (Google/OpenAI) is disabled by default unless configured.
"""

from __future__ import annotations

import logging
import os
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from config_loader import (
    get_output_dir,
    TARGET_COLUMNS,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    OPENAI_TEMPERATURE,
)
from core.db.connection import CountryDB
from core.db.models import generate_run_id
from db.schema import apply_argentina_schema
from db.repositories import ArgentinaRepository


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("translate")


# Optional translators
_google_translator = None
try:
    from deep_translator import GoogleTranslator  # type: ignore
    _google_translator = GoogleTranslator(source="auto", target="en")
    log.info("GoogleTranslator initialized (deep_translator)")
except Exception:
    _google_translator = None
    log.info("deep_translator not available; Google translation disabled")

_openai_client = None
if OPENAI_API_KEY:
    try:
        from openai import OpenAI
        _openai_client = OpenAI(api_key=OPENAI_API_KEY.strip())
        log.info("OpenAI client initialized (model=%s)", OPENAI_MODEL)
    except Exception as exc:
        log.warning("OpenAI client init failed: %s", exc)


OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"


def _get_run_id() -> str:
    rid = os.environ.get("ARGENTINA_RUN_ID")
    if rid:
        return rid
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    rid = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = rid
    _RUN_ID_FILE.write_text(rid, encoding="utf-8")
    return rid


_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)
_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)


# Translation cache now stored in DB (ar_translation_cache table)
# Cache file removed - using DB cache via repository
_TRANSLATION_CACHE: Dict[str, str] = {}


def fix_mojibake(s: str) -> str:
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s


def normalize_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.replace("\u00A0", " ")
    s = unicodedata.normalize("NFC", s)
    return s.strip().lower()


CURRENCY_CHARS = "$€£ARS "


def is_numeric_like(s: str) -> bool:
    if not s:
        return False
    t = s.strip()
    t = t.strip(CURRENCY_CHARS)
    if t.endswith("%"):
        t = t[:-1]
    t = t.replace(",", "").replace(" ", "")
    try:
        float(t)
        return True
    except ValueError:
        return False


def load_translation_cache_from_db() -> Dict[str, str]:
    """Load translation cache from DB (replaces JSON file cache)."""
    return _REPO.get_translation_cache('es', 'en')


def save_translation_cache_to_db(cache: Dict[str, str]) -> None:
    """Save translation cache to DB (replaces JSON file cache)."""
    _REPO.save_translation_cache(cache, 'es', 'en')


def translate_with_google(text: str) -> str | None:
    if not _google_translator:
        return None
    nkey = normalize_text(text)
    if not nkey:
        return None
    # Check in-memory cache first
    cached = _TRANSLATION_CACHE.get(nkey)
    if isinstance(cached, str) and cached.strip():
        return cached
    # Check DB cache
    db_cached = _REPO.get_cached_translation(nkey, 'es', 'en')
    if isinstance(db_cached, str) and db_cached.strip():
        _TRANSLATION_CACHE[nkey] = db_cached  # Update in-memory cache
        return db_cached
    try:
        out = _google_translator.translate(text)
        if isinstance(out, str):
            out = out.strip()
        if out:
            _TRANSLATION_CACHE[nkey] = out
            _REPO.save_single_translation(nkey, out, 'es', 'en')  # Save to DB cache
            return out
    except Exception as exc:
        log.warning("[GOOGLE] Translation failed: %s", exc)
    return None


def translate_with_openai(text: str) -> str | None:
    if not _openai_client:
        return None
    nkey = normalize_text(text)
    # Check in-memory cache first
    cached = _TRANSLATION_CACHE.get(nkey)
    if isinstance(cached, str) and cached.strip():
        return cached
    # Check DB cache
    db_cached = _REPO.get_cached_translation(nkey, 'es', 'en')
    if isinstance(db_cached, str) and db_cached.strip():
        _TRANSLATION_CACHE[nkey] = db_cached  # Update in-memory cache
        return db_cached
    try:
        response = _openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=OPENAI_TEMPERATURE,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional medical translator. Translate the "
                        "following Spanish text to English. Return only the translation."
                    ),
                },
                {"role": "user", "content": f"Translate to English: {text}"},
            ],
            max_tokens=200,
        )
        result = response.choices[0].message.content.strip()
        if result:
            _TRANSLATION_CACHE[nkey] = result
            _REPO.save_single_translation(nkey, result, 'es', 'en')  # Save to DB cache
        return result
    except Exception as exc:
        log.warning("[OpenAI] Translation failed: %s", exc)
        return None


def load_dictionary_from_db() -> Tuple[Dict[str, str], set]:
    es_to_en: Dict[str, str] = {}
    english_set = set()
    with _DB.cursor() as cur:
        cur.execute("SELECT es, en FROM ar_dictionary")
        rows = cur.fetchall()
    for row in rows:
        if isinstance(row, dict):
            es_raw = row.get("es", "")
            en_raw = row.get("en", "")
        else:
            es_raw, en_raw = row[0], row[1]
        es_raw = fix_mojibake(str(es_raw or ""))
        en_raw = fix_mojibake(str(en_raw or ""))
        es_key = normalize_text(es_raw)
        en_key = normalize_text(en_raw)
        if es_key:
            es_to_en[es_key] = en_raw.strip()
        if en_key:
            english_set.add(en_key)
    return es_to_en, english_set


def translate_cell(
    value: str,
    es_to_en: Dict[str, str],
    english_set: set,
    missing_counter: Dict[str, int],
    new_entries: Dict[str, str],
) -> Tuple[str, str]:
    """Return (translated_value, source_tag)."""
    if not isinstance(value, str):
        return value, "none"
    raw = fix_mojibake(value)
    key = raw.strip()
    if not key:
        return raw, "none"
    if is_numeric_like(key):
        return raw, "none"
    nkey = normalize_text(key)
    if nkey in es_to_en:
        return es_to_en[nkey], "dictionary"
    if nkey in english_set:
        return raw, "none"

    # Optional external translation
    translation = translate_with_google(raw)
    source = "google"
    if not translation and _openai_client:
        translation = translate_with_openai(raw)
        source = "openai"
    if translation:
        es_to_en[nkey] = translation
        english_set.add(normalize_text(translation))
        new_entries[raw] = translation
        return translation, source

    missing_counter[raw] = missing_counter.get(raw, 0) + 1
    return raw, "missing"


def main() -> None:
    log.info("Loading dictionary from DB...")
    es_to_en, english_set = load_dictionary_from_db()
    log.info("Dictionary entries: %d", len(es_to_en))

    # Load products for current run_id
    with _DB.cursor(dict_cursor=True) as cur:
        cur.execute("SELECT * FROM ar_products WHERE run_id = %s ORDER BY id", (_RUN_ID,))
        products = cur.fetchall()

    if not products:
        log.warning("No products found in ar_products for run_id=%s", _RUN_ID)
        return

    # Normalize target columns (DB uses lower-case names)
    targets = [c.strip() for c in TARGET_COLUMNS if c and c.strip()]
    target_cols = [t.lower() for t in targets]

    missing_counter: Dict[str, int] = {}
    new_entries: Dict[str, str] = {}
    translated_rows: List[Dict] = []

    total = len(products)
    log.info("Translating %d rows (%d target columns)...", total, len(target_cols))

    for idx, row in enumerate(products, 1):
        translated = dict(row)
        translation_sources = set()

        for col in target_cols:
            if col not in translated:
                continue
            val = translated.get(col)
            translated_val, source = translate_cell(
                val, es_to_en, english_set, missing_counter, new_entries
            )
            translated[col] = translated_val
            if source not in ("none",):
                translation_sources.add(source)

        translated_rows.append(
            {
                "product_id": row.get("id"),
                "company": translated.get("company"),
                "product_name": translated.get("product_name"),
                "active_ingredient": translated.get("active_ingredient"),
                "therapeutic_class": translated.get("therapeutic_class"),
                "description": translated.get("description"),
                "price_ars": translated.get("price_ars"),
                "date": translated.get("date"),
                "sifar_detail": translated.get("sifar_detail"),
                "pami_af": translated.get("pami_af"),
                "pami_os": translated.get("pami_os"),
                "ioma_detail": translated.get("ioma_detail"),
                "ioma_af": translated.get("ioma_af"),
                "ioma_os": translated.get("ioma_os"),
                "import_status": translated.get("import_status"),
                "coverage_json": translated.get("coverage_json"),
                "translation_source": ",".join(sorted(translation_sources)) if translation_sources else "none",
            }
        )

        if idx % 2000 == 0 or idx == total:
            pct = round(idx / total * 100, 1)
            print(f"[PROGRESS] Translating: {idx}/{total} ({pct}%)", flush=True)

    # Persist translations (replace current run)
    _REPO.clear_translated()
    inserted = _REPO.insert_translated(translated_rows)
    log.info("Inserted %d translated rows into ar_products_translated", inserted)
    if inserted != total:
        raise RuntimeError(
            f"Translation count mismatch: products={total} translated={inserted} (run_id={_RUN_ID})"
        )

    # Upsert new dictionary entries if any
    if new_entries:
        entries = [{"es": k, "en": v, "source": "auto"} for k, v in new_entries.items()]
        _REPO.upsert_dictionary_entries(entries)
        log.info("Dictionary updated with %d new entries", len(entries))

    # Save translation cache to DB (replaces JSON file cache)
    save_translation_cache_to_db(_TRANSLATION_CACHE)

    if missing_counter:
        top_missing = sorted(missing_counter.items(), key=lambda x: (-x[1], x[0].lower()))[:10]
        log.info("Missing translations: %d unique (top 10 shown)", len(missing_counter))
        for val, cnt in top_missing:
            log.info("  - %s (x%d)", val, cnt)
    else:
        log.info("All translations resolved via dictionary/auto-translation")


if __name__ == "__main__":
    main()
