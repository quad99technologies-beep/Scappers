#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Netherlands FK - Translation Step (Step 4)

Translates Dutch indication text to English using:
1. DB dictionary (nl_fk_dictionary) - fastest, free
2. Google Translate fallback - auto-saves to dictionary for future runs
3. Batch saves every 50 entries per universal translation pattern
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Path wiring
SCRIPT_DIR = Path(__file__).resolve().parent
_repo_root = SCRIPT_DIR.parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from core.utils.logger import get_logger
from core.db.postgres_connection import get_db
from core.pipeline.standalone_checkpoint import run_with_checkpoint

for _m in list(sys.modules.keys()):
    if _m == "db" or _m.startswith("db."):
        del sys.modules[_m]

from config_loader import getenv_bool, get_output_dir
from db.schema import apply_netherlands_schema
from db.repositories import NetherlandsRepository

log = get_logger(__name__, "Netherlands")

SCRIPT_ID = "Netherlands"
STEP_NUMBER = 4
STEP_NAME = "FK Translation"

# ---------------------------------------------------------------
# Seed dictionary: hardcoded Dutch→English translations
# Ported from new source.py lines 576-661
# ---------------------------------------------------------------

SEED_DICTIONARY = {
    # Diseases and conditions
    "schizofrenie": "schizophrenia",
    "bipolaire stoornis": "bipolar disorder",
    "depressie": "depression",
    "angststoornis": "anxiety disorder",
    "paniekstoornis": "panic disorder",
    "psychose": "psychosis",
    "epilepsie": "epilepsy",
    "ziekte van parkinson": "Parkinson's disease",
    "alzheimer": "Alzheimer's disease",
    "dementie": "dementia",
    "diabetes mellitus": "diabetes mellitus",
    "hypertensie": "hypertension",
    "hartfalen": "heart failure",
    "myocardinfarct": "myocardial infarction",
    "angina pectoris": "angina pectoris",
    "atriumfibrilleren": "atrial fibrillation",
    "trombo-embolie": "thromboembolism",
    "beroerte": "stroke",
    "migraine": "migraine",
    "hoofdpijn": "headache",
    "pijn": "pain",
    "koorts": "fever",
    "infectie": "infection",
    "ontsteking": "inflammation",
    "pneumonie": "pneumonia",
    "bronchitis": "bronchitis",
    "astma": "asthma",
    "copd": "COPD",
    "longembolie": "pulmonary embolism",
    "tuberculose": "tuberculosis",
    "hepatitis": "hepatitis",
    "cirrose": "cirrhosis",
    "nierfalen": "kidney failure",
    "urineweginfectie": "urinary tract infection",
    "prostaatcarcinoom": "prostate carcinoma",
    "borstkanker": "breast cancer",
    "longkanker": "lung cancer",
    "darmkanker": "colorectal cancer",
    "melanoom": "melanoma",
    "leukemie": "leukemia",
    "lymfoom": "lymphoma",
    "multipel myeloom": "multiple myeloma",
    "kanker": "cancer",
    "reumatoide artritis": "rheumatoid arthritis",
    "artrose": "osteoarthritis",
    "multiple sclerose": "multiple sclerosis",
    # Populations
    "volwassenen": "adults",
    "volwassene": "adult",
    "kinderen": "children",
    "kind": "child",
    "adolescenten": "adolescents",
    "jongeren": "young people",
    "ouderen": "elderly",
    "neonaten": "neonates",
    "zuigelingen": "infants",
    # Treatment terms
    "behandeling": "treatment",
    "therapie": "therapy",
    "profylaxe": "prophylaxis",
    "preventie": "prevention",
    "onderhoudsbehandeling": "maintenance treatment",
    "onderhoud": "maintenance",
    "kortdurende": "short-term",
    "langdurige": "long-term",
    "chronische": "chronic",
    "acuut": "acute",
    "ernstige": "severe",
    "matige": "moderate",
    "lichte": "mild",
    # General terms
    "bij": "in",
    "met": "with",
    "en": "and",
    "of": "or",
    "voor": "for",
    "als": "as",
    "die": "who",
    "waarbij": "where",
    "waarvan": "of which",
}


# ---------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------

_google_translator = None


def _init_google_translator():
    """Lazy-init Google Translator."""
    global _google_translator
    if _google_translator is not None:
        return _google_translator
    try:
        from deep_translator import GoogleTranslator
        _google_translator = GoogleTranslator(source="nl", target="en")
        log.info("Google Translate initialized (nl -> en)")
        return _google_translator
    except ImportError:
        log.warning("deep_translator not installed — Google Translate unavailable")
        return None
    except Exception as e:
        log.warning(f"Failed to init Google Translate: {e}")
        return None


def _has_dutch_chars(text: str) -> bool:
    """Heuristic: check if text likely contains Dutch words (not fully English)."""
    dutch_markers = [
        "ij", "oe", "ui", "aa", "ee", "oo", "uu",
        "therapie", "behandeling", "ziekte", "stoornis",
        "bij ", " en ", " of ", " voor ", " met ",
    ]
    lower = text.lower()
    return any(m in lower for m in dutch_markers)


def translate_indication(
    text: str,
    dictionary: Dict[str, str],
    use_google: bool = False,
    new_entries: List[Dict] = None,
    stats: Dict[str, int] = None,
) -> str:
    """
    Translate a single indication text.
    1. Dictionary whole-word replacement
    2. If still has Dutch markers, Google Translate the whole text
    3. Save new Google translations for batch insert
    """
    if not text or not text.strip():
        return ""

    # Split on ; separator, translate each bullet, re-join
    bullets = [b.strip() for b in text.split(" ; ") if b.strip()]
    translated_bullets = []

    for bullet in bullets:
        result = bullet

        # Dictionary pass: replace known terms
        for dutch_norm, english in dictionary.items():
            pattern = r"\b" + re.escape(dutch_norm) + r"\b"
            result = re.sub(pattern, english, result, flags=re.IGNORECASE)

        # Check if Google needed
        if use_google and _has_dutch_chars(result):
            translator = _init_google_translator()
            if translator:
                try:
                    google_result = translator.translate(bullet)
                    if google_result and google_result.strip():
                        result = google_result.strip()
                        if stats is not None:
                            stats["google_fallback"] = stats.get("google_fallback", 0) + 1
                        if new_entries is not None:
                            new_entries.append({
                                "source_term": bullet.lower().strip(),
                                "translated_term": result,
                                "category": "google_auto",
                            })
                except Exception as e:
                    log.debug(f"Google translate failed for '{bullet[:50]}...': {e}")

        translated_bullets.append(result)

    if stats is not None:
        stats["dict_translated"] = stats.get("dict_translated", 0) + 1

    return " ; ".join(translated_bullets)


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def _get_run_id() -> str:
    run_id = os.environ.get("NL_RUN_ID", "").strip()
    if run_id:
        return run_id
    run_id_file = get_output_dir() / ".current_run_id"
    if run_id_file.exists():
        try:
            return run_id_file.read_text(encoding="utf-8").strip()
        except Exception:
            pass
    return ""


def main() -> None:
    run_id = _get_run_id()
    if not run_id:
        log.error("No run_id found.")
        raise SystemExit(1)

    log.info(f"Step {STEP_NUMBER}: {STEP_NAME} | run_id={run_id}")

    db = get_db("Netherlands")
    apply_netherlands_schema(db)
    repo = NetherlandsRepository(db, run_id)

    use_google = getenv_bool("FK_ENABLE_GOOGLE_TRANSLATE", True)

    # 1. Seed dictionary if empty
    existing_dict = repo.load_fk_dictionary()
    if not existing_dict:
        entries = [
            {"source_term": k, "translated_term": v, "category": "hardcoded_seed"}
            for k, v in SEED_DICTIONARY.items()
        ]
        count = repo.seed_fk_dictionary(entries)
        log.info(f"Seeded {count} hardcoded terms into nl_fk_dictionary")
        existing_dict = repo.load_fk_dictionary()

    log.info(f"Dictionary loaded: {len(existing_dict)} entries")

    # 2. Get untranslated rows
    rows = repo.get_untranslated_fk_rows(limit=100000)
    if not rows:
        log.info("No untranslated rows found — translation complete")
        return

    log.info(f"Translating {len(rows)} rows (Google={use_google})")

    # 3. Process rows
    stats: Dict[str, int] = {"dict_translated": 0, "google_fallback": 0, "no_dutch": 0}
    new_dict_entries: List[Dict] = []
    updates: List[Dict] = []
    total = len(rows)

    for i, row in enumerate(rows):
        indication_nl = row.get("indication_nl", "")

        if not indication_nl or not indication_nl.strip():
            updates.append({"id": row["id"], "indication_en": "", "status": "no_dutch"})
            stats["no_dutch"] += 1
        else:
            translated = translate_indication(
                indication_nl,
                existing_dict,
                use_google=use_google,
                new_entries=new_dict_entries,
                stats=stats,
            )
            updates.append({"id": row["id"], "indication_en": translated, "status": "translated"})

        # Batch save new dict entries every 50 (MEMORY.md pattern)
        if len(new_dict_entries) >= 50:
            repo.upsert_fk_dictionary_batch(new_dict_entries)
            for e in new_dict_entries:
                existing_dict[e["source_term"].lower()] = e["translated_term"]
            new_dict_entries.clear()

        # Batch update translations every 200
        if len(updates) >= 200:
            repo.update_fk_translations_batch(updates)
            updates.clear()

        if (i + 1) % 500 == 0:
            log.info(f"Progress: {i + 1}/{total} ({(i + 1) * 100 / total:.1f}%) | stats={stats}")

    # Flush remaining
    if new_dict_entries:
        repo.upsert_fk_dictionary_batch(new_dict_entries)
    if updates:
        repo.update_fk_translations_batch(updates)

    log.info(f"Translation complete: {stats}")
    log.info(f"Dictionary now has {len(repo.load_fk_dictionary())} entries")


if __name__ == "__main__":
    run_with_checkpoint(main, SCRIPT_ID, STEP_NUMBER, STEP_NAME)
