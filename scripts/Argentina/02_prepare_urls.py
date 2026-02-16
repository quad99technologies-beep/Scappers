#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Argentina - URL Preparation Step (DB-only)

Keeps Product and Company exactly as scraped in Step 1 and builds URL.
Updates ar_product_index.url for every row.
"""

import logging
from pathlib import Path
from typing import List, Dict, Tuple

from config_loader import (
    get_output_dir,
    PRODUCTS_URL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("prepare_urls")

import re
import unicodedata
from pathlib import Path

# DB setup
OUTPUT_DIR = get_output_dir()
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"
from core.db.connection import CountryDB
# Ensure Argentina directory is at the front of sys.path to prioritize local 'db' package
# This fixes conflict with core/db which might be in sys.path
import sys
from pathlib import Path
sys.path = [p for p in sys.path if not Path(p).name == 'core']
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) in sys.path:
    sys.path.remove(str(_script_dir))
sys.path.insert(0, str(_script_dir))

# Force re-import of db module if it was incorrectly loaded from core/db
if 'db' in sys.modules:
    del sys.modules['db']

from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id
import os

_DB = CountryDB("Argentina")
apply_argentina_schema(_DB)

def _get_run_id() -> str:
    run_id = os.environ.get("ARGENTINA_RUN_ID")
    if run_id:
        return run_id
    if _RUN_ID_FILE.exists():
        try:
            txt = _RUN_ID_FILE.read_text(encoding="utf-8").strip()
            if txt:
                return txt
        except Exception:
            pass
    run_id = generate_run_id()
    os.environ["ARGENTINA_RUN_ID"] = run_id
    _RUN_ID_FILE.write_text(run_id, encoding="utf-8")
    return run_id

_RUN_ID = _get_run_id()
_REPO = ArgentinaRepository(_DB, _RUN_ID)

from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id
import os

def strip_accents(s: str) -> str:
    """Remove accents and normalize special characters for URL construction."""
    if not s:
        return ""
    # Pre-process special characters before NFKD normalization
    # German sharp S (ß) -> ss
    s = s.replace("ß", "ss").replace("ẞ", "SS")
    # Handle other common special characters
    s = s.replace("æ", "ae").replace("Æ", "AE")
    s = s.replace("œ", "oe").replace("Œ", "OE")
    s = s.replace("ø", "o").replace("Ø", "O")
    s = s.replace("ð", "d").replace("Ð", "D")
    s = s.replace("þ", "th").replace("Þ", "TH")
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def sanitize_product_name_for_url(product_name: str) -> str:
    if not product_name:
        return ""

    sanitized = strip_accents(product_name)

    # Handle "+" sign: " + " (with spaces) becomes separator, standalone "+" preserved as "-"
    # e.g., "NEWGEL+ LOLYPOP" -> "newgel-lolypop"
    # e.g., "CIRUELAS+FIBRAS+VITAMINA C" -> "ciruelas-fibras-vitamina-c"
    sanitized = re.sub(r"\s*\+\s*", " ", sanitized)  # Replace + with space (will become -)

    # Remove other special characters but preserve alphanumeric, spaces, and hyphens
    sanitized = re.sub(r"[^a-zA-Z0-9\s-]", "", sanitized)

    # Collapse multiple spaces to single space, then convert to hyphens
    sanitized = re.sub(r"\s+", "-", sanitized.strip())

    # Collapse multiple hyphens to double hyphen (for "word + word" -> "word--word" distinction)
    sanitized = re.sub(r"-{3,}", "--", sanitized)

    sanitized = sanitized.lower()
    sanitized = sanitized.strip("-")

    if sanitized:
        return f"{sanitized}.html"
    
    # Fallback for product names with no alphanumeric characters
    # e.g. "!!!" -> "product-hashed-<md5>.html"
    import hashlib
    fallback_hash = hashlib.md5(product_name.encode('utf-8', errors='ignore')).hexdigest()[:12]
    return f"product-hashed-{fallback_hash}.html"

def construct_product_url(product_name: str, base_url: str = None) -> str:
    if base_url is None:
        base_url = PRODUCTS_URL
    base_url = base_url.rstrip("/")
    slug = sanitize_product_name_for_url(product_name)
    if not slug:
        return ""
    return f"{base_url}/{slug}"

def main():
    # DB-only: require product index from Step 1
    products_db = _REPO.get_all_product_index()
    if not products_db:
        raise RuntimeError("No product rows available in DB. Run Step 1 first.")

    total = len(products_db)
    url_updates = []
    for idx, row in enumerate(products_db, 1):
        prod = row["product"]
        comp = row["company"]
        existing_url = row.get("url")
        
        # If we have a valid URL from scraping, preserve it
        if existing_url and existing_url.startswith("http"):
            url = existing_url
        else:
            url = construct_product_url(prod)
            
        url_updates.append({"product": prod, "company": comp, "url": url})
        if idx % 100 == 0 or idx == total:
            pct = round((idx / total) * 100, 1) if total else 0
            print(f"[PROGRESS] Preparing URLs: {idx}/{total} ({pct}%)", flush=True)

    updated = _REPO.set_urls(url_updates)

    total = _REPO.get_product_index_count()
    url_count = _REPO.get_urls_prepared_count()
    log.info(f"[DB] Updated URLs for {updated} rows in ar_product_index (run_id={_RUN_ID})")
    print(f"[DB] URLs prepared for {url_count} products", flush=True)
    if url_count != total:
        log.error(
            "[COUNT_MISMATCH] product_index=%s url_prepared=%s (run_id=%s)",
            total,
            url_count,
            _RUN_ID,
        )
        raise RuntimeError(
            f"URL count mismatch: product_index={total} url_prepared={url_count} (run_id={_RUN_ID})"
        )

    # Write metrics for pipeline runner
    try:
        metrics_file = os.environ.get("PIPELINE_METRICS_FILE")
        if metrics_file:
            import json
            metrics = {
                "rows_processed": url_count,
                "rows_updated": updated,
                "rows_read": total
            }
            with open(metrics_file, "w", encoding="utf-8") as f:
                json.dump(metrics, f)
            print(f"[METRICS] Wrote metrics to {metrics_file}: {metrics}", flush=True)
    except Exception as e:
        log.warning(f"[METRICS] Failed to write metrics: {e}")

if __name__ == "__main__":
    main()
