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
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def sanitize_product_name_for_url(product_name: str) -> str:
    if not product_name:
        return ""

    sanitized = strip_accents(product_name)

    sanitized = re.sub(r"\s+\+\s+", "  ", sanitized)
    sanitized = re.sub(r"\+", "", sanitized)

    sanitized = re.sub(r"[^a-zA-Z0-9\s-]", "", sanitized)

    sanitized = re.sub(r"  ", " __DOUBLE__ ", sanitized)
    sanitized = re.sub(r"\s+", "-", sanitized)
    sanitized = re.sub(r"__DOUBLE__", "-", sanitized)

    sanitized = re.sub(r"-{3,}", "--", sanitized)

    sanitized = sanitized.lower()
    sanitized = sanitized.strip("-")

    if sanitized:
        return f"{sanitized}.html"
    return ""

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

if __name__ == "__main__":
    main()
