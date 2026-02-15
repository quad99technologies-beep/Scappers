#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scrape only the products from pcid_no_data.csv file.

These are products in the PCID mapping that weren't found in the scraped data.
This script:
1. Reads the no_data CSV
2. Inserts products into ar_product_index with 'pending' status
3. Prepares URLs for them
4. You can then run 04_alfabeta_api_scraper.py to scrape just these

Usage:
    python scrape_no_data_products.py <path_to_no_data.csv>
    python scrape_no_data_products.py  # Uses latest from exports folder
"""

import sys
import os
import logging
import pandas as pd
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config_loader import get_output_dir, PRODUCTS_URL
from core.db.connection import CountryDB
from db.repositories import ArgentinaRepository
from db.schema import apply_argentina_schema
from core.db.models import generate_run_id
from core.data.pcid_mapping import PCIDMapping

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("scrape_no_data")

OUTPUT_DIR = get_output_dir()
_RUN_ID_FILE = OUTPUT_DIR / ".current_run_id"

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

# Import URL construction from 02_prepare_urls
import re
import unicodedata


def strip_accents(s: str) -> str:
    if not s:
        return ""
    s = s.replace("ß", "ss").replace("ẞ", "SS")
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
    sanitized = re.sub(r"\s*\+\s*", " ", sanitized)
    sanitized = re.sub(r"[^a-zA-Z0-9\s-]", "", sanitized)
    sanitized = re.sub(r"\s+", "-", sanitized.strip())
    sanitized = re.sub(r"-{3,}", "--", sanitized)
    sanitized = sanitized.lower().strip("-")
    if sanitized:
        return f"{sanitized}.html"
    return ""


def construct_product_url(product_name: str) -> str:
    base_url = PRODUCTS_URL.rstrip("/")
    slug = sanitize_product_name_for_url(product_name)
    if not slug:
        return ""
    return f"{base_url}/{slug}"


def find_latest_no_data_csv() -> Path:
    """Find the latest no_data CSV from exports folder."""
    exports_dir = OUTPUT_DIR / "exports"
    if not exports_dir.exists():
        # Try backups
        backups_dir = Path(__file__).parent.parent.parent / "backups" / "Argentina"
        if backups_dir.exists():
            # Find latest output folder
            output_dirs = sorted(backups_dir.glob("output_*"), reverse=True)
            for od in output_dirs:
                exports = od / "exports"
                if exports.exists():
                    no_data_files = list(exports.glob("*_pcid_no_data.csv"))
                    if no_data_files:
                        return no_data_files[0]
    else:
        no_data_files = list(exports_dir.glob("*_pcid_no_data.csv"))
        if no_data_files:
            return sorted(no_data_files, reverse=True)[0]
    return None


def load_no_data_products(csv_path: Path) -> list:
    """Load products from no_data CSV."""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    products = []
    for _, row in df.iterrows():
        company = str(row.get("Company", "")).strip()
        product = str(row.get("Local Product Name", "")).strip()

        if company and product:
            products.append({
                "company": company,
                "product": product,
            })

    return products


def insert_targeted_products(products: list) -> int:
    """Insert products into ar_product_index with pending status and URLs."""
    if not products:
        return 0

    rows = []
    for p in products:
        url = construct_product_url(p["product"])
        rows.append({
            "product": p["product"],
            "company": p["company"],
            "url": url,
            "status": "pending",
            "total_records": 0,
            "loop_count": 0,
        })

    # Use upsert to add/update products
    with _DB.cursor() as cur:
        inserted = 0
        for r in rows:
            try:
                cur.execute("""
                    INSERT INTO ar_product_index (run_id, product, company, url, status, total_records, loop_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, company, product)
                    DO UPDATE SET url = EXCLUDED.url, status = 'pending', total_records = 0
                """, (_RUN_ID, r["product"], r["company"], r["url"], r["status"], r["total_records"], r["loop_count"]))
                inserted += 1
            except Exception as e:
                log.error(f"Failed to insert {r['company']}/{r['product']}: {e}")
        _DB.commit()
        return inserted


def main():
    # Find CSV file
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    else:
        csv_path = find_latest_no_data_csv()

    if not csv_path or not csv_path.exists():
        print("[ERROR] No pcid_no_data.csv file found!")
        print("Usage: python scrape_no_data_products.py <path_to_no_data.csv>")
        sys.exit(1)

    print(f"[INPUT] Loading from: {csv_path}")
    products = load_no_data_products(csv_path)
    print(f"[LOADED] {len(products)} products from no_data file")

    if not products:
        print("[WARN] No products to scrape")
        sys.exit(0)

    # Insert into product index
    inserted = insert_targeted_products(products)
    print(f"[DB] Inserted/updated {inserted} products into ar_product_index (run_id={_RUN_ID})")

    # Show some examples
    print("\n[SAMPLE] First 10 products queued:")
    for p in products[:10]:
        url = construct_product_url(p["product"])
        print(f"  - {p['company']} / {p['product']}")
        print(f"    URL: {url}")

    # Run API scraper automatically
    print(f"\n[SCRAPING] Starting API scraper for {len(products)} products...")
    print("=" * 60)

    import subprocess
    scraper_path = Path(__file__).parent / "04_alfabeta_api_scraper.py"

    try:
        result = subprocess.run(
            [sys.executable, str(scraper_path)],
            cwd=str(Path(__file__).parent),
            env={**os.environ, "ARGENTINA_RUN_ID": _RUN_ID},
        )
        if result.returncode == 0:
            print("\n[OK] API scraper completed successfully")
            print("[NEXT] Run Step 05 (Translate) and Step 06 (Generate Output) to update CSVs")
        else:
            print(f"\n[WARN] API scraper exited with code {result.returncode}")
    except Exception as e:
        print(f"\n[ERROR] Failed to run API scraper: {e}")
        print(f"[MANUAL] Run: python 04_alfabeta_api_scraper.py")


if __name__ == "__main__":
    main()
