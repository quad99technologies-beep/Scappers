#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Argentina. URL Preparation Step (Simple State File)

Keeps Product and Company exactly as in Productlist.csv, builds URL.

Writes Productlist_with_urls.csv with UTF-8 BOM for Excel using a minimal schema:
- Product
- Company
- URL
- Loop Count
- Total Records
"""

import csv
import logging
from pathlib import Path
from typing import List, Dict, Tuple

from config_loader import (
    get_input_dir, get_output_dir,
    PRODUCTLIST_FILE, PRODUCTS_URL, PREPARED_URLS_FILE
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("prepare_urls")

import re
import unicodedata

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
    input_dir = get_input_dir()
    output_dir = get_output_dir()

    input_file = input_dir / PRODUCTLIST_FILE
    output_file = output_dir / PREPARED_URLS_FILE

    if not input_file.exists():
        found_file = None
        if input_dir.exists():
            for file in input_dir.iterdir():
                if file.is_file() and file.name.lower() == input_file.name.lower():
                    found_file = file
                    break
        if found_file:
            input_file = found_file
        else:
            raise FileNotFoundError(f"Input file not found: {input_file}")

    log.info(f"Reading products from: {input_file}")

    all_products: List[Tuple[str, str]] = []
    with open(input_file, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError("Missing headers in Productlist.csv")

        pcol = "Product" if "Product" in reader.fieldnames else reader.fieldnames[0]
        ccol = "Company" if "Company" in reader.fieldnames else reader.fieldnames[1]

        for row in reader:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            if prod and comp:
                all_products.append((prod, comp))

    log.info(f"Loaded {len(all_products)} products")

    output_data: List[Dict[str, str]] = []
    total = len(all_products)

    for idx, (prod, comp) in enumerate(all_products, 1):
        url = construct_product_url(prod)

        output_data.append({
            "Product": prod,
            "Company": comp,
            "URL": url,
            "Loop Count": "0",
            "Total Records": "0",
        })

        if idx % 100 == 0 or idx == total:
            pct = round((idx / total) * 100, 1) if total else 0
            print(f"[PROGRESS] Preparing URLs: {idx}/{total} ({pct}%)", flush=True)

    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = [
            "Product", "Company", "URL",
            "Loop Count", "Total Records",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_data)

    log.info(f"Wrote {len(output_data)} rows to: {output_file}")

if __name__ == "__main__":
    main()
