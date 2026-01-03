#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - URL Preparation Step
Prepares product URLs and determines scraping source (API vs Selenium) before scraping.

This script:
1. Reads Productlist.csv (from step 01)
2. Determines if products are duplicates (same product name appears multiple times)
3. Constructs URLs for each product
4. Determines source: "api" for single products, "selenium" for duplicates
5. Outputs Productlist_with_urls.csv with: Product, Company, Source, URL, IsDuplicate
"""

import csv
import logging
from pathlib import Path
from collections import Counter
from typing import List, Tuple, Dict

# ====== CONFIG ======
from config_loader import (
    get_input_dir, get_output_dir,
    PRODUCTLIST_FILE, PRODUCTS_URL, PREPARED_URLS_FILE
)

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("prepare_urls")

# Import URL sanitization functions from scraper
import sys
sys.path.insert(0, str(Path(__file__).parent))

# We'll import the sanitization function, but define it here to avoid circular imports
import re
import unicodedata

def strip_accents(s: str) -> str:
    """Remove accents from string."""
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def nk(s: str) -> str:
    """Normalize string for comparison (lowercase, no accents, single spaces)."""
    if not s:
        return ""
    normalized = strip_accents(s.strip())
    return re.sub(r"\s+", " ", normalized).lower()

def sanitize_product_name_for_url(product_name: str) -> str:
    """
    Sanitize product name for URL construction based on AlfaBeta URL patterns.
    """
    if not product_name:
        return ""
    
    sanitized = strip_accents(product_name)
    
    # Handle + character:
    sanitized = re.sub(r'\s+\+\s+', '  ', sanitized)  # "+" between spaces -> double space
    sanitized = re.sub(r'\+', '', sanitized)  # Remove remaining + characters
    
    # Remove special characters (keep only alphanumeric, spaces, and hyphens)
    sanitized = re.sub(r'[^a-zA-Z0-9\s-]', '', sanitized)
    
    # Replace spaces with hyphens, preserving multiple spaces as multiple hyphens
    sanitized = re.sub(r'  ', ' __DOUBLE__ ', sanitized)  # Preserve double spaces
    sanitized = re.sub(r'\s+', '-', sanitized)  # Replace all spaces with single hyphen
    sanitized = re.sub(r'__DOUBLE__', '-', sanitized)  # Restore double hyphens
    
    # Remove more than 2 consecutive hyphens (keep double hyphens, remove triple+)
    sanitized = re.sub(r'-{3,}', '--', sanitized)
    
    # Convert to lowercase
    sanitized = sanitized.lower()
    
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip('-')
    
    # Format as productname.html
    if sanitized:
        return f"{sanitized}.html"
    return ""

def construct_product_url(product_name: str, base_url: str = None) -> str:
    """
    Construct product URL from product name.
    Format: https://www.alfabeta.net/precio/productname.html
    """
    if base_url is None:
        base_url = PRODUCTS_URL
    
    # Ensure base_url doesn't end with /
    base_url = base_url.rstrip('/')
    
    # Sanitize product name
    sanitized = sanitize_product_name_for_url(product_name)
    
    if not sanitized:
        return ""
    
    # Construct full URL
    return f"{base_url}/{sanitized}"

def main():
    """Main function to prepare URLs and determine sources."""
    input_dir = get_input_dir()
    output_dir = get_output_dir()
    
    input_file = input_dir / PRODUCTLIST_FILE
    output_file = output_dir / PREPARED_URLS_FILE
    
    # Check if input file exists
    if not input_file.exists():
        # Try case-insensitive search
        found_file = None
        if input_dir.exists():
            for file in input_dir.iterdir():
                if file.is_file() and file.name.lower() == input_file.name.lower():
                    found_file = file
                    break
        
        if found_file:
            log.warning(f"Input file found with different casing: {found_file}")
            input_file = found_file
        else:
            error_msg = f"Input file not found: {input_file}\n"
            error_msg += f"Please run script 01 (getProdList.py) first to generate {PRODUCTLIST_FILE}\n"
            raise FileNotFoundError(error_msg)
    
    log.info(f"Reading products from: {input_file}")
    
    # Load all products
    all_products: List[Tuple[str, str]] = []
    
    with open(input_file, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = {nk(h): h for h in (reader.fieldnames or [])}
        pcol = headers.get(nk("Product")) or headers.get("product") or "Product"
        ccol = headers.get(nk("Company")) or headers.get("company") or "Company"
        
        for row in reader:
            prod = (row.get(pcol) or "").strip()
            comp = (row.get(ccol) or "").strip()
            if prod and comp:
                all_products.append((prod, comp))
    
    log.info(f"Loaded {len(all_products)} products from {PRODUCTLIST_FILE}")
    
    # Count product occurrences (by product name only, case-insensitive)
    product_counts = Counter(nk(prod) for prod, _ in all_products)
    
    # Determine duplicates
    duplicate_products = {prod for prod, count in product_counts.items() if count > 1}
    single_products = {prod for prod, count in product_counts.items() if count == 1}
    
    log.info(f"Found {len(single_products)} unique single products")
    log.info(f"Found {len(duplicate_products)} unique duplicate products")
    
    # Prepare output data
    output_data: List[Dict[str, str]] = []
    
    for prod, comp in all_products:
        prod_norm = nk(prod)
        is_duplicate = prod_norm in duplicate_products
        
        # Construct URL
        url = construct_product_url(prod)
        
        # Determine source:
        # - Selenium for duplicates
        # - Selenium if URL contains "--" (double hyphens)
        # - API for single products with normal URLs
        if is_duplicate or (url and "--" in url):
            source = "selenium"
        else:
            source = "api"
        
        output_data.append({
            "Product": prod,
            "Company": comp,
            "Source": source,
            "URL": url,
            "IsDuplicate": "true" if is_duplicate else "false"
        })
    
    # Write output CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["Product", "Company", "Source", "URL", "IsDuplicate"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_data)
    
    log.info(f"Wrote {len(output_data)} products to: {output_file}")
    
    # Summary statistics
    api_count = sum(1 for row in output_data if row["Source"] == "api")
    selenium_count = sum(1 for row in output_data if row["Source"] == "selenium")
    url_count = sum(1 for row in output_data if row["URL"])
    double_hyphen_count = sum(1 for row in output_data if row["URL"] and "--" in row["URL"])
    
    log.info("=" * 60)
    log.info("Summary:")
    log.info(f"  Total products: {len(output_data)}")
    log.info(f"  API source (singles with normal URLs): {api_count}")
    log.info(f"  Selenium source (duplicates + URLs with --): {selenium_count}")
    log.info(f"  Products with URLs: {url_count}")
    log.info(f"  Products without URLs: {len(output_data) - url_count}")
    log.info(f"  URLs with double hyphens (--): {double_hyphen_count}")
    log.info("=" * 60)
    
    log.info("URL preparation completed successfully!")

if __name__ == "__main__":
    main()

