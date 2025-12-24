"""
QUEST3+ Product Details Scraper

Fetches product details (Product Name, Holder) from QUEST3+ detail pages
using registration numbers from malaysia_drug_prices_view_all.csv

URL pattern: https://quest3plus.bpfk.gov.my/pmo2/detail.php?type=product&id={registration_number}

Output: quest3_product_details.csv with columns:
  - Registration No
  - Product Name
  - Holder
  - Holder Address
  - Manufacturer
  - Manufacturer Address
  - Phone No

Dependencies:
  pip install playwright pandas
  playwright install
"""

from __future__ import annotations

import re
import time
from pathlib import Path
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


BASE_URL = "https://quest3plus.bpfk.gov.my/pmo2/detail.php"

# Paths
OUTPUT_DIR = Path("../Output")
INPUT_FILE = OUTPUT_DIR / "malaysia_drug_prices_view_all.csv"
OUTPUT_FILE = OUTPUT_DIR / "quest3_product_details.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def extract_text_from_td(td_element) -> str:
    """Extract text from <p><b> inside td, handling line breaks."""
    try:
        p_elem = td_element.query_selector("p")
        if p_elem:
            # Get text content and clean up
            text = p_elem.inner_text()
            # Replace multiple line breaks with single space
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        return ""
    except:
        return ""


def scrape_product_detail(page, registration_no: str) -> dict:
    """Scrape product details from QUEST3+ detail page."""
    url = f"{BASE_URL}?type=product&id={registration_no}"

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(1)  # Wait for content to load

        # Look for the table with product details
        table = page.query_selector('table.table[width="100%"]')

        if not table:
            return {
                "Registration No": registration_no,
                "Product Name": None,
                "Holder": None,
                "Holder Address": None,
                "Manufacturer": None,
                "Manufacturer Address": None,
                "Phone No": None
            }

        result = {"Registration No": registration_no}

        # Extract all td elements
        tds = table.query_selector_all("td")

        # Parse the table structure
        # Row 1: Product Name, Registration No
        # Row 2: Holder, Holder Address
        # Row 3: Phone No (colspan=2)
        # Row 4: Manufacturer, Manufacturer Address

        for td in tds:
            text = td.inner_text().strip()

            if text.startswith("Product Name"):
                result["Product Name"] = extract_text_from_td(td)
            elif text.startswith("Registration No"):
                result["Registration No (verified)"] = extract_text_from_td(td)
            elif text.startswith("Holder :") and "Holder Address" not in text:
                result["Holder"] = extract_text_from_td(td)
            elif text.startswith("Holder Address"):
                result["Holder Address"] = extract_text_from_td(td)
            elif text.startswith("Phone No"):
                result["Phone No"] = extract_text_from_td(td)
            elif text.startswith("Manufacturer :") and "Manufacturer Address" not in text:
                result["Manufacturer"] = extract_text_from_td(td)
            elif text.startswith("Manufacturer Address"):
                result["Manufacturer Address"] = extract_text_from_td(td)

        return result

    except PWTimeoutError:
        print(f"  [TIMEOUT] Could not load page for {registration_no}")
        return {
            "Registration No": registration_no,
            "Product Name": None,
            "Holder": None,
            "Holder Address": None,
            "Manufacturer": None,
            "Manufacturer Address": None,
            "Phone No": None
        }
    except Exception as e:
        print(f"  [ERROR] Failed to scrape {registration_no}: {e}")
        return {
            "Registration No": registration_no,
            "Product Name": None,
            "Holder": None,
            "Holder Address": None,
            "Manufacturer": None,
            "Manufacturer Address": None,
            "Phone No": None
        }


def load_registration_numbers() -> list[str]:
    """Load registration numbers from malaysia_drug_prices_view_all.csv"""
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}\n"
            f"Please run Script 01 first to generate malaysia_drug_prices_view_all.csv"
        )

    df = pd.read_csv(INPUT_FILE, dtype=str)

    # Get the first column (Registration Number)
    reg_col = df.columns[0]

    # Extract unique registration numbers, removing empty/NaN values
    reg_numbers = df[reg_col].dropna().unique().tolist()
    reg_numbers = [str(rn).strip() for rn in reg_numbers if str(rn).strip()]

    print(f"Loaded {len(reg_numbers)} unique registration numbers from {INPUT_FILE}")

    return reg_numbers


def main() -> None:
    registration_numbers = load_registration_numbers()

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # Set True for headless mode
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()

        total = len(registration_numbers)

        for idx, reg_no in enumerate(registration_numbers, 1):
            print(f"\n[{idx}/{total}] Processing: {reg_no}")

            result = scrape_product_detail(page, reg_no)
            results.append(result)

            if result.get("Product Name"):
                print(f"  ✓ Found: {result['Product Name']}")
                if result.get("Holder"):
                    print(f"  ✓ Holder: {result['Holder']}")
            else:
                print(f"  ✗ No details found")

            # Rate limiting - wait between requests
            if idx < total:
                time.sleep(2)  # 2 second delay between requests

        context.close()
        browser.close()

    # Save results to CSV
    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"\n{'='*60}")
    print(f"✅ Scraped {len(results)} product details")
    print(f"✅ Saved to: {OUTPUT_FILE}")

    # Statistics
    found_count = df_results["Product Name"].notna().sum()
    print(f"\nStatistics:")
    print(f"  Total: {len(results)}")
    print(f"  Found: {found_count}")
    print(f"  Not Found: {len(results) - found_count}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
