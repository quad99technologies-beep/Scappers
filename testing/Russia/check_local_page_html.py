#!/usr/bin/env python3
"""
Check locally saved Russia farmcom page HTML (e.g. page 52.html).

Parses the file with the same structure the scraper expects and reports:
- Total <tr> in table.report tbody
- Main rows (with img.bullet[linkhref])
- Gray/EAN rows (no bullet)
- Barcode links (a.info with getEanCode)
- item_id count and sample
- EANs found in package cells (if any)

Usage:
  python check_local_page_html.py "C:\\Users\\Vishw\\OneDrive\\Desktop\\page 52.html"
  python check_local_page_html.py   # uses default path below
"""

import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

DEFAULT_HTML = r"C:\Users\Vishw\OneDrive\Desktop\page 52.html"


def extract_item_id_from_linkhref(linkhref: str) -> str:
    """Parse item_id from linkhref (same as scraper)."""
    if not linkhref or "?" not in linkhref:
        return ""
    qs = parse_qs(urlparse("http://x/?" + linkhref.split("?", 1)[-1]).query)
    return (qs.get("item_id", [""]) or [""])[0]


def extract_ean(text: str) -> str:
    """Extract EAN digits (8-14) from text (same as scraper)."""
    if not text:
        return ""
    compact = re.sub(r"\s+", "", text)
    matches = re.findall(r"\d{8,14}", compact)
    return max(matches, key=len) if matches else ""


def main():
    html_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_HTML
    path = Path(html_path)
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("Install beautifulsoup4: pip install beautifulsoup4")
        sys.exit(1)

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        html = f.read()

    soup = BeautifulSoup(html, "lxml")
    report = soup.select_one("table.report")
    if not report:
        print("No table.report found in HTML.")
        sys.exit(1)

    tbody = report.find("tbody")
    if not tbody:
        print("No tbody in table.report.")
        sys.exit(1)

    tr_list = tbody.find_all("tr")
    total_tr = len(tr_list)

    main_rows = 0
    gray_rows = 0
    item_ids = []
    barcode_links_in_report = 0
    ean_in_cells = 0
    rows_with_ean = 0

    for tr in tr_list:
        bullet = tr.select_one("img.bullet[linkhref]")
        if bullet:
            main_rows += 1
            linkhref = bullet.get("linkhref") or ""
            item_id = extract_item_id_from_linkhref(linkhref)
            if item_id:
                item_ids.append(item_id)
            # Package cell is 6th td (index 5)
            tds = tr.find_all("td")
            if len(tds) >= 6:
                package_text = tds[5].get_text(strip=True)
                ean = extract_ean(package_text)
                if ean:
                    ean_in_cells += 1
                    rows_with_ean += 1
        else:
            gray_rows += 1
            tds = tr.find_all("td")
            if len(tds) >= 6:
                package_text = tds[5].get_text(strip=True)
                if extract_ean(package_text):
                    ean_in_cells += 1

    # Barcode links: a.info with onclick getEanCode, inside table.report only
    for a in report.select('a.info[onclick*="getEanCode"]'):
        barcode_links_in_report += 1
    if barcode_links_in_report == 0:
        # Fallback: a with id starting with "e" (e.g. e6801)
        for a in report.select('a[id^="e"]'):
            if a.get("onclick") and "getEanCode" in (a.get("onclick") or ""):
                barcode_links_in_report += 1

    unique_item_ids = len(set(item_ids))

    print("=" * 60)
    print("LOCAL HTML CHECK:", path.name)
    print("=" * 60)
    print(f"  Total <tr> in table.report tbody: {total_tr}")
    print(f"  Main rows (img.bullet[linkhref]):  {main_rows}")
    print(f"  Gray/EAN rows (no bullet):         {gray_rows}")
    print(f"  Barcode links (a.info getEanCode): {barcode_links_in_report}")
    print(f"  item_id extracted:                 {len(item_ids)} (unique: {unique_item_ids})")
    print(f"  Rows with EAN in package cell:     {rows_with_ean} (main) + gray EAN cells: {ean_in_cells - rows_with_ean}")
    print(f"  -> Would pass validation (rows == EAN)? {main_rows == rows_with_ean and gray_rows == 0 or main_rows == ean_in_cells}")
    if item_ids:
        print(f"  Sample item_ids: {item_ids[:3]} ... {item_ids[-2:]}")
    print("=" * 60)


if __name__ == "__main__":
    main()
