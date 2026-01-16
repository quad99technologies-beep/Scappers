#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 1: Get Redirect URLs and Extract qs Parameters
====================================================
Reads input/Tender_Chile/tender_list.csv (CN numbers provided by client),
builds DetailsAcquisition URLs, opens them in Selenium to capture
final redirect URL containing qs=..., then writes output/Tender_Chile/tender_redirect_urls.csv.

INPUT:
  - input/Tender_Chile/tender_list.csv (or TenderList.csv)

OUTPUT:
  - output/Tender_Chile/tender_redirect_urls.csv
    Required columns for downstream:
      - tender_details_url  (Script 2)
      - tender_award_url    (Script 3)
"""

from __future__ import annotations

import csv
import time
import re
import sys
import os
from pathlib import Path
from typing import Dict, Optional, Any, List, Tuple

# Add repo root to path for imports
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Add scripts/Tender- Chile to path for config_loader
_script_dir = Path(__file__).parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Import config_loader for platform integration
try:
    from config_loader import (
        load_env_file, getenv, getenv_int, getenv_bool,
        get_input_dir as _get_input_dir,
        get_output_dir as _get_output_dir
    )
    load_env_file()
    _CONFIG_LOADER_AVAILABLE = True
except ImportError:
    _CONFIG_LOADER_AVAILABLE = False

# ----------------------------
# Constants (from config)
# ----------------------------
if _CONFIG_LOADER_AVAILABLE:
    MAX_TENDERS = getenv_int("MAX_TENDERS", 100)
    HEADLESS = getenv_bool("HEADLESS", True)
    WAIT_SECONDS = getenv_int("WAIT_SECONDS", 60)
else:
    MAX_TENDERS = int(os.getenv("MAX_TENDERS", "100"))
    HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
    WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "60"))

INPUT_FILENAME = "tender_list.csv"
OUTPUT_FILENAME = "tender_redirect_urls.csv"

REQUIRED_OUTPUT_COLUMNS = [
    "original_url",
    "redirect_url",
    "qs_parameter",
    "tender_details_url",
    "tender_award_url",
]


# ----------------------------
# Paths (platform-aware)
# ----------------------------
def get_root_dir() -> Path:
    """Get repository root directory"""
    return _repo_root


def get_input_dir() -> Path:
    """Input directory path - uses platform config if available"""
    if _CONFIG_LOADER_AVAILABLE:
        return _get_input_dir()
    return _repo_root / "input" / "Tender_Chile"


def get_output_dir() -> Path:
    """Output directory path - uses platform config if available"""
    if _CONFIG_LOADER_AVAILABLE:
        return _get_output_dir()
    return _repo_root / "output" / "Tender_Chile"


def get_debug_dir() -> Path:
    return get_output_dir() / "debug_redirect_urls"


# ----------------------------
# CSV helpers (delimiter + encoding)
# ----------------------------
def detect_csv_dialect_and_encoding(path: Path) -> Tuple[str, str]:
    """
    Detect delimiter and encoding using a practical heuristic.
    Returns: (delimiter, encoding)
    """
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1", "windows-1252"]
    sample = None
    used_enc = "utf-8"

    for enc in encodings:
        try:
            sample = path.read_text(encoding=enc, errors="strict")[:8192]
            used_enc = enc
            break
        except Exception:
            continue

    if not sample:
        return (",", "utf-8")

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return (dialect.delimiter, used_enc)
    except Exception:
        delims = [",", ";", "\t", "|"]
        counts = {d: 0 for d in delims}
        lines = [ln for ln in sample.splitlines()[:10] if ln.strip()]
        for ln in lines:
            for d in delims:
                counts[d] += ln.count(d)
        best = max(counts.items(), key=lambda x: x[1])[0]
        return (best if counts[best] > 0 else ",", used_enc)


# ----------------------------
# URL logic
# ----------------------------
def validate_url_format(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False
    url = url.strip()
    return url.startswith("http://") or url.startswith("https://")


def extract_qs_from_url(url: str) -> Optional[str]:
    m = re.search(r"[?&]qs=([^&]+)", url)
    return m.group(1) if m else None


def extract_url_from_csv_row(row: Dict[str, Any]) -> Optional[str]:
    """
    Extract a starting URL or tender id from any row.

    Supports:
      - explicit URL columns (notice/award/detail)
      - any cell containing a MercadoPublico URL
      - tender identifiers including CN Document Number (your case)
    """

    # 1) Try explicit URL columns first
    url_columns = [
        "Original_Publication_Link_Notice",
        "Original_Publication_Link_Award",
        "Source URL",
        "Detail URL",
        "URL", "url", "Link", "link",
        "Enlace", "enlace", "Detalle", "detalle",
        "EnlaceDetalle", "Enlace Detalle", "Link Detalle", "LinkDetalle",
    ]
    for col in url_columns:
        if col in row and row[col]:
            v = str(row[col]).strip()
            if validate_url_format(v):
                return v

    # 2) Any cell that looks like a MercadoPublico URL
    for _, value in row.items():
        if value and isinstance(value, str):
            v = value.strip()
            if validate_url_format(v) and "mercadopublico.cl" in v:
                return v

    # 3) Construct from tender id columns (EXPANDED)
    id_columns = [
        "IDLicitacion", "idlicitacion",
        "CN Document Number",            # [OK] your current data
        "CodigoExterno", "CódigoExterno",
        "Source Tender Id", "Tender ID",
        "ID", "id", "Licitacion", "licitacion",
    ]
    for col in id_columns:
        if col in row and row[col]:
            lic_id = str(row[col]).strip()
            if lic_id:
                # Build DetailsAcquisition URL
                return (
                    "https://www.mercadopublico.cl/Procurement/Modules/RFB/"
                    f"DetailsAcquisition.aspx?idlicitacion={lic_id}"
                )

    return None


def convert_to_details_url(url: str) -> str:
    """
    Ensure we end up at a DetailsAcquisition.aspx URL.
    If already DetailsAcquisition, return as-is.
    If contains idlicitacion=, normalize to DetailsAcquisition.
    Otherwise, return as-is.
    """
    if "DetailsAcquisition.aspx" in url:
        return url

    if "idlicitacion=" in url:
        m = re.search(r"idlicitacion=([^&]+)", url)
        if m:
            lic_id = m.group(1)
            return (
                "https://www.mercadopublico.cl/Procurement/Modules/RFB/"
                f"DetailsAcquisition.aspx?idlicitacion={lic_id}"
            )

    return url


# ----------------------------
# Selenium redirect capture
# ----------------------------
def get_redirect_url(start_url: str) -> str:
    """
    Open the URL in Selenium and wait until current_url stabilizes.
    Returns the final current_url (often includes qs=...).
    """
    if not validate_url_format(start_url):
        raise ValueError(f"Invalid URL format: {start_url}")

    chrome_opts = Options()
    if HEADLESS:
        chrome_opts.add_argument("--headless=new")
    chrome_opts.add_argument("--disable-gpu")
    chrome_opts.add_argument("--no-sandbox")
    chrome_opts.add_argument("--disable-dev-shm-usage")
    chrome_opts.add_argument("--window-size=1400,900")
    chrome_opts.add_argument("--lang=es-CL")

    driver = webdriver.Chrome(options=chrome_opts)
    try:
        driver.set_page_load_timeout(WAIT_SECONDS)
        driver.get(start_url)

        last = ""
        stable_count = 0
        end_time = time.time() + WAIT_SECONDS

        while time.time() < end_time:
            current = driver.current_url

            if current == last:
                stable_count += 1
            else:
                stable_count = 0
                last = current

            # stable for ~1.5 seconds
            if stable_count >= 3:
                break

            time.sleep(0.5)

        return driver.current_url
    finally:
        driver.quit()


# ----------------------------
# Validation
# ----------------------------
def validate_input_file(path: Path) -> bool:
    if not path.exists():
        print(f"[ERROR] {path} not found. Run Script 1 first.")
        return False
    return True


# ----------------------------
# Main
# ----------------------------
def main():
    root = get_root_dir()
    input_dir = get_input_dir()
    output_dir = get_output_dir()
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    debug_dir = get_debug_dir()
    debug_dir.mkdir(parents=True, exist_ok=True)

    # Try multiple filename variants
    input_path = input_dir / INPUT_FILENAME
    if not input_path.exists():
        # Try alternative filename
        input_path = input_dir / "TenderList.csv"
    if not input_path.exists():
        print(f"[ERROR] No tender list found in {input_dir}")
        print(f"   Expected: tender_list.csv or TenderList.csv")
        sys.exit(1)

    print("=" * 80)
    print("GETTING REDIRECT URLs FOR TENDERS")
    print("=" * 80)
    print(f"Reading: {input_path}")

    delim, enc = detect_csv_dialect_and_encoding(input_path)
    print(f"   Detected delimiter: {repr(delim)} | encoding: {enc}")

    # Inspect structure
    with open(input_path, "r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        fieldnames = reader.fieldnames or []
        print(f"   Available columns: {', '.join(fieldnames) if fieldnames else '(none)'}")

        try:
            first_row = next(reader)
            print("\n   Sample row (first row):")
            shown = 0
            for k, v in first_row.items():
                if v and shown < 8:
                    sv = str(v)
                    sv = sv[:80] + ("..." if len(sv) > 80 else "")
                    print(f"     {k}: {sv}")
                    shown += 1
        except StopIteration:
            print(f"[ERROR] {input_path.name} is empty (no data rows).")
            sys.exit(1)

    # Build list of tender URLs
    tender_urls: List[str] = []
    with open(input_path, "r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)

        count = 0
        for row_num, row in enumerate(reader, start=2):  # header is row 1
            if count >= MAX_TENDERS:
                break

            raw = extract_url_from_csv_row(row)
            if raw:
                raw = raw.strip()
                details_url = convert_to_details_url(raw)

                if validate_url_format(details_url):
                    tender_urls.append(details_url)
                    count += 1
                    if count <= 3:
                        print(f"   [OK] Row {row_num}: URL ready -> {details_url[:90]}")
                else:
                    if row_num <= 5:
                        print(f"   [WARN] Row {row_num}: Not a valid URL after conversion: {details_url[:90]}")
            else:
                if row_num <= 5:
                    print(f"   [WARN] Row {row_num}: No URL/ID found in row")

    if not tender_urls:
        print("[ERROR] No valid tender URLs found in CSV")
        print("   Fix options:")
        print("   - Ensure CSV delimiter is correct (we auto-detect now)")
        print("   - Ensure you have CN Document Number / IDLicitacion / or a URL column")
        sys.exit(1)

    print(f"[OK] Found {len(tender_urls)} tender URLs to process")
    print("=" * 80)

    redirect_rows: List[Dict[str, str]] = []

    for i, url in enumerate(tender_urls, 1):
        print(f"\n[{i}/{len(tender_urls)}] Processing: {url[:90]}")

        row_out = {
            "original_url": url,
            "redirect_url": "",
            "qs_parameter": "",
            "tender_details_url": url,  # fallback
            "tender_award_url": "",
        }

        try:
            redirect_url = get_redirect_url(url)

            if validate_url_format(redirect_url):
                row_out["redirect_url"] = redirect_url

                qs = extract_qs_from_url(redirect_url)
                if qs:
                    row_out["qs_parameter"] = qs
                    row_out["tender_details_url"] = (
                        "https://www.mercadopublico.cl/Procurement/Modules/RFB/"
                        f"DetailsAcquisition.aspx?qs={qs}"
                    )
                    row_out["tender_award_url"] = (
                        "https://www.mercadopublico.cl/Procurement/Modules/RFB/StepsProcessAward/"
                        f"PreviewAwardAct.aspx?qs={qs}"
                    )
                    print(f"   [OK] qs: {qs}")
                else:
                    # still usable for Script 3 if it is DetailsAcquisition
                    row_out["tender_details_url"] = redirect_url
                    print("   [WARN] No qs parameter found; using redirect_url as tender_details_url")

            else:
                raise ValueError(f"Redirect URL is not a valid URL: {redirect_url}")

        except Exception as e:
            print(f"   [ERROR] {e}")

        redirect_rows.append(row_out)
        time.sleep(1)

    # Write output CSV
    out_path = output_dir / OUTPUT_FILENAME
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REQUIRED_OUTPUT_COLUMNS)
        w.writeheader()
        for r in redirect_rows:
            w.writerow(r)

    print("\n" + "=" * 80)
    print(f"[OK] Saved: {out_path}")
    print("Next:")
    print(" - Run Script 3 (needs tender_details_url)")
    print(" - Run Script 4 (needs tender_details_url + tender_award_url)")
    print("=" * 80)


if __name__ == "__main__":
    main()
