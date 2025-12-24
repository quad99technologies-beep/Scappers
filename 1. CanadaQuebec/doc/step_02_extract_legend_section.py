#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF Legend Section Extractor

Extracts the section from Annexe IV.1 to the end of the PDF document.
This is the first step in the pharmaceutical data extraction pipeline.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import json
import unicodedata
from typing import Optional, Dict, Any
from PyPDF2 import PdfReader, PdfWriter

try:
    from step_00_utils_encoding import clean_extracted_text
except ImportError:
    def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
        return unicodedata.normalize('NFC', str(text)) if text else ""

# Configuration
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output" / "split_pdf"
DEFAULT_INPUT_NAME = "annexe_v.pdf"
INPUT_PDF = INPUT_DIR / DEFAULT_INPUT_NAME
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def strip_accents(text: str) -> str:
    """Remove accents for case-insensitive matching."""
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")


def extract_page_text(reader: PdfReader, page_idx: int) -> str:
    """Extract and clean text from a PDF page."""
    try:
        raw_text = reader.pages[page_idx].extract_text() or ""
        return clean_extracted_text(raw_text, enforce_utf8=True)
    except Exception:
        return ""


def find_last_annexe_v_page(reader: PdfReader) -> Optional[int]:
    """Find the last page containing 'ANNEXE V'."""
    last_page = None
    for i in range(len(reader.pages)):
        text = extract_page_text(reader, i)
        if "annexe v" in strip_accents(text).lower():
            last_page = i
    return last_page


def find_first_legend_after(reader: PdfReader, start_page: int) -> Optional[int]:
    """Find first LÉGENDE page after start_page."""
    for i in range(max(0, start_page), len(reader.pages)):
        text = extract_page_text(reader, i)
        if "legende" in strip_accents(text).lower():
            return i
    return None


def find_last_legend_anywhere(reader: PdfReader) -> Optional[int]:
    """Find last LÉGENDE page anywhere in document."""
    last_page = None
    for i in range(len(reader.pages)):
        text = extract_page_text(reader, i)
        if "legende" in strip_accents(text).lower():
            last_page = i
    return last_page


def find_annexe_iv1_page(reader: PdfReader) -> Optional[int]:
    """Find the page containing 'ANNEXE IV.1' with the target heading and actual data."""
    for i in range(len(reader.pages)):
        text = extract_page_text(reader, i)
        text_normalized = strip_accents(text).lower()
        # Look for ANNEXE IV.1
        has_annexe = "annexe iv.1" in text_normalized
        # Look for the heading
        has_heading = "medicaments d" in text_normalized
        # Look for actual drug data (ADALIMUMAB is first drug in Annexe IV.1)
        has_data = "adalimumab" in text_normalized
        if has_annexe and has_heading and has_data:
            return i
    return None


def extract_pdf_section(reader: PdfReader, start_page: int, end_page: int,
                        output_filename: str) -> Path:
    """Extract pages from PDF and save to new file."""
    writer = PdfWriter()
    for page_num in range(start_page, end_page + 1):
        writer.add_page(reader.pages[page_num])

    output_path = OUTPUT_DIR / output_filename
    with open(output_path, "wb") as f:
        writer.write(f)
    return output_path


def main() -> Dict[str, Any]:
    """Main extraction function."""
    # Locate input PDF
    pdf_path = INPUT_PDF if INPUT_PDF.exists() else next(INPUT_DIR.glob("*.pdf"), None)
    if not pdf_path:
        return {"status": "error", "message": f"No PDF found in {INPUT_DIR}"}

    # Read PDF
    reader = PdfReader(str(pdf_path))
    total_pages = len(reader.pages)
    if total_pages == 0:
        return {"status": "error", "message": "PDF has 0 pages"}

    # Find ANNEXE V page
    annexe_v_page = find_last_annexe_v_page(reader)

    # Find Annexe IV.1 page (instead of LÉGENDE)
    legend_page = find_annexe_iv1_page(reader)

    if legend_page is None:
        return {"status": "error",
                "message": "Could not find 'ANNEXE IV.1' in the PDF"}

    # Extract section
    output_pdf = extract_pdf_section(reader, legend_page, total_pages - 1,
                                     "legend_to_end.pdf")

    # Write metadata
    metadata_path = OUTPUT_DIR / "index.json"
    metadata = {
        "input": str(pdf_path),
        "total_pages": total_pages,
        "annexe_v_page": annexe_v_page,
        "legend_page": legend_page,
        "output_file": str(output_pdf),
        "range_extracted": [legend_page, total_pages - 1],
    }
    with open(metadata_path, "w", encoding="utf-8", errors='replace') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    return {
        "status": "ok",
        "input": str(pdf_path),
        "total_pages": total_pages,
        "annexe_v_page": annexe_v_page,
        "legend_page": legend_page,
        "output_file": str(output_pdf),
        "index_json": str(metadata_path),
    }


if __name__ == "__main__":
    result = main()
    print(json.dumps(result, ensure_ascii=False, indent=2))
