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
    # Try importing from doc directory
    import sys
    from pathlib import Path
    # Docs are now in repo root doc/ folder
    doc_path = Path(__file__).resolve().parents[2] / "doc"
    if doc_path.exists():
        sys.path.insert(0, str(doc_path))
        try:
            from step_00_utils_encoding import clean_extracted_text
        except ImportError:
            def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
                return unicodedata.normalize('NFC', str(text)) if text else ""
    else:
        def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
            return unicodedata.normalize('NFC', str(text)) if text else ""

# Configuration
import sys
from pathlib import Path
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
from config_loader import (
    get_base_dir, get_input_dir, get_split_pdf_dir,
    DEFAULT_INPUT_PDF_NAME, INDEX_JSON_NAME,
    ANNEXE_IV1_PDF_NAME, ANNEXE_IV2_PDF_NAME, ANNEXE_V_PDF_NAME
)
BASE_DIR = get_base_dir()
INPUT_DIR = get_input_dir()
OUTPUT_DIR = get_split_pdf_dir()
DEFAULT_INPUT_NAME = DEFAULT_INPUT_PDF_NAME

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
def find_first_annexe_iv2_page(reader: PdfReader, start_after: int = 0) -> Optional[int]:
    """Find the first page containing 'ANNEXE IV.2' (after a given page index)."""
    for i in range(start_after, len(reader.pages)):
        text = extract_page_text(reader, i)
        t = strip_accents(text).lower()
        has_annexe = "annexe iv.2" in t
        has_heading = "medicaments d" in t  # common heading for these annexes
        if has_annexe and has_heading:
            return i
    return None


def find_first_annexe_v_page(reader: PdfReader, start_after: int = 0) -> Optional[int]:
    """Find the first page containing 'ANNEXE V' (after a given page index)."""
    for i in range(start_after, len(reader.pages)):
        text = extract_page_text(reader, i)
        t = strip_accents(text).lower()
        has_annexe = "annexe v" in t
        # Add a light qualifier to avoid accidental matches in footers/refs
        has_context = ("prix le plus bas" in t) or ("exclu" in t) or ("medicament" in t)
        if has_annexe and has_context:
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
    # Locate input PDF - try platform config path first, then local directory
    pdf_path = None
    if INPUT_PDF.exists():
        pdf_path = INPUT_PDF
    else:
        # Try platform config input directory
        pdf_path = next(INPUT_DIR.glob("*.pdf"), None)
        # If not found, try local scraper input directory as fallback
        if not pdf_path:
            # Get scraper root (parent of scripts directory)
            scraper_root = Path(__file__).resolve().parents[1]
            local_input_dir = scraper_root / "input"
            if local_input_dir.exists():
                pdf_path = next(local_input_dir.glob("*.pdf"), None)
    
    if not pdf_path:
        # Also check repo root input folder as final fallback
        repo_root = Path(__file__).resolve().parents[2]
        repo_input_dir = repo_root / "input" / "CanadaQuebec"
        if repo_input_dir.exists():
            pdf_path = next(repo_input_dir.glob("*.pdf"), None)
    
    if not pdf_path:
        return {"status": "error", "message": f"No PDF found in {INPUT_DIR}. Please place your PDF file (liste-med.pdf) in: {INPUT_DIR}"}

    # Read PDF
    reader = PdfReader(str(pdf_path))
    total_pages = len(reader.pages)
    if total_pages == 0:
        return {"status": "error", "message": "PDF has 0 pages"}

    # Find start pages for each ANNEXE inside the PDF (robust, text-based)
    # Start searching after TOC (Page 20)
    SEARCH_START_OFFSET = 20
    
    def find_annexe_iii_page(reader: PdfReader) -> Optional[int]:
        for i in range(SEARCH_START_OFFSET, len(reader.pages)):
            text = extract_page_text(reader, i)
            t = strip_accents(text).lower()
            if "annexe iii" in t and "grossiste" in t:
                return i
        return None

    def find_annexe_iv_page(reader: PdfReader, start_after: int = 0) -> Optional[int]:
        for i in range(max(SEARCH_START_OFFSET, start_after), len(reader.pages)):
            text = extract_page_text(reader, i)
            t = strip_accents(text).lower()
            # Look for Annexe IV specifically (Exception Drugs)
            if "annexe iv" in t and "annexe iv.1" not in t and ("medicaments d" in t or "indications" in t):
                return i
        return None

    iii_start = find_annexe_iii_page(reader)
    iv_start = find_annexe_iv_page(reader, start_after=iii_start + 1 if iii_start is not None else 0)
    iv1_start = find_annexe_iv1_page(reader)
    iv2_start = find_first_annexe_iv2_page(reader, start_after=iv1_start + 1)
    v_start = find_first_annexe_v_page(reader, start_after=iv2_start + 1 if iv2_start is not None else iv1_start + 1)

    if iv1_start is None:
        return {"status": "error", "message": "Could not find 'ANNEXE IV.1' in the PDF"}
    
    # Build page ranges (inclusive)
    # We'll extract III and IV if found
    annexes_metadata = {}
    
    if iii_start is not None:
        iii_end = iv_start - 1 if iv_start is not None else iv1_start - 1
        iii_pdf = extract_pdf_section(reader, iii_start, iii_end, "annexe_iii.pdf")
        annexes_metadata["III"] = {"start_page": iii_start, "end_page": iii_end, "output_file": str(iii_pdf)}

    if iv_start is not None:
        iv_end = iv1_start - 1
        iv_pdf = extract_pdf_section(reader, iv_start, iv_end, "annexe_iv.pdf")
        annexes_metadata["IV"] = {"start_page": iv_start, "end_page": iv_end, "output_file": str(iv_pdf)}

    iv1_range = (iv1_start, iv2_start - 1)
    iv2_range = (iv2_start, v_start - 1)
    v_range = (v_start, total_pages - 1)

    # Extract each ANNEXE into its own PDF
    iv1_pdf = extract_pdf_section(reader, iv1_range[0], iv1_range[1], ANNEXE_IV1_PDF_NAME)
    iv2_pdf = extract_pdf_section(reader, iv2_range[0], iv2_range[1], ANNEXE_IV2_PDF_NAME)
    v_pdf = extract_pdf_section(reader, v_range[0], v_range[1], ANNEXE_V_PDF_NAME)

    annexes_metadata.update({
        "IV.1": {"start_page": iv1_range[0], "end_page": iv1_range[1], "output_file": str(iv1_pdf)},
        "IV.2": {"start_page": iv2_range[0], "end_page": iv2_range[1], "output_file": str(iv2_pdf)},
        "V": {"start_page": v_range[0], "end_page": v_range[1], "output_file": str(v_pdf)},
    })

    # Write metadata / audit index
    metadata_path = OUTPUT_DIR / INDEX_JSON_NAME
    metadata = {
        "input": str(pdf_path),
        "total_pages": total_pages,
        "annexes": annexes_metadata,
    }
    with open(metadata_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)


    return {
        "status": "ok",
        "input": str(pdf_path),
        "total_pages": total_pages,
        "annexe_files": {k: v["output_file"] for k, v in annexes_metadata.items()},
        "index_json": str(metadata_path),
    }



if __name__ == "__main__":
    result = main()
    print(json.dumps(result, ensure_ascii=False, indent=2))
