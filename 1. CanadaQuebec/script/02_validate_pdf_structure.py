#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PDF Structure Validator

Validates the structure of extracted PDF sections and determines if they are
suitable for data extraction. Generates quality assurance reports.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

from pathlib import Path
import json
import csv
import re
import unicodedata
from typing import List, Dict, Any, Tuple, Optional

try:
    from step_00_utils_encoding import clean_extracted_text, clean_word_token
except ImportError:
    def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
        return unicodedata.normalize('NFC', str(text)) if text else ""
    def clean_word_token(word_dict: dict) -> dict:
        if 'text' in word_dict:
            word_dict = word_dict.copy()
            word_dict['text'] = clean_extracted_text(word_dict['text'])
        return word_dict

try:
    import pdfplumber
except ImportError:
    raise SystemExit("Missing dependency: pdfplumber\nInstall with: pip install pdfplumber")

# Configuration
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
try:
    from config_loader import (
        get_base_dir, get_input_dir, get_qa_output_dir, get_split_pdf_dir,
        Y_TOL, X_TOL, DEFAULT_BAND,
        MIN_PAGES_WITH_DIN, MIN_HEADERS_RATIO, MIN_ROW_SHAPE_RATIO, MAX_FLAGGED_RATIO
    )
    BASE_DIR = get_base_dir()
    INPUT_DIR = get_input_dir()
    OUTPUT_DIR = get_qa_output_dir()
    SPLIT_PDF_DIR = get_split_pdf_dir()
    INPUT_PDF = SPLIT_PDF_DIR / "legend_to_end.pdf"
except ImportError:
    # Fallback to original values if config_loader not available
    BASE_DIR = Path(__file__).resolve().parents[1]
    INPUT_DIR = BASE_DIR / "input"
    OUTPUT_DIR = BASE_DIR / "output" / "qa"
    SPLIT_PDF_DIR = BASE_DIR / "output" / "split_pdf"
    INPUT_PDF = SPLIT_PDF_DIR / "legend_to_end.pdf"
    Y_TOL = 1.6
    X_TOL = 1.0
    DEFAULT_BAND = {
        "brand_max": 0.42, "manuf_min": 0.42, "manuf_max": 0.60,
        "pack_min": 0.58, "unit_min": 0.73
    }
    MIN_PAGES_WITH_DIN = 10
    MIN_HEADERS_RATIO = 0.40
    MIN_ROW_SHAPE_RATIO = 0.55
    MAX_FLAGGED_RATIO = 0.35

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Regex patterns
RE_ALLCAPS = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+$")
RE_FORMLINE = re.compile(r"(?i)\b(Co\.|Caps\.|Sol\.|Susp\.|Pd\.|Perf\.|Sir\.|Gel\.|I\.V\.|I\.M\.|S\.C\.|Orale)\b")
RE_STRENGTH = re.compile(r"(?i)\b\d+(?:[.,]\d+)?(?:\s*[-/]\s*\d+(?:[.,]\d+)?)?\s*(mg|g|mcg|µg|U|UI|U/mL|UI/mL|mg/mL|mg/5\s?mL|mL)\b")
RE_HAS_PPB = re.compile(r"\bPPB\b", re.IGNORECASE)
RE_DIN = re.compile(r"^\d{6,9}$")
RE_PACK_ONLY = re.compile(r"^\d{1,4}$")
RE_VOL_ONE = re.compile(r"^(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)
RE_VOL = re.compile(r"^\d{1,4}\s?(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)
RE_PRICE_NUM = re.compile(r"^[\d\s.,]+$")

BLOCKLIST_HEADINGS = {
    "ANNEXE", "MEDICAMENTS D'EXCEPTION", "MÉDICAMENTS D'EXCEPTION",
    "CODE", "MARQUE", "FABRICANT", "FORMAT", "PRIX", "UNITAIRE"
}


def strip_accents(text: str) -> str:
    """Remove accents for matching."""
    return "".join(c for c in unicodedata.normalize("NFD", text) 
                   if unicodedata.category(c) != "Mn")


def tokens_to_text(tokens: List[Dict[str, Any]]) -> str:
    """Combine token texts with encoding cleaning."""
    cleaned = [clean_extracted_text(t.get("text", ""), enforce_utf8=True) 
               for t in tokens]
    return " ".join(cleaned).strip()


def is_allcaps_candidate(text: str) -> bool:
    """Check if text is a generic name candidate."""
    text = text.strip()
    if not text or re.match(r"^\d", text):
        return False
    if any(h in strip_accents(text).upper() for h in BLOCKLIST_HEADINGS):
        return False
    return bool(RE_ALLCAPS.match(text))


def extract_page_lines(page) -> List[Dict[str, Any]]:
    """Extract words and organize into lines."""
    words = page.extract_words(x_tolerance=X_TOL, y_tolerance=Y_TOL, 
                               keep_blank_chars=False)
    if not words:
        return []
    
    words = [clean_word_token(w) for w in words]
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))
    
    lines, current, cur_top = [], [], None
    
    def push_line():
        current.sort(key=lambda z: z["x0"])
        lines.append({
            "top": min(t["top"] for t in current),
            "bottom": max(t["bottom"] for t in current),
            "tokens": current[:],
            "text": tokens_to_text(current)
        })
    
    for word in words:
        if cur_top is None:
            cur_top = word["top"]
            current = [word]
            continue
        if abs(word["top"] - cur_top) <= Y_TOL:
            current.append(word)
        else:
            push_line()
            cur_top = word["top"]
            current = [word]
    
    if current:
        push_line()
    
    return lines


def calibrate_column_bands(page, width: float) -> Tuple[Dict[str, float], bool, bool]:
    """Calibrate column positions from headers."""
    bands = DEFAULT_BAND.copy()
    words = page.extract_words(x_tolerance=2.0, y_tolerance=2.0, 
                               keep_blank_chars=False) or []
    words = [clean_word_token(w) for w in words]
    words.sort(key=lambda w: (w["top"], w["x0"]))
    
    entries = [(strip_accents(w["text"]).upper(), w) for w in words]
    pack_x = unit_x = None
    found_pack = found_unit = False
    
    for i, (txt, w) in enumerate(entries):
        if txt in {"COUT", "COÛT"}:
            y = w["top"]
            same_line = [t for t, ww in entries[i:i+8] 
                        if abs(ww["top"] - y) < 2.0]
            if any("FORMAT" in t for t in same_line):
                pack_x = w["x0"]
                found_pack = True
        if txt == "PRIX":
            y = w["top"]
            same_line = [t for t, ww in entries[i:i+10] 
                        if abs(ww["top"] - y) < 2.0]
            if any("UNITAIRE" in t for t in same_line):
                unit_x = w["x0"]
                found_unit = True
    
    if pack_x is not None:
        bands["pack_min"] = max(0.50, (pack_x - 10.0) / width)
    if unit_x is not None:
        bands["unit_min"] = max(bands["pack_min"] + 0.05, 
                                (unit_x - 10.0) / width)
    
    return bands, found_pack, found_unit


def classify_line_type(line: Dict[str, Any], width: float, 
                       bands: Dict[str, float]) -> str:
    """Classify line type."""
    text = line["text"].strip()
    if not text:
        return "noise"
    
    if is_allcaps_candidate(text):
        return "generic"
    if RE_FORMLINE.search(text) or RE_STRENGTH.search(text) or RE_HAS_PPB.search(text):
        return "form"
    if line["tokens"]:
        first = line["tokens"][0]["text"].strip()
        if RE_DIN.match(first):
            return "din_row"
    
    t0 = line["tokens"][0]
    left_x = t0["x0"] / width
    first_txt = t0["text"].strip()
    
    if (RE_PACK_ONLY.match(first_txt) or RE_VOL.match(first_txt)) and \
       left_x >= bands["pack_min"]:
        return "pack_cont"
    
    if RE_PRICE_NUM.match(text):
        last_tok = line["tokens"][-1]
        if last_tok["x0"] / width >= bands["unit_min"]:
            return "unit_only"
    
    return "noise"


def detect_manufacturer_pack_leak(line: Dict[str, Any], width: float, 
                                  bands: Dict[str, float]) -> bool:
    """Detect if manufacturer column contains pack tokens."""
    manuf_tokens = [t for t in line["tokens"] 
                   if bands["manuf_min"] <= (t["x0"]/width) <= bands["manuf_max"]]
    if not manuf_tokens:
        return False
    last = manuf_tokens[-1]["text"].strip()
    return bool(RE_PACK_ONLY.match(last) or RE_VOL.match(last) or 
                RE_VOL_ONE.match(last))


def validate_pdf(pdf_path: Path) -> Dict[str, Any]:
    """Validate PDF structure and generate report."""
    if not pdf_path.exists():
        return {"status": "error", "message": f"File not found: {pdf_path}"}

    with pdfplumber.open(str(pdf_path)) as pdf:
        total_pages = len(pdf.pages)
        if total_pages == 0:
            return {"status": "error", "message": "Empty PDF"}

        legend_detected = False
        pages_info = []
        pages_with_din = 0
        pages_with_headers = 0
        pages_rowshape_ok = 0
        flagged_pages = []

        for page_idx, page in enumerate(pdf.pages):
            width = float(page.width)
            bands, pack_hdr, unit_hdr = calibrate_column_bands(page, width)
            if pack_hdr or unit_hdr:
                pages_with_headers += 1

            # Detect legend in first 3 pages
            if page_idx < 3:
                words = page.extract_words() or []
                words = [clean_word_token(w) for w in words]
                raw_text = " ".join(w["text"] for w in words)
                raw_text = clean_extracted_text(raw_text, enforce_utf8=True)
                if any(x in strip_accents(raw_text).upper() 
                      for x in ["LEGEND", "LEGENDE"]) or "LÉGENDE" in raw_text.upper():
                    legend_detected = True

            lines = extract_page_lines(page)
            page_generics = din_rows = ok_rows = 0
            unit_only_count = manuf_pack_leaks = pack_cont_count = 0

            for line in lines:
                line_type = classify_line_type(line, width, bands)
                if line_type == "generic":
                    page_generics += 1
                elif line_type == "din_row":
                    din_rows += 1
                    right_tokens = [t for t in line["tokens"] 
                                   if (t["x0"]/width) >= bands["pack_min"]]
                    if any(RE_PRICE_NUM.match(t["text"].strip()) 
                          for t in right_tokens):
                        ok_rows += 1
                    if detect_manufacturer_pack_leak(line, width, bands):
                        manuf_pack_leaks += 1
                elif line_type == "pack_cont":
                    pack_cont_count += 1
                elif line_type == "unit_only":
                    unit_only_count += 1

            page_issues = []
            if din_rows > 0:
                pages_with_din += 1
                if (ok_rows >= max(1, int(0.6 * din_rows))) or unit_only_count > 0:
                    pages_rowshape_ok += 1
                else:
                    page_issues.append("Few prices detected for DIN rows")
                if not (pack_hdr and unit_hdr):
                    page_issues.append("Header calibration weak")
                if manuf_pack_leaks > 0:
                    page_issues.append(f"Manufacturer pack leaks ({manuf_pack_leaks})")
                if page_generics >= 3:
                    page_issues.append(f"Multiple generics ({page_generics})")

            if page_issues:
                flagged_pages.append({
                    "page": page_idx + 1,
                    "din_rows": din_rows,
                    "ok_rows": ok_rows,
                    "unit_only_lines": unit_only_count,
                    "pack_continuations": pack_cont_count,
                    "generics_detected": page_generics,
                    "headers_found": bool(pack_hdr or unit_hdr),
                    "issues": page_issues
                })

            pages_info.append({
                "page": page_idx + 1,
                "din_rows": din_rows,
                "ok_rows": ok_rows,
                "unit_only_lines": unit_only_count,
                "pack_continuations": pack_cont_count,
                "generics_detected": page_generics,
                "headers_found": bool(pack_hdr or unit_hdr)
            })

        # Calculate ratios
        din_pages = max(1, pages_with_din)
        headers_ratio = pages_with_headers / total_pages if total_pages else 0.0
        headers_ratio_on_din = pages_with_headers / din_pages if din_pages else 0.0
        rowshape_ratio = pages_rowshape_ok / din_pages if din_pages else 0.0
        flagged_ratio = len(flagged_pages) / din_pages if din_pages else 1.0

        # Determine fitness
        fit_reasons = []
        fit_to_run = True

        if not legend_detected:
            fit_to_run = False
            fit_reasons.append("Legend page not detected in first 3 pages")
        if pages_with_din < MIN_PAGES_WITH_DIN:
            fit_to_run = False
            fit_reasons.append(f"Too few DIN pages ({pages_with_din} < {MIN_PAGES_WITH_DIN})")
        if headers_ratio_on_din < MIN_HEADERS_RATIO:
            fit_reasons.append(f"Low header detection ({headers_ratio_on_din:.0%})")
        if rowshape_ratio < MIN_ROW_SHAPE_RATIO:
            fit_to_run = False
            fit_reasons.append(f"Weak price structure ({rowshape_ratio:.0%})")
        if flagged_ratio > MAX_FLAGGED_RATIO:
            fit_reasons.append(f"High flagged pages ({flagged_ratio:.0%})")
            if flagged_ratio > 0.65:
                fit_to_run = False

        summary = {
            "status": "ok",
            "input": str(pdf_path),
            "pages_total": total_pages,
            "pages_with_din": pages_with_din,
            "pages_with_headers": pages_with_headers,
            "pages_rowshape_ok": pages_rowshape_ok,
            "ratios": {
                "headers_over_all_pages": round(headers_ratio, 4),
                "headers_over_din_pages": round(headers_ratio_on_din, 4),
                "rowshape_over_din_pages": round(rowshape_ratio, 4),
                "flagged_over_din_pages": round(flagged_ratio, 4)
            },
            "legend_detected": legend_detected,
            "fit_to_run": fit_to_run,
            "fit_reasons": fit_reasons,
            "flagged_pages_count": len(flagged_pages)
        }
        
        return {"summary": summary, "pages_info": pages_info, 
                "flagged": flagged_pages}



def write_reports(result: Dict[str, Any], *, stem: str = "pdf") -> Dict[str, Path]:
    """Write validation reports to files.

    Returns a dict with written paths.
    """
    safe_stem = re.sub(r"[^a-zA-Z0-9_.-]+", "_", stem).strip("_") or "pdf"

    json_path = OUTPUT_DIR / f"{safe_stem}_pdf_structure_report.json"
    txt_path = OUTPUT_DIR / f"{safe_stem}_pdf_structure_report.txt"
    csv_path = OUTPUT_DIR / f"{safe_stem}_pdf_structure_flags.csv"

    # JSON report
    with open(json_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # Text report
    s = result["summary"]
    txt_lines = [
        f"Input: {s['input']}",
        f"Total pages: {s['pages_total']}",
        f"Legend detected: {s['legend_detected']}",
        f"Pages with DIN: {s['pages_with_din']}",
        f"Pages with headers: {s['pages_with_headers']}",
        f"Pages with row shape OK: {s['pages_rowshape_ok']}",
        f"Header ratio (all pages): {s['ratios']['headers_over_all_pages']:.2%}",
        f"Header ratio (DIN pages): {s['ratios']['headers_over_din_pages']:.2%}",
        f"Row shape ratio (DIN pages): {s['ratios']['rowshape_over_din_pages']:.2%}",
        f"Flagged ratio (DIN pages): {s['ratios']['flagged_over_din_pages']:.2%}",
        f"Fit to run: {s['fit_to_run']}",
        "",
        "Fit reasons:",
    ]
    for reason in s["fit_reasons"]:
        txt_lines.append(f"- {reason}")

    txt_lines.append("")
    txt_lines.append(f"Flagged pages ({s['flagged_pages_count']}):")
    for fp in result["flagged"]:
        txt_lines.append(f"- Page {fp['page']}: {', '.join(fp['issues'])}")

    with open(txt_path, "w", encoding="utf-8", errors="replace") as f:
        f.write("\n".join(txt_lines))

    # CSV flags
    import csv
    with open(csv_path, "w", newline="", encoding="utf-8", errors="replace") as f:
        w = csv.DictWriter(f, fieldnames=["page", "din_rows", "ok_rows", "issues"])
        w.writeheader()
        for fp in result["flagged"]:
            w.writerow({
                "page": fp["page"],
                "din_rows": fp["din_rows"],
                "ok_rows": fp["ok_rows"],
                "issues": ", ".join(fp["issues"])
            })

    return {"json": json_path, "txt": txt_path, "csv": csv_path}

def main() -> None:
    """Main validation function.

    If ANNEXE-split PDFs are present in output/split_pdf (annexe_iv1.pdf, annexe_iv2.pdf, annexe_v.pdf),
    validates each one and writes per-annex reports into output/qa/.

    Falls back to validating a single PDF if no split outputs exist.
    """
    # Prefer validating split ANNEXE PDFs if present
    split_pdfs = sorted(SPLIT_PDF_DIR.glob("annexe_*.pdf"))

    # If step_02 wrote an index.json, use it to get exact filenames and ordering
    index_path = SPLIT_PDF_DIR / "index.json"
    annex_plan = None
    if index_path.exists():
        try:
            with open(index_path, "r", encoding="utf-8", errors="replace") as f:
                annex_plan = json.load(f).get("annexes")
        except Exception:
            annex_plan = None

    results_by_annex: Dict[str, Any] = {}

    if annex_plan:
        # annex_plan keys like "IV.1", "IV.2", "V"
        for annex_key, info in annex_plan.items():
            pdf_path = Path(info.get("output_file", ""))
            if not pdf_path.exists():
                # try relative to split dir
                pdf_path = SPLIT_PDF_DIR / Path(info.get("output_file", "")).name
            if not pdf_path.exists():
                results_by_annex[annex_key] = {"status": "error", "message": f"Missing split PDF for {annex_key}"}
                continue

            result = validate_pdf(pdf_path)
            results_by_annex[annex_key] = result
            if result.get("status") != "error":
                write_reports(result, stem=f"annexe_{annex_key.replace('.', '_')}")
    elif split_pdfs:
        for pdf_path in split_pdfs:
            stem = pdf_path.stem  # e.g. annexe_iv1
            result = validate_pdf(pdf_path)
            results_by_annex[stem] = result
            if result.get("status") != "error":
                write_reports(result, stem=stem)
    else:
        # Single-PDF fallback (previous behavior)
        pdf_path = (INPUT_PDF if INPUT_PDF.exists() else next(SPLIT_PDF_DIR.glob("*.pdf"), None))
        if not pdf_path:
            pdf_path = next(INPUT_DIR.glob("*.pdf"), None)
        if not pdf_path:
            print(json.dumps({"status": "error", "message": f"No PDF found in {SPLIT_PDF_DIR} or {INPUT_DIR}"}))
            return

        result = validate_pdf(pdf_path)
        if result.get("status") == "error":
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return

        write_reports(result, stem=pdf_path.stem)
        print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
        return

    # Write a combined summary for ANNEXE mode
    summary_path = OUTPUT_DIR / "pdf_structure_summary.json"
    combined = {
        "status": "ok",
        "mode": "annexe_split",
        "results": {k: (v.get("summary", v) if isinstance(v, dict) else v) for k, v in results_by_annex.items()},
    }
    with open(summary_path, "w", encoding="utf-8", errors="replace") as f:
        json.dump(combined, f, ensure_ascii=False, indent=2)

    print(json.dumps(combined, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
