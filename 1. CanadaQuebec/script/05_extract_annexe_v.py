# -*- coding: utf-8 -*-
"""
ANNEXE V Extractor (FIXED VERSION)

FIXES APPLIED:
1) [OK] START_PAGE_1IDX = 1 (was 6) - Now starts from page 1, no data loss
2) [OK] MAX_ROWS configurable via env var, default None (was 500) - No artificial limit
3) [OK] Integrated encoding utilities for UTF-8/mojibake fixes
4) [OK] Improved error handling and logging
5) [OK] Consistent BASE_DIR path handling
6) [OK] Better parsing logic with validation

Original fixes preserved:
- Ex Factory Wholesale Price = COÛT DU FORMAT (NOT PRIX UNITAIRE)
- Marketing Authority (manufacturer) excludes format/price tokens
- Fill Size extraction from format field
- UTF-8-SIG encoding for Excel compatibility

Output columns:
Generic Name, Currency, Ex Factory Wholesale Price, Unit Price, Region,
Product Group, Marketing Authority, Local Pack Description, Formulation,
Fill Size, Strength, Strength Unit, LOCAL_PACK_CODE
"""

from pathlib import Path
import os
import re
import csv
import logging
import unicodedata
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import pdfplumber

# Try to import encoding utilities (if available)
try:
    from step_00_utils_encoding import clean_extracted_text, csv_writer_utf8, csv_reader_utf8
    ENCODING_UTILS_AVAILABLE = True
except ImportError:
    # Fallback if encoding utils not available
    ENCODING_UTILS_AVAILABLE = False
    import io
    def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
        if not text:
            return ""
        return unicodedata.normalize('NFC', str(text))
    def csv_writer_utf8(file_path, add_bom=True):
        encoding = 'utf-8-sig' if add_bom else 'utf-8'
        return io.open(file_path, 'w', encoding=encoding, newline='', errors='replace')
    def csv_reader_utf8(file_path):
        return io.open(file_path, 'r', encoding='utf-8-sig', newline='', errors='replace')

# =========================
# CONFIGURATION
# =========================
# Base directory - consistent with other pipeline scripts
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
try:
    from config_loader import (
        get_base_dir, get_split_pdf_dir, get_csv_output_dir, get_input_dir,
        ANNEXE_V_PDF_NAME, ANNEXE_V_CSV_NAME, LOG_FILE_ANNEXE_V,
        STATIC_CURRENCY, STATIC_REGION,
        X_TOL, Y_TOL,
        ANNEXE_V_START_PAGE_1IDX, ANNEXE_V_MAX_ROWS
    )
    BASE_DIR = get_base_dir()
    INPUT_DIR = get_split_pdf_dir()
    OUTPUT_DIR = get_csv_output_dir()
    START_PAGE_1IDX = ANNEXE_V_START_PAGE_1IDX
    MAX_ROWS = ANNEXE_V_MAX_ROWS
except ImportError:
    # Fallback to original values if config_loader not available
    BASE_DIR = Path(__file__).resolve().parents[1]
    INPUT_DIR = BASE_DIR / "output" / "split_pdf"
    OUTPUT_DIR = BASE_DIR / "output" / "csv"
    STATIC_CURRENCY = "CAD"
    STATIC_REGION = "NORTH AMERICA"
    X_TOL = 1.0
    Y_TOL = 1.6
    START_PAGE_1IDX = 1
    MAX_ROWS_ENV = os.environ.get("ANNEXE_V_MAX_ROWS", "").strip()
    MAX_ROWS = int(MAX_ROWS_ENV) if MAX_ROWS_ENV and MAX_ROWS_ENV.isdigit() else None
    ANNEXE_V_PDF_NAME = "annexe_v.pdf"
    ANNEXE_V_CSV_NAME = "annexe_v_extracted.csv"
    LOG_FILE_ANNEXE_V = "annexe_v_extraction_log.txt"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input PDF - prefer split PDF, fallback to direct path
INPUT_PDF = INPUT_DIR / ANNEXE_V_PDF_NAME
if not INPUT_PDF.exists():
    # Try alternative locations
    try:
        import sys
        script_path = Path(__file__).resolve().parent
        sys.path.insert(0, str(script_path))
        from config_loader import get_input_dir
        input_dir = get_input_dir()
    except ImportError:
        input_dir = BASE_DIR / "input"
    alt_paths = [
        input_dir / ANNEXE_V_PDF_NAME,
        Path(__file__).resolve().parent / ANNEXE_V_PDF_NAME,
    ]
    for alt in alt_paths:
        if alt.exists():
            INPUT_PDF = alt
            break

OUTPUT_CSV = OUTPUT_DIR / ANNEXE_V_CSV_NAME
LOG_FILE = OUTPUT_DIR / LOG_FILE_ANNEXE_V

FINAL_COLS = [
    "Generic Name",
    "Currency",
    "Ex Factory Wholesale Price",   # COÛT DU FORMAT (pack/format price)
    "Unit Price",                    # PRIX UNITAIRE
    "Region",
    "Product Group",                 # MARQUE DE COMMERCE (brand)
    "Marketing Authority",           # FABRICANT (manufacturer)
    "Local Pack Description",
    "Formulation",
    "Fill Size",
    "Strength",
    "Strength Unit",
    "LOCAL_PACK_CODE",
]

# =========================
# EXTRACTION TUNING
# =========================
# X_TOL and Y_TOL are loaded from config_loader above

# =========================
# REGEX PATTERNS
# =========================
RE_DIN = re.compile(r"^\d{6,9}$")

# FORMAT tokens
RE_PACK_ONLY = re.compile(r"^\d{1,4}$")                 # 60, 100, 1, 10...
RE_VOL = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s?(mL|ml|L)$", re.I)  # "1 ml", "0,8 mL" (single token)
RE_VOL_TWO = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s+(mL|ml|L)$", re.I)  # "1 ml" (two tokens)

# Headers/context
RE_ALLCAPS = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+:?$")
RE_FORM_WORD = re.compile(
    r"(?i)\b(Sol\.|Pd\.|Perf\.|Susp\.|Caps\.|Comp\.|Gel\.|Crème|Gouttes|I\.V\.|I\.M\.|S\.C\.|Orale)\b"
)
RE_PPB = re.compile(r"\bPPB\b", re.IGNORECASE)
RE_STRENGTH = re.compile(r"(?i)(\d+(?:[.,]\d+)?)\s*(mg|g|mcg|µg|U|UI|IU)\s*(?:/|$|\s)")

HDR_WORDS = {"CODE", "MARQUE", "FABRICANT", "FORMAT", "COUT", "COÛT", "PRIX", "UNITAIRE"}

# =========================
# UTILITY FUNCTIONS
# =========================
def norm_spaces(s: str) -> str:
    """Normalize spaces and non-breaking spaces."""
    return (s or "").replace("\u00A0", " ").strip()

def strip_acc(s: str) -> str:
    """Remove accents for case-insensitive matching."""
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def upper_key(s: str) -> str:
    """Normalize to uppercase without accents."""
    return strip_acc(norm_spaces(s)).upper()

def french_to_float(s: str) -> Optional[float]:
    """
    Converts '4,04' or '▶ 4,04' to 4.04
    Handles French decimal comma format.
    """
    if s is None:
        return None
    t = norm_spaces(s)
    t = re.sub(r"[^\d,.\s]", "", t)  # remove arrows/symbols
    t = re.sub(r"\s+", "", t)
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None

def tokens_to_text(tokens: List[Dict[str, Any]]) -> str:
    """Convert token list to cleaned text string."""
    tokens = sorted(tokens, key=lambda x: x["x0"])
    text = " ".join(t["text"] for t in tokens)
    # Apply encoding cleanup
    return clean_extracted_text(text, enforce_utf8=True)

def page_to_lines(page) -> List[Dict[str, Any]]:
    """Extract lines from PDF page with proper encoding."""
    words = page.extract_words(x_tolerance=X_TOL, y_tolerance=Y_TOL, keep_blank_chars=False) or []
    if not words:
        return []
    
    # Clean word text using encoding utilities
    for w in words:
        if "text" in w:
            w["text"] = clean_extracted_text(w["text"], enforce_utf8=True)
    
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))

    lines = []
    cur: List[Dict[str, Any]] = []
    cur_top: Optional[float] = None

    def push():
        cur.sort(key=lambda z: z["x0"])
        lines.append({
            "top": min(t["top"] for t in cur),
            "tokens": cur[:],
            "text": tokens_to_text(cur)
        })

    for w in words:
        if cur_top is None:
            cur_top = w["top"]
            cur = [w]
            continue
        if abs(w["top"] - cur_top) <= Y_TOL:
            cur.append(w)
        else:
            push()
            cur_top = w["top"]
            cur = [w]
    if cur:
        push()
    return lines

# =========================
# CONTEXT DETECTION
# =========================
def is_generic_header(text: str) -> bool:
    """Detect if line is a generic drug name header."""
    s = norm_spaces(text).rstrip(":")
    if not s:
        return False
    if re.match(r"^\d", s):  # avoid "4:04" etc.
        return False
    up = upper_key(s)
    if any(w in up for w in HDR_WORDS):
        return False
    return bool(RE_ALLCAPS.match(s))

def is_form_line(text: str) -> bool:
    """Detect if line contains formulation information."""
    t = norm_spaces(text)
    up = upper_key(t)
    if any(w in up for w in HDR_WORDS):
        return False
    return bool(RE_FORM_WORD.search(t)) or bool(RE_PPB.search(t)) or bool(RE_STRENGTH.search(t))

def parse_form_strength(text: str) -> Tuple[str, str, Optional[float], Optional[str]]:
    """
    Parse form line to extract:
    - Local Pack Description = full form line (PPB removed)
    - Formulation = before first digit
    - Strength = value+unit BEFORE '/'
    """
    local_desc = norm_spaces(text)
    local_desc = re.sub(r"\bPPB\b", "", local_desc, flags=re.I).strip()

    m_digit = re.search(r"\d", local_desc)
    formulation = (local_desc[:m_digit.start()].strip().rstrip(".,;:") if m_digit else local_desc.rstrip(".,;:"))

    strength_val = None
    strength_unit = None
    m = RE_STRENGTH.search(local_desc)
    if m:
        val_s = m.group(1).replace(",", ".")
        unit_raw = m.group(2).upper().replace("µG", "MCG")
        unit_map = {"MG": "MG", "G": "G", "MCG": "MCG", "U": "U", "UI": "U", "IU": "IU"}
        strength_unit = unit_map.get(unit_raw, unit_raw)
        try:
            v = float(val_s)
            strength_val = int(v) if float(v).is_integer() else v
        except Exception:
            pass

    return local_desc, formulation, strength_val, strength_unit

def fill_size_from_format(fmt: Optional[str]) -> Optional[float]:
    """
    Extract fill size from format field:
    - "1 ml" => 1
    - "0.8 mL" => 0.8
    - "60" => 60
    """
    if not fmt:
        return None
    f = norm_spaces(fmt)
    m = re.search(r"(\d+(?:[.,]\d+)?)", f)
    if not m:
        return None
    val = float(m.group(1).replace(",", "."))
    return int(val) if val.is_integer() else val

# =========================
# DIN ROW PARSING HELPERS
# =========================
def find_din_token_idx(tokens: List[Dict[str, Any]]) -> Optional[int]:
    """
    Find DIN token index (may not be at index 0 due to table borders).
    Returns leftmost DIN-like token on the line.
    """
    candidates = []
    for i, t in enumerate(tokens):
        s = t["text"].strip()
        if RE_DIN.match(s):
            candidates.append((t["x0"], i))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

def find_leftmost_price_x(tokens: List[Dict[str, Any]]) -> Optional[float]:
    """Find leftmost x-coordinate of price-like tokens."""
    px = None
    for t in tokens:
        v = french_to_float(t["text"])
        if v is None:
            continue
        if px is None or t["x0"] < px:
            px = t["x0"]
    return px

def find_format_token(tokens_sorted: List[Dict[str, Any]], leftmost_price_x: Optional[float]) -> Tuple[Optional[str], Optional[float]]:
    """
    Find FORMAT token (must appear BEFORE the leftmost price).
    Handles "1 ml" as two tokens ("1" + "ml") or one token ("1 ml").
    Returns (format_text, format_start_x).
    """
    toks = tokens_sorted

    for i in range(len(toks)):
        # stop when we reach prices area
        if leftmost_price_x is not None and toks[i]["x0"] >= leftmost_price_x:
            break

        a = norm_spaces(toks[i]["text"])

        # 2-token volume (common): "1" + "ml"
        if i + 1 < len(toks):
            b = norm_spaces(toks[i + 1]["text"])
            cand2 = norm_spaces(a + " " + b)
            if RE_VOL_TWO.match(cand2):
                return cand2, toks[i]["x0"]

        # single token format
        if RE_VOL.match(a) or RE_PACK_ONLY.match(a):
            return a, toks[i]["x0"]

    return None, None

def parse_line_format_cost(after_tokens: List[Dict[str, Any]]) -> List[Tuple[Optional[str], Optional[float], Optional[float]]]:
    """
    Parse ONE physical line into (FORMAT, COST, UNIT_PRICE).

    Annexe V column order:
      ... | FORMAT | COÛT DU FORMAT | PRIX UNITAIRE

    Key nuance:
      FORMAT is often a *plain integer* (e.g., "10") which would otherwise look numeric.
      Prices almost always include decimals (comma/dot) and/or have >=3 digits.
    """
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return []

    def is_price_like(txt: str) -> bool:
        """Identify price-like tokens (have decimals or >=3 digits).
        Handles arrow characters and other symbols that might be attached to prices.
        """
        txt = norm_spaces(txt)
        if not txt:
            return False
        # Clean text by removing arrows and symbols BEFORE checking (e.g., "58,95▶" -> "58,95")
        # This is important because prices in the PDF often have arrows attached
        cleaned_txt = re.sub(r"[^\d,.\s]", "", txt)  # Remove arrows/symbols like ▶
        cleaned_txt = cleaned_txt.strip()
        if not cleaned_txt:
            return False
        v = french_to_float(cleaned_txt)
        if v is None:
            return False
        # Prices ALWAYS have decimal separators (comma or dot) in annexe V
        # This is the primary indicator - prices like "58,95", "0,5895" have commas
        # Format tokens like "100" do NOT have decimals, so they won't match
        has_decimal = ("," in cleaned_txt) or ("." in cleaned_txt)
        
        # Primary rule: Must have decimals to be a price
        if has_decimal:
            return True
        
        # Fallback: For numbers without decimals, be very strict
        # Only consider if it's clearly a price (not format, not DIN)
        digits = re.sub(r"\D", "", cleaned_txt)
        if len(digits) >= 4:  # At least 4 digits (formats are usually 1-3 digits)
            # Must be in reasonable price range AND not look like a format/DIN
            if 10 <= v <= 10000:  # Reasonable price range, excluding formats (1-1000)
                return True
        
        return False

    # Find format token first - it helps anchor where prices start
    # Try to find format token by looking for it before any prices
    fmt = None
    fmt_x = None
    leftmost_price_x = None
    
    # First pass: find leftmost price to help locate format
    # Try both original text and cleaned text (in case arrows are attached)
    for t in toks:
        txt = norm_spaces(t["text"])
        # Check original text
        if is_price_like(txt):
            leftmost_price_x = t["x0"] if leftmost_price_x is None else min(leftmost_price_x, t["x0"])
        else:
            # Also try cleaning arrows/symbols first
            cleaned = re.sub(r"[^\d,.\s]", "", txt).strip()
            if cleaned and is_price_like(cleaned):
                leftmost_price_x = t["x0"] if leftmost_price_x is None else min(leftmost_price_x, t["x0"])
    
    # Find format token (should be before prices)
    if leftmost_price_x is not None:
        fmt, fmt_x = find_format_token(toks, leftmost_price_x)
    
    # Price region starts at the leftmost price-like token
    price_x = leftmost_price_x

    # If we couldn't find any price-like tokens, we can't parse prices
    if price_x is None:
        return []
    
    # If format not found, we can still try to extract prices
    # Format is helpful but not strictly required - continue even if format not found
    # We'll extract prices regardless

    # Collect only price-like values (with decimals), excluding the format token
    # Important: Collect ALL price-like tokens after price_x, not just the first one
    nums: List[float] = []
    price_tokens_found: List[Tuple[float, str]] = []  # (x0, text) for debugging
    
    i = 0
    while i < len(toks):
        t = toks[i]
        if t["x0"] < price_x:
            i += 1
            continue
        # Check if this token is price-like, or if it's an arrow/symbol following a price
        txt = norm_spaces(t["text"])
        
        # Try combining with next token if current token looks like it might be part of a price
        # This handles cases where arrow is separate token: "58,95" + "▶"
        combined_txt = txt
        skip_next = False
        if i + 1 < len(toks) and toks[i + 1]["x0"] >= price_x:
            next_txt = norm_spaces(toks[i + 1]["text"])
            # If next token is just symbols/arrows, combine them
            if re.match(r"^[▶→>►■▪•\s]+$", next_txt):
                combined_txt = txt + next_txt
                skip_next = True
        
        if is_price_like(combined_txt):
            v = french_to_float(combined_txt)
            if v is not None:
                nums.append(v)
                price_tokens_found.append((t["x0"], combined_txt))
                # Skip next token if we combined it
                if skip_next:
                    i += 2
                    continue
        
        i += 1

    # Ensure we capture both cost and unit price
    # Cost is first price, Unit price is second price
    cost = nums[0] if len(nums) >= 1 else None
    unit = nums[1] if len(nums) >= 2 else None
    
    # If we only found one price, it might be that unit price is missing or on next line
    # But we should still return what we found
    return [(fmt, cost, unit)]

def brand_and_manufacturer_from_after_tokens(after_tokens: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Split MARQUE DE COMMERCE (brand) and FABRICANT (manufacturer) from tokens after DIN.

    Strategy:
    - Compute FORMAT start-x (it anchors the right boundary of the left-side text columns).
    - Consider tokens strictly left of FORMAT as "text columns" (brand + manufacturer).
    - Find the *largest horizontal gap* between consecutive tokens in that region and split there.
    """
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return (None, None)

    left_price_x = find_leftmost_price_x(toks)
    _fmt, fmt_x = find_format_token(toks, left_price_x)

    # If we can't anchor by FORMAT, fall back to coarse column ranges
    if fmt_x is None:
        xmax = toks[-1]["x0"] if toks else 1.0
        if xmax == 0:
            xmax = 1.0
        brand_toks = [t for t in toks if (t["x0"] / xmax) <= 0.45]
        manuf_toks = [t for t in toks if 0.45 < (t["x0"] / xmax) <= 0.70]
        brand = tokens_to_text(brand_toks).strip() or None
        manu = tokens_to_text(manuf_toks).strip() or None
        return (brand, manu)

    text_toks = [t for t in toks if t["x0"] < fmt_x]
    if not text_toks:
        return (None, None)

    text_toks = sorted(text_toks, key=lambda t: t["x0"])

    # Find the biggest gap between consecutive tokens; that's likely the column boundary
    gaps = []
    for a, b in zip(text_toks, text_toks[1:]):
        gap = b["x0"] - a["x1"]
        gaps.append(gap)

    split_idx = None
    if gaps:
        max_gap = max(gaps)
        # Threshold tuned for pdfplumber points; adjust if needed
        if max_gap >= 10:
            split_idx = gaps.index(max_gap) + 1

    if split_idx is None:
        # No clear column gap: manufacturer is usually the last 1-2 tokens before FORMAT
        manu_toks = text_toks[-2:] if len(text_toks) >= 2 else text_toks[-1:]
        brand_toks = text_toks[:-len(manu_toks)] if len(text_toks) > len(manu_toks) else []
    else:
        brand_toks = text_toks[:split_idx]
        manu_toks = text_toks[split_idx:]

    brand = tokens_to_text(brand_toks).strip() or None
    manu = tokens_to_text(manu_toks).strip() or None
    return (brand, manu)


# =========================
# POST-PROCESSING: CLEAN FOOTER VALUES
# =========================
def clean_footer_values_from_csv(csv_path: Path, logger: logging.Logger) -> int:
    """
    Remove rows where Ex Factory Wholesale Price contains footer date values.
    Footer values are 6-digit dates in YYYYMM format (202508 = 2025-08, 202509 = 2025-09, etc.)
    
    Returns number of rows removed.
    """
    if not csv_path.exists():
        return 0
    
    # Read all rows
    rows = []
    removed_count = 0
    
    try:
        with csv_reader_utf8(csv_path) as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            
            for row in reader:
                price_val = row.get("Ex Factory Wholesale Price", "")
                
                # Check if price is a 6-digit date (footer value)
                is_footer_date = False
                
                if price_val:
                    # Convert to string and check patterns
                    price_str = str(price_val).strip()
                    
                    # Only check for 6-digit dates (YYYYMM format)
                    if price_str.isdigit() and len(price_str) == 6:
                        try:
                            year = int(price_str[:4])
                            month = int(price_str[4:6])
                            # Check if it's a valid date in reasonable range (2020-2099)
                            if 2020 <= year <= 2099 and 1 <= month <= 12:
                                is_footer_date = True
                                logger.info(f"Removed row with footer date in price: {price_str} (YYYYMM={year}-{month:02d}, DIN: {row.get('LOCAL_PACK_CODE', 'N/A')})")
                        except (ValueError, IndexError):
                            # Not a valid date format, keep the row
                            pass
                
                if not is_footer_date:
                    rows.append(row)
                else:
                    removed_count += 1
        
        # Write cleaned rows back
        if removed_count > 0:
            with csv_writer_utf8(csv_path, add_bom=True) as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            
            logger.info(f"Cleaned CSV: Removed {removed_count} rows with 6-digit date footer values")
        
        return removed_count
        
    except Exception as e:
        logger.error(f"Error cleaning footer values from CSV: {e}", exc_info=True)
        return 0


# =========================
# MAIN EXTRACTION FUNCTION
# =========================
def extract_annexe_v():
    """Main extraction function with proper error handling and logging."""
    # Setup logging
    logging.basicConfig(
        filename=str(LOG_FILE),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 80)
    logger.info("ANNEXE V EXTRACTION STARTED")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Input PDF: {INPUT_PDF}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info(f"Start page (1-indexed): {START_PAGE_1IDX}")
    logger.info(f"Max rows limit: {MAX_ROWS if MAX_ROWS else 'None (unlimited)'}")
    logger.info("=" * 80)

    if not INPUT_PDF.exists():
        error_msg = f"Input PDF not found: {INPUT_PDF}"
        logger.error(error_msg)
        raise SystemExit(error_msg)

    total_rows = 0
    pages_processed = 0
    errors_encountered = 0

    try:
        with pdfplumber.open(str(INPUT_PDF)) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"Total pages in PDF: {total_pages}")
            
            if START_PAGE_1IDX > total_pages:
                error_msg = f"START_PAGE_1IDX ({START_PAGE_1IDX}) exceeds total pages ({total_pages})"
                logger.error(error_msg)
                raise SystemExit(error_msg)

            with csv_writer_utf8(OUTPUT_CSV, add_bom=True) as f:
                writer = csv.DictWriter(f, fieldnames=FINAL_COLS)
                writer.writeheader()

                current_generic: Optional[str] = None
                current_formline: Optional[str] = None

                for page_no_1idx in range(START_PAGE_1IDX, total_pages + 1):
                    try:
                        page = pdf.pages[page_no_1idx - 1]
                        lines = page_to_lines(page)

                        i = 0
                        page_rows = 0
                        
                        while i < len(lines):
                            try:
                                line = lines[i]
                                text = line["text"]

                                # Generic header
                                if is_generic_header(text):
                                    g = text.rstrip(":").strip()
                                    g = re.sub(r"\s+[A-Z]$", "", g).strip()  # remove trailing marker like " X"
                                    current_generic = clean_extracted_text(g, enforce_utf8=True) if g else current_generic
                                    current_formline = None
                                    i += 1
                                    continue

                                # Formline (only if NOT a DIN row)
                                din_idx = find_din_token_idx(line["tokens"])
                                if din_idx is None and is_form_line(text):
                                    current_formline = clean_extracted_text(text.strip(), enforce_utf8=True)
                                    i += 1
                                    continue

                                # DIN row
                                if din_idx is not None:
                                    din_raw = line["tokens"][din_idx]["text"].strip()
                                    din = din_raw.zfill(8)

                                    # everything after DIN
                                    after_tokens = sorted(line["tokens"][din_idx + 1:], key=lambda t: t["x0"])

                                    product_group, manufacturer = brand_and_manufacturer_from_after_tokens(after_tokens)

                                    # parse (format, cost) from main line
                                    fmts: List[Optional[str]] = []
                                    costs: List[Optional[float]] = []
                                    units: List[Optional[float]] = []
                                    
                                    for fmt, cost, unit in parse_line_format_cost(after_tokens):
                                        fmts.append(fmt)
                                        if cost is not None:
                                            costs.append(cost)
                                        if unit is not None:
                                            units.append(unit)

                                    # Continuations until next DIN or next generic header
                                    j = i + 1
                                    while j < len(lines):
                                        nxt = lines[j]
                                        nxt_text = nxt["text"]

                                        if is_generic_header(nxt_text):
                                            break
                                        if find_din_token_idx(nxt["tokens"]) is not None:
                                            break

                                        # parse continuation line as if it were a format/cost line
                                        pairs = parse_line_format_cost(nxt["tokens"])
                                        for fmt, cost, unit in pairs:
                                            if fmt is not None:
                                                fmts.append(fmt)
                                            if cost is not None:
                                                costs.append(cost)
                                            if unit is not None:
                                                units.append(unit)

                                        j += 1

                                    # Context fields
                                    local_desc, formulation, strength_val, strength_unit = ("", "", None, None)
                                    if current_formline:
                                        local_desc, formulation, strength_val, strength_unit = parse_form_strength(current_formline)

                                    # Pair format/cost lists
                                    n = max(len(fmts), len(costs), 1)
                                    for k in range(n):
                                        fmt_k = fmts[k] if k < len(fmts) else None
                                        cost_k = costs[k] if k < len(costs) else (costs[0] if len(costs) == 1 else None)

                                        row = {
                                            "Generic Name": current_generic,
                                            "Currency": STATIC_CURRENCY,
                                            "Ex Factory Wholesale Price": cost_k,
                                            "Unit Price": (units[k] if k < len(units) else (units[0] if len(units) == 1 else None)),
                                            "Region": STATIC_REGION,
                                            "Product Group": product_group,
                                            "Marketing Authority": manufacturer,
                                            "Local Pack Description": local_desc,
                                            "Formulation": formulation,
                                            "Fill Size": fill_size_from_format(fmt_k),
                                            "Strength": strength_val,
                                            "Strength Unit": strength_unit,
                                            "LOCAL_PACK_CODE": din,
                                        }
                                        
                                        writer.writerow(row)
                                        total_rows += 1
                                        page_rows += 1

                                        if MAX_ROWS is not None and total_rows >= MAX_ROWS:
                                            logger.warning(f"MAX_ROWS limit reached ({MAX_ROWS}). Stopping early.")
                                            print(f"\n[WARN] MAX_ROWS reached ({MAX_ROWS}). Stopping early for test.")
                                            print(f"[OK] Saved: {OUTPUT_CSV}")
                                            logger.info(f"Extraction stopped at {total_rows} rows due to MAX_ROWS limit")
                                            return

                                    i = j
                                    continue

                                i += 1
                                
                            except Exception as e:
                                errors_encountered += 1
                                logger.warning(f"Error processing line {i} on page {page_no_1idx}: {e}")
                                i += 1  # Continue to next line
                                continue

                        pages_processed += 1
                        if page_no_1idx % 50 == 0 or page_rows > 0:
                            print(f"Page {page_no_1idx}/{total_pages} done | rows so far: {total_rows:,}")
                            logger.info(f"Page {page_no_1idx}/{total_pages} processed | rows: {total_rows:,}")
                            
                    except Exception as e:
                        errors_encountered += 1
                        logger.error(f"Error processing page {page_no_1idx}: {e}", exc_info=True)
                        print(f"[WARN] Error on page {page_no_1idx}: {e}")
                        continue

    except Exception as e:
        logger.error(f"Fatal error during extraction: {e}", exc_info=True)
        raise

    logger.info("=" * 80)
    logger.info("EXTRACTION COMPLETED")
    logger.info(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total rows extracted: {total_rows:,}")
    logger.info(f"Pages processed: {pages_processed}/{total_pages}")
    logger.info(f"Errors encountered: {errors_encountered}")
    logger.info("=" * 80)

    print(f"\n[OK] Done. Total rows: {total_rows:,}")
    print(f"[OK] Pages processed: {pages_processed}/{total_pages}")
    if errors_encountered > 0:
        print(f"[WARN] Errors encountered: {errors_encountered}")
    
    # Post-processing: Remove rows with footer values in price column
    print(f"\n[CLEAN] Cleaning footer values from price column...")
    cleaned_count = clean_footer_values_from_csv(OUTPUT_CSV, logger)
    print(f"[OK] Removed {cleaned_count} rows with footer values (dates/identifiers)")
    
    print(f"[OK] Saved: {OUTPUT_CSV}")
    print(f"[OK] Log file: {LOG_FILE}")


if __name__ == "__main__":
    extract_annexe_v()

