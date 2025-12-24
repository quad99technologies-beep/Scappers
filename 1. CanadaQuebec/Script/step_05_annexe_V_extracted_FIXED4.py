# -*- coding: utf-8 -*-
"""
ANNEXE V (page 6 -> end) extractor (END-TO-END)

Fixes based on your issues:
1) Ex Factory Wholesale Price = COÛT DU FORMAT (NOT PRIX UNITAIRE)
   - We parse: FORMAT -> first numeric AFTER format = COST
   - This prevents unit price (e.g., 0,7203) from being used
   - Prevents token-splitting issues where "0,7203" becomes "... 3" (no more Fill Size=3)
2) Marketing Authority (manufacturer) must NOT include "1 ml", "60", "43,22" etc.
   - We find the FORMAT start-x and only take manufacturer tokens LEFT of it
3) Fill Size rule per your request:
   - For "1 ml" => Fill Size = 1
   - For "0.8 mL" => Fill Size = 0.8
   - For "60" => Fill Size = 60
4) MAX_ROWS limit for testing
5) Writes UTF-8-SIG so Excel shows Oméga correctly

Output columns:
Generic Name, Currency, Ex Factory Wholesale Price, Region, Marketing Authority,
Local Pack Description, Formulation, Fill Size, Strength, Strength Unit, LOCAL_PACK_CODE
"""

from pathlib import Path
import os
import re, csv, unicodedata
from typing import List, Dict, Any, Optional, Tuple

import pdfplumber

# =========================
# CONFIG (EDIT PATHS)
# =========================
# You can override paths via environment variables:
# - SCRAPER_BASE_DIR : root folder for inputs/outputs (defaults to script folder)
# - ANNEXE_V_PDF     : full path to annexe_v.pdf (defaults to <base>/output/split_pdf/annexe_v.pdf)
#
# Examples (Windows PowerShell):
#   $env:SCRAPER_BASE_DIR="D:\quad99\Scappers\1. CanadaQuebec"
#   $env:ANNEXE_V_PDF="D:\quad99\Scappers\1. CanadaQuebec\output\split_pdf\annexe_v.pdf"
#
BASE_DIR = Path(os.environ.get("SCRAPER_BASE_DIR", Path(__file__).resolve().parent))

# Prefer explicit override, else default under base dir, else fall back to script folder.
INPUT_PDF = Path(os.environ.get("ANNEXE_V_PDF", "")) if os.environ.get("ANNEXE_V_PDF") else (BASE_DIR / "output" / "split_pdf" / "annexe_v.pdf")
if not INPUT_PDF.exists():
    alt = Path(__file__).resolve().parent / "annexe_v.pdf"
    if alt.exists():
        INPUT_PDF = alt

OUTPUT_DIR = BASE_DIR / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTPUT_DIR / "annexe_v_page6_to_end_FINAL.csv"

START_PAGE_1IDX = 6            # page 6 -> end
MAX_ROWS = 500                 # <<< TEST LIMIT. Set None for full extraction.

STATIC_CURRENCY = "CAD"
STATIC_REGION = "NORTH AMERICA"

FINAL_COLS = [
    "Generic Name",
    "Currency",
    "Ex Factory Wholesale Price",   # COÛT DU FORMAT (pack/format price)
    "Unit Price",                  # PRIX UNITAIRE
    "Region",
    "Product Group",               # MARQUE DE COMMERCE (brand)
    "Marketing Authority",         # FABRICANT (manufacturer)
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
X_TOL = 1.0
Y_TOL = 1.6

# =========================
# REGEX
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
# UTILS
# =========================
def norm_spaces(s: str) -> str:
    return (s or "").replace("\u00A0", " ").strip()

def strip_acc(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def upper_key(s: str) -> str:
    return strip_acc(norm_spaces(s)).upper()

def french_to_float(s: str) -> Optional[float]:
    """
    Converts '4,04' or '▶ 4,04' to 4.04
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
    tokens = sorted(tokens, key=lambda x: x["x0"])
    return norm_spaces(" ".join(t["text"] for t in tokens))

def page_to_lines(page) -> List[Dict[str, Any]]:
    words = page.extract_words(x_tolerance=X_TOL, y_tolerance=Y_TOL, keep_blank_chars=False) or []
    if not words:
        return []
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))

    lines = []
    cur: List[Dict[str, Any]] = []
    cur_top: Optional[float] = None

    def push():
        cur.sort(key=lambda z: z["x0"])
        lines.append({"top": min(t["top"] for t in cur), "tokens": cur[:], "text": tokens_to_text(cur)})

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
    t = norm_spaces(text)
    up = upper_key(t)
    if any(w in up for w in HDR_WORDS):
        return False
    return bool(RE_FORM_WORD.search(t)) or bool(RE_PPB.search(t)) or bool(RE_STRENGTH.search(t))

def parse_form_strength(text: str) -> Tuple[str, str, Optional[float], Optional[str]]:
    """
    Local Pack Description = full form line (PPB removed)
    Formulation = before first digit
    Strength = value+unit BEFORE '/' (your rule)
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
    Per your request:
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
# DIN ROW HELPERS
# =========================
def find_din_token_idx(tokens: List[Dict[str, Any]]) -> Optional[int]:
    """
    DIN token may not be at index 0 due to table borders.
    Pick the leftmost DIN-like token on the line.
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
    FORMAT must appear BEFORE the leftmost price on the line.
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

    Approach:
      1) Find the left edge of the *price region* using a stricter "price-like" rule.
      2) Find FORMAT before that region.
      3) Take the first 2 numeric values inside the price region as (COST, UNIT_PRICE).
    """
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return []

    def is_price_like(txt: str) -> bool:
        txt = norm_spaces(txt)
        v = french_to_float(txt)
        if v is None:
            return False
        digits = re.sub(r"\D", "", txt)
        return ("," in txt) or ("." in txt) or (len(digits) >= 3)

    # Price region starts at the leftmost price-like token (skip FORMAT like "10")
    price_x = None
    for t in toks:
        if is_price_like(t["text"]):
            price_x = t["x0"] if price_x is None else min(price_x, t["x0"])

    # If we couldn't find a price-like token, we can't reliably parse prices.
    if price_x is None:
        return []

    fmt, fmt_x = find_format_token(toks, price_x)
    if fmt is None or fmt_x is None:
        return []

    nums: List[float] = []
    for t in toks:
        if t["x0"] < price_x:
            continue
        v = french_to_float(t["text"])
        if v is None:
            continue
        nums.append(v)

    cost = nums[0] if len(nums) >= 1 else None
    unit = nums[1] if len(nums) >= 2 else None
    return [(fmt, cost, unit)]
def brand_and_manufacturer_from_after_tokens(after_tokens: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Split MARQUE DE COMMERCE (brand) and FABRICANT (manufacturer) from tokens after DIN.

    Strategy:
    - Compute FORMAT start-x (it anchors the right boundary of the left-side text columns).
    - Consider tokens strictly left of FORMAT as "text columns" (brand + manufacturer).
    - Find the *largest horizontal gap* between consecutive tokens in that region and split there:
        left side -> brand, right side -> manufacturer.
    - Fallbacks:
        - If we cannot find FORMAT, split by mid-column heuristic (brand left, manufacturer mid).
        - If no meaningful gap, treat the last 1-2 tokens as manufacturer and the rest as brand.
    """
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return (None, None)

    left_price_x = find_leftmost_price_x(toks)
    _fmt, fmt_x = find_format_token(toks, left_price_x)

    # If we can't anchor by FORMAT, fall back to coarse column ranges
    if fmt_x is None:
        xmax = toks[-1]["x0"] if toks[-1]["x0"] else 1.0
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
# MAIN EXTRACTION
# =========================
def extract_annexe_v():
    if not INPUT_PDF.exists():
        raise SystemExit(f"Input PDF not found: {INPUT_PDF}")

    total_rows = 0

    start_idx = max(0, START_PAGE_1IDX - 1)  # 1-index -> 0-index

    with pdfplumber.open(str(INPUT_PDF)) as pdf, open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FINAL_COLS)
        writer.writeheader()

        current_generic: Optional[str] = None
        current_formline: Optional[str] = None

        for page_no_1idx in range(START_PAGE_1IDX, len(pdf.pages) + 1):
            page = pdf.pages[page_no_1idx - 1]
            lines = page_to_lines(page)

            i = 0
            while i < len(lines):
                line = lines[i]
                text = line["text"]

                # Generic header
                if is_generic_header(text):
                    g = text.rstrip(":").strip()
                    g = re.sub(r"\s+[A-Z]$", "", g).strip()  # remove trailing marker like " X"
                    current_generic = g if g else current_generic
                    current_formline = None
                    i += 1
                    continue

                # Formline (only if NOT a DIN row)
                din_idx = find_din_token_idx(line["tokens"])
                if din_idx is None and is_form_line(text):
                    current_formline = text.strip()
                    i += 1
                    continue

                # DIN row
                if din_idx is not None:
                    din_raw = line["tokens"][din_idx]["text"].strip()
                    din = din_raw.zfill(8)

                    # everything after DIN
                    after_tokens = sorted(line["tokens"][din_idx + 1 :], key=lambda t: t["x0"])

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

                    # Pair format/cost lists:
                    # Usually for FLUNARIZINE:
                    #   fmts = [60, 100], costs = [43.22, 72.03]
                    n = max(len(fmts), len(costs), 1)
                    for k in range(n):
                        fmt_k = fmts[k] if k < len(fmts) else None
                        cost_k = costs[k] if k < len(costs) else (costs[0] if len(costs) == 1 else None)

                        writer.writerow({
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
                        })

                        total_rows += 1
                        if MAX_ROWS is not None and total_rows >= MAX_ROWS:
                            print(f"\n🧪 MAX_ROWS reached ({MAX_ROWS}). Stopping early for test.")
                            print(f"✅ Saved: {OUTPUT_CSV}")
                            return

                    i = j
                    continue

                i += 1

            print(f"Page {page_no_1idx} done | rows so far: {total_rows}")

    print(f"\n✅ Done. Total rows: {total_rows}")
    print(f"✅ Saved: {OUTPUT_CSV}")

if __name__ == "__main__":
    extract_annexe_v()