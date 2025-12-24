# -*- coding: utf-8 -*-
"""
Hybrid extraction (Spatial-first) for legend_to_end.pdf
======================================================

This version implements the "Hybrid" strategy you chose, but with ONLY the Spatial-first stage enabled.
(OpenAI repair is NOT implemented here; we leave clear hook points.)

Key behaviors (locked):
1) Layout-aware routing (pattern-based, no static page numbers)
   - ANNEXE IV.1 => IV1 (BLOCK parser)
   - ANNEXE IV.2 => IV2 (TABLE parser)
   - ANNEXE V    => V   (SKIP product extraction)
   - otherwise   => MAIN (main table)

2) Same canonical output columns + confidence scoring for every emitted row.

3) Output gating:
   - legend_to_end_extracted.csv: ONLY rows with confidence >= keep_threshold (default 0.96)
   - qa_lowest_20.csv: ONLY TOP N lowest-confidence rows below keep_threshold (default N=20)
     (Schema identical to final output.)

4) Clean console:
   - progress bar (single line)
   - layout ranges printed with parser type and page ranges

Hybrid (Spatial-first) fix stage:
- For each DIN "block" (DIN row + following non-anchor lines until next DIN/generic/form),
  we use x/y proximity and column bands to harvest pack/price continuation lines safely.
- This significantly reduces broken rows without hallucinating values.

Run:
  python step_04_extract_din_data_layout_routing_conf96_v8_hybrid_spatial.py
  python step_04_extract_din_data_layout_routing_conf96_v8_hybrid_spatial.py --keep-threshold 0.92 --qa-limit 20
"""

from __future__ import annotations

from pathlib import Path
import re
import csv
import time
import argparse
import logging
import unicodedata
import heapq
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime

try:
    import pdfplumber
except ImportError:
    raise SystemExit("Missing dependency: pdfplumber\nInstall with: pip install pdfplumber")

# ----------------------------- Paths -----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
SPLIT_PDF_DIR = BASE_DIR / "output" / "split_pdf"
OUTPUT_DIR = BASE_DIR / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_PDF = SPLIT_PDF_DIR / "legend_to_end.pdf"
FINAL_CSV = OUTPUT_DIR / "legend_to_end_extracted.csv"
QA_CSV = OUTPUT_DIR / "qa_lowest_20.csv"
LOG_FILE = OUTPUT_DIR / "extraction_log.txt"

# ----------------------------- Controls -----------------------------
DEFAULT_KEEP_THRESHOLD = 0.96
DEFAULT_QA_LIMIT = 20

Y_TOL = 1.3
X_TOL = 1.0
BRAND_JOIN_HYPHEN_NO_SPACE = True

DEFAULT_BAND = {"brand_max": 0.42, "manuf_min": 0.42, "manuf_max": 0.60, "pack_min": 0.58, "unit_min": 0.73}

# ----------------------------- Regex -----------------------------
RE_ALLCAPS = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+$")
RE_FORMLINE = re.compile(r"(?i)\b(Co\.|Caps\.|Sol\.|Susp\.|Suspension|Pd\.|Perf\.|Sir\.|Gel\.|I\.V\.|I\.M\.|S\.C\.|Orale|Co\. L\.A\.)\b")
RE_STRENGTH = re.compile(r"(?i)\b\d+(?:[.,]\d+)?(?:\s*[-/]\s*\d+(?:[.,]\d+)?)?\s*(?:mg|g|mcg|µg|U|UI|U/mL|UI/mL|mg/mL|mg/5\s?mL|mL)\b")
RE_STRENGTH_SPLIT = re.compile(r"(?i)(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>mg|g|mcg|µg|U|UI|U/mL|UI/mL|mg/mL|mg/5\s?mL|mL)\b")
RE_HAS_PPB = re.compile(r"\bPPB\b", re.IGNORECASE)

RE_DIN = re.compile(r"^\d{6,9}$")
RE_PACK_ONLY = re.compile(r"^\d{1,4}$")
RE_VOL_ONE = re.compile(r"^(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)
RE_VOL = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s?(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)

RE_PRICE_NUM = re.compile(r"^[\d\s.,]+$")  # allow 45,00
RE_CLASS_CODE = re.compile(r"\b\d{1,2}:\d{2}(?:\.\d{2})?\b")
FLAG_TOKENS = {"X", "UE", "Z", "Y", "V", "*", "+"}

# Layout headings (explicit only)
RE_ANNEX_IV_1 = re.compile(r"ANNEXE\s+IV\.?1", re.IGNORECASE)
RE_ANNEX_IV_2 = re.compile(r"ANNEXE\s+IV\.?2", re.IGNORECASE)
RE_ANNEX_V = re.compile(r"ANNEXE\s+V\b", re.IGNORECASE)

# Narrative / bullet
RE_BULLET = re.compile(r"[•●◦▪♦�]|ï‚¨")
RE_YEAR = re.compile(r"\b(19|20)\d{2}\b")
RE_SENT_PUNCT = re.compile(r"[.;:!?]")

FR_FUNCTION_WORDS = {
    "DE","DES","DU","LA","LE","LES","UN","UNE","ET","OU","DONT","POUR","AVANT","APRÈS","APRES",
    "QUE","QUI","A","À","AU","AUX","EN","SUR","DANS","PAR","SANS","SI","SE","SONT","EST","ETRE","ÊTRE",
    "DOIT","DEVRAIT","PEUT","PEUVENT","AVOIR","RECEVOIR","REÇU","RECU","COMMENCÉ","COMMENCE","CONDITION"
}

BLOCKLIST_HEADINGS = [
    "ANNEXE","ANNEXES","LISTE","CODE","MARQUE","FABRICANT","FORMAT","PRIX","UNITAIRE","COUT","COÛT","PAGE",
    "LEGENDE","LÉGENDE"
]


def is_section_header_line(text: str) -> bool:
    """
    Detects a new class/section header like '8:12.02' which indicates
    a new therapeutic class block. We reset sticky context on these lines
    to prevent form/strength from leaking across sections.
    """
    t = strip_acc(norm_spaces(text)).upper()
    if not RE_CLASS_CODE.search(t):
        return False
    # Heuristic: code near start and line is short-ish
    return len(t) <= 40 and t[:6].replace(" ", "").find(":") != -1

# ----------------------------- Output schema -----------------------------
HEADERS = [
    "Generic","Flags","Form","Strength","StrengthValue","StrengthUnit","PPB",
    "DIN","Brand","Manufacturer","Pack","PackPrice","UnitPrice","UnitPriceSource",
    "Page","confidence","confidence_label","confidence_reason"
]

# ----------------------------- Helpers -----------------------------
def strip_acc(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_spaces(s: str) -> str:
    return (s or "").replace("\u00A0", " ").strip()

def normalize_words(text: str) -> List[str]:
    t = strip_acc(norm_spaces(text)).upper()
    t = re.sub(r"[^A-Z0-9()\-'/\s]", " ", t)
    return [w for w in t.split() if w]

def function_word_ratio(words: List[str]) -> float:
    if not words:
        return 0.0
    hits = sum(1 for w in words if w in FR_FUNCTION_WORDS)
    return hits / max(1, len(words))

def looks_like_narrative(text: str) -> bool:
    t = strip_acc(norm_spaces(text)).upper()
    words = normalize_words(text)
    if not t:
        return False
    if RE_BULLET.search(text):
        return True
    if RE_SENT_PUNCT.search(text):
        return True
    if RE_YEAR.search(t) and len(words) >= 8:
        return True
    if len(words) >= 7 and function_word_ratio(words) >= 0.25:
        return True
    if len(t) >= 80 and len(words) >= 10:
        return True
    return False

def is_allcaps_candidate(s: str) -> bool:
    s2 = norm_spaces(s)
    if not s2 or re.match(r"^\d", s2):
        return False
    up = strip_acc(s2).upper()
    if any(h in up for h in BLOCKLIST_HEADINGS):
        return False
    return bool(RE_ALLCAPS.match(s2))

def looks_like_generic_heading(text: str) -> bool:
    if not is_allcaps_candidate(text):
        return False
    if looks_like_narrative(text):
        return False
    return len(normalize_words(text)) <= 6

def french_to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    t = norm_spaces(s)
    t = re.sub(r"[^\d,.\s]", "", t)
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

def tokens_text(tokens: List[Dict[str, Any]]) -> str:
    s = ""
    for t in tokens:
        if not s:
            s = t["text"]
        else:
            if s.endswith("-") and BRAND_JOIN_HYPHEN_NO_SPACE:
                s = s[:-1] + t["text"]
            else:
                s = s + " " + t["text"]
    return s.strip()

def median_ratio(tokens: List[Dict[str, Any]], width: float) -> float:
    xs = sorted((t["x0"]/width for t in tokens))
    n = len(xs)
    if n == 0:
        return 0.0
    if n % 2:
        return xs[n//2]
    return 0.5*(xs[n//2-1] + xs[n//2])

# ----------------------------- Progress + layout tracker -----------------------------
class CleanProgress:
    def __init__(self, total_pages: int):
        self.total_pages = total_pages
        self.start = time.time()
        self.kept = 0
        self.rejected = 0  # total below threshold (informational)

    def update(self, page_no: int, kept_add: int, rejected_add: int):
        self.kept += kept_add
        self.rejected += rejected_add
        bar_len = 24
        frac = page_no / max(1, self.total_pages)
        filled = int(bar_len * frac)
        bar = "█"*filled + "░"*(bar_len-filled)
        print(f"\r[{bar}] {page_no}/{self.total_pages} pages | keep={self.kept:,} | reject={self.rejected:,}", end="", flush=True)

    def done(self):
        elapsed = time.time() - self.start
        print(f"\n✅ Done in {elapsed:.1f}s | keep={self.kept:,} | reject={self.rejected:,}")

class LayoutTracker:
    """Prints mode transitions with parser type and page ranges."""
    def __init__(self):
        self.current: Optional[str] = None
        self.start_page: Optional[int] = None

    @staticmethod
    def mode_label(mode: str) -> str:
        return {"IV1": "BLOCK", "IV2": "TABLE", "V": "SKIP ", "MAIN": "MAIN ", "UNKNOWN": "MAIN "}.get(mode, mode)

    def update(self, page_no: int, mode: str):
        if mode != self.current:
            if self.current is not None and self.start_page is not None:
                print(f"\n→ Layout {self.current:<4} ({self.mode_label(self.current)}) pages {self.start_page}–{page_no-1}")
            self.current = mode
            self.start_page = page_no

    def close(self, last_page: int):
        if self.current is not None and self.start_page is not None:
            print(f"\n→ Layout {self.current:<4} ({self.mode_label(self.current)}) pages {self.start_page}–{last_page}")

# ----------------------------- PDF to lines -----------------------------
def page_to_lines(page) -> List[Dict[str, Any]]:
    words = page.extract_words(x_tolerance=X_TOL, y_tolerance=Y_TOL, keep_blank_chars=False) or []
    if not words:
        return []
    words.sort(key=lambda w: (round(w["top"], 1), w["x0"]))

    lines: List[Dict[str, Any]] = []
    cur: List[Dict[str, Any]] = []
    cur_top: Optional[float] = None

    def flush():
        cur.sort(key=lambda z: z["x0"])
        lines.append({
            "top": min(t["top"] for t in cur),
            "bottom": max(t["bottom"] for t in cur),
            "tokens": cur[:],
            "text": tokens_text(cur)
        })

    for w in words:
        if cur_top is None:
            cur_top = w["top"]
            cur = [w]
            continue
        if abs(w["top"] - cur_top) <= Y_TOL:
            cur.append(w)
        else:
            flush()
            cur_top = w["top"]
            cur = [w]
    if cur:
        flush()
    return lines

# ----------------------------- Bands -----------------------------
def calibrate_bands(page, width: float) -> Dict[str, float]:
    bands = DEFAULT_BAND.copy()
    words = page.extract_words(x_tolerance=2.0, y_tolerance=2.0, keep_blank_chars=False) or []
    words.sort(key=lambda w: (w["top"], w["x0"]))
    texts = [(strip_acc(w["text"]).upper(), w) for w in words]

    pack_x = None
    unit_x = None
    for idx, (txt, w) in enumerate(texts):
        if txt in {"COUT", "COÛT"}:
            y = w["top"]
            same_line = [t for t, ww in texts[idx:idx+12] if abs(ww["top"]-y) < 2.2]
            if any("FORMAT" in t for t in same_line):
                pack_x = w["x0"]
        if txt == "PRIX":
            y = w["top"]
            same_line = [t for t, ww in texts[idx:idx+14] if abs(ww["top"]-y) < 2.2]
            if any("UNITAIRE" in t for t in same_line):
                unit_x = w["x0"]

    if pack_x is not None:
        bands["pack_min"] = max(0.35, (pack_x - 12.0) / width)
    if unit_x is not None:
        bands["unit_min"] = max(bands["pack_min"] + 0.05, (unit_x - 12.0) / width)

    if pack_x is None or unit_x is None:
        price_xs = []
        packish_xs = []
        for w in words:
            s = w["text"].strip()
            x = float(w["x0"]) / width
            if RE_PRICE_NUM.match(s) and french_to_float(s) is not None:
                price_xs.append(x)
            if RE_PACK_ONLY.match(s) or RE_VOL.match(s):
                packish_xs.append(x)
        if pack_x is None and packish_xs:
            packish_xs.sort()
            p = packish_xs[int(len(packish_xs) * 0.70)]
            bands["pack_min"] = max(0.30, min(0.80, p - 0.03))
        if unit_x is None and len(price_xs) >= 6:
            price_xs.sort()
            mid = price_xs[len(price_xs)//2]
            left = [x for x in price_xs if x <= mid]
            right = [x for x in price_xs if x > mid]
            if right and (min(right) - max(left)) > 0.05:
                bands["unit_min"] = max(bands["pack_min"] + 0.06, min(right) - 0.03)
            else:
                bands["unit_min"] = max(bands["pack_min"] + 0.08, bands["unit_min"])

    bands["pack_min"] = float(max(0.30, min(0.85, bands["pack_min"])))
    bands["unit_min"] = float(max(bands["pack_min"] + 0.05, min(0.95, bands["unit_min"])))
    return bands

# ----------------------------- Layout routing -----------------------------
@dataclass
class LayoutState:
    mode: str = "UNKNOWN"  # IV1, IV2, V, MAIN

def detect_layout_mode(page_text: str, prev_mode: str) -> str:
    t = strip_acc(norm_spaces(page_text)).upper()

    if RE_ANNEX_IV_1.search(t):
        return "IV1"
    if RE_ANNEX_IV_2.search(t):
        return "IV2"
    if RE_ANNEX_V.search(t):
        return "V"

    # Exit Annex V on legend / main-list table headers
    if "LEGENDE" in t or "LÉGENDE" in t:
        return "MAIN"
    if "CODE MARQUE DE COMMERCE" in t or ("MARQUE DE COMMERCE" in t and "FABRICANT" in t and "FORMAT" in t):
        return "MAIN"

    return prev_mode if prev_mode != "UNKNOWN" else "MAIN"

# ----------------------------- Line classification per layout -----------------------------
def find_din_index(line: Dict[str, Any], width: float, bands: Dict[str, float]) -> Optional[int]:
    for j, t in enumerate(line.get("tokens", [])):
        txt = t["text"].strip()
        if RE_DIN.match(txt) and (t["x0"]/width) <= (bands["pack_min"] - 0.10):
            return j
    return None

def cls_common(line: Dict[str, Any], width: float, bands: Dict[str, float]) -> Optional[str]:
    txt = norm_spaces(line.get("text", ""))
    if not txt:
        return "noise"
    if looks_like_narrative(txt):
        return "noise"
    if looks_like_generic_heading(txt):
        return "generic"
    if RE_FORMLINE.search(txt) or RE_STRENGTH.search(txt) or RE_HAS_PPB.search(txt):
        return "form"
    if line.get("tokens"):
        first = line["tokens"][0]["text"].strip()
        if RE_DIN.match(first):
            return "din_row"
        if find_din_index(line, width, bands) is not None:
            return "din_row"
    return None

def classify_line(mode: str, line: Dict[str, Any], width: float, bands: Dict[str, float]) -> str:
    base = cls_common(line, width, bands)
    if base:
        return base

    if mode == "IV1":
        # IV1: allow pack/price continuation lines that are right-side and look like pack+price
        if line.get("tokens"):
            tok_texts = [norm_spaces(t["text"]) for t in line["tokens"] if norm_spaces(t["text"])]
            has_pack = any(RE_VOL.match(s) for s in tok_texts) or (len(tok_texts) >= 2 and RE_PACK_ONLY.match(tok_texts[0]) and RE_VOL_ONE.match(tok_texts[1]))
            has_price = any(french_to_float(s) is not None for s in tok_texts)
            if has_pack and has_price and median_ratio(line["tokens"], width) >= bands["pack_min"] - 0.10:
                return "annex_packprice"
        return "noise"

    # IV2/MAIN: only conservative continuations
    if line.get("tokens"):
        x0 = line["tokens"][0]["x0"]/width
        first_txt = line["tokens"][0]["text"].strip()
        if (RE_PACK_ONLY.match(first_txt) or RE_VOL.match(first_txt)) and x0 >= bands["pack_min"] - 0.02:
            return "pack_cont"
        vals = [french_to_float(t["text"]) for t in line["tokens"] if french_to_float(t["text"]) is not None]
        if vals:
            rx = line["tokens"][-1]["x0"]/width
            if rx >= bands["unit_min"] - 0.02:
                return "unit_only"
            mx = median_ratio(line["tokens"], width)
            if mx >= bands["pack_min"] - 0.02 and mode == "IV2":
                return "packprice_only"
    return "noise"

# ----------------------------- Parsing helpers -----------------------------
def parse_form_strength(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], bool]:
    t = norm_spaces(text)
    ppb = bool(RE_HAS_PPB.search(t))
    m_strength = RE_STRENGTH.search(t)
    strength = m_strength.group(0) if m_strength else None

    if m_strength:
        tail = t[m_strength.end():]
        m_paren = re.search(r"^\s*\(([^)]*?\b\d+(?:[.,]\d+)?\s*(?:mL|ML|L)\b[^)]*)\)", tail)
        if m_paren and strength:
            strength = f"{strength} ({m_paren.group(1).strip()})"

    m_form = RE_FORMLINE.search(t)
    if m_form:
        form = m_form.group(0)
    elif strength and m_strength:
        form = t[:t.find(m_strength.group(0))].strip()
    else:
        form = t.strip()

    form = re.sub(r"\bPPB\b", "", form, flags=re.I).strip() if form else None
    strength = re.sub(r"\bPPB\b", "", strength, flags=re.I).strip() if strength else None

    s_val, s_unit = None, None
    if strength:
        m = RE_STRENGTH_SPLIT.search(strength)
        if m:
            s_val = m.group("val").replace(",", ".")
            s_unit = m.group("unit")
        else:
            m2 = re.search(r"(?i)(\d+(?:[.,]\d+)?)(.*)", strength)
            if m2:
                s_val = m2.group(1).replace(",", ".").strip()
                s_unit = m2.group(2).strip()
    return (form or None), (strength or None), (s_val or None), (s_unit or None), ppb

def parse_generic_and_flags(tokens: List[Dict[str,Any]]) -> Tuple[str, List[str]]:
    gen_tokens = []
    flags: List[str] = []
    for t in tokens:
        txt = norm_spaces(t["text"])
        up = strip_acc(txt).upper()
        if up in FLAG_TOKENS:
            if up not in flags:
                flags.append(up)
            continue
        gen_tokens.append(t)
    return tokens_text(gen_tokens).strip(), flags

def tokens_contain_pack_and_price(tokens: List[Dict[str,Any]]) -> bool:
    txts = [norm_spaces(t["text"]) for t in tokens if norm_spaces(t["text"])]
    if not txts:
        return False
    has_pack = any(RE_VOL.match(s) for s in txts) or any(RE_PACK_ONLY.match(s) for s in txts)
    has_price = any(french_to_float(s) is not None for s in txts)
    numericish = sum(1 for s in txts if french_to_float(s) is not None or RE_PACK_ONLY.match(s) or RE_VOL.match(s))
    return has_pack and has_price and numericish >= max(2, int(0.6 * len(txts)))

def split_brand_manuf(tokens: List[Dict[str, Any]], width: float, bands: Dict[str, float]) -> Tuple[List[Dict[str,Any]], List[Dict[str,Any]], List[Dict[str,Any]]]:
    brand_tokens: List[Dict[str, Any]] = []
    manuf_tokens: List[Dict[str, Any]] = []
    right_candidates: List[Dict[str, Any]] = []
    for t in tokens:
        x = t["x0"]/width
        if x <= bands["brand_max"]:
            brand_tokens.append(t)
        elif bands["manuf_min"] <= x <= bands["manuf_max"]:
            manuf_tokens.append(t)
        else:
            right_candidates.append(t)
    # move pack-like tokens out of manufacturer band (common in MAIN tables where pack count sits close)
    if manuf_tokens:
        keep_manuf: List[Dict[str, Any]] = []
        for t in manuf_tokens:
            s = norm_spaces(t["text"])
            if RE_PACK_ONLY.match(s) or RE_VOL.match(s):
                right_candidates.append(t)
            else:
                keep_manuf.append(t)
        manuf_tokens = keep_manuf

    if tokens_contain_pack_and_price(manuf_tokens):
        right_candidates.extend(manuf_tokens)
        manuf_tokens = []
    return brand_tokens, manuf_tokens, right_candidates

def harvest_wrapped_brand(mode: str, lines: List[Dict[str, Any]], idx: int, width: float, bands: Dict[str, float], brand_tokens: List[Dict[str, Any]]) -> bool:
    if not brand_tokens:
        return False
    wrapped = False
    max_lines = 2 if mode == "IV1" else 1
    for k in range(1, max_lines+1):
        if idx + k >= len(lines):
            break
        nxt = lines[idx + k]
        if not nxt.get("tokens"):
            continue
        nxt_text = nxt.get("text","")
        if looks_like_narrative(nxt_text):
            break
        nxt_cls = classify_line(mode, nxt, width, bands)
        if nxt_cls in {"din_row","generic","form"}:
            break
        if nxt["tokens"][0]["x0"]/width <= bands["brand_max"]:
            if mode != "IV1" and len(nxt_text) > 40:
                break
            brand_tokens.extend(nxt["tokens"])
            wrapped = True
        else:
            break
    return wrapped

def parse_pack_and_prices_from_tokens(tokens: List[Dict[str,Any]]) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    txts = [norm_spaces(t["text"]) for t in tokens if norm_spaces(t["text"])]
    if not txts:
        return None, None, None

    pack = None
    rest = txts[:]

    if RE_VOL.match(rest[0]):
        pack = rest[0]; rest = rest[1:]
    elif len(rest) >= 2 and RE_PACK_ONLY.match(rest[0]) and RE_VOL_ONE.match(rest[1]):
        pack = f"{rest[0]} {rest[1]}"; rest = rest[2:]
    elif RE_PACK_ONLY.match(rest[0]):
        pack = rest[0]; rest = rest[1:]

    nums = []
    for s in rest:
        if RE_PRICE_NUM.match(s) and french_to_float(s) is not None:
            nums.append(s)
    # dedup adjacent equals
    dedup = []
    for n in nums:
        if not dedup or dedup[-1] != n:
            dedup.append(n)
    nums = dedup

    pack_price = None
    unit_price = None
    if nums:
        pack_price = french_to_float(nums[0])
        if len(nums) >= 2 and nums[-1] != nums[0]:
            unit_price = french_to_float(nums[-1])
    return pack, pack_price, unit_price

# ----------------------------- Spatial-first repair within a DIN block -----------------------------
def spatial_collect_packprice_candidates(mode: str, block_lines: List[Dict[str, Any]], width: float, bands: Dict[str, float]) -> List[Tuple[Optional[str], Optional[float], Optional[float]]]:
    """
    Spatial-first: from the DIN block continuation lines, collect candidate (Pack, PackPrice, UnitPrice) tuples.
    Safe rules:
      - Candidate line must be predominantly on the right side (>= pack_min - small margin)
      - Candidate must contain a pack token OR numeric tokens that look like a price
      - In IV1, allow 'pack+price' patterns even when headerless (boxed blocks)
      - In IV2/MAIN, accept only conservative right-aligned numeric/pack lines
    """
    cands: List[Tuple[Optional[str], Optional[float], Optional[float]]] = []
    for ln in block_lines:
        if not ln.get("tokens"):
            continue
        cls = classify_line(mode, ln, width, bands)
        if cls not in {"annex_packprice", "pack_cont", "packprice_only", "unit_only"}:
            continue

        # use only tokens in pack/price region
        right_tokens = [t for t in ln["tokens"] if (t["x0"]/width) >= (bands["pack_min"] - (0.10 if mode=="IV1" else 0.02))]
        if not right_tokens:
            continue
        pk, pp, up = parse_pack_and_prices_from_tokens(right_tokens)
        if pk is None and pp is None and up is None:
            # in IV2 packprice_only/unit_only may have just price numbers
            vals = [french_to_float(t["text"]) for t in right_tokens if french_to_float(t["text"]) is not None]
            if vals:
                pk = None
                pp = vals[0]
                up = vals[-1] if (mode == "IV2" and len(vals) >= 2) else None
        if pk is not None or pp is not None or up is not None:
            cands.append((pk, pp, up))

    # de-dup exact tuples while preserving order
    seen = set()
    out: List[Tuple[Optional[str], Optional[float], Optional[float]]] = []
    for t in cands:
        key = (t[0], t[1], t[2])
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out

# ----------------------------- Confidence -----------------------------
def apply_penalty(score: float, reasons: List[str], amount: float, reason: str) -> float:
    score -= amount
    reasons.append(f"{reason} (-{amount:.2f})")
    return score

def label_from_score(score: float) -> str:
    if score >= 0.85:
        return "high"
    if score >= 0.60:
        return "medium"
    return "low"

# ----------------------------- Extraction -----------------------------
def extract_pdf(input_pdf: Path, final_csv: Path, qa_csv: Path, keep_threshold: float, qa_limit: int) -> None:
    logging.basicConfig(filename=str(LOG_FILE), filemode="w", level=logging.INFO, format="%(message)s")
    logging.info("PDF EXTRACTION STARTED")
    logging.info(f"Start: {datetime.now():%Y-%m-%d %H:%M:%S}")
    logging.info(f"Input: {input_pdf}")

    with pdfplumber.open(str(input_pdf)) as pdf, \
         open(final_csv, "w", newline="", encoding="utf-8") as f_final, \
         open(qa_csv, "w", newline="", encoding="utf-8") as f_qa:

        w_final = csv.DictWriter(f_final, fieldnames=HEADERS)
        w_qa = csv.DictWriter(f_qa, fieldnames=HEADERS)
        w_final.writeheader()
        w_qa.writeheader()

        prog = CleanProgress(len(pdf.pages))
        tracker = LayoutTracker()
        state = LayoutState(mode="UNKNOWN")

        # bounded heap for QA lowest N rows below threshold
        # store (-score, seq, row). heap[0] is most-negative => highest score among kept QA rows.
        qa_heap: List[Tuple[float, int, Dict[str, Any]]] = []
        qa_seen = 0
        seq = 0

        # sticky context (IV1/IV2/MAIN)
        current_generic: Optional[str] = None
        current_flags: List[str] = []
        current_form: Optional[str] = None
        cur_strength: Optional[str] = None
        s_val: Optional[str] = None
        s_unit: Optional[str] = None
        ppb: bool = False

        for pageno, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            state.mode = detect_layout_mode(page_text, state.mode)
            tracker.update(pageno, state.mode)

            if state.mode == "V":
                prog.update(pageno, kept_add=0, rejected_add=0)
                continue

            width = float(page.width)
            bands = calibrate_bands(page, width)
            lines = page_to_lines(page)

            kept_add = 0
            rejected_add = 0

            i = 0
            while i < len(lines):
                ln = lines[i]
                if is_section_header_line(ln.get("text", "")):
                    # reset sticky context at new class header to avoid cross-section leakage
                    current_form = None
                    cur_strength = None
                    s_val = None
                    s_unit = None
                    ppb = False
                    i += 1
                    continue

                cls = classify_line(state.mode, ln, width, bands)

                if cls == "generic":
                    gen, flags = parse_generic_and_flags(ln["tokens"])
                    if gen and not looks_like_narrative(gen):
                        current_generic = gen
                        current_flags = flags
                        # new generic starts a fresh product block; reset form/strength unless explicitly re-set next
                        current_form = None
                        cur_strength = None
                        s_val = None
                        s_unit = None
                        ppb = False
                    i += 1
                    continue

                if cls == "form":
                    form, strength, s_val, s_unit, has_ppb = parse_form_strength(ln["text"])
                    if form and not looks_like_narrative(form):
                        current_form = form
                    if strength:
                        cur_strength = strength
                    ppb = has_ppb or ppb
                    i += 1
                    continue

                if cls == "din_row":
                    din_idx = find_din_index(ln, width, bands)
                    if din_idx is None:
                        i += 1
                        continue
                    din = ln["tokens"][din_idx]["text"].strip()

                    # identify DIN block continuation lines (until next anchor)
                    j = i + 1
                    block_lines: List[Dict[str, Any]] = []
                    while j < len(lines):
                        nxt = lines[j]
                        nxt_cls = classify_line(state.mode, nxt, width, bands)
                        if nxt_cls in {"din_row","generic","form"}:
                            break
                        block_lines.append(nxt)
                        j += 1

                    brand_tokens, manuf_tokens, right_candidates = split_brand_manuf(ln["tokens"][din_idx+1:], width, bands)
                    brand_wrapped = harvest_wrapped_brand(state.mode, lines, i, width, bands, brand_tokens)

                    brand = tokens_text(brand_tokens).strip() or None
                    manufacturer = tokens_text(manuf_tokens).strip() or None

                    # Parse from DIN row right side first
                    packs: List[Optional[str]] = []
                    pack_prices: List[Optional[float]] = []
                    unit_prices: List[Optional[float]] = []

                    pk0, pp0, up0 = parse_pack_and_prices_from_tokens(
                        [t for t in ln["tokens"] if (t["x0"]/width) >= (bands["pack_min"] - 0.02)] + right_candidates
                    )
                    if pk0 or pp0 is not None or up0 is not None:
                        packs.append(pk0); pack_prices.append(pp0); unit_prices.append(up0)

                    # Spatial-first repair: harvest continuation candidates inside the DIN block
                    cont = spatial_collect_packprice_candidates(state.mode, block_lines, width, bands)
                    for (pk, pp, up) in cont:
                        packs.append(pk); pack_prices.append(pp); unit_prices.append(up)

                    # If we still have nothing, keep one empty row to preserve DIN presence
                    if not packs:
                        packs = [None]; pack_prices=[None]; unit_prices=[None]

                    # normalize lengths
                    n_rows = max(len(packs), len(pack_prices), len(unit_prices), 1)
                    def get_at(lst, idx):
                        return lst[idx] if idx < len(lst) else (lst[0] if len(lst)==1 else None)

                    for ridx in range(n_rows):
                        pack_text = get_at(packs, ridx)
                        pack_price = get_at(pack_prices, ridx)
                        unit_price = get_at(unit_prices, ridx)

                        # Derivation: if pack numeric and unit present, derive pack price (marked)
                        derived_pack = False
                        pack_num = None
                        if pack_text:
                            if RE_VOL.match(pack_text):
                                m = re.findall(r"\d+(?:[.,]\d+)?", pack_text)
                                if m:
                                    pack_num = float(m[0].replace(",", "."))
                            elif RE_PACK_ONLY.match(pack_text.strip()):
                                pack_num = float(pack_text.strip())

                        if pack_price is None and unit_price is not None and pack_num is not None:
                            pack_price = round(pack_num * float(unit_price), 2)
                            derived_pack = True

                        unit_src = "Printed" if unit_price is not None else None
                        if derived_pack and unit_src is None:
                            unit_src = "Derived"

                        # confidence (same across layouts)
                        score = 1.0
                        reasons: List[str] = []

                        if brand_wrapped:
                            score = apply_penalty(score, reasons, 0.04, "Brand wrapped across lines")
                        if not current_generic:
                            score = apply_penalty(score, reasons, 0.22, "Missing Generic context")
                        if not current_form:
                            score = apply_penalty(score, reasons, 0.08, "Missing Form context")
                        if not brand:
                            score = apply_penalty(score, reasons, 0.18, "Missing Brand")
                        if not manufacturer:
                            score = apply_penalty(score, reasons, 0.07, "Missing Manufacturer")
                        if pack_text is None:
                            score = apply_penalty(score, reasons, 0.05, "Missing Pack/Format")
                        if unit_price is None:
                            if pack_price is not None:
                                score = apply_penalty(score, reasons, 0.08, "Unit price missing (pack cost present)")
                            else:
                                score = apply_penalty(score, reasons, 0.15, "Unit price missing")
                        if pack_price is None:
                            if unit_price is not None and pack_num is not None:
                                score = apply_penalty(score, reasons, 0.10, "Pack cost missing (derivable)")
                            else:
                                score = apply_penalty(score, reasons, 0.22, "Pack cost missing")
                        if derived_pack:
                            score = apply_penalty(score, reasons, 0.06, "Pack cost derived from unit×pack")

                        score = max(0.0, min(1.0, score))
                        label = label_from_score(score)
                        conf_reason = "; ".join(reasons) if reasons else "No penalties"
                        out_strength = f"{s_val} {s_unit}".strip() if (s_val and s_unit) else cur_strength

                        row = {
                            "Generic": current_generic,
                            "Flags": " ".join(current_flags) if current_flags else None,
                            "Form": current_form,
                            "Strength": out_strength,
                            "StrengthValue": s_val,
                            "StrengthUnit": s_unit,
                            "PPB": str(bool(ppb)).upper(),
                            "DIN": din,
                            "Brand": brand,
                            "Manufacturer": manufacturer,
                            "Pack": pack_text,
                            "PackPrice": pack_price,
                            "UnitPrice": unit_price,
                            "UnitPriceSource": unit_src,
                            "Page": pageno,
                            "confidence": round(score, 2),
                            "confidence_label": label,
                            "confidence_reason": conf_reason,
                        }

                        if score >= keep_threshold:
                            w_final.writerow(row)
                            kept_add += 1
                        else:
                            rejected_add += 1
                            qa_seen += 1
                            seq += 1
                            item = (-float(score), seq, row)

                            if qa_limit > 0:
                                if len(qa_heap) < qa_limit:
                                    heapq.heappush(qa_heap, item)
                                else:
                                    # replace the highest-confidence among kept QA rows if this one is worse (lower confidence)
                                    if qa_heap and item[0] > qa_heap[0][0]:
                                        heapq.heapreplace(qa_heap, item)

                    i = j
                    continue

                i += 1

            prog.update(pageno, kept_add=kept_add, rejected_add=rejected_add)

        # Write QA lowest rows (sorted by confidence ascending)
        qa_rows = [t[2] for t in sorted(qa_heap, key=lambda x: (-x[0], x[1]))]
        for r in qa_rows:
            w_qa.writerow(r)

        logging.info(f"Done. kept={prog.kept} rejected={prog.rejected}")
        logging.info(f"QA below threshold total: {qa_seen}")
        logging.info(f"QA written (lowest): {len(qa_rows)} (limit={qa_limit})")

        tracker.close(len(pdf.pages))
        prog.done()
        print(f"📌 QA below threshold total: {qa_seen:,} | QA written (lowest): {len(qa_rows)}")

# ----------------------------- Main -----------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--keep-threshold",
        type=float,
        default=DEFAULT_KEEP_THRESHOLD,
        help="Rows with confidence >= threshold go to legend_to_end_extracted.csv (default 0.96)"
    )
    parser.add_argument(
        "--qa-limit",
        type=int,
        default=DEFAULT_QA_LIMIT,
        help="Write only the lowest N rows below keep-threshold to qa_lowest_20.csv (default 20)"
    )
    args = parser.parse_args()

    if not INPUT_PDF.exists():
        raise SystemExit(f"Input PDF not found: {INPUT_PDF}")

    extract_pdf(
        INPUT_PDF,
        FINAL_CSV,
        QA_CSV,
        keep_threshold=float(args.keep_threshold),
        qa_limit=int(args.qa_limit),
    )

if __name__ == "__main__":
    main()
