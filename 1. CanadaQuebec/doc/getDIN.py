# -*- coding: utf-8 -*-
"""
Legend→End PDF → CSV extractor (DIN-driven, position-aware)
END-TO-END • v9 (with v7 Form parsing merged)

What changed vs your v9:
- Adopted v7-style Form/Strength parsing:
  * looks_like_formline(): considers form tokens, PPB, or any inline strength.
  * parse_form_strength(): if no explicit form token, use text before strength; splits strength into value/unit; strips PPB.
Everything else remains identical to your v9.
"""

from pathlib import Path
import re, csv, unicodedata, time, logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime

try:
    import pdfplumber
except ImportError:
    raise SystemExit("Missing dependency: pdfplumber\nInstall with: pip install pdfplumber")

# ----------------------------- Paths -----------------------------
BASE_DIR   = Path(__file__).resolve().parents[1]
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output" / "csv"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_PDF  = INPUT_DIR / "annexe_v.pdf"
OUTPUT_CSV = OUTPUT_DIR / "legend_to_end_extracted.csv"
LOG_FILE   = OUTPUT_DIR / "extraction_log.txt"

# ----------------------------- Tunables -----------------------------
Y_TOL = 1.3
X_TOL = 1.0
BRAND_JOIN_HYPHEN_NO_SPACE = True

DEFAULT_BAND = {"brand_max": 0.42, "manuf_min": 0.42, "manuf_max": 0.60, "pack_min": 0.58, "unit_min": 0.73}

BLOCKLIST_HEADINGS = [
    "ANNEXE","MEDICAMENTS D’EXCEPTION","MÉDICAMENTS D’EXCEPTION",
    "CODE","MARQUE","FABRICANT","FORMAT","PRIX","UNITAIRE",
    "COUT","COÛT","COUT DU","COÛT DU FORMAT"
]

# ----------------------------- Regex -----------------------------
RE_ALLCAPS    = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+$")
# keep v9’s richer token list (includes Co. L.A.)
RE_FORMLINE   = re.compile(r"(?i)\b(Co\.|Caps\.|Sol\.|Susp\.|Suspension|Pd\.|Perf\.|Sir\.|Gel\.|I\.V\.|I\.M\.|S\.C\.|Orale|Co\. L\.A\.)\b")
# v7-style strength detection and splitting
RE_STRENGTH        = re.compile(r"(?i)\b\d+(?:[.,]\d+)?(?:\s*[-/]\s*\d+(?:[.,]\d+)?)?\s*(?:mg|g|mcg|µg|U|UI|U/mL|UI/mL|mg/mL|mg/5\s?mL|mL)\b")
RE_STRENGTH_SPLIT  = re.compile(r"(?i)(?P<val>\d+(?:[.,]\d+)?)\s*(?P<unit>mg|g|mcg|µg|U|UI|U/mL|UI/mL|mg/mL|mg/5\s?mL|mL)\b")
RE_HAS_PPB    = re.compile(r"\bPPB\b", re.IGNORECASE)

RE_DIN        = re.compile(r"^\d{6,9}$")
RE_PACK_ONLY  = re.compile(r"^\d{1,4}$")
RE_VOL_ONE    = re.compile(r"^(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)
RE_VOL        = re.compile(r"^\d{1,4}\s?(mL|ml|L|g|mg|mcg|µg|U|UI)$", re.I)
RE_PRICE_NUM  = re.compile(r"^[\d\s.,]+$")
FLAG_TOKENS   = {"X","UE","Z","Y","V","*","+"}

# ----------------------------- Progress -----------------------------
class Progress:
    def __init__(self, total_pages: int, log_file: Path):
        self.total_pages = total_pages
        self.start = time.time()
        self.recs = 0
        self.pages = 0
        self.logger = logging.getLogger("extract")
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(fh)
        self.logger.info("PDF EXTRACTION STARTED")
        self.logger.info(f"Start: {datetime.now():%Y-%m-%d %H:%M:%S}")
        self.logger.info(f"Total pages: {total_pages}")
    def tick(self, pageno: int, new_records: int):
        self.pages += 1; self.recs += new_records
        pct = (self.pages / self.total_pages) * 100
        print(f"\n\rPage {pageno}/{self.total_pages}  {pct:5.1f}%  | rows: {self.recs}", end="", flush=True)
        self.logger.info(f"\nPage {pageno}: +{new_records} rows (cum {self.recs})")
    def done(self):
        elapsed = time.time() - self.start
        print(f"\n✅ Done. {self.recs} rows in {elapsed:.1f}s")
        self.logger.info(f"\nCOMPLETE: {self.recs} rows in {elapsed:.1f}s")

# ----------------------------- Utils -----------------------------
def strip_acc(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def norm_spaces(s: str) -> str:
    return s.replace("\u00A0", " ").strip()

def is_allcaps_candidate(s: str) -> bool:
    s2 = norm_spaces(s)
    if not s2 or re.match(r"^\d", s2): return False
    s2s = strip_acc(s2).upper()
    if any(h in s2s for h in BLOCKLIST_HEADINGS): return False
    return bool(RE_ALLCAPS.match(s2))

def french_to_float(s: str) -> Optional[float]:
    if s is None: return None
    t = norm_spaces(s); t = re.sub(r"[^\d,.\s]", "", t); t = re.sub(r"\s+", "", t)
    if not t: return None
    if "," in t and "." in t: t = t.replace(".", "").replace(",", ".")
    elif "," in t: t = t.replace(",", ".")
    try: return float(t)
    except ValueError: return None

def join_hyphen_wrap(a: str, b: str) -> str:
    if not a: return b
    if a.endswith("-") and BRAND_JOIN_HYPHEN_NO_SPACE: return a[:-1] + b
    return a + " " + b

def tokens_text(tokens: List[Dict[str, Any]]) -> str:
    s = ""
    for t in tokens: s = join_hyphen_wrap(s, t["text"]) if s else t["text"]
    return s.strip()

def median_ratio(tokens: List[Dict[str,Any]], width: float) -> float:
    xs = sorted((t["x0"]/width for t in tokens)); n = len(xs)
    return 0.0 if n == 0 else (xs[n//2] if n % 2 else 0.5*(xs[n//2-1]+xs[n//2]))

def is_integer_like(x: Optional[float]) -> bool:
    try: return float(x).is_integer()
    except Exception: return False

# ----------------------------- Lines -----------------------------
def page_to_lines(page) -> List[Dict[str, Any]]:
    words = page.extract_words(x_tolerance=X_TOL, y_tolerance=Y_TOL, keep_blank_chars=False)
    if not words: return []
    words.sort(key=lambda w: (round(w["top"],1), w["x0"]))
    lines, current, cur_top = [], [], None
    def push():
        current.sort(key=lambda z: z["x0"])
        lines.append({"top":min(t["top"] for t in current),
                      "bottom":max(t["bottom"] for t in current),
                      "tokens":current[:],
                      "text":tokens_text(current)})
    for w in words:
        if cur_top is None: cur_top=w["top"]; current=[w]; continue
        if abs(w["top"]-cur_top) <= Y_TOL: current.append(w)
        else: push(); cur_top=w["top"]; current=[w]
    if current: push()
    return lines

# ----------------------------- Bands -----------------------------
def calibrate_bands(page, width: float) -> Dict[str, float]:
    bands = DEFAULT_BAND.copy()
    words = page.extract_words(x_tolerance=2.0, y_tolerance=2.0, keep_blank_chars=False) or []
    words.sort(key=lambda w: (w["top"], w["x0"]))
    texts = [(strip_acc(w["text"]).upper(), w) for w in words]
    pack_x = unit_x = None
    for idx, (txt, w) in enumerate(texts):
        if txt in {"COUT","COÛT"}:
            y = w["top"]; nexts = [t for t, ww in texts[idx:idx+8] if abs(ww["top"]-y) < 2.0]
            if any("FORMAT" in t for t in nexts): pack_x = w["x0"]
        if txt == "PRIX":
            y = w["top"]; nexts = [t for t, ww in texts[idx:idx+10] if abs(ww["top"]-y) < 2.0]
            if any("UNITAIRE" in t for t in nexts): unit_x = w["x0"]
    if pack_x is not None: bands["pack_min"] = max(0.50, (pack_x - 10.0) / width)
    if unit_x is not None: bands["unit_min"] = max(bands["pack_min"] + 0.05, (unit_x - 10.0) / width)
    return bands

# ----------------------------- Classify -----------------------------
def looks_like_formline(text: str) -> bool:
    """
    v7 behavior: treat as 'form' if it has a form token, any strength, or PPB.
    """
    t = norm_spaces(text)
    return bool(RE_FORMLINE.search(t)) or bool(RE_STRENGTH.search(t)) or bool(RE_HAS_PPB.search(t))

def classify_line(line: Dict[str, Any], width: float, bands: Dict[str, float]) -> str:
    """
    'generic','form','din_row','pack_cont','packprice_only','unit_only','noise'
    """
    txt = line["text"].strip()
    if not txt: return "noise"
    # FORM before GENERIC (v7 rule)
    if looks_like_formline(txt):  return "form"
    if is_allcaps_candidate(txt): return "generic"
    if line["tokens"]:
        first = line["tokens"][0]["text"].strip()
        if RE_DIN.match(first): return "din_row"
    first_tok = line["tokens"][0]; left_x = first_tok["x0"] / width; first_txt = first_tok["text"].strip()
    if (RE_PACK_ONLY.match(first_txt) or RE_VOL.match(first_txt)) and left_x >= bands["pack_min"]:
        return "pack_cont"
    if RE_PRICE_NUM.match(txt):
        last_tok = line["tokens"][-1]; right_x = last_tok["x0"] / width; med_x = median_ratio(line["tokens"], width)
        if right_x >= bands["unit_min"]: return "unit_only"
        if bands["pack_min"] - 0.02 <= med_x < bands["unit_min"]: return "packprice_only"
    s = strip_acc(norm_spaces(txt)).upper()
    if (("COUT" in s or "COÛT" in s or "FORMAT" in s) and any(french_to_float(t["text"]) is not None for t in line["tokens"])):
        med_x = median_ratio(line["tokens"], width)
        if bands["pack_min"] - 0.05 <= med_x < bands["unit_min"] + 0.02: return "packprice_only"
    return "noise"

# ----------------------------- Helpers -----------------------------
def find_din_index(line: Dict[str, Any], width: float, bands: Dict[str, float]) -> Optional[int]:
    for j, t in enumerate(line["tokens"]):
        txt = t["text"].strip()
        if RE_DIN.match(txt) and (t["x0"]/width) <= bands["brand_max"] + 0.05:
            return j
    return None

def parse_form_strength(text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], bool]:
    """
    v7 behavior:
      - If a known form token present → that’s the form.
      - Else if strength present → take text before strength as form.
      - Else → whole line as form.
      - Strip PPB from both form and strength; split strength into value/unit.
    Returns: (form, strength_pretty, strength_value, strength_unit, ppb)
    """
    t = norm_spaces(text)
    ppb = bool(RE_HAS_PPB.search(t))
    m_strength = RE_STRENGTH.search(t)
    strength = m_strength.group(0) if m_strength else None
    m_form = RE_FORMLINE.search(t)
    if m_form:
        form = m_form.group(0)
    elif strength:
        form = t[:t.find(strength)].strip()
    else:
        form = t.strip()
    # remove PPB tokens
    form = re.sub(r"\bPPB\b","", form).strip() if form else None
    strength = re.sub(r"\bPPB\b","", strength).strip() if strength else None
    s_val, s_unit = None, None
    if strength:
        m = RE_STRENGTH_SPLIT.search(strength)
        if m:
            s_val  = m.group("val").replace(",", ".")
            s_unit = m.group("unit")
        else:
            m2 = re.search(r"(?i)(\d+(?:[.,]\d+)?)(.*)", strength)
            if m2:
                s_val = m2.group(1).replace(",", ".").strip()
                s_unit = m2.group(2).strip()
    return (form or None), (strength or None), (s_val or None), (s_unit or None), ppb

def split_brand_manuf(tokens: List[Dict[str, Any]], width: float, bands: Dict[str, float]):
    brand_tokens: List[Dict[str, Any]] = []; manuf_tokens: List[Dict[str, Any]] = []; right_candidates: List[Dict[str, Any]] = []
    for t in tokens:
        x = t["x0"]/width
        if x <= bands["brand_max"]: brand_tokens.append(t)
        elif bands["manuf_min"] <= x <= bands["manuf_max"]: manuf_tokens.append(t)
        else: right_candidates.append(t)
    def is_packish(txt: str) -> bool:
        tt = txt.strip()
        return bool(RE_PACK_ONLY.match(tt) or RE_VOL.match(tt) or RE_VOL_ONE.match(tt))
    moved = []
    while manuf_tokens and is_packish(manuf_tokens[-1]["text"]):
        moved.insert(0, manuf_tokens.pop())
        if moved and RE_VOL_ONE.match(moved[0]["text"]) and manuf_tokens and RE_PACK_ONLY.match(manuf_tokens[-1]["text"].strip()):
            moved.insert(0, manuf_tokens.pop())
    right_candidates.extend(moved)
    return brand_tokens, manuf_tokens, right_candidates

def harvest_wrapped_brand(lines: List[Dict[str, Any]], idx: int, width: float, bands: Dict[str, float], brand_tokens: List[Dict[str, Any]]) -> bool:
    if not brand_tokens: return False
    wrapped = False
    for k in range(1,3):
        if idx+k >= len(lines): break
        nxt = lines[idx+k]
        if not nxt["tokens"]: continue
        x = nxt["tokens"][0]["x0"]/width; cls = classify_line(nxt, width, bands)
        if cls in {"din_row","pack_cont"}: break
        if x <= bands["brand_max"] and cls=="noise":
            brand_tokens.extend(nxt["tokens"]); wrapped=True
        else: break
    return wrapped

def parse_generic_and_flags(tokens: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
    gen_tokens = []; flags = []
    for t in tokens:
        txt = norm_spaces(t["text"])
        up = strip_acc(txt).upper()
        if up in FLAG_TOKENS:
            if up not in flags: flags.append(up)
            continue
        gen_tokens.append(t)
    return tokens_text(gen_tokens).strip(), flags

def parse_pack_and_prices(line: Dict[str, Any], width: float, bands: Dict[str, float], extra_right_tokens: Optional[List[Dict[str,Any]]]=None):
    pack = None; pack_price=None; unit_price=None; unit_src=None
    right_tokens = [t for t in line["tokens"] if (t["x0"]/width) >= bands["pack_min"]]
    if extra_right_tokens: right_tokens.extend(extra_right_tokens)
    if not right_tokens: return pack, pack_price, unit_price, unit_src
    right_tokens.sort(key=lambda t: t["x0"])
    texts = [t["text"].strip() for t in right_tokens]
    rest = texts[:]
    if len(texts)>=2 and RE_VOL.match(texts[0]+" "+texts[1]): pack = texts[0]+" "+texts[1]; rest = texts[2:]
    elif RE_VOL.match(texts[0]): pack = texts[0]; rest = texts[1:]
    elif RE_PACK_ONLY.match(texts[0]): pack = texts[0]; rest = texts[1:]
    nums = [s for s in rest if RE_PRICE_NUM.match(s)]
    if nums:
        pack_price = french_to_float(nums[0])
        if len(nums)>=2: unit_price = french_to_float(nums[-1]); unit_src="Printed"
    # safety: kill only integer-equals-pack artefacts (not decimals)
    try:
        if pack is not None and pack_price is not None:
            if RE_PACK_ONLY.match(str(pack)) and is_integer_like(pack_price) and int(float(pack_price)) == int(str(pack)):
                pack_price = None
    except Exception: pass
    return pack, pack_price, unit_price, unit_src

# ----------------------------- Extraction -----------------------------
def extract_pdf(input_pdf: Path, output_csv: Path):
    headers = ["Generic","Flags","Form","Strength","StrengthValue","StrengthUnit","PPB",
               "DIN","Brand","Manufacturer","Pack","PackPrice","UnitPrice","UnitPriceSource",
               "Page","confidence","confidence_label"]

    with pdfplumber.open(str(input_pdf)) as pdf, open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers); w.writeheader()
        prog = Progress(len(pdf.pages), LOG_FILE)

        # --- STICKY STATE ACROSS PAGES ---
        current_generic: Optional[str] = None
        current_flags:   List[str] = []
        current_form:    Optional[str] = None
        cur_strength:    Optional[str] = None
        s_val = s_unit   = None
        ppb             = False

        for pageno, page in enumerate(pdf.pages, start=1):
            width  = float(page.width)
            bands  = calibrate_bands(page, width)
            lines  = page_to_lines(page)

            rows_this_page = 0
            i = 0
            while i < len(lines):
                ln  = lines[i]
                cls = classify_line(ln, width, bands)

                if cls == "generic":
                    # Parse generic + flags from the GENERIC line (v9 behavior)
                    gen, flags = parse_generic_and_flags(ln["tokens"])
                    current_generic = gen if gen else current_generic
                    current_flags = flags
                    i += 1; continue

                if cls == "form":
                    # v7 parser merged here
                    form, strength, s_val, s_unit, has_ppb = parse_form_strength(ln["text"])
                    if form: current_form = form
                    if strength: cur_strength = strength
                    ppb = has_ppb or ppb
                    i += 1; continue

                if cls == "din_row":
                    din_idx = find_din_index(ln, width, bands)
                    if din_idx is None: i += 1; continue

                    # Split brand/manufacturer
                    brand_tokens, manuf_tokens, right_candidates = split_brand_manuf(ln["tokens"][din_idx+1:], width, bands)
                    brand_wrapped = harvest_wrapped_brand(lines, i, width, bands, brand_tokens)
                    brand = tokens_text(brand_tokens).strip() or None
                    manufacturer = tokens_text(manuf_tokens).strip() or None

                    # Gather packs/prices (including continuations)
                    j = i + 1
                    packs_col: List[str] = []
                    pack_prices: List[Optional[float]] = []
                    unit_prices: List[Optional[float]] = []

                    pk, pkp, up, ups = parse_pack_and_prices(ln, width, bands, extra_right_tokens=right_candidates)
                    if pk: packs_col.append(pk)
                    if pkp is not None: pack_prices.append(pkp)
                    if up  is not None: unit_prices.append(up)

                    while j < len(lines):
                        nxt = lines[j]; nxt_cls = classify_line(nxt, width, bands)
                        if nxt_cls in {"din_row","generic","form"}: break
                        if nxt_cls == "pack_cont":
                            txts = [t["text"].strip() for t in nxt["tokens"] if (t["x0"]/width) >= bands["pack_min"]]
                            for t in txts:
                                if RE_VOL.match(t) or RE_PACK_ONLY.match(t): packs_col.append(t)
                        elif nxt_cls == "packprice_only":
                            vals = [french_to_float(t["text"]) for t in nxt["tokens"] if french_to_float(t["text"]) is not None]
                            pack_prices.extend(vals)
                        elif nxt_cls == "unit_only":
                            vals = [french_to_float(t["text"]) for t in nxt["tokens"] if french_to_float(t["text"]) is not None]
                            unit_prices.extend(vals)
                        j += 1

                    unit_prices = [u for u in unit_prices if u is not None]
                    default_unit = unit_prices[0] if unit_prices else None

                    if len(packs_col) > 1 and len(pack_prices) < len(packs_col) and default_unit is not None:
                        fixed_prices: List[Optional[float]] = []
                        for idx, ptxt in enumerate(packs_col):
                            if idx < len(pack_prices) and pack_prices[idx] is not None:
                                fixed_prices.append(pack_prices[idx])
                            else:
                                try:
                                    if RE_VOL.match(ptxt):
                                        pnum = float(re.findall(r"\d+(?:[.,]\d+)?", ptxt)[0].replace(",", "."))
                                    elif RE_PACK_ONLY.match(ptxt):
                                        pnum = float(ptxt)
                                    else:
                                        pnum = None
                                    fixed_prices.append(round(pnum * default_unit, 2) if pnum is not None else None)
                                except Exception:
                                    fixed_prices.append(None)
                        pack_prices = fixed_prices

                    n_rows = max(len(packs_col), 1)
                    for row_idx in range(n_rows):
                        pack_text = packs_col[row_idx] if row_idx < len(packs_col) else None
                        try:
                            pack_num = None
                            if pack_text:
                                if RE_VOL.match(pack_text):
                                    pack_num = float(re.findall(r"\d+(?:[.,]\d+)?", pack_text)[0].replace(",", "."))
                                elif RE_PACK_ONLY.match(pack_text):
                                    pack_num = float(pack_text)
                        except Exception:
                            pack_num = None

                        pack_price = pack_prices[row_idx] if row_idx < len(pack_prices) else (pack_prices[0] if len(pack_prices)==1 else None)
                        unit_price = (unit_prices[row_idx] if row_idx < len(unit_prices) else default_unit)
                        unit_src   = "Printed" if unit_price is not None else None

                        # final price leak protection (integers only)
                        if pack_text and pack_price is not None:
                            try:
                                if RE_PACK_ONLY.match(str(pack_text)) and is_integer_like(pack_price) and int(float(pack_price)) == int(str(pack_text)):
                                    pack_price = None
                            except Exception: pass

                        # SINGLE-ROW FALLBACK
                        if n_rows == 1 and pack_price is None and (unit_price is not None) and (pack_num is not None):
                            pack_price = round(pack_num * float(unit_price), 2)
                            unit_src = unit_src or "Derived"

                        # ---------- Confidence ----------
                        score = 1.0
                        if brand_wrapped: score -= 0.05
                        if not current_generic: score -= 0.25
                        if not current_form: score -= 0.10
                        if not brand: score -= 0.20
                        if not manufacturer: score -= 0.10
                        if unit_price is None: score -= 0.20
                        if pack_price is None: score -= 0.30
                        if pack_text is None: score -= 0.05
                        score = max(0.0, min(1.0, score))
                        conf_label = "high" if score >= 0.85 else ("medium" if score >= 0.60 else "low")

                        row = {
                            "Generic": (current_generic or None),
                            "Flags": " ".join(current_flags) if current_flags else None,
                            "Form": current_form,
                            "Strength": (f"{s_val} {s_unit}".strip() if s_val and s_unit else cur_strength),
                            "StrengthValue": s_val,
                            "StrengthUnit": s_unit,
                            "PPB": str(bool(ppb)).upper(),
                            "DIN": ln["tokens"][din_idx]["text"].strip(),
                            "Brand": brand,
                            "Manufacturer": manufacturer,
                            "Pack": pack_text,
                            "PackPrice": pack_price,
                            "UnitPrice": unit_price,
                            "UnitPriceSource": unit_src,
                            "Page": pageno,
                            "confidence": round(score, 2),
                            "confidence_label": conf_label
                        }
                        w.writerow(row); rows_this_page += 1

                    i = j; continue

                i += 1

            prog.tick(pageno, rows_this_page)

        prog.done()

# ----------------------------- Run -----------------------------
if __name__ == "__main__":
    if not INPUT_PDF.exists():
        raise SystemExit(f"Input PDF not found at: {INPUT_PDF}")
    extract_pdf(INPUT_PDF, OUTPUT_CSV)
