# -*- coding: utf-8 -*-
"""
ANNEXE V Extractor + POST-REPAIR WITH OPENAI (End-to-End, No Drama Switch)

Pipeline:
1) Extract Annexe V PDF -> annexe_v_extracted.csv
2) QA flags bad DINs (Product Group blank / Manufacturer blank / Manufacturer leaks format / etc.)
3) Re-scan PDF once to capture ORIGINAL row snippets for those DINs
4) AI repair (row-scoped) using OpenAI:
   - fixes Product Group (MARQUE DE COMMERCE / brand)
   - fixes Marketing Authority (FABRICANT / manufacturer)
   - fixes FORMAT + COÛT DU FORMAT + PRIX UNITAIRE (when clearly present)
   - updates Fill Size from corrected FORMAT
5) Writes:
   - annexe_v_extracted_REPAIRED.csv
   - annexe_v_extracted_AI_AUDIT.csv

AI toggle logic (no more confusion):
- If USE_OPENAI_REPAIR explicitly true/false -> obey it
- Else (not set) -> auto-enable if OPENAI_API_KEY is set

Windows:
  set OPENAI_API_KEY=...
  python 05_extract_annexe_v_COORD_WRAPFIX.py

Optional env:
  USE_OPENAI_REPAIR=true|false
  OPENAI_MODEL=gpt-5-mini
  OPENAI_MIN_CONF=0.70
  OPENAI_MAX_CALLS=2000
  X_TOL_EXTRACT=5
"""

from __future__ import annotations

from pathlib import Path
import os
import re
import csv
import json
import hashlib
import logging
import unicodedata
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

import pdfplumber

# -------------------------
# Optional encoding utilities
# -------------------------
try:
    from step_00_utils_encoding import clean_extracted_text, csv_writer_utf8, csv_reader_utf8
except ImportError:
    import io

    def clean_extracted_text(text: str, enforce_utf8: bool = True) -> str:
        if not text:
            return ""
        return unicodedata.normalize("NFC", str(text))

    def csv_writer_utf8(file_path, add_bom=True):
        encoding = "utf-8-sig" if add_bom else "utf-8"
        return io.open(file_path, "w", encoding=encoding, newline="", errors="replace")

    def csv_reader_utf8(file_path):
        return io.open(file_path, "r", encoding="utf-8-sig", newline="", errors="replace")


# =========================
# CONFIG LOADER (your pipeline)
# =========================
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))

from config_loader import (
    get_base_dir, get_split_pdf_dir, get_csv_output_dir, get_input_dir,
    get_env, get_env_bool,
    ANNEXE_V_PDF_NAME, ANNEXE_V_CSV_NAME, LOG_FILE_ANNEXE_V,
    STATIC_CURRENCY, STATIC_REGION,
    X_TOL, Y_TOL,
    ANNEXE_V_START_PAGE_1IDX, ANNEXE_V_MAX_ROWS,
    FINAL_COLUMNS
)

BASE_DIR = get_base_dir()
INPUT_DIR = get_split_pdf_dir()
OUTPUT_DIR = get_csv_output_dir()
START_PAGE_1IDX = ANNEXE_V_START_PAGE_1IDX
MAX_ROWS = ANNEXE_V_MAX_ROWS
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Input PDF - prefer split PDF, fallback to direct path
INPUT_PDF = INPUT_DIR / ANNEXE_V_PDF_NAME
if not INPUT_PDF.exists():
    CONF_STRICT_MODE = get_env_bool("CONF_STRICT_MODE", False)
    ALLOW_SCRIPT_DIR_PDF_FALLBACK = get_env_bool("ALLOW_SCRIPT_DIR_PDF_FALLBACK", False)

    if CONF_STRICT_MODE:
        raise FileNotFoundError(
            f"Input PDF not found in strict mode: {INPUT_PDF}\n"
            f"Please ensure Step 1 (Split PDF) completed successfully."
        )

    alt_paths = []
    try:
        input_dir = get_input_dir()
        alt_paths.append(input_dir / ANNEXE_V_PDF_NAME)
    except Exception:
        legacy_input = get_env("LEGACY_INPUT_DIR", "input")
        if Path(legacy_input).is_absolute():
            alt_paths.append(Path(legacy_input) / ANNEXE_V_PDF_NAME)
        else:
            alt_paths.append(BASE_DIR / legacy_input / ANNEXE_V_PDF_NAME)

    if ALLOW_SCRIPT_DIR_PDF_FALLBACK:
        alt_paths.append(Path(__file__).resolve().parent / ANNEXE_V_PDF_NAME)

    for alt in alt_paths:
        if alt.exists():
            INPUT_PDF = alt
            break
    else:
        raise FileNotFoundError(
            f"Input PDF not found: {INPUT_PDF}\n"
            f"Tried alternatives: {alt_paths}\n"
            f"Please ensure Step 1 (Split PDF) completed successfully."
        )

OUTPUT_CSV = OUTPUT_DIR / ANNEXE_V_CSV_NAME
LOG_FILE = OUTPUT_DIR / LOG_FILE_ANNEXE_V
FINAL_COLS = FINAL_COLUMNS

REPAIRED_CSV = OUTPUT_DIR / (OUTPUT_CSV.stem + "_REPAIRED.csv")
AUDIT_CSV = OUTPUT_DIR / (OUTPUT_CSV.stem + "_AI_AUDIT.csv")

# =========================
# HARDCODED API KEY
# =========================
HARDCODED_OPENAI_API_KEY = "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"  # Replace with your actual API key

# =========================
# NO-DRAMA AI SWITCH
# =========================
def resolve_use_openai_repair(logger: logging.Logger) -> bool:
    """
    Decide whether to run AI repair.
    Priority:
      1) USE_OPENAI_REPAIR env var explicitly true/false
      2) If not explicitly set -> auto-enable if OPENAI_API_KEY is present
    """
    raw = (os.getenv("USE_OPENAI_REPAIR") or "").strip().lower()
    has_key = bool((HARDCODED_OPENAI_API_KEY or "").strip())

    if raw in ("1", "true", "yes", "y", "on"):
        decision = True
        reason = "USE_OPENAI_REPAIR explicitly TRUE"
    elif raw in ("0", "false", "no", "n", "off"):
        decision = False
        reason = "USE_OPENAI_REPAIR explicitly FALSE"
    else:
        decision = has_key
        reason = "AUTO (OPENAI_API_KEY present)" if has_key else "AUTO (no OPENAI_API_KEY)"

    msg = f"[CFG] USE_OPENAI_REPAIR='{raw or '(not set)'}' | OPENAI_API_KEY={'SET' if has_key else 'NOT SET'} -> AI_REPAIR={'ON' if decision else 'OFF'} ({reason})"
    print(msg)
    logger.info(msg)
    return decision


# =========================
# REGEX PATTERNS
# =========================
RE_DIN = re.compile(r"^\d{6,9}$")

RE_PACK_ONLY = re.compile(r"^\d{1,4}$")
RE_VOL = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s?(mL|ml|L)$", re.I)
RE_VOL_TWO = re.compile(r"^\d{1,4}(?:[.,]\d+)?\s+(mL|ml|L)$", re.I)

RE_ALLCAPS = re.compile(r"^[A-ZÀ-ÖØ-Þ][A-ZÀ-ÖØ-Þ0-9/ '\-().]+:?$")
RE_FORM_WORD = re.compile(
    r"(?i)\b(Sol\.|Pd\.|Perf\.|Susp\.|Caps\.|Comp\.|Gel\.|Crème|Gouttes|I\.V\.|I\.M\.|S\.C\.|Orale)\b"
)
RE_PPB = re.compile(r"\bPPB\b", re.IGNORECASE)
RE_STRENGTH = re.compile(r"(?i)(\d+(?:[.,]\d+)?)\s*(mg|g|mcg|µg|U|UI|IU)\b")

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

def tokens_to_text(tokens: List[Dict[str, Any]]) -> str:
    tokens = sorted(tokens, key=lambda x: x["x0"])
    text = " ".join(t["text"] for t in tokens)
    return clean_extracted_text(text, enforce_utf8=True)

def page_to_lines(page) -> List[Dict[str, Any]]:
    x_tol_env = (os.getenv("X_TOL_EXTRACT") or "").strip()
    if x_tol_env:
        try:
            x_tol_extract = float(x_tol_env)
        except Exception:
            x_tol_extract = 5.0
    else:
        x_tol_extract = min(float(X_TOL), 5.0)

    words = page.extract_words(
        x_tolerance=x_tol_extract,
        y_tolerance=Y_TOL,
        keep_blank_chars=False
    ) or []
    if not words:
        return []

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
            "text": tokens_to_text(cur),
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

def is_generic_header(text: str) -> bool:
    s = norm_spaces(text).rstrip(":")
    if not s:
        return False
    if re.match(r"^\d", s):
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
    if not fmt:
        return None
    f = norm_spaces(fmt)
    m = re.search(r"(\d+(?:[.,]\d+)?)", f)
    if not m:
        return None
    val = float(m.group(1).replace(",", "."))
    return int(val) if val.is_integer() else val

def find_din_token_idx(tokens: List[Dict[str, Any]]) -> Optional[int]:
    candidates = []
    for i, t in enumerate(tokens):
        s = t["text"].strip()
        if RE_DIN.match(s):
            candidates.append((t["x0"], i))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]

def gather_wrapped_after_tokens(
    lines: List[Dict[str, Any]],
    start_i: int,
    din_idx: int,
    din_x: float,
) -> Tuple[List[Dict[str, Any]], int]:
    after_tokens = sorted(lines[start_i]["tokens"][din_idx + 1:], key=lambda t: t["x0"])
    last_i = start_i

    MAX_CONT_LINES = 3
    MAX_Y_GAP = 14.0
    base_top = lines[start_i].get("top", None)

    def looks_like_new_header(txt: str) -> bool:
        t = (txt or "").strip()
        if not t:
            return False
        letters = re.sub(r"[^A-ZÀÂÄÇÉÈÊËÎÏÔÖÙÛÜŸÆŒ]", "", t.upper())
        return len(t) >= 6 and len(letters) / max(1, len(t)) >= 0.70 and t == t.upper()

    consumed = 0
    i = start_i + 1
    while i < len(lines) and consumed < MAX_CONT_LINES:
        nxt = lines[i]
        txt_clean = clean_extracted_text(nxt.get("text", ""), enforce_utf8=True).strip()

        if find_din_token_idx(nxt.get("tokens", [])) is not None:
            break
        if looks_like_new_header(txt_clean):
            break
        if is_form_line(txt_clean):
            break
        if base_top is not None and nxt.get("top") is not None:
            if abs(nxt["top"] - base_top) > (MAX_Y_GAP * 2):
                break

        right_side = [t for t in nxt.get("tokens", []) if t.get("x0", 0) >= (din_x - 2.0)]
        if not right_side:
            break

        after_tokens.extend(right_side)
        last_i = i
        consumed += 1
        i += 1

    return (sorted(after_tokens, key=lambda t: t["x0"]), last_i)

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
    toks = tokens_sorted
    for i in range(len(toks)):
        if leftmost_price_x is not None and toks[i]["x0"] >= leftmost_price_x:
            break
        a = norm_spaces(toks[i]["text"])
        if i + 1 < len(toks):
            b = norm_spaces(toks[i + 1]["text"])
            cand2 = norm_spaces(a + " " + b)
            if RE_VOL_TWO.match(cand2):
                return cand2, toks[i]["x0"]
        if RE_VOL.match(a) or RE_PACK_ONLY.match(a):
            return a, toks[i]["x0"]
    return None, None

def parse_line_format_cost(after_tokens: List[Dict[str, Any]]) -> List[Tuple[Optional[str], Optional[float], Optional[float]]]:
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return []

    def is_price_like(txt: str) -> bool:
        txt = norm_spaces(txt)
        if not txt:
            return False
        cleaned = re.sub(r"[^\d,.\s]", "", txt).strip()
        if not cleaned:
            return False
        v = french_to_float(cleaned)
        if v is None:
            return False
        if "," in cleaned or "." in cleaned:
            return True
        digits = re.sub(r"\D", "", cleaned)
        return len(digits) >= 4 and 10 <= v <= 100000

    leftmost_price_x = None
    for t in toks:
        if is_price_like(t["text"]):
            leftmost_price_x = t["x0"] if leftmost_price_x is None else min(leftmost_price_x, t["x0"])

    if leftmost_price_x is None:
        return []

    fmt, _ = find_format_token(toks, leftmost_price_x)

    nums: List[float] = []
    for t in toks:
        if t["x0"] < leftmost_price_x:
            continue
        if is_price_like(t["text"]):
            v = french_to_float(t["text"])
            if v is not None:
                nums.append(v)

    cost = nums[0] if len(nums) >= 1 else None
    unit = nums[1] if len(nums) >= 2 else None
    return [(fmt, cost, unit)]

def brand_and_manufacturer_from_after_tokens(after_tokens: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    toks = sorted(after_tokens, key=lambda t: t["x0"])
    if not toks:
        return (None, None)

    left_price_x = find_leftmost_price_x(toks)
    _fmt, fmt_x = find_format_token(toks, left_price_x)

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
    gaps = [b["x0"] - a["x1"] for a, b in zip(text_toks, text_toks[1:])]

    split_idx = None
    if gaps and max(gaps) >= 10:
        split_idx = gaps.index(max(gaps)) + 1

    if split_idx is None:
        manu_toks = text_toks[-2:] if len(text_toks) >= 2 else text_toks[-1:]
        brand_toks = text_toks[:-len(manu_toks)] if len(text_toks) > len(manu_toks) else []
    else:
        brand_toks = text_toks[:split_idx]
        manu_toks = text_toks[split_idx:]

    brand = tokens_to_text(brand_toks).strip() or None
    manu = tokens_to_text(manu_toks).strip() or None
    return (brand, manu)


# =========================
# CSV helpers
# =========================
def load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with csv_reader_utf8(path) as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def write_csv_rows(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]):
    with csv_writer_utf8(path, add_bom=True) as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


# =========================
# QA flags
# =========================
def looks_like_leak(marketing_authority: str) -> bool:
    ma = norm_spaces(marketing_authority)
    if not ma:
        return False
    return bool(re.search(r"\b\d+(?:[.,]\d+)?\s*(mg|g|mcg|µg|U|UI|IU|mL|ml|L)\b", ma, flags=re.I))

def qa_flag_row(row: Dict[str, Any]) -> List[str]:
    flags = []
    pg = norm_spaces(row.get("Product Group", ""))
    ma = norm_spaces(row.get("Marketing Authority", ""))
    cost = row.get("Ex Factory Wholesale Price", "")
    unit = row.get("Unit Price", "")

    if not pg:
        flags.append("PRODUCT_GROUP_BLANK")
    if not ma:
        flags.append("MARKETING_AUTHORITY_BLANK")
    if ma and looks_like_leak(ma):
        flags.append("MANUFACTURER_LEAKS_FORMAT")
    if (cost in ("", None)) and (unit in ("", None)):
        flags.append("BOTH_PRICES_BLANK")
    return flags


# =========================
# OpenAI integration
# =========================
def _openai_client():
    api_key = (HARDCODED_OPENAI_API_KEY or "").strip()
    if not api_key:
        return None
    
    # Validate API key format
    if not api_key.startswith("sk-"):
        raise ValueError(
            f"Invalid API key format. OpenAI API keys should start with 'sk-'.\n"
            f"Current key starts with: {api_key[:10] if len(api_key) >= 10 else api_key}...\n"
            f"Please verify your API key at https://platform.openai.com/account/api-keys"
        )
    
    if len(api_key) < 20:
        raise ValueError(
            f"API key appears to be too short ({len(api_key)} characters). "
            f"OpenAI API keys are typically 51+ characters long.\n"
            f"Please verify your API key at https://platform.openai.com/account/api-keys"
        )
    
    try:
        from openai import OpenAI
    except Exception:
        return None
    return OpenAI(api_key=api_key)

OPENAI_REPAIR_SCHEMA = {
    "name": "annexe_v_row_repair",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "product_group": {"type": ["string", "null"]},
            "marketing_authority": {"type": ["string", "null"]},
            "format_token": {"type": ["string", "null"]},
            "ex_factory_wholesale_price": {"type": ["number", "null"]},
            "unit_price": {"type": ["number", "null"]},
            "confidence": {"type": "number"},
            "note": {"type": "string"},
        },
        "required": [
            "product_group", "marketing_authority", "format_token",
            "ex_factory_wholesale_price", "unit_price",
            "confidence", "note"
        ],
        "additionalProperties": False,
    },
}

def repair_with_openai(client, snippet: str, current_row: Dict[str, Any]) -> Tuple[Dict[str, Any], float, str]:
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    prompt = f"""
You are repairing ONE row extracted from a French PDF table (Annexe V).

Correct ONLY these fields if they are visible in the snippet:
- Product Group = MARQUE DE COMMERCE (brand)
- Marketing Authority = FABRICANT (manufacturer)
- Format token = FORMAT column (pack count / volume)
- Ex Factory Wholesale Price = COÛT DU FORMAT
- Unit Price = PRIX UNITAIRE

Rules:
- Use ONLY what is in the snippet. If missing, return null.
- Do NOT invent.
- If manufacturer currently contains dose/format (e.g., "100 mg Apotex"), remove the dose and keep the manufacturer.
- Return confidence 0..1 + a short note.

PDF snippet:
{snippet}

Current parsed row:
{json.dumps(current_row, ensure_ascii=False)}
""".strip()

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_schema", "json_schema": OPENAI_REPAIR_SCHEMA},
        )
    except Exception as e:
        error_msg = str(e)
        if "401" in error_msg or "invalid_api_key" in error_msg.lower() or "authentication" in error_msg.lower():
            raise SystemExit(
                f"\n{'='*80}\n"
                f"OPENAI API AUTHENTICATION ERROR\n"
                f"{'='*80}\n"
                f"The API key is invalid, expired, or doesn't have the required permissions.\n\n"
                f"To fix this:\n"
                f"1. Go to https://platform.openai.com/account/api-keys\n"
                f"2. Create a new API key or verify your existing key\n"
                f"3. Update HARDCODED_OPENAI_API_KEY in the script (line 142)\n"
                f"4. Make sure the key starts with 'sk-' and is complete (51+ characters)\n"
                f"{'='*80}\n"
            ) from e
        raise

    if not resp.choices or not resp.choices[0].message.content:
        raise ValueError("OpenAI API returned empty response")
    
    data = json.loads(resp.choices[0].message.content)
    conf = float(data.get("confidence", 0.0))
    note = data.get("note", "")

    # apply to the working row (only if non-null)
    if data.get("product_group") is not None:
        current_row["Product Group"] = norm_spaces(data["product_group"]) or ""
    if data.get("marketing_authority") is not None:
        current_row["Marketing Authority"] = norm_spaces(data["marketing_authority"]) or ""
    if data.get("format_token") is not None:
        fmt = norm_spaces(data["format_token"]) or ""
        if fmt:
            current_row["_AI_FORMAT_TOKEN"] = fmt
            fs = fill_size_from_format(fmt)
            current_row["Fill Size"] = "" if fs is None else str(fs)
    if data.get("ex_factory_wholesale_price") is not None:
        v = data["ex_factory_wholesale_price"]
        current_row["Ex Factory Wholesale Price"] = "" if v is None else str(v)
    if data.get("unit_price") is not None:
        v = data["unit_price"]
        current_row["Unit Price"] = "" if v is None else str(v)

    current_row["_AI_NOTE"] = note
    current_row["_AI_CONF"] = f"{conf:.3f}"
    return current_row, conf, note


# =========================
# Snippets for target DINs
# =========================
def make_row_snippet(
    generic: Optional[str],
    formline: Optional[str],
    lines: List[Dict[str, Any]],
    din_line_i: int,
    last_i: int,
) -> str:
    parts = []
    parts.append(f"[GENERIC]\n{generic or ''}")
    parts.append(f"[FORM]\n{formline or ''}")
    for idx in range(din_line_i, min(len(lines), last_i + 2)):
        parts.append(f"[LINE {idx}]\n{lines[idx].get('text','')}")
    return "\n\n".join(parts)

def collect_snippets_for_dins(pdf_path: Path, target_dins: set[str], start_page_1idx: int, logger: logging.Logger) -> Dict[str, str]:
    found: Dict[str, str] = {}
    with pdfplumber.open(str(pdf_path)) as pdf:
        total_pages = len(pdf.pages)
        current_generic: Optional[str] = None
        current_formline: Optional[str] = None

        for page_no_1idx in range(start_page_1idx, total_pages + 1):
            page = pdf.pages[page_no_1idx - 1]
            lines = page_to_lines(page)

            i = 0
            while i < len(lines):
                text = lines[i].get("text", "")

                if is_generic_header(text):
                    g = text.rstrip(":").strip()
                    g = re.sub(r"\s+[A-Z]$", "", g).strip()
                    current_generic = clean_extracted_text(g, enforce_utf8=True) if g else current_generic
                    current_formline = None
                    i += 1
                    continue

                din_idx = find_din_token_idx(lines[i].get("tokens", []))
                if din_idx is None and is_form_line(text):
                    current_formline = clean_extracted_text(text.strip(), enforce_utf8=True)
                    i += 1
                    continue

                if din_idx is not None:
                    din_raw = lines[i]["tokens"][din_idx]["text"].strip()
                    din = din_raw.zfill(8)
                    din_x = lines[i]["tokens"][din_idx]["x0"]
                    _after_tokens, last_i = gather_wrapped_after_tokens(lines, i, din_idx, din_x)

                    if din in target_dins and din not in found:
                        found[din] = make_row_snippet(current_generic, current_formline, lines, i, last_i)
                        logger.info(f"Snippet captured for DIN={din} on page {page_no_1idx}")

                    i = last_i + 1
                    continue

                i += 1

            if len(found) == len(target_dins):
                break

    missing = target_dins - set(found.keys())
    if missing:
        logger.warning(f"Snippets missing for {len(missing)} DINs. Example: {list(sorted(missing))[:10]}")
    return found


# =========================
# Extraction
# =========================
def extract_annexe_v_to_csv(logger: logging.Logger) -> None:
    if not INPUT_PDF.exists():
        raise SystemExit(f"Input PDF not found: {INPUT_PDF}")

    total_rows = 0
    with pdfplumber.open(str(INPUT_PDF)) as pdf:
        total_pages = len(pdf.pages)
        if START_PAGE_1IDX > total_pages:
            raise SystemExit(f"START_PAGE_1IDX={START_PAGE_1IDX} exceeds total pages={total_pages}")

        with csv_writer_utf8(OUTPUT_CSV, add_bom=True) as f:
            writer = csv.DictWriter(f, fieldnames=FINAL_COLS)
            writer.writeheader()

            current_generic: Optional[str] = None
            current_formline: Optional[str] = None

            for page_no_1idx in range(START_PAGE_1IDX, total_pages + 1):
                page = pdf.pages[page_no_1idx - 1]
                lines = page_to_lines(page)

                i = 0
                page_rows = 0

                while i < len(lines):
                    text = lines[i].get("text", "")

                    if is_generic_header(text):
                        g = text.rstrip(":").strip()
                        g = re.sub(r"\s+[A-Z]$", "", g).strip()
                        current_generic = clean_extracted_text(g, enforce_utf8=True) if g else current_generic
                        current_formline = None
                        i += 1
                        continue

                    din_idx = find_din_token_idx(lines[i].get("tokens", []))
                    if din_idx is None and is_form_line(text):
                        current_formline = clean_extracted_text(text.strip(), enforce_utf8=True)
                        i += 1
                        continue

                    if din_idx is not None:
                        din_raw = lines[i]["tokens"][din_idx]["text"].strip()
                        din = din_raw.zfill(8)
                        din_x = lines[i]["tokens"][din_idx]["x0"]

                        after_tokens, last_i = gather_wrapped_after_tokens(lines, i, din_idx, din_x)
                        product_group, manufacturer = brand_and_manufacturer_from_after_tokens(after_tokens)

                        fmts: List[Optional[str]] = []
                        costs: List[Optional[float]] = []
                        units: List[Optional[float]] = []

                        for fmt, cost, unit in parse_line_format_cost(after_tokens):
                            fmts.append(fmt)
                            if cost is not None:
                                costs.append(cost)
                            if unit is not None:
                                units.append(unit)

                        local_desc, formulation, strength_val, strength_unit = ("", "", None, None)
                        if current_formline:
                            local_desc, formulation, strength_val, strength_unit = parse_form_strength(current_formline)

                        n = max(len(fmts), len(costs), 1)
                        for k in range(n):
                            fmt_k = fmts[k] if k < len(fmts) else None
                            cost_k = costs[k] if k < len(costs) else (costs[0] if len(costs) == 1 else None)
                            unit_k = units[k] if k < len(units) else (units[0] if len(units) == 1 else None)

                            fs = fill_size_from_format(fmt_k)

                            row = {
                                "Generic Name": current_generic or "",
                                "Currency": STATIC_CURRENCY,
                                "Ex Factory Wholesale Price": "" if cost_k is None else str(cost_k),
                                "Unit Price": "" if unit_k is None else str(unit_k),
                                "Region": STATIC_REGION,
                                "Product Group": product_group or "",
                                "Marketing Authority": manufacturer or "",
                                "Local Pack Description": local_desc or "",
                                "Formulation": formulation or "",
                                "Fill Size": "" if fs is None else str(fs),
                                "Strength": "" if strength_val is None else str(strength_val),
                                "Strength Unit": strength_unit or "",
                                "LOCAL_PACK_CODE": din,
                            }

                            writer.writerow(row)
                            total_rows += 1
                            page_rows += 1

                            if MAX_ROWS is not None and total_rows >= MAX_ROWS:
                                logger.warning(f"MAX_ROWS reached ({MAX_ROWS}). Stopping early.")
                                return

                        i = last_i + 1
                        continue

                    i += 1

                if page_no_1idx % 50 == 0 or page_rows > 0:
                    print(f"Page {page_no_1idx}/{total_pages} done | rows so far: {total_rows:,}")
                    logger.info(f"Page {page_no_1idx}/{total_pages} done | rows={total_rows:,}")


# =========================
# Main pipeline
# =========================
def run_pipeline():
    logging.basicConfig(
        filename=str(LOG_FILE),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("annexe_v_pipeline")

    logger.info("=" * 80)
    logger.info("ANNEXE V PIPELINE STARTED (EXTRACT + POST AI REPAIR)")
    logger.info(f"Start: {datetime.now().isoformat(sep=' ', timespec='seconds')}")
    logger.info(f"Input PDF: {INPUT_PDF}")
    logger.info(f"Output CSV: {OUTPUT_CSV}")
    logger.info("=" * 80)

    # 1) Extract
    extract_annexe_v_to_csv(logger)
    print(f"[OK] Extracted CSV: {OUTPUT_CSV}")

    # 2) QA flags
    rows = load_csv_rows(OUTPUT_CSV)
    din_to_flags: Dict[str, List[str]] = {}
    for row in rows:
        din = norm_spaces(row.get("LOCAL_PACK_CODE", "")).zfill(8)
        flags = qa_flag_row(row)
        if flags:
            din_to_flags.setdefault(din, [])
            for fl in flags:
                if fl not in din_to_flags[din]:
                    din_to_flags[din].append(fl)

    target_dins = set(din_to_flags.keys())
    print(f"[QA] Flagged DINs needing repair: {len(target_dins)}")
    if not target_dins:
        write_csv_rows(REPAIRED_CSV, rows, FINAL_COLS)
        print(f"[OK] No repairs needed. Final: {REPAIRED_CSV}")
        return

    # 3) Collect PDF snippets for those DINs
    snippets = collect_snippets_for_dins(INPUT_PDF, target_dins, START_PAGE_1IDX, logger)
    print(f"[PDF] Snippets found: {len(snippets)}/{len(target_dins)}")

    # 4) Decide AI repair
    USE_OPENAI_REPAIR = resolve_use_openai_repair(logger)
    if not USE_OPENAI_REPAIR:
        print("[INFO] AI repair OFF -> writing extracted output as REPAIRED (no changes).")
        write_csv_rows(REPAIRED_CSV, rows, FINAL_COLS)
        print(f"[OK] Final (no AI repair): {REPAIRED_CSV}")
        return

    # 5) Create OpenAI client
    client = _openai_client()
    if client is None:
        raise SystemExit(
            "AI repair is ON but OpenAI client unavailable.\n"
            "Fix:\n"
            "  pip install openai\n"
            "  set OPENAI_API_KEY=...\n"
        )

    min_conf = float(os.getenv("OPENAI_MIN_CONF", "0.70"))
    max_calls = int(os.getenv("OPENAI_MAX_CALLS", "2000"))
    calls = 0

    # Cache by snippet hash (avoid repeated calls)
    cache: Dict[str, Tuple[Dict[str, Any], float, str]] = {}

    audit_rows: List[Dict[str, Any]] = []
    repaired_count = 0
    skipped_low_conf = 0

    for idx, row in enumerate(rows):
        din = norm_spaces(row.get("LOCAL_PACK_CODE", "")).zfill(8)
        if din not in target_dins:
            continue

        snippet = snippets.get(din, "")
        if not snippet:
            continue

        flags = din_to_flags.get(din, [])
        if not flags:
            continue

        before = {k: row.get(k, "") for k in ["Product Group", "Marketing Authority", "Ex Factory Wholesale Price", "Unit Price", "Fill Size"]}

        h = hashlib.sha256(snippet.encode("utf-8", errors="ignore")).hexdigest()
        cache_key = f"{din}:{h}"

        if cache_key in cache:
            fixed_row, conf, note = cache[cache_key]
        else:
            if calls >= max_calls:
                print(f"[WARN] OPENAI_MAX_CALLS reached ({max_calls}). Stopping AI repair early.")
                break

            working = dict(row)
            working["_QA_FLAGS"] = ",".join(flags)
            fixed_row, conf, note = repair_with_openai(client, snippet, working)
            calls += 1
            cache[cache_key] = (fixed_row, conf, note)

        # Apply only if confident
        if conf >= min_conf:
            for k in ["Product Group", "Marketing Authority", "Ex Factory Wholesale Price", "Unit Price", "Fill Size"]:
                if k in fixed_row:
                    row[k] = fixed_row[k]
            repaired_count += 1
        else:
            skipped_low_conf += 1

        after = {k: row.get(k, "") for k in ["Product Group", "Marketing Authority", "Ex Factory Wholesale Price", "Unit Price", "Fill Size"]}

        audit_rows.append({
            "row_index": idx,
            "DIN": din,
            "flags": ",".join(flags),
            "ai_confidence": f"{conf:.3f}",
            "ai_note": note,
            "before_Product Group": before["Product Group"],
            "after_Product Group": after["Product Group"],
            "before_Marketing Authority": before["Marketing Authority"],
            "after_Marketing Authority": after["Marketing Authority"],
            "before_Ex Factory Wholesale Price": before["Ex Factory Wholesale Price"],
            "after_Ex Factory Wholesale Price": after["Ex Factory Wholesale Price"],
            "before_Unit Price": before["Unit Price"],
            "after_Unit Price": after["Unit Price"],
            "before_Fill Size": before["Fill Size"],
            "after_Fill Size": after["Fill Size"],
        })

        if (len(audit_rows) % 50) == 0:
            print(f"[AI] fixed={repaired_count} | low_conf_skipped={skipped_low_conf} | calls={calls}/{max_calls}")

    # 6) Write outputs
    write_csv_rows(REPAIRED_CSV, rows, FINAL_COLS)
    print(f"[OK] Repaired CSV saved: {REPAIRED_CSV}")

    if audit_rows:
        audit_fields = list(audit_rows[0].keys())
        write_csv_rows(AUDIT_CSV, audit_rows, audit_fields)
        print(f"[OK] Audit CSV saved: {AUDIT_CSV}")

    print(f"[DONE] calls={calls} | repaired(conf>={min_conf})={repaired_count} | low_conf_skipped={skipped_low_conf}")


if __name__ == "__main__":
    run_pipeline()
