"""
Annexe V (RAMQ / Canada-Quebec) PDF → text → section/subsection chunking → Routeway.ai LLM parsing → final CSV

What this script does (end-to-end):
1) Extracts text from the PDF (page-by-page) using pdfplumber
2) Detects "structure codes" like 24:32.08 and uses them to chunk content
3) Sends each chunk to Routeway.ai with the main structure label and subsection code
4) Forces STRICT JSON output from the LLM (so commas in prices don't break CSV)
5) Normalizes prices (comma → dot), handles multiline gracefully
6) Writes one final CSV with the exact columns you requested

REQUIREMENTS:
- pip install pdfplumber requests
- Set env var with MULTIPLE API KEYS (rotation supported):
    Windows (PowerShell):
      setx ROUTEWAY_API_KEYS "sk-...key1...,sk-...key2..."
    Or set one key:
      setx ROUTEWAY_API_KEYS "sk-...key1..."
- Optional:
    setx ROUTEWAY_MODEL "gpt-oss-120b:free"

USAGE:
- Put your PDF path in PDF_PATH below
- Run: python annexe_v_to_csv_routeway.py
"""

import os
import re
import json
import time
import csv
import random
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

import requests
import pdfplumber


# -------------------------
# CONFIG
# -------------------------
PDF_PATH = r"D:\quad99\Scappers\output\CanadaQuebec\split_pdf\annexe_v.pdf"
OUT_CSV = "annexe_v_extraction.csv"

ROUTEWAY_BASE_URL = "https://api.routeway.ai/v1"
CHAT_ENDPOINT = f"{ROUTEWAY_BASE_URL}/chat/completions"
MODEL = os.getenv("ROUTEWAY_MODEL", "gpt-oss-120b:free")

# Multi-key support (comma-separated)
API_KEYS_RAW = "sk-ogati1lM0lwp7Z59leYfUkJ0mhc8UsquZrI5jbZX7dAfiy3EjNG6z_DKClDt2S8SJDl1gblApuwIetU"   # <-- put your NEW key here
API_KEYS = [k.strip() for k in API_KEYS_RAW.split(",") if k.strip()]
if not API_KEYS:
    raise SystemExit("Missing ROUTEWAY_API_KEYS env var. Set one or multiple keys comma-separated.")

# Constant fields (Quebec / RAMQ)
DEFAULT_COUNTRY = "CANADA-QUEBEC"
DEFAULT_CURRENCY = "CAD"
DEFAULT_REGION = "NORTH AMERICA"

# Chunking controls
MIN_LINES_PER_CHUNK = 30
MAX_CHUNK_CHARS = 20_000  # keep prompts safe; adjust if needed
PAGE_OVERLAP_LINES = 8     # overlap between chunks to avoid boundary cuts

# Retry controls
MAX_RETRIES_PER_CHUNK = 5
RETRY_BACKOFF_BASE = 1.8
MAX_RETRY_SLEEP = 60
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Failure handling
SKIP_FAILED_CHUNKS = True
FAILED_CHUNKS_PATH = "routeway_failed_chunks.jsonl"

# Output schema (exact column order requested)
FIELDNAMES = [
    "Country",
    "Company",
    "Local Product Name",
    "Generic Name",
    "Currency",
    "Ex Factory Wholesale Price",
    "Region",
    "Marketing Authority",
    "Local Pack Description",
    "Formulation",
    "Fill Size",
    "Strength",
    "Strength Unit",
    "LOCAL_PACK_CODE",
    # helpful trace columns (comment out if you don't want)
    "Main Structure",
    "Sub-Structure",
    "Source Page"
]

# Regex for structure codes
STRUCT_RE = re.compile(r"^\s*(\d+:\d+(?:\.\d+)?)\s*$")
MAIN_STRUCT_RE = re.compile(r"^\s*(\d+):00\s*$")  # e.g., 24:00


# -------------------------
# DATA MODELS
# -------------------------
@dataclass
class Chunk:
    main_structure: str          # e.g. "24:00"
    sub_structure: str           # e.g. "24:32.08"
    start_page: int              # 1-indexed
    end_page: int                # 1-indexed
    text: str                    # chunk text


# -------------------------
# HELPERS
# -------------------------
def normalize_decimal(s: str) -> str:
    """
    Convert European decimal commas to dots for price-like strings.
    Example: "1428,48" -> "1428.48"
    Leaves other strings unchanged.
    """
    if s is None:
        return ""
    s = str(s).strip()
    # Replace comma between digits with dot
    s = re.sub(r"(?<=\d),(?=\d)", ".", s)
    return s


def safe_str(x) -> str:
    return "" if x is None else str(x).strip()


def pick_api_key(i: int) -> str:
    # deterministic rotation by chunk index
    return API_KEYS[i % len(API_KEYS)]


class RoutewayError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(f"Routeway error {status_code}: {message}")
        self.status_code = status_code


def routeway_chat(api_key: str, messages: List[Dict], temperature: float = 0.0, timeout: int = 120) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    r = requests.post(CHAT_ENDPOINT, headers=headers, json=payload, timeout=timeout)
    if not r.ok:
        raise RoutewayError(r.status_code, r.text)
    data = r.json()
    return data["choices"][0]["message"]["content"]


def strip_code_fences(text: str) -> str:
    t = text.strip()
    if t.startswith("```"):
        # remove first fence line
        t = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", t)
        # remove ending fence
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def json_loads_loose(text: str) -> dict:
    t = strip_code_fences(text)
    return json.loads(t)


def clean_extracted_lines(page_text: str) -> List[str]:
    """
    Clean and keep lines. Avoid empty lines, trim, and remove obvious repeated headers if present.
    """
    if not page_text:
        return []
    lines = [ln.strip() for ln in page_text.splitlines()]
    lines = [ln for ln in lines if ln]

    # Optional: drop very common repeating column header lines
    # (Keep conservative; don't over-delete)
    drop_patterns = [
        r"^CODE\b.*",  # table header sometimes starts with CODE
        r"^ANNEXE\s+V\b.*",
        r"^Liste des médicaments\b.*",
        r"^\d+\s*/\s*\d+$",  # page x/y patterns sometimes
    ]
    cleaned = []
    for ln in lines:
        if any(re.match(pat, ln, re.IGNORECASE) for pat in drop_patterns):
            continue
        cleaned.append(ln)
    return cleaned


# -------------------------
# 1) PDF → TEXT (page lines)
# -------------------------
def extract_pdf_lines(pdf_path: str) -> List[Tuple[int, List[str]]]:
    """
    Returns list of (page_number_1_indexed, lines[])
    """
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for idx, page in enumerate(pdf.pages, start=1):
            txt = page.extract_text() or ""
            lines = clean_extracted_lines(txt)
            out.append((idx, lines))
    return out


# -------------------------
# 2) STRUCTURE-AWARE CHUNKING
# -------------------------
def detect_structure_lines(lines: List[str]) -> List[Tuple[int, str]]:
    """
    Return [(line_index, code)] for lines matching STRUCT_RE
    """
    hits = []
    for i, ln in enumerate(lines):
        m = STRUCT_RE.match(ln)
        if m:
            hits.append((i, m.group(1)))
    return hits


def build_chunks(pages: List[Tuple[int, List[str]]]) -> List[Chunk]:
    """
    Build chunks by sub-structure boundaries.

    Strategy:
    - Track current main_structure (X:00)
    - Track current sub_structure (X:YY or X:YY.ZZ)
    - Collect lines until next sub_structure appears
    - Chunk size guard by MAX_CHUNK_CHARS with overlap
    """
    chunks: List[Chunk] = []
    current_main = ""
    current_sub = ""
    buffer_lines: List[str] = []
    buffer_start_page = None
    buffer_end_page = None

    def flush_if_ready(force: bool = False):
        nonlocal buffer_lines, buffer_start_page, buffer_end_page, current_main, current_sub
        if not buffer_lines:
            return
        text = "\n".join(buffer_lines).strip()
        if not force and (len(text) < 200 or len(buffer_lines) < MIN_LINES_PER_CHUNK):
            return

        chunks.append(
            Chunk(
                main_structure=current_main or "",
                sub_structure=current_sub or "",
                start_page=buffer_start_page or 1,
                end_page=buffer_end_page or (buffer_start_page or 1),
                text=text,
            )
        )
        buffer_lines = []
        buffer_start_page = None
        buffer_end_page = None

    for page_num, lines in pages:
        if not lines:
            continue

        # If page contains new structure codes, we handle boundaries line-by-line
        i = 0
        while i < len(lines):
            ln = lines[i]
            sm = STRUCT_RE.match(ln)
            if sm:
                code = sm.group(1)
                # Update main structure if X:00
                mm = MAIN_STRUCT_RE.match(code)
                if mm:
                    # main structure switch: flush old
                    flush_if_ready(force=True)
                    current_main = code
                    current_sub = code  # default sub = main until we see deeper
                    # start new buffer with this code line too
                    if buffer_start_page is None:
                        buffer_start_page = page_num
                    buffer_end_page = page_num
                    buffer_lines.append(code)
                    i += 1
                    continue

                # sub-structure boundary (e.g., 8:12.06)
                # flush previous buffer, then start new with the new sub code
                flush_if_ready(force=True)
                # if main not set, infer from prefix
                if not current_main:
                    prefix = code.split(":")[0]
                    current_main = f"{prefix}:00"
                current_sub = code
                buffer_start_page = page_num
                buffer_end_page = page_num
                buffer_lines.append(code)
                i += 1
                continue

            # regular line
            if buffer_start_page is None:
                buffer_start_page = page_num
            buffer_end_page = page_num
            buffer_lines.append(ln)

            # enforce size limit → split with overlap
            if len("\n".join(buffer_lines)) > MAX_CHUNK_CHARS:
                # Split chunk but keep overlap lines at end as seed for next chunk
                overlap = buffer_lines[-PAGE_OVERLAP_LINES:] if len(buffer_lines) > PAGE_OVERLAP_LINES else buffer_lines[:]
                flush_if_ready(force=True)
                # start new buffer with overlap
                buffer_start_page = page_num
                buffer_end_page = page_num
                buffer_lines = overlap.copy()

            i += 1

    # final flush
    flush_if_ready(force=True)
    return chunks


# -------------------------
# 3) LLM PROMPT (STRICT JSON)
# -------------------------
def make_messages(chunk: Chunk) -> List[Dict]:
    """
    Force JSON output. Include main structure label and substructure.
    """
    system = (
        "You are a strict data extraction engine. "
        "You MUST return valid JSON only. No markdown. No commentary."
    )

    user = f"""
Extract Quebec (RAMQ Annexe V) medicine pricing rows from the text below.

CONTEXT:
- Country is always: {DEFAULT_COUNTRY}
- Currency is always: {DEFAULT_CURRENCY}
- Region is always: {DEFAULT_REGION}
- This chunk belongs to:
  Main Structure: {chunk.main_structure}
  Sub-Structure: {chunk.sub_structure}

OUTPUT:
Return JSON with this exact schema:
{{
  "rows": [
    {{
      "Country": "{DEFAULT_COUNTRY}",
      "Company": "<manufacturer/company if present else empty>",
      "Local Product Name": "<brand/local product name>",
      "Generic Name": "<molecule / INN>",
      "Currency": "{DEFAULT_CURRENCY}",
      "Ex Factory Wholesale Price": "<pack price / cost du format>",
      "Region": "{DEFAULT_REGION}",
      "Marketing Authority": "<usually same as Company; if not present use Company>",
      "Local Pack Description": "<single-line pack description combining formulation+strength+fill when available>",
      "Formulation": "<e.g., Sol. Inj. S.C>",
      "Fill Size": "<e.g., 2 or 0.8 ml etc. if available else empty>",
      "Strength": "<numeric strength only if possible>",
      "Strength Unit": "<e.g., MG, g, mL etc.>",
      "LOCAL_PACK_CODE": "<local code/product code>",
      "Main Structure": "{chunk.main_structure}",
      "Sub-Structure": "{chunk.sub_structure}",
      "Source Page": "<page number if inferable else empty>"
    }}
  ]
}}

RULES (do not violate):
1) DO NOT MISS ANY DATA. If you are unsure, keep the field empty but still create the row.
2) Handle multi-line gracefully: product name, company, pack description can wrap to next line.
3) If one product code has multiple pack sizes / prices, produce MULTIPLE rows (one per pack/price).
4) Treat numbers like unit price (e.g., 0,7203) as not a separate row unless clearly a pack price.
5) Prices may use comma decimals. Preserve them as given (e.g., 1428,48). We'll normalize later.
6) Return JSON ONLY.

TEXT:
{chunk.text}
""".strip()

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


# -------------------------
# 4) PARSE CHUNK WITH RETRIES + KEY ROTATION
# -------------------------
def parse_chunk_with_llm(chunk: Chunk, chunk_index: int) -> List[Dict]:
    messages = make_messages(chunk)

    last_err = None
    for attempt in range(1, MAX_RETRIES_PER_CHUNK + 1):
        api_key = pick_api_key(chunk_index + attempt - 1)  # rotate per attempt
        try:
            raw = routeway_chat(api_key, messages, temperature=0.0, timeout=160)
            data = json_loads_loose(raw)
            rows = data.get("rows", [])
            if not isinstance(rows, list):
                raise ValueError("JSON does not contain 'rows' list.")
            return rows
        except Exception as e:
            last_err = e
            if isinstance(e, RoutewayError) and e.status_code not in RETRYABLE_STATUS:
                break
            # backoff with jitter
            sleep_s = (RETRY_BACKOFF_BASE ** (attempt - 1)) + random.uniform(0, 0.8)
            if isinstance(e, RoutewayError) and e.status_code in {502, 503, 504}:
                sleep_s = max(sleep_s, 8 + random.uniform(0, 2))
            time.sleep(min(sleep_s, MAX_RETRY_SLEEP))

    raise RuntimeError(f"Failed parsing chunk after retries. Last error: {last_err}")


# -------------------------
# 5) NORMALIZE + WRITE CSV
# -------------------------
def normalize_row(r: Dict) -> Dict:
    out = {k: "" for k in FIELDNAMES}

    # Copy known keys if present
    for k in out.keys():
        if k in r:
            out[k] = safe_str(r.get(k))

    # Force constants (do not allow model to change)
    out["Country"] = DEFAULT_COUNTRY
    out["Currency"] = DEFAULT_CURRENCY
    out["Region"] = DEFAULT_REGION

    # Marketing Authority default = Company if empty
    if not out["Marketing Authority"] and out["Company"]:
        out["Marketing Authority"] = out["Company"]

    # Normalize decimal comma for Ex Factory price (and optionally fill size if numeric with comma)
    out["Ex Factory Wholesale Price"] = normalize_decimal(out["Ex Factory Wholesale Price"])
    out["Fill Size"] = normalize_decimal(out["Fill Size"])

    # Uppercase Strength Unit if present
    if out["Strength Unit"]:
        out["Strength Unit"] = out["Strength Unit"].strip().upper()

    return out


def write_csv(rows: List[Dict], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def append_failed_chunk(path: str, chunk: Chunk, err: Exception, chunk_index: int):
    record = {
        "chunk_index": chunk_index,
        "main_structure": chunk.main_structure,
        "sub_structure": chunk.sub_structure,
        "start_page": chunk.start_page,
        "end_page": chunk.end_page,
        "chars": len(chunk.text),
        "error": str(err),
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# -------------------------
# MAIN
# -------------------------
def main():
    print(f"[1/5] Extracting text from PDF: {PDF_PATH}")
    pages = extract_pdf_lines(PDF_PATH)
    total_pages = len(pages)
    print(f"  - pages: {total_pages}")

    print("[2/5] Building structure-aware chunks (by codes like 24:32.08)...")
    chunks = build_chunks(pages)
    print(f"  - chunks created: {len(chunks)}")

    print("[3/5] Sending chunks to Routeway.ai for JSON extraction (with key rotation)...")
    all_rows: List[Dict] = []
    failed_chunks = 0
    for idx, ch in enumerate(chunks):
        # Skip empty/noise chunks
        if len(ch.text.strip()) < 200:
            continue

        print(f"  - chunk {idx+1}/{len(chunks)} | {ch.main_structure} / {ch.sub_structure} | pages {ch.start_page}-{ch.end_page} | chars {len(ch.text)}")
        try:
            rows = parse_chunk_with_llm(ch, idx)
        except Exception as e:
            failed_chunks += 1
            append_failed_chunk(FAILED_CHUNKS_PATH, ch, e, idx)
            print(f"    ! failed chunk {idx+1}: {e}")
            if not SKIP_FAILED_CHUNKS:
                raise
            continue

        # add trace defaults if missing
        for r in rows:
            r.setdefault("Main Structure", ch.main_structure)
            r.setdefault("Sub-Structure", ch.sub_structure)
            r.setdefault("Source Page", str(ch.start_page) if ch.start_page == ch.end_page else f"{ch.start_page}-{ch.end_page}")

        all_rows.extend(rows)

    print("[4/5] Normalizing rows + validating minimal fields...")
    normalized: List[Dict] = []
    for r in all_rows:
        nr = normalize_row(r)

        # Minimal: LOCAL_PACK_CODE and Local Product Name are often present; keep rows even if missing (per your request)
        normalized.append(nr)

    print("[5/5] Writing CSV...")
    write_csv(normalized, OUT_CSV)
    print(f"Done. Output saved to: {OUT_CSV}")
    print(f"Total rows: {len(normalized)}")
    if failed_chunks:
        print(f"Failed chunks: {failed_chunks} (see {FAILED_CHUNKS_PATH})")


if __name__ == "__main__":
    main()
