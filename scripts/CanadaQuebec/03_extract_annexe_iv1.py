import base64
import json
import os
import re
from pathlib import Path

import pandas as pd
from openai import OpenAI

# ----------------------------
# Paths (relative to script location)
# ----------------------------
import sys
script_path = Path(__file__).resolve().parent
if script_path.exists():
    sys.path.insert(0, str(script_path))
from config_loader import (
    get_base_dir, get_split_pdf_dir, get_csv_output_dir,
    ANNEXE_IV1_PDF_NAME, ANNEXE_IV1_CSV_NAME,
    OPENAI_API_KEY, OPENAI_MODEL, OPENAI_TEMPERATURE,
    STATIC_CURRENCY, STATIC_REGION, FINAL_COLUMNS
)
BASE_DIR = get_base_dir()
PDF_PATH = get_split_pdf_dir() / ANNEXE_IV1_PDF_NAME
OUT_CSV = get_csv_output_dir() / ANNEXE_IV1_CSV_NAME
MODEL = OPENAI_MODEL
TEMPERATURE = OPENAI_TEMPERATURE

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ----------------------------
# OpenAI Configuration
# ----------------------------
# Validate API key
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY must be set in config file or environment variable")

# Strip any whitespace from the API key
OPENAI_API_KEY = OPENAI_API_KEY.strip()

# Validate API key format (should start with sk- or sk-proj-)
if not (OPENAI_API_KEY.startswith("sk-") or OPENAI_API_KEY.startswith("sk-proj-")):
    raise ValueError(f"Invalid API key format. Key should start with 'sk-' or 'sk-proj-'. Got: {OPENAI_API_KEY[:10]}...")

# Diagnostic: Log key info (first 10 and last 4 chars only for security)
import logging
log = logging.getLogger(__name__)
log.info(f"API Key loaded: length={len(OPENAI_API_KEY)}, starts_with={OPENAI_API_KEY[:10]}..., ends_with=...{OPENAI_API_KEY[-4:]}")

# Initialize OpenAI client
# Note: The API key will be validated on the first API call
client = OpenAI(api_key=OPENAI_API_KEY)

# ----------------------------
# JSON Schema (Structured Outputs)
# ----------------------------
# We keep the schema fairly permissive, then normalize deterministically into your final columns.
SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "rows": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "Generic": {"type": ["string", "null"]},

                    # IMPORTANT: in the prompt below, ask the model to put the header formulation text here
                    # e.g. "Sol. Inj. S.C 50 mg/mL (0,8 mL)"
                    "Formulation": {"type": ["string", "null"]},

                    # We'll recompute Strength + FillSize deterministically, but keep these fields to satisfy schema.
                    "Strength": {"type": ["string", "null"]},
                    "FillSize": {"type": ["string", "null"]},

                    "DIN": {"type": "string"},  # keep as string (leading zeros)
                    "Brand": {"type": ["string", "null"]},
                    "Manufacturer": {"type": ["string", "null"]},
                    "PackDescriptor": {"type": ["string", "null"]},
                    "Pack": {"type": ["integer", "null"]},
                    "PackPrice": {"type": ["number", "null"]},
                    "UnitPrice": {"type": ["number", "null"]},
                },
                "required": [
                    "Generic","Formulation","Strength","FillSize",
                    "DIN","Brand","Manufacturer","PackDescriptor",
                    "Pack","PackPrice","UnitPrice"
                ],
            },
        }
    },
    "required": ["rows"],
}

PROMPT = """
Extract ALL product rows from this ANNEXE IV.1 PDF into the schema.

IMPORTANT:
- Put the shared header formulation text (e.g., "Sol. Inj. S.C 50 mg/mL (0,8 mL)") into the field "Formulation".
- "Pack" must be the pack count column in the table (e.g., 2, 10, 50).
- "PackPrice" and "UnitPrice" must be numeric with '.' decimal separator.
- DIN must be exactly 8 digits (left-pad with zeros if needed).

BRAND FIELD (CRITICAL):
- "Brand" must be the row-level product name/presentation (NOT the generic name).
- Examples of Brand values:
  * "Humira (seringue)" - specific product presentation
  * "Humira (stylo)" - different presentation of same brand
  * "NovoRapid FlexTouch" - brand product name
- If the row has no explicit product name, extract Brand from parentheses in the Generic header.
  Example: If Generic header is "ADALIMUMAB (HUMIRA)" and row has no separate product name, set Brand="HUMIRA".
- If Generic is "INSULINE LISPRO (Humalog)" and row shows no product name, set Brand="Humalog".
- Always prioritize row-level product names over header parentheses when both exist.

Cleaning rules:
- If a value like '1428.48714.2400' appears, split into PackPrice=1428.48 and UnitPrice=714.2400.
- If Pack is missing but PackPrice exists, set Pack=1 and UnitPrice=PackPrice.
- For RITUXIMAB (RITUXAN) if multiple pack descriptors exist (e.g., 10 mL and 50 mL),
  output one row per pack descriptor with its PackPrice.

Return ONLY JSON matching the schema.
"""


# ----------------------------
# Normalization helpers (your final required semantics)
# ----------------------------
def _norm_desc(desc: str) -> str:
    if not desc:
        return ""
    s = str(desc).strip()

    # French decimal comma -> dot in numeric contexts (0,8 -> 0.8)
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)

    # normalize ml casing; final expected uses "(0.8 ml)" in lower-case "ml"
    s = re.sub(r"\bml\b", "mL", s, flags=re.IGNORECASE)
    s = re.sub(r"\(\s*(\d+(?:\.\d+)?)\s*mL\s*\)", r"(\1 ml)", s)  # "(0.8 mL)" -> "(0.8 ml)"

    # normalize "mg / mL" spacing
    s = re.sub(r"mg\s*/\s*mL", "mg/mL", s, flags=re.IGNORECASE)

    return s


def generic_base(generic: str) -> str:
    # "ADALIMUMAB (HUMIRA)" -> "ADALIMUMAB"
    if not generic:
        return ""
    g = str(generic).strip()
    g = re.sub(r"\s*\(.*?\)\s*$", "", g).strip()
    return g


def infer_product_group(generic: str, brand: str | None) -> str:
    """
    Derive Product Group from Brand and Generic fields.
    
    Product Group is a derived field with the following priority:
    1. Primary: Use Brand (row-level product name/presentation) if present
       Examples: "Humira (seringue)", "Humira (stylo)", "NovoRapid FlexTouch"
    2. Fallback: If Brand is empty, extract from parentheses in Generic header
       Example: "ADALIMUMAB (HUMIRA)" -> "HUMIRA"
    3. If neither available, return empty string (do not invent data)
    
    Args:
        generic: Generic name string (may contain parentheses with brand name)
        brand: Brand/product name string or None
        
    Returns:
        Product Group string (cleaned and trimmed, or empty string)
    """
    # Primary: Use Brand if present
    if brand:
        return str(brand).strip()
    
    # Fallback: Extract from parentheses in Generic
    if generic:
        match = re.search(r"\(([^)]+)\)", str(generic))
        if match:
            return match.group(1).strip()
    
    # No data available
    return ""


# Unit tests for infer_product_group (lightweight inline asserts)
def _test_infer_product_group():
    """Lightweight unit tests for infer_product_group function."""
    # Test 1: Brand present (primary)
    assert infer_product_group("ADALIMUMAB (HUMIRA)", "Humira (seringue)") == "Humira (seringue)"
    assert infer_product_group("INSULINE LISPRO", "NovoRapid FlexTouch") == "NovoRapid FlexTouch"
    
    # Test 2: Brand empty, extract from Generic parentheses (fallback)
    assert infer_product_group("ADALIMUMAB (HUMIRA)", None) == "HUMIRA"
    assert infer_product_group("ADALIMUMAB (HUMIRA)", "") == "HUMIRA"
    assert infer_product_group("INSULINE LISPRO (Humalog)", None) == "Humalog"
    
    # Test 3: Neither available (empty string)
    assert infer_product_group("ADALIMUMAB", None) == ""
    assert infer_product_group("ADALIMUMAB", "") == ""
    assert infer_product_group("", None) == ""
    
    # Test 4: Brand with whitespace trimming
    assert infer_product_group("GENERIC", "  Brand Name  ") == "Brand Name"
    
    return True  # All tests passed




def extract_formulation(desc: str) -> str:
    # "Sol. Inj. S.C 50 mg/mL (0.8 ml)" -> "Sol. Inj. S.C"
    if not desc:
        return ""
    s = _norm_desc(desc)
    m = re.search(r"\d", s)
    if not m:
        return s.strip()
    return s[:m.start()].strip().rstrip("-:;,")


def extract_strength(desc: str):
    """
    Returns (strength_value, strength_unit) where unit is MG or MCG.

    USER RULE:
      - Strength is whatever appears BEFORE the first '/'.
        Examples:
          "50 mg/mL (0.8 mL)"  -> 50 MG
          "162 mg/0.9 mL"      -> 162 MG
          "300 mcg/mL (1.6 mL)"-> 300 MCG
      - If there is no '/', fall back to the first "<number> <mg|mcg>" found.
    """
    if not desc:
        return None, None

    s = str(desc).strip()
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)  # 0,8 -> 0.8
    s = re.sub(r"\bml\b", "mL", s, flags=re.IGNORECASE)

    # 1) Before-slash rule (covers mg/mL, mcg/mL, mg/0.9 mL, etc.)
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg)\s*/", s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        unit = m.group(2).lower()
        return (int(val) if val.is_integer() else val), ("MG" if unit == "mg" else "MCG")

    # 2) Fallback: first occurrence of "<number> mg" or "<number> mcg"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg)\b", s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        unit = m.group(2).lower()
        return (int(val) if val.is_integer() else val), ("MG" if unit == "mg" else "MCG")

    return None, None

    s = _norm_desc(desc)

    # 1) Dose-style: "45 mg/0.5 mL" or "162 mg/0.9 mL"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg)\s*/\s*(\d+(?:\.\d+)?)\s*mL", s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        unit = m.group(2).lower()
        return (int(val) if val.is_integer() else val), ("MG" if unit == "mg" else "MCG")

    # 2) Concentration-style: "50 mg/mL (0.8 mL)" or "300 mcg/mL (1.6 mL)"
    c = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg)\s*/\s*mL", s, flags=re.IGNORECASE)
    v = re.search(r"\(\s*(\d+(?:\.\d+)?)\s*ml\s*\)", s, flags=re.IGNORECASE)
    if c and v:
        conc = float(c.group(1))
        unit = c.group(2).lower()
        vol = float(v.group(1))
        dose = conc * vol
        dose = int(dose) if float(dose).is_integer() else round(dose, 4)
        return dose, ("MG" if unit == "mg" else "MCG")

    # 3) Simple dose fallback: "100 mg"
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg)\b", s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        unit = m.group(2).lower()
        return (int(val) if val.is_integer() else val), ("MG" if unit == "mg" else "MCG")

    return None, None


def to_final_rows(rows: list[dict]) -> pd.DataFrame:
    """
    Build the EXACT output format you want:

    Generic Name, Currency, Ex Factory Wholesale Price, Region, Marketing Authority,
    Local Pack Description, Formulation, Fill Size, Strength, Strength Unit, LOCAL_PACK_CODE

    Currency = CAD (static)
    Region   = NORTH AMERICA (static)
    """
    out = []
    for r in rows:
        desc = _norm_desc(r.get("Formulation") or "")
        pack = r.get("Pack")
        din = str(r.get("DIN") or "").strip().zfill(8)

        strength_val, strength_unit = extract_strength(desc)

        out.append({
            "Generic Name": generic_base(r.get("Generic") or ""),
            "Currency": STATIC_CURRENCY,
            "Ex Factory Wholesale Price": r.get("PackPrice"),
            "Region": STATIC_REGION,
            "Product Group": infer_product_group(r.get("Generic") or "", r.get("Brand")),
            "Marketing Authority": (r.get("Manufacturer") or "").strip(),
            "Local Pack Description": desc,
            "Formulation": extract_formulation(desc),
            "Fill Size": int(pack) if pack is not None else None,
            "Strength": strength_val,
            "Strength Unit": strength_unit,
            "LOCAL_PACK_CODE": din,
        })

    df = pd.DataFrame(out)

    # Keep exact column order from config (filter to only columns that exist in df)
    available_cols = [col for col in FINAL_COLUMNS if col in df.columns]
    return df[available_cols]


def main():
    if not PDF_PATH.exists():
        raise FileNotFoundError(
            f"PDF not found: {PDF_PATH}\n"
            f"Please ensure Step 1 (Split PDF) completed successfully.\n"
            f"Expected file: {PDF_PATH}"
        )

    # Read PDF and base64-encode
    print(f"[PROGRESS] Extracting Annexe IV.1: Loading PDF (1/3)", flush=True)
    pdf_bytes = PDF_PATH.read_bytes()
    b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    file_data = f"data:application/pdf;base64,{b64}"

    # Call OpenAI with PDF as input_file + prompt, and strict JSON schema output
    print(f"[PROGRESS] Extracting Annexe IV.1: Processing with OpenAI (2/3)", flush=True)
    try:
        resp = client.responses.create(
            model=MODEL,
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_file", "filename": ANNEXE_IV1_PDF_NAME, "file_data": file_data},
                    {"type": "input_text", "text": PROMPT},
                ],
            }],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "annexe_iv1_rows",
                    "schema": SCHEMA,
                    "strict": True
                }
            },
            temperature=TEMPERATURE,
        )
    except Exception as api_error:
        # Provide helpful error message for authentication errors
        error_msg = str(api_error)
        if "401" in error_msg or "invalid_api_key" in error_msg or "AuthenticationError" in str(type(api_error).__name__):
            print("\n" + "="*80)
            print("OPENAI API AUTHENTICATION ERROR")
            print("="*80)
            print(f"Error: {error_msg}")
            print("\nTroubleshooting steps:")
            print("1. Verify your API key is active at: https://platform.openai.com/account/api-keys")
            print("2. Check that your OpenAI account has billing set up and credits available")
            print("3. Ensure the API key has access to the model: gpt-4o-mini")
            print("4. If the key is expired or revoked, generate a new one and update config/CanadaQuebec.env.json")
            print("5. Verify the key in your config file matches exactly (no extra spaces or characters)")
            print(f"\nAPI Key info: length={len(OPENAI_API_KEY)}, starts_with={OPENAI_API_KEY[:10]}..., ends_with=...{OPENAI_API_KEY[-4:]}")
            print("="*80 + "\n")
        raise

    obj = json.loads(resp.output_text)
    rows = obj["rows"]

    # Build final output format
    print(f"[PROGRESS] Extracting Annexe IV.1: Building output (3/3)", flush=True)
    df = to_final_rows(rows)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"[PROGRESS] Extracting Annexe IV.1: {len(df)}/{len(df)} (100%)", flush=True)
    print(f"[OK] Saved: {OUT_CSV}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    # Run unit tests before main execution
    try:
        _test_infer_product_group()
        print("[OK] infer_product_group tests passed")
    except AssertionError as e:
        print(f"[ERROR] infer_product_group test failed: {e}")
        raise
    
    main()
