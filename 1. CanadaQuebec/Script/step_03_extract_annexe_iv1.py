import base64
import json
import re
from pathlib import Path

import pandas as pd
from openai import OpenAI

# ----------------------------
# Paths (relative to script location)
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
PDF_PATH = BASE_DIR / "output" / "split_pdf" / "annexe_iv1.pdf"
OUT_CSV = BASE_DIR / "output" / "csv" / "annexe_iv1_extracted.csv"
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# ----------------------------
# OpenAI (no env, as you requested)
# ----------------------------
# IMPORTANT: don't commit real keys to git. Put your key here locally or load from a local file.
client = OpenAI(api_key="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")

# Must be a model that supports PDF (vision + text)
MODEL = "gpt-4o-mini"  # or "gpt-4o"

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
            "Currency": "CAD",
            "Ex Factory Wholesale Price": r.get("PackPrice"),
            "Region": "NORTH AMERICA",
            "Marketing Authority": (r.get("Manufacturer") or "").strip(),
            "Local Pack Description": desc,
            "Formulation": extract_formulation(desc),
            "Fill Size": int(pack) if pack is not None else None,
            "Strength": strength_val,
            "Strength Unit": strength_unit,
            "LOCAL_PACK_CODE": din,
        })

    df = pd.DataFrame(out)

    # Keep exact column order
    return df[[
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
    ]]


def main():
    if not PDF_PATH.exists():
        raise FileNotFoundError(str(PDF_PATH))

    # Read PDF and base64-encode
    pdf_bytes = PDF_PATH.read_bytes()
    b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    file_data = f"data:application/pdf;base64,{b64}"

    # Call OpenAI with PDF as input_file + prompt, and strict JSON schema output
    resp = client.responses.create(
        model=MODEL,
        input=[{
            "role": "user",
            "content": [
                {"type": "input_file", "filename": "annexe_iv1.pdf", "file_data": file_data},
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
        temperature=0,
    )

    obj = json.loads(resp.output_text)
    rows = obj["rows"]

    # Build final output format
    df = to_final_rows(rows)

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {OUT_CSV}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
