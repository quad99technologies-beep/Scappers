import base64
import json
import re
from pathlib import Path

import pandas as pd
from openai import OpenAI

# =========================
# CONFIG
# =========================
PDF_PATH = Path(r"D:\quad99\Scappers\1. CanadaQuebec\output\split_pdf\annexe_iv2.pdf")
OUT_CSV = Path(r"D:\quad99\Scappers\1. CanadaQuebec\output\csv\annexe_iv2_final_expected_format.csv")
OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

STATIC_CURRENCY = "CAD"
STATIC_REGION = "NORTH AMERICA"

# Put your key here
client = OpenAI(api_key="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")

MODEL = "gpt-4o-mini"  # or "gpt-4o"

FINAL_COLS = [
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
]

# =========================
# HELPERS
# =========================
def norm_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    # French decimal comma -> dot in numeric contexts
    s = re.sub(r"(\d),(\d)", r"\1.\2", s)
    # normalize "ml" casing inside description
    s = re.sub(r"\bml\b", "mL", s, flags=re.IGNORECASE)
    # prefer parentheses volume like "(0.8 ml)" (lowercase ml) for display
    s = re.sub(r"\(\s*(\d+(?:\.\d+)?)\s*mL\s*\)", lambda m: f"({m.group(1)} ml)", s)
    return s

def generic_base(g: str) -> str:
    g = norm_text(g)
    # "INSULINE LISPRO (Humalog ...)" -> "INSULINE LISPRO"
    g = re.sub(r"\s*\(.*?\)\s*$", "", g).strip()
    return g

def extract_formulation(desc: str) -> str:
    """
    Formulation = text before first digit.
    "Sol. Inj. S.C 100 mg/mL (3 ml)" -> "Sol. Inj. S.C"
    """
    desc = norm_text(desc)
    m = re.search(r"\d", desc)
    if not m:
        return desc.strip().rstrip(".")
    return desc[:m.start()].strip().rstrip(".").strip()

def extract_strength_before_slash(desc: str):
    """
    Your rule:
      Strength is whatever appears before the first '/' (or before '/mL', '/0.3 mL', etc.)
      Examples:
        "50 mg/mL (0.8 ml)" -> 50 MG
        "162 mg/0.9 mL"     -> 162 MG
        "100 U/mL (3 ml)"   -> 100 U
        "Pd. Inj 1.1 mg"    -> 1.1 MG
    """
    desc = norm_text(desc)

    # Find "<number> <unit>" followed by "/" OR just present
    m = re.search(r"(\d+(?:\.\d+)?)\s*(mg|mcg|g|u|ui|iu)\s*(?:/|$|\s)", desc, flags=re.IGNORECASE)
    if not m:
        return None, None

    val_str = m.group(1)
    unit_raw = m.group(2).upper()
    unit_map = {"MG": "MG", "MCG": "MCG", "G": "G", "U": "U", "UI": "U", "IU": "IU"}
    unit = unit_map.get(unit_raw, unit_raw)

    try:
        val = float(val_str)
        if val.is_integer():
            val = int(val)
    except Exception:
        val = None

    return val, unit

def looks_like_bandelette_generic(g: str) -> bool:
    gg = (g or "").upper()
    return "RÉACTIF QUANTITATIF" in gg or "GLUCOSE" in gg or "BANDELETTE" in gg

def parse_pack_options_from_text(text_blob: str):
    """
    Parse "50 34.23 100 63.90" into [(50,34.23),(100,63.90)].
    Also works if lines are spaced weirdly.
    """
    s = norm_text(text_blob)
    nums = re.findall(r"\d+(?:\.\d+)?", s)
    pairs = []
    # Expect alternating packsize, price
    for i in range(0, len(nums) - 1, 2):
        try:
            psize = int(float(nums[i]))
            price = float(nums[i + 1])
            # pack sizes are usually small integers like 25/50/100, filter obvious garbage
            if 1 <= psize <= 1000 and price >= 0:
                pairs.append((psize, price))
        except Exception:
            continue
    return pairs

# =========================
# OPENAI SCHEMA + PROMPT
# =========================
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
                    # section header like "ÉNOXAPARINE (Lovenox)" or "RÉACTIF QUANTITATIF..."
                    "GenericGroup": {"type": ["string", "null"]},

                    "DIN": {"type": ["string", "null"]},
                    "Brand": {"type": ["string", "null"]},
                    "MarketingAuthority": {"type": ["string", "null"]},

                    # full description line like "Sol. Inj. S.C 100 mg/mL (3 mL)" or "Bandelette"
                    "LocalPackDescription": {"type": ["string", "null"]},

                    # normal medication: pack count (1,2,5,10,50) or sometimes "10 ml" appears (keep as string here)
                    "PackRaw": {"type": ["string", "null"]},

                    # normal medication price (CAD)
                    "Price": {"type": ["number", "null"]},

                    # Bandelette / multi-pack: raw blob containing pack-size/price pairs
                    # Example for DIN 99101469: "50 34,23 100 63,90"
                    "PackOptionsRaw": {"type": ["string", "null"]},
                },
                "required": [
                    "GenericGroup", "DIN", "Brand", "MarketingAuthority",
                    "LocalPackDescription", "PackRaw", "Price", "PackOptionsRaw"
                ],
            }
        }
    },
    "required": ["rows"]
}

PROMPT = r"""
Extract ALL rows from ANNEXE IV.2.

Return one object per visible line/row in reading order.

Fields:
- GenericGroup: the current section header (e.g., "ÉNOXAPARINE (Lovenox)", "INSULINE ASPARTE (NovoRapid)", "RÉACTIF QUANTITATIF DU GLUCOSE DANS LE SANG"). If the line does not restate the header, you may leave it null.
- DIN: 8-digit code (digits only), if present on the line.
- Brand: product/brand text on the row (e.g., "Lovenox", "NovoRapid Flex Touch", "Dario", "iTest", etc.)
- MarketingAuthority: the manufacturer/MA text (e.g., SanofiAven, N.Nordisk, Auto. Cont., Ignite, etc.)
- LocalPackDescription: the formulation/descriptor text (e.g., "Sol. Inj. S.C 100 mg/mL", "Pd. Inj. 1,1 mg", or "Bandelette")
- PackRaw: pack count when it appears (e.g., 10, 5, 1) or "3 ml" / "10 ml" if that is what is shown.
- Price: the main price shown for that row (e.g., 62,51 -> 62.51). If there is no direct price on that same line, set null.
- PackOptionsRaw: ONLY for Bandelette / glucose test strips where the pack sizes and prices appear like "50 34,23 100 63,90" possibly over multiple lines for the same DIN. Put the raw text of those pack-size/price pairs here; otherwise null.

Use '.' as decimal separator in numbers. Return ONLY JSON matching the schema.
"""

# =========================
# MAIN
# =========================
def main():
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    pdf_bytes = PDF_PATH.read_bytes()
    b64 = base64.b64encode(pdf_bytes).decode("utf-8")
    file_data = f"data:application/pdf;base64,{b64}"

    resp = client.responses.create(
        model=MODEL,
        input=[{
            "role": "user",
            "content": [
                {"type": "input_file", "filename": PDF_PATH.name, "file_data": file_data},
                {"type": "input_text", "text": PROMPT},
            ],
        }],
        text={
            "format": {
                "type": "json_schema",
                "name": "annexe_iv2_extract",
                "schema": SCHEMA,
                "strict": True
            }
        },
        temperature=0,
    )

    raw_rows = json.loads(resp.output_text)["rows"]

    out_rows = []
    current_generic = ""  # carry-forward generic group

    # We also need to “carry” bandelette pack options that appear on following lines
    pending_bandelette = {}  # DIN -> {"generic":..., "brand":..., "ma":..., "options_text":...}

    for r in raw_rows:
        raw_generic = (r.get("GenericGroup") or "").strip()
        if raw_generic:
            current_generic = generic_base(raw_generic)

        din = (r.get("DIN") or "").strip()
        local_pack_code = din.zfill(8) if din else ""

        brand = norm_text(r.get("Brand"))
        ma = norm_text(r.get("MarketingAuthority"))
        desc = norm_text(r.get("LocalPackDescription"))
        pack_raw = norm_text(r.get("PackRaw"))
        price = r.get("Price")
        pack_options_raw = norm_text(r.get("PackOptionsRaw"))

        generic = current_generic  # ALWAYS use carried value

        # ---- Handle Bandelette continuation logic ----
        # In the PDF, pack-size/price lines may continue without repeating DIN
        # BUT in practice OpenAI sometimes still outputs DIN, sometimes not.
        # Strategy:
        # 1) If row has DIN + PackOptionsRaw -> store/append to pending
        # 2) If row has NO DIN but looks like "100 63,90" and we have pending last DIN -> append
        if looks_like_bandelette_generic(generic):
            # If we got a DIN on this row, start or update pending
            if din:
                if din not in pending_bandelette:
                    pending_bandelette[din] = {
                        "generic": generic,
                        "brand": brand,
                        "ma": ma,
                        "options_text": "",
                    }
                # If brand/ma missing in this row, keep what we already have
                if brand:
                    pending_bandelette[din]["brand"] = brand
                if ma:
                    pending_bandelette[din]["ma"] = ma
                if pack_options_raw:
                    pending_bandelette[din]["options_text"] += " " + pack_options_raw
                # If price present with pack_raw (like "100 66,00") we can also treat it as one option
                if price is not None and pack_raw and re.fullmatch(r"\d+", pack_raw):
                    pending_bandelette[din]["options_text"] += f" {pack_raw} {price}"
                continue

            # No DIN: might be continuation line like "100 63,90"
            cont = (pack_options_raw or pack_raw or desc).strip()
            if cont:
                # attach to the most recent pending DIN (last inserted)
                if pending_bandelette:
                    last_din = next(reversed(pending_bandelette.keys()))
                    pending_bandelette[last_din]["options_text"] += " " + cont
                continue

        # ---- Normal medication rows ----
        # Fill Size = numeric pack count only (ignore "3 ml" in that column; keep it in description)
        fill_size = int(pack_raw) if re.fullmatch(r"\d+", pack_raw) else None

        strength_val, strength_unit = extract_strength_before_slash(desc)
        formulation = extract_formulation(desc)

        out_rows.append({
            "Generic Name": generic,
            "Currency": STATIC_CURRENCY,
            "Ex Factory Wholesale Price": float(price) if price is not None else None,
            "Region": STATIC_REGION,
            "Marketing Authority": ma,
            "Local Pack Description": desc,
            "Formulation": formulation,
            "Fill Size": fill_size,
            "Strength": strength_val,
            "Strength Unit": strength_unit,
            "LOCAL_PACK_CODE": local_pack_code,
        })

    # ---- Flush pending Bandelette DINs into rows ----
    for din, info in pending_bandelette.items():
        options = parse_pack_options_from_text(info["options_text"])
        # If options empty, skip (shouldn't happen, but safe)
        if not options:
            continue

        for psize, pprice in options:
            out_rows.append({
                "Generic Name": info["generic"],                 # carry-forward generic
                "Currency": STATIC_CURRENCY,
                "Ex Factory Wholesale Price": float(pprice),
                "Region": STATIC_REGION,
                "Marketing Authority": info["ma"],
                "Local Pack Description": info["brand"] or "Bandelette",
                "Formulation": "",
                "Fill Size": int(psize),
                "Strength": None,
                "Strength Unit": None,
                "LOCAL_PACK_CODE": str(din).zfill(8),
            })

    df = pd.DataFrame(out_rows)

    # Hard-fix any remaining blanks in Generic Name
    df["Generic Name"] = df["Generic Name"].replace("", pd.NA).ffill()

    # Enforce final column order
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[FINAL_COLS]

    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {OUT_CSV}")
    print(f"Rows: {len(df)}")


if __name__ == "__main__":
    main()
