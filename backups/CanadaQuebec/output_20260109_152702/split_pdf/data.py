

"""
UPDATED extractor for annexe_v.pdf
---------------------------------
Outputs ONE flat CSV in the exact column layout you showed:

Generic Name, Currency, Ex Factory Wholesale Price, Unit Price, Region,
Product Group, Marketing Authority, Local Pack Description, Formulation,
Fill Size, Strength, Strength Unit, LOCAL_PACK_CODE

Mapping (from Annexe V PDF):
- Generic Name              -> molecule (INN) line
- Currency                  -> constant "CAD"
- Ex Factory Wholesale Price-> COÛT DU FORMAT
- Unit Price                -> PRIX UNITAIRE
- Region                    -> constant "NORTH AMERICA"
- Product Group             -> MARQUE DE COMMERCE (brand)
- Marketing Authority       -> FABRICANT (manufacturer)
- Local Pack Description    -> presentation line (dosage form + strength), e.g. "Sol. Inj. S.C 50 mg/mL (0.8 ml)"
- Formulation               -> dosage form only (e.g., "Sol. Inj. S.C")
- Fill Size                 -> FORMAT (pack size/count) from table (kept as raw)
- Strength / Strength Unit  -> parsed from presentation strength (best-effort)
- LOCAL_PACK_CODE           -> CODE column (product code)

It still uses pdfplumber (text-based). If a page has no text (image-only),
it will be skipped.

Install:
  pip install pdfplumber
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import pdfplumber



PDF_PATH = Path(r"D:\quad99\Scappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")
OUT_CSV = Path("./annexe_v_flat_output.csv")

CURRENCY = "CAD"
REGION = "NORTH AMERICA"

# --- Patterns ---
RE_SECTION_CODE = re.compile(r"^\d{1,3}:\d{1,3}(?:\.\d{1,3})*$")
RE_EDITION = re.compile(r"\b(?:EDITION|ÉDITION)\s+(\d{4}-\d{2})\b", re.IGNORECASE)

RE_PRESENTATION_START = re.compile(
    r"^(Caps\.|Cap\.|Co\.|Comp\.|Comprim[eé]s?\.?|Pd\.|Pdr\.|Sol\.|Susp\.|Inj\.|Cr\.|Cr[eè]me|Ung\.|Gouttes|Sirop|Supp\.|Onguent|Lotion|Gel|Pomm\.)\b",
    re.IGNORECASE,
)

RE_PRODUCT_CODE = re.compile(r"^\d{4,}$")
RE_DECIMAL_COMMA = re.compile(r"^\d{1,6},\d{1,6}$")

KNOWN_FLAGS = {"X", "Y", "Z", "V", "R", "UE", "+", "*", "PPB"}


def clean_spaces(s: str) -> str:
    s = (s or "").replace("\u00a0", " ").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def parse_decimal_comma(raw: str) -> Optional[float]:
    raw = clean_spaces(raw)
    if not raw:
        return None
    # Best-effort: if thousands separators exist, remove "."
    raw2 = raw.replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(raw2)
    except ValueError:
        return None


def is_probable_molecule_line(line: str) -> bool:
    if not line or len(line) < 4:
        return False
    if RE_SECTION_CODE.match(line):
        return False
    if RE_PRESENTATION_START.match(line):
        return False

    bad_starts = ("ANNEXE", "LISTE", "PAGE", "TABLE", "ÉDITION", "EDITION", "MÉTHODE", "PRIX")
    if line.upper().startswith(bad_starts):
        return False

    if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", line):
        return False

    letters = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]", line)
    if not letters:
        return False
    upper_letters = [ch for ch in letters if ch.upper() == ch and ch.lower() != ch]
    ratio = len(upper_letters) / max(1, len(letters))
    return ratio >= 0.75


def split_molecule_and_flags(line: str) -> Tuple[str, List[str]]:
    parts = clean_spaces(line).split(" ")
    flags: List[str] = []
    i = len(parts) - 1
    while i >= 0:
        token = parts[i].strip()
        if token in KNOWN_FLAGS:
            flags.append(token)
            i -= 1
        else:
            break
    name = " ".join(parts[: i + 1]).strip()
    flags.reverse()
    return name, flags


def parse_strength_from_presentation(strength_raw: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Best-effort extraction:
      - Finds the first occurrence of: number + unit (mg, g, mcg, µg, mL, mmol, %, UI/IU)
    Examples:
      "0,5 mg" -> ("0.5","MG")
      "50 mg/mL (0.8 ml)" -> ("50","MG")
      "10 mg -10 mg" -> ("10","MG") (takes first)
      "1 % (2 mL à 5 mL)" -> ("1","%")
    """
    s = clean_spaces(strength_raw)
    if not s:
        return None, None

    # normalize comma decimal to dot for the extracted number only
    # match first number + unit
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(mg|g|mcg|µg|ug|ml|mL|mmol|%|UI|IU)\b", s, flags=re.IGNORECASE)
    if not m:
        return None, None

    num = m.group(1).replace(",", ".")
    unit = m.group(2).upper()
    if unit == "UG" or unit == "ΜG":  # just in case
        unit = "MCG"
    if unit == "ML":
        unit = "ML"
    if unit == "IU":
        unit = "IU"
    return num, unit


def parse_product_row(tokens: List[str]) -> Optional[Dict[str, str]]:
    """
    Best-effort parse of a product row:
      CODE + (brand/manufacturer tokens) + FORMAT + COST + UNIT_PRICE

    We parse from the end:
      - last 2 decimal-comma tokens => cost, unit
      - token before cost => format
      - middle => brand/manufacturer (best-effort split)
    """
    if not tokens:
        return None

    code = tokens[0]
    if not RE_PRODUCT_CODE.match(code):
        return None

    dec_idxs = [i for i, t in enumerate(tokens) if RE_DECIMAL_COMMA.match(t)]
    if len(dec_idxs) < 2:
        return None

    cost_i, unit_i = dec_idxs[-2], dec_idxs[-1]
    if cost_i <= 1 or unit_i <= cost_i:
        return None

    fmt_i = cost_i - 1
    if fmt_i < 1:
        return None

    fmt_raw = tokens[fmt_i]
    cost_raw = tokens[cost_i]
    unit_raw = tokens[unit_i]

    mid = tokens[1:fmt_i]
    brand = ""
    manufacturer = ""

    # Heuristic: if we have many tokens, use last token(s) as manufacturer.
    # This is imperfect, so we also keep raw_line in output rows.
    if len(mid) == 1:
        brand = mid[0]
    elif len(mid) == 2:
        brand = " ".join(mid)
    elif len(mid) >= 3:
        brand = " ".join(mid[:2])
        manufacturer = " ".join(mid[2:])

    return {
        "product_code": code,
        "brand": brand.strip(),
        "manufacturer": manufacturer.strip(),
        "format_raw": fmt_raw.strip(),
        "cost_raw": cost_raw.strip(),
        "unit_raw": unit_raw.strip(),
    }


def main() -> None:
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"PDF not found: {PDF_PATH}")

    edition_code = "unknown"

    # State machine context
    current_molecule: Optional[str] = None
    current_presentation_form: Optional[str] = None
    current_presentation_strength: Optional[str] = None
    current_presentation_desc: Optional[str] = None  # full combined
    current_ppb_marker: Optional[bool] = None

    out_rows: List[Dict[str, str]] = []

    with pdfplumber.open(str(PDF_PATH)) as pdf:
        for pageno, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            text = text.replace("\r", "\n")

            if edition_code == "unknown":
                m = RE_EDITION.search(text)
                if m:
                    edition_code = m.group(1)

            lines = [clean_spaces(l) for l in text.split("\n")]
            lines = [l for l in lines if l]

            # skip image-only pages
            if not lines:
                continue

            i = 0
            while i < len(lines):
                line = lines[i]

                # section code resets lower-level context (optional)
                if RE_SECTION_CODE.match(line):
                    # new section: reset molecule/presentation
                    current_molecule = None
                    current_presentation_form = None
                    current_presentation_strength = None
                    current_presentation_desc = None
                    current_ppb_marker = None
                    i += 1
                    continue

                # molecule header
                if is_probable_molecule_line(line):
                    mol, flags = split_molecule_and_flags(line)
                    current_molecule = mol
                    # reset presentation on new molecule
                    current_presentation_form = None
                    current_presentation_strength = None
                    current_presentation_desc = None
                    current_ppb_marker = None
                    i += 1
                    continue

                # presentation line
                if current_molecule and RE_PRESENTATION_START.match(line):
                    ppb = "PPB" in line.upper()
                    # split: first token is form
                    parts = line.split(" ", 1)
                    form = parts[0].strip()
                    rest = parts[1].strip() if len(parts) > 1 else ""
                    rest_clean = re.sub(r"\bPPB\b", "", rest, flags=re.IGNORECASE).strip()

                    current_presentation_form = form
                    current_presentation_strength = rest_clean
                    current_ppb_marker = ppb

                    # Local Pack Description should look like a human-readable pack description.
                    # We keep it as: "{form} {strength}" (same feel as your screenshot)
                    current_presentation_desc = clean_spaces(f"{form} {rest_clean}").strip()

                    i += 1
                    continue

                # product row (needs molecule + presentation context)
                if current_molecule and current_presentation_desc:
                    tokens = line.split(" ")
                    parsed = parse_product_row(tokens)
                    if parsed:
                        strength_val, strength_unit = parse_strength_from_presentation(current_presentation_strength or "")

                        out_rows.append(
                            {
                                "Generic Name": current_molecule,
                                "Currency": CURRENCY,
                                "Ex Factory Wholesale Price": str(parse_decimal_comma(parsed["cost_raw"]) or "").strip(),
                                "Unit Price": str(parse_decimal_comma(parsed["unit_raw"]) or "").strip(),
                                "Region": REGION,
                                "Product Group": parsed["brand"],
                                "Marketing Authority": parsed["manufacturer"],
                                "Local Pack Description": current_presentation_desc,
                                "Formulation": current_presentation_form or "",
                                "Fill Size": parsed["format_raw"],  # keep raw to preserve '10', '1', etc.
                                "Strength": strength_val or "",
                                "Strength Unit": strength_unit or "",
                                "LOCAL_PACK_CODE": parsed["product_code"],
                                # helpful audit fields (optional)
                                "Page": str(pageno),
                                "Edition": edition_code,
                            }
                        )
                        i += 1
                        continue

                i += 1

    # Write output CSV in your requested order
    fieldnames = [
        "Generic Name",
        "Currency",
        "Ex Factory Wholesale Price",
        "Unit Price",
        "Region",
        "Product Group",
        "Marketing Authority",
        "Local Pack Description",
        "Formulation",
        "Fill Size",
        "Strength",
        "Strength Unit",
        "LOCAL_PACK_CODE",
        # audit (keep or remove as you like)
        "Edition",
        "Page",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print("Done")
    print(f"Edition: {edition_code}")
    print(f"Rows: {len(out_rows)}")
    print(f"Output: {OUT_CSV.resolve()}")


if __name__ == "__main__":
    main()
