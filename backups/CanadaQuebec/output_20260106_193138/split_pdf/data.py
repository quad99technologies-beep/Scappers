"""
Extract structured data from Annexe V PDF (hierarchy + molecule + presentations + product/price rows)
and write 5 CSVs:
  - editions.csv
  - sections.csv
  - molecules.csv
  - presentations.csv
  - products.csv
  - pack_prices.csv

Designed for PDFs like /mnt/data/annexe_v.pdf where text exists (not fully scanned).
It uses a rule-based “state machine” parser:
  Section -> Molecule -> Presentation -> Product rows

NOTE:
- PDF table layouts vary across pages. This script extracts *reliably* the hierarchy context,
  and extracts product rows best-effort (keeps raw_text fields to audit/fix edge cases).
- If some pages are image-only, pdfplumber will return empty text for them (those pages will be skipped).
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple

import pdfplumber


PDF_PATH = Path(r"D:\quad99\Scappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")
OUT_DIR = Path("./annexe_v_extracted")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# Regex patterns (tuned for this doc style)
# -----------------------------

# Section codes like "56:28.12" or "8:12.06"
RE_SECTION_CODE = re.compile(r"^\d{1,3}:\d{1,3}(?:\.\d{1,3})*$")

# Edition header like "ÉDITION 2025-08" (best effort)
RE_EDITION = re.compile(r"\b(?:EDITION|ÉDITION)\s+(\d{4}-\d{2})\b", re.IGNORECASE)

# Presentation lines tend to start with abbreviations
RE_PRESENTATION_START = re.compile(
    r"^(Caps\.|Cap\.|Co\.|Comp\.|Comprim[eé]s?\.?|Pd\.|Pdr\.|Sol\.|Susp\.|Inj\.|Cr\.|Cr[eè]me|Ung\.|Gouttes|Sirop|Supp\.|Onguent|Lotion|Gel|Pomm\.)\b",
    re.IGNORECASE,
)

# Product "CODE" column often a long number (DIN-like or similar)
RE_PRODUCT_CODE = re.compile(r"^\d{4,}$")

# Price with decimal comma (e.g., 2,7172 or 19,02); allow 1-6 decimals
RE_DECIMAL_COMMA = re.compile(r"^\d{1,6},\d{1,6}$")

# Flags (legend-defined) sometimes appear next to molecule names
KNOWN_FLAGS = {"X", "Y", "Z", "V", "R", "UE", "+", "*", "PPB"}


def clean_spaces(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()


def is_probable_molecule_line(line: str) -> bool:
    """
    Heuristic:
    - Mostly uppercase letters (incl accents), spaces and punctuation
    - Not a section code
    - Not a presentation line
    - Not obviously a header/footer
    """
    if not line or len(line) < 4:
        return False
    if RE_SECTION_CODE.match(line):
        return False
    if RE_PRESENTATION_START.match(line):
        return False

    # Exclude common non-content
    bad_starts = ("ANNEXE", "LISTE", "PAGE", "TABLE", "ÉDITION", "EDITION", "MÉTHODE", "PRIX")
    if line.upper().startswith(bad_starts):
        return False

    # Must contain at least one letter
    if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", line):
        return False

    # Uppercase ratio heuristic
    letters = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ]", line)
    if not letters:
        return False
    upper_letters = [ch for ch in letters if ch.upper() == ch and ch.lower() != ch]
    ratio = len(upper_letters) / max(1, len(letters))

    # Many molecule lines are full uppercase; allow some punctuation/parentheses
    return ratio >= 0.75


def split_molecule_and_flags(line: str) -> Tuple[str, List[str]]:
    """
    Try to split "MOLECULE NAME X Z UE *" -> ("MOLECULE NAME", ["X","Z","UE","*"])
    Keeps unknown trailing tokens in name.
    """
    parts = clean_spaces(line).split(" ")
    flags: List[str] = []
    name_parts: List[str] = []

    # Walk from end backward collecting flags
    i = len(parts) - 1
    while i >= 0:
        token = parts[i].strip()
        if token in KNOWN_FLAGS:
            flags.append(token)
            i -= 1
        else:
            break

    name_parts = parts[: i + 1]
    flags.reverse()
    return (" ".join(name_parts).strip(), flags)


def parse_decimal_comma(raw: str) -> Optional[float]:
    raw = clean_spaces(raw)
    if not raw:
        return None
    # Remove thousand separators if any (best-effort):
    # If doc uses dot for thousands and comma for decimals: "1.234,56"
    raw2 = raw.replace(" ", "")
    raw2 = raw2.replace(".", "")
    raw2 = raw2.replace(",", ".")
    try:
        return float(raw2)
    except ValueError:
        return None


@dataclass
class Section:
    section_id: int
    edition_code: str
    section_code: str
    section_title: str
    parent_section_id: Optional[int]
    page_no: int


@dataclass
class Molecule:
    molecule_id: int
    edition_code: str
    section_id: Optional[int]
    inn_name: str
    flags_raw: str
    page_no: int


@dataclass
class Presentation:
    presentation_id: int
    molecule_id: int
    dosage_form_raw: str
    strength_raw: str
    ppb_marker: Optional[bool]
    raw_line: str
    page_no: int


@dataclass
class Product:
    product_id: int
    edition_code: str
    product_code: str
    brand_name: str
    manufacturer: str
    raw_line: str
    page_no: int


@dataclass
class PackPrice:
    pack_price_id: int
    product_id: int
    presentation_id: int
    pack_format_raw: str
    pack_cost_raw: str
    unit_price_raw: str
    pack_cost: Optional[float]
    unit_price: Optional[float]
    raw_tail: str
    page_no: int


def parse_product_row(tokens: List[str]) -> Optional[Dict[str, str]]:
    """
    Best-effort parse from a tokenized line:
      CODE BRAND ... MANUFACTURER ... FORMAT COST UNIT

    In this PDF, spacing is inconsistent. We parse from the end:
      - last 2 decimal-comma tokens -> cost, unit
      - token before cost -> format (could be numeric or '1', '10', '500 ml' etc; we keep as raw)
      - remaining middle part -> brand/manufacturer (best effort split: brand first, manufacturer last)
    """
    if not tokens:
        return None

    # Must start with a numeric code
    code = tokens[0]
    if not RE_PRODUCT_CODE.match(code):
        return None

    # Find last two decimal-comma numbers in the line
    dec_idxs = [i for i, t in enumerate(tokens) if RE_DECIMAL_COMMA.match(t)]
    if len(dec_idxs) < 2:
        return None

    cost_i, unit_i = dec_idxs[-2], dec_idxs[-1]
    if not (cost_i < unit_i):
        return None

    cost_raw = tokens[cost_i]
    unit_raw = tokens[unit_i]

    # Format token(s): usually immediately before cost
    fmt_i = cost_i - 1
    if fmt_i < 1:
        return None
    fmt_raw = tokens[fmt_i]

    # The remaining chunk between code and format is "brand/manufacturer" area.
    mid = tokens[1:fmt_i]
    if not mid:
        brand = ""
        mfg = ""
    else:
        # Best effort: manufacturer is often the last token(s). But we don't have a definitive separator.
        # Keep it simple:
        # - brand_name = first 1-3 tokens
        # - manufacturer = remaining tokens
        # This is why raw_line is saved for auditing.
        if len(mid) <= 2:
            brand = " ".join(mid)
            mfg = ""
        else:
            brand = " ".join(mid[:2])
            mfg = " ".join(mid[2:])

    raw_tail = " ".join(tokens[fmt_i:])

    return {
        "product_code": code,
        "brand_name": brand.strip(),
        "manufacturer": mfg.strip(),
        "pack_format_raw": fmt_raw.strip(),
        "pack_cost_raw": cost_raw.strip(),
        "unit_price_raw": unit_raw.strip(),
        "raw_tail": raw_tail.strip(),
    }


def main() -> None:
    if not PDF_PATH.exists():
        raise FileNotFoundError(f"PDF not found at: {PDF_PATH}")

    edition_code = "unknown"
    sections: List[Section] = []
    molecules: List[Molecule] = []
    presentations: List[Presentation] = []
    products: List[Product] = []
    pack_prices: List[PackPrice] = []

    # ID counters
    section_id = 0
    molecule_id = 0
    presentation_id = 0
    product_id = 0
    pack_price_id = 0

    # State
    current_section_id: Optional[int] = None
    section_stack: List[Tuple[str, int]] = []  # (code, section_id) to infer parent by prefix
    current_molecule_id: Optional[int] = None
    current_presentation_id: Optional[int] = None

    # Helper: infer parent section based on code prefix (e.g., 56:28 is parent of 56:28.12)
    def infer_parent(sec_code: str) -> Optional[int]:
        # Exact prefix matching on code segments
        # We'll pick the longest existing prefix
        best: Optional[int] = None
        best_len = -1
        for existing_code, sid in section_stack:
            if sec_code == existing_code:
                continue
            if sec_code.startswith(existing_code) and len(existing_code) > best_len:
                # ensure prefix boundary (":" or "."), avoid accidental starts
                # e.g. 5:1 shouldn't parent 5:10 by raw prefix
                if sec_code[len(existing_code) : len(existing_code) + 1] in {".", ""}:
                    best = sid
                    best_len = len(existing_code)
        return best

    with pdfplumber.open(str(PDF_PATH)) as pdf:
        for pageno, page in enumerate(pdf.pages):
            page_text = page.extract_text() or ""
            page_text = page_text.replace("\r", "\n")
            lines = [clean_spaces(l) for l in page_text.split("\n")]
            lines = [l for l in lines if l]  # remove empties

            # Try detect edition code from any page header/footer
            if edition_code == "unknown":
                m = RE_EDITION.search(page_text)
                if m:
                    edition_code = m.group(1)

            # Parsing loop
            i = 0
            while i < len(lines):
                line = lines[i]

                # 1) Section code line
                if RE_SECTION_CODE.match(line):
                    # Next non-empty line usually title
                    title = ""
                    j = i + 1
                    while j < len(lines) and not lines[j]:
                        j += 1
                    if j < len(lines):
                        title = lines[j]

                    section_id += 1
                    parent_id = infer_parent(line)
                    sections.append(
                        Section(
                            section_id=section_id,
                            edition_code=edition_code,
                            section_code=line,
                            section_title=title,
                            parent_section_id=parent_id,
                            page_no=pageno + 1,
                        )
                    )
                    current_section_id = section_id
                    section_stack.append((line, section_id))

                    # Advance past title if we consumed it
                    i = j + 1 if title else i + 1
                    # When a new section starts, reset lower-level context
                    current_molecule_id = None
                    current_presentation_id = None
                    continue

                # 2) Molecule line
                if is_probable_molecule_line(line):
                    name, flags = split_molecule_and_flags(line)
                    molecule_id += 1
                    molecules.append(
                        Molecule(
                            molecule_id=molecule_id,
                            edition_code=edition_code,
                            section_id=current_section_id,
                            inn_name=name,
                            flags_raw=" ".join(flags),
                            page_no=pageno + 1,
                        )
                    )
                    current_molecule_id = molecule_id
                    current_presentation_id = None
                    i += 1
                    continue

                # 3) Presentation line
                if current_molecule_id is not None and RE_PRESENTATION_START.match(line):
                    # Detect PPB marker if present
                    ppb_marker = None
                    if "PPB" in line.upper():
                        ppb_marker = True

                    # crude split: first token = dosage form, rest = strength/qualifiers
                    # Example: "Caps. 0,5 mg PPB"
                    # Keep raw_line anyway.
                    parts = line.split(" ", 1)
                    dosage = parts[0]
                    rest = parts[1] if len(parts) > 1 else ""
                    # Remove trailing PPB from strength field (keep marker separately)
                    rest_clean = rest.replace("PPB", "").replace("ppb", "").strip()

                    presentation_id += 1
                    presentations.append(
                        Presentation(
                            presentation_id=presentation_id,
                            molecule_id=current_molecule_id,
                            dosage_form_raw=dosage,
                            strength_raw=rest_clean,
                            ppb_marker=ppb_marker,
                            raw_line=line,
                            page_no=pageno + 1,
                        )
                    )
                    current_presentation_id = presentation_id
                    i += 1
                    continue

                # 4) Product row (needs a current presentation context)
                if current_presentation_id is not None:
                    tokens = line.split(" ")
                    parsed = parse_product_row(tokens)
                    if parsed:
                        product_id += 1
                        products.append(
                            Product(
                                product_id=product_id,
                                edition_code=edition_code,
                                product_code=parsed["product_code"],
                                brand_name=parsed["brand_name"],
                                manufacturer=parsed["manufacturer"],
                                raw_line=line,
                                page_no=pageno + 1,
                            )
                        )

                        pack_price_id += 1
                        pack_prices.append(
                            PackPrice(
                                pack_price_id=pack_price_id,
                                product_id=product_id,
                                presentation_id=current_presentation_id,
                                pack_format_raw=parsed["pack_format_raw"],
                                pack_cost_raw=parsed["pack_cost_raw"],
                                unit_price_raw=parsed["unit_price_raw"],
                                pack_cost=parse_decimal_comma(parsed["pack_cost_raw"]),
                                unit_price=parse_decimal_comma(parsed["unit_price_raw"]),
                                raw_tail=parsed["raw_tail"],
                                page_no=pageno + 1,
                            )
                        )
                        i += 1
                        continue

                i += 1

    # -----------------------------
    # Write CSVs
    # -----------------------------
    def write_csv(path: Path, fieldnames: List[str], rows: List[dict]) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)

    # edition
    write_csv(
        OUT_DIR / "editions.csv",
        ["edition_code", "source_file"],
        [{"edition_code": edition_code, "source_file": str(PDF_PATH.name)}],
    )

    write_csv(
        OUT_DIR / "sections.csv",
        ["section_id", "edition_code", "section_code", "section_title", "parent_section_id", "page_no"],
        [s.__dict__ for s in sections],
    )

    write_csv(
        OUT_DIR / "molecules.csv",
        ["molecule_id", "edition_code", "section_id", "inn_name", "flags_raw", "page_no"],
        [m.__dict__ for m in molecules],
    )

    write_csv(
        OUT_DIR / "presentations.csv",
        ["presentation_id", "molecule_id", "dosage_form_raw", "strength_raw", "ppb_marker", "raw_line", "page_no"],
        [p.__dict__ for p in presentations],
    )

    write_csv(
        OUT_DIR / "products.csv",
        ["product_id", "edition_code", "product_code", "brand_name", "manufacturer", "raw_line", "page_no"],
        [p.__dict__ for p in products],
    )

    write_csv(
        OUT_DIR / "pack_prices.csv",
        [
            "pack_price_id",
            "product_id",
            "presentation_id",
            "pack_format_raw",
            "pack_cost_raw",
            "unit_price_raw",
            "pack_cost",
            "unit_price",
            "raw_tail",
            "page_no",
        ],
        [pp.__dict__ for pp in pack_prices],
    )

    print("Done.")
    print(f"Edition: {edition_code}")
    print(f"Sections: {len(sections)}")
    print(f"Molecules: {len(molecules)}")
    print(f"Presentations: {len(presentations)}")
    print(f"Products: {len(products)}")
    print(f"Pack/Prices: {len(pack_prices)}")
    print(f"Output folder: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
