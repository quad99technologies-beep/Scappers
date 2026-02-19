import pdfplumber, re, unicodedata
from pathlib import Path

def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")

p = Path(r"D:\quad99\Scrappers\input\CanadaQuebec\liste-med.pdf")
with pdfplumber.open(p) as pdf:
    for i, page in enumerate(pdf.pages):
        text = page.extract_text() or ""
        text_norm = strip_accents(text).upper()
        if "ANNEXE" in text_norm:
            # Look for "ANNEXE <IDENTIFIER>" in first 200 chars
            m = re.search(r"ANNEXE\s+([A-Z0-9.]+)", text_norm[:200])
            if m:
                print(f"Page {i+1}: ANNEXE {m.group(1)}")
