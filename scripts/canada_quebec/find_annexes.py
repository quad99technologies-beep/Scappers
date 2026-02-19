import pdfplumber, re, unicodedata
from pathlib import Path

def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")

p = Path(r"D:\quad99\Scrappers\input\CanadaQuebec\liste-med.pdf")
with pdfplumber.open(p) as pdf:
    for i, page in enumerate(pdf.pages):
        text = page.extract_text()
        if not text: continue
        t = strip_accents(text).lower()
        if "annexe" in t:
            # check the first few words for the annexe number
            first_line = text.split('\n')[0].strip()
            if "ANNEXE" in first_line.upper():
                 print(f"Page {i+1}: {first_line}")
