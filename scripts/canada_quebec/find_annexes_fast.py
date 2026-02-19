from pypdf import PdfReader
import re, unicodedata
from pathlib import Path

def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text)
                   if unicodedata.category(c) != "Mn")

p = Path(r"D:\quad99\Scrappers\input\CanadaQuebec\liste-med.pdf")
reader = PdfReader(p)
for i in range(len(reader.pages)):
    try:
        text = reader.pages[i].extract_text() or ""
        text_norm = strip_accents(text).upper()
        if "ANNEXE" in text_norm:
            m = re.search(r"ANNEXE\s+([A-Z0-9.]+)", text_norm[:300])
            if m:
                print(f"Page {i+1}: ANNEXE {m.group(1)}")
    except:
        pass
