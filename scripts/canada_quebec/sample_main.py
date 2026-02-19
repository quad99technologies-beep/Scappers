import pdfplumber
from pathlib import Path

p = Path(r"D:\quad99\Scrappers\input\CanadaQuebec\liste-med.pdf")
with pdfplumber.open(p) as pdf:
    # Check a few pages between 10 and 30
    for i in range(10, 30):
        text = pdf.pages[i].extract_text() or ""
        if "0" in text: # likely has DINs
            print(f"--- PAGE {i+1} ---")
            # print first 500 chars safely
            print(text[:500].encode('ascii', 'ignore').decode('ascii'))
            print("-" * 40)
