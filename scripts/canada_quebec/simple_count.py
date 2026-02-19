import pdfplumber, re
from pathlib import Path

def count_dins(pdf_path):
    re_din = re.compile(r'\b\d{6,8}\b')
    with pdfplumber.open(pdf_path) as pdf:
        all_text = ""
        for page in pdf.pages:
            all_text += (page.extract_text() or "") + "\n"
        matches = re_din.findall(all_text)
        print(f"{pdf_path.name}: {len(matches)} matches found.")

iv1 = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv1.pdf")
iv2 = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv2.pdf")

if iv1.exists(): count_dins(iv1)
if iv2.exists(): count_dins(iv2)
