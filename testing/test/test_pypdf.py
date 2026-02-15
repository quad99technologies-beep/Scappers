from pypdf import PdfReader
from pathlib import Path

pdf_path = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")

try:
    reader = PdfReader(pdf_path)
    # Page 6 is index 5
    page = reader.pages[5]
    text = page.extract_text()
    print(f"--- PyPDF Extraction Page 2 ---")
    print(f"Length: {len(text)}")
    print(text[:500])
except Exception as e:
    print(f"Error: {e}")
