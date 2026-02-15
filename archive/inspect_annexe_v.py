import pdfplumber
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

# The split PDF
pdf_path = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")

def inspect_pages(pdf_path, start, count):
    print(f"\n--- Inspecting {pdf_path.name} Pages {start} to {start+count-1} ---")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i in range(start-1, min(start-1+count, len(pdf.pages))):
                page = pdf.pages[i]
                text = page.extract_text()
                print(f"\n[PAGE {i+1}] TEXT PREVIEW:")
                if text:
                    safe_text = text.encode('utf-8', 'replace').decode('utf-8')
                    print(safe_text[:500])
                    
                    # Check for price-like tokens
                    words = page.extract_words()
                    # Look for DINs (6-8 digits)
                    dins = [w for w in words if w['text'].isdigit() and 6 <= len(w['text']) <= 8]
                    if dins:
                        print(f"Sample DINs found: {[w['text'] for w in dins[:5]]}")
                else:
                    print("NO TEXT FOUND")
    except Exception as e:
        print(f"Error: {e}")

inspect_pages(pdf_path, 1, 10)
