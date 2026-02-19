import pdfplumber
import re
from pathlib import Path

def test_extraction(pdf_path):
    print(f"\nTesting {pdf_path.name}")
    re_din = re.compile(r"\b(\d{6,8})\b")
    dins_found = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            words = page.extract_words(x_tolerance=3, y_tolerance=3)
            # Better grouping: cluster by Y with tolerance
            lines = []
            if not words: continue
            
            words.sort(key=lambda w: w['top'])
            current_line = [words[0]]
            for w in words[1:]:
                if abs(w['top'] - current_line[-1]['top']) < 3:
                    current_line.append(w)
                else:
                    lines.append(current_line)
                    current_line = [w]
            lines.append(current_line)
            
            for line in lines:
                text = " ".join([w['text'] for w in sorted(line, key=lambda w: w['x0'])])
                matches = re_din.findall(text)
                if matches:
                    dins_found.extend(matches)
                    # print(f"P{i+1}: {text}")
                    
    print(f"Total DINs found with relaxed grouping: {len(dins_found)}")
    print(f"Unique DINs: {len(set(dins_found))}")

iv1 = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv1.pdf")
iv2 = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv2.pdf")
v = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")

if iv1.exists(): test_extraction(iv1)
if iv2.exists(): test_extraction(iv2)
if v.exists(): 
    # Only test first 10 pages of V
    print("\nTesting Annexe V (first 10 pages only)")
    re_din = re.compile(r"\b(\d{6,8})\b")
    dins_found = []
    with pdfplumber.open(v) as pdf:
        for i in range(min(10, len(pdf.pages))):
            page = pdf.pages[i]
            text = page.extract_text() or ""
            matches = re_din.findall(text)
            dins_found.extend(matches)
    print(f"Total DINs in first 10 pages: {len(dins_found)}")
