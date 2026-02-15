import pdfplumber
import sys
from pathlib import Path
import re

sys.stdout.reconfigure(encoding='utf-8')

pdf_path = Path(r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_v.pdf")

def detailed_inspect(page_num):
    print(f"\n--- Detailed Inspection of Page {page_num} ---")
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[page_num-1]
        words = page.extract_words(x_tolerance=1.5, y_tolerance=1.5)
        
        # Sort words by top then x0
        words.sort(key=lambda w: (round(w['top'], 1), w['x0']))
        
        lines = []
        current_line = []
        current_top = None
        for w in words:
            if current_top is None:
                current_top = w['top']
                current_line = [w]
            elif abs(w['top'] - current_top) < 1.5:
                current_line.append(w)
            else:
                lines.append(current_line)
                current_top = w['top']
                current_line = [w]
        if current_line:
            lines.append(current_line)
            
        for line in lines:
            text = " ".join([w['text'] for w in line])
            # Check if it looks like a DIN row
            if re.search(r"^\d{6,8}", text):
                print(f"DIN ROW: {text}")
                # Print token details to see gaps
                for w in line:
                    print(f"  [{w['text']}] x0:{w['x0']:.1f} x1:{w['x1']:.1f} gap_to_prev:{w['x0']-(line[line.index(w)-1]['x1']) if line.index(w)>0 else 0:.1f}")
            elif any(x in text.upper() for x in ["SOL.", "CAPS.", "COMP.", "MG/ML"]):
                print(f"FORM ROW: {text}")
            elif text.isupper() and len(text) > 5:
                 print(f"GENERIC?: {text}")

for p in [3, 5, 7]:
    detailed_inspect(p)
