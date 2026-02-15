import pdfplumber
import sys

# Page 54 is 0-indexed 53.
PAGE_IDX = 2
PDF_PATH = r"D:\quad99\Scrappers\output\CanadaQuebec\split_pdf\annexe_iv1.pdf"

with pdfplumber.open(PDF_PATH) as pdf:
    page = pdf.pages[PAGE_IDX]
    words = page.extract_words(keep_blank_chars=False)
    
    print(f"--- Page {PAGE_IDX + 1} Layout ---")
    
    # Sort by top, then x
    lines = {}
    for w in words:
        top = round(w['top'])
        if top not in lines: lines[top] = []
        lines[top].append(w)
        
    for top in sorted(lines.keys()):
        line_words = sorted(lines[top], key=lambda w: w['x0'])
        line_str = ""
        for w in line_words:
            line_str += f"[{w['text']} x={int(w['x0'])}] "
        
        # Filter for lines with potentially interesting data (digits)
        if any(c.isdigit() for c in line_str):
             try:
                print(f"y={top}: {line_str}")
             except UnicodeEncodeError:
                print(f"y={top}: {line_str.encode('ascii', 'replace').decode()}")
