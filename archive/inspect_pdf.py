import pdfplumber
import sys
from pathlib import Path

# Force UTF-8 for printing
sys.stdout.reconfigure(encoding='utf-8')

pdf_path = Path(r"D:\quad99\Scrappers\input\CanadaQuebec\liste-med.pdf")

def inspect_annexe(name, start_page):
    print(f"\n--- {name} (Starts on Page {start_page+1} / 0-idx {start_page}) ---")
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[start_page]
            text = page.extract_text()
            if text:
                # Replace undecodable characters
                safe_text = text.encode('utf-8', 'replace').decode('utf-8')
                print(f"PAGE {start_page+1} TEXT PREVIEW:")
                print(safe_text[:1500])
            else:
                print("NO TEXT FOUND")
            
            if name == "V":
                # For Annexe V, let's also look at the words/layout
                words = page.extract_words()
                if words:
                    print(f"FOUND {len(words)} WORDS. SAMPLE:")
                    for w in words[:20]:
                        print(f"Text: {w['text']}, x0: {w['x0']:.2f}, top: {w['top']:.2f}")
    except Exception as e:
        print(f"Error inspecting {name}: {e}")

inspect_annexe("IV.1", 230)
inspect_annexe("IV.2", 244)
inspect_annexe("V", 248)
