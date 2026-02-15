import csv
from pathlib import Path
import sys

# Path to the CSV
csv_path = r"D:\quad99\Scrappers\output\CanadaQuebec\csv\annexe_v_robust.csv"

def verify_csv(path):
    print(f"Verifying: {path}")
    if not Path(path).exists():
        print("File not found.")
        return

    rows = []
    with open(path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"Total Rows: {len(rows)}")
    
    # Check for missing values
    missing_din = 0
    missing_price = 0
    missing_generic = 0
    missing_brand = 0
    
    # Check specific DIN for validation
    target_din = "00596612" # Diphenhydramine
    found_target = False
    
    for r in rows:
        if not r['DIN']: missing_din += 1
        if not r['Price']: missing_price += 1
        if not r['Generic Name']: missing_generic += 1
        if not r['Brand']: missing_brand += 1
        
        if r['DIN'] == target_din:
            found_target = True
            print(f"\n--- Found Target DIN {target_din} ---")
            print(r)

    print("\n--- Summary ---")
    print(f"Missing DIN: {missing_din}")
    print(f"Missing Price: {missing_price}")
    print(f"Missing Generic Name: {missing_generic}")
    print(f"Missing Brand: {missing_brand}")
    
    if found_target:
        print("\nTarget DIN found validation: PASS")
    else:
        print("\nTarget DIN found validation: FAIL")

    if missing_price > 0:
        print("\n--- Rows with Missing Price (First 5) ---")
        count = 0
        for r in rows:
            if not r['Price']:
                print(r)
                count += 1
                if count >= 5: break

if __name__ == "__main__":
    verify_csv(csv_path)
