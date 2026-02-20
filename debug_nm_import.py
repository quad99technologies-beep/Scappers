
import csv
from pathlib import Path

def reproduce():
    csv_path = r"C:\Users\Vishw\Downloads\NM mapping file.csv"
    print(f"Reading {csv_path}...")
    
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.reader(f)
        try:
            original_headers = next(reader)
        except StopIteration:
            print("Empty file")
            return

        print(f"Original Headers: {original_headers}")
        
        # Build header map
        header_map = {h.strip().lower(): h.strip() for h in original_headers}
        print(f"Header Map keys: {list(header_map.keys())}")
        
        clean_headers = [h.strip() for h in original_headers]
        print(f"Clean Headers: {clean_headers}")
        
        # Create DictReader
        dict_reader = csv.DictReader(f, fieldnames=clean_headers)
        
        def get_val(row_dict, *keys):
            for key in keys:
                # 1. Try exact match
                if key in row_dict:
                    val = row_dict[key]
                    if val and val.strip():
                        return val.strip()
                
                # 2. Try map
                key_lower = key.lower()
                if key_lower in header_map:
                    real_key = header_map[key_lower]
                    if real_key in row_dict:
                        val = row_dict[real_key]
                        if val and val.strip():
                            return val.strip()
            return None

        # Check first 3 rows
        for i, row in enumerate(dict_reader):
            if i >= 3: break
            
            print(f"\n--- Row {i+1} ---")
            print(f"Raw Row Keys: {list(row.keys())}")
            
            pack_code = get_val(row, 'LOCAL_PACK_CODE', 'Local Pack Code', 'local_pack_code')
            atc_code = get_val(row, 'WHO ATC Code', 'ATC Code', 'atc code', 'atc')
            strength = get_val(row, 'Strength', 'Strength Size', 'strength size')
            fill_size = get_val(row, 'Fill Size', 'fill size')
            
            print(f"Pack Code: '{pack_code}'")
            print(f"ATC Code: '{atc_code}'")
            print(f"Strength: '{strength}'")
            print(f"Fill Size: '{fill_size}'")
            print(f"Raw Row Content for these keys:")
            for k in ['Local Pack Code', 'WHO ATC Code', 'Strength Size', 'Fill Size']:
                if k in row:
                    print(f"  '{k}': '{row[k]}'")
                else:
                    print(f"  '{k}': NOT IN ROW")

if __name__ == "__main__":
    reproduce()
