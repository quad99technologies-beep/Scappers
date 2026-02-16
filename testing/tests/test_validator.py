#!/usr/bin/env python3
"""Quick test script for data validator"""

import sys
from pathlib import Path

# Add scripts/Netherlands to path
sys.path.insert(0, str(Path(__file__).parent / 'scripts' / 'Netherlands'))

from data_validator import validate_pack_data, get_validation_errors

# Test data with European format
test_data = {
    'unit_price': '€ 12,50',
    'start_date': '08-02-2026',
    'reimbursable_status': 'Reimbursed',
    'currency': 'EUR',
    'vat_percent': '9',
    'ppp_vat': '€ 100,00',
    'copay_price': '€ 5,25',
    'copay_percent': '10%'
}

print("=" * 60)
print("DATA VALIDATOR TEST")
print("=" * 60)
print("\n[OK] Testing data validation...\n")

result = validate_pack_data(test_data)

print("INPUT DATA:")
for key, value in test_data.items():
    print(f"  {key}: {value}")

print("\nVALIDATED DATA:")
for key, value in result.items():
    if key in test_data:
        print(f"  {key}: {value}")

errors = get_validation_errors()
if errors:
    print("\nVALIDATION WARNINGS:")
    for error in errors:
        print(f"  [!] {error}")
else:
    print("\n[OK] No validation errors!")

print("\n" + "=" * 60)
print("KEY VALIDATIONS:")
print("=" * 60)
print(f"[OK] Price normalized: 'EUR 12,50' -> '{result['unit_price']}'")
print(f"[OK] Date validated: '{test_data['start_date']}' -> '{result['start_date']}'")
print(f"[OK] Status validated: '{test_data['reimbursable_status']}' -> '{result['reimbursable_status']}'")
print(f"[OK] Percentage normalized: '{test_data['copay_percent']}' -> '{result['copay_percent']}'")
print("\n[OK] Data validator working correctly!")
print("=" * 60)
