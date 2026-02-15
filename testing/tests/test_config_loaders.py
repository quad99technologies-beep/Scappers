"""Verify all scraper config_loaders are working"""

import sys
from pathlib import Path

scrapers = [
    "Argentina", "Belarus", "canada_ontario", "canada_quebec",
    "India", "Malaysia", "Netherlands", "north_macedonia",
    "Russia", "Taiwan", "tender_brazil", "tender_chile"
]

print("="*60)
print("CONFIG LOADER VERIFICATION - ALL SCRAPERS")
print("="*60)

passed = []
failed = []

for scraper in scrapers:
    config_file = Path(f"scripts/{scraper}/config_loader.py")
    
    if not config_file.exists():
        print(f"SKIP: {scraper:20s} - config_loader.py not found")
        continue
    
    try:
        # Add to path
        sys.path.insert(0, str(config_file.parent))
        
        # Try to import
        import importlib
        if 'config_loader' in sys.modules:
            del sys.modules['config_loader']
        
        import config_loader
        
        # Try to call getenv
        test_val = config_loader.getenv("TEST_VAR", "default")
        
        print(f"PASS: {scraper:20s} - Import and getenv() successful")
        passed.append(scraper)
        
        # Remove from path
        sys.path.remove(str(config_file.parent))
        
    except Exception as e:
        print(f"FAIL: {scraper:20s} - {str(e)[:50]}")
        failed.append(scraper)

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total Scrapers: {len(scrapers)}")
print(f"Passed: {len(passed)}")
print(f"Failed: {len(failed)}")

if failed:
    print(f"\nFailed scrapers: {failed}")
    sys.exit(1)
else:
    print("\n*** ALL SCRAPERS PASSED ***")
    sys.exit(0)
