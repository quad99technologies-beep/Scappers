#!/usr/bin/env python3
import sys
from pathlib import Path

# Add repo root to path
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB
from scripts.tender_brazil.db import apply_brazil_schema

def main():
    print("Initializing Brazil Database...")
    db = CountryDB("Tender_Brazil")
    apply_brazil_schema(db)
    db.close()
    print("Done.")

if __name__ == "__main__":
    main()
