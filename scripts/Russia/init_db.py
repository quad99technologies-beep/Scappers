#!/usr/bin/env python3
"""
Initialize Russia database schema.
Run this once to create all tables.
"""

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.db.connection import CountryDB
from db.schema import apply_russia_schema

print("[INIT] Connecting to Russia database...")
db = CountryDB("Russia")

print("[INIT] Applying schema...")
apply_russia_schema(db)

print("[INIT] Russia database initialized successfully!")
print("\nCreated tables:")
print("  - ru_ved_products")
print("  - ru_excluded_products")
print("  - ru_translated_products")
print("  - ru_export_ready")
print("  - ru_step_progress")
print("  - ru_failed_pages")
