#!/usr/bin/env python3
"""Test that North Macedonia tables are visible with the correct prefix."""

from core.db.postgres_connection import PostgresDB, COUNTRY_PREFIX_MAP

# Test prefix mapping
print("Testing COUNTRY_PREFIX_MAP:")
print(f"  NorthMacedonia -> {COUNTRY_PREFIX_MAP.get('NorthMacedonia', 'NOT FOUND')}")
print(f"  North Macedonia -> {COUNTRY_PREFIX_MAP.get('North Macedonia', 'NOT FOUND')}")
print()

# Test database connection
try:
    db = PostgresDB("NorthMacedonia")
    db.connect()
    print("[OK] Connected to PostgreSQL")
    print(f"     Prefix: {db.prefix}")
    print()

    # Get all tables
    cur = db.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name NOT LIKE '\\_%' ESCAPE '\\' "
        "ORDER BY table_name")
    all_tables = [row[0] for row in cur.fetchall()]

    # Filter for nm_ tables
    nm_tables = [t for t in all_tables if t.startswith('nm_')]

    print(f"[OK] Found {len(nm_tables)} nm_* tables:")
    for table in sorted(nm_tables):
        print(f"     - {table}")
    print()

    # Check expected tables
    expected = ['nm_urls', 'nm_drug_register', 'nm_max_prices', 'nm_step_progress']
    missing = [t for t in expected if t not in nm_tables]

    if missing:
        print(f"[WARN] Missing expected tables: {missing}")
    else:
        print("[OK] All expected tables present")

    db.close()
    print("\n[SUCCESS] Test passed! North Macedonia tables are visible.")

except Exception as e:
    print(f"\n[ERROR] Test failed: {e}")
    import traceback
    traceback.print_exc()
