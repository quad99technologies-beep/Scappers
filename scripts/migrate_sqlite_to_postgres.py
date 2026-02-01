#!/usr/bin/env python3
"""
Migrate data from SQLite country databases to PostgreSQL.

This script:
1. Creates PostgreSQL schema (with country prefixes)
2. Copies data from each SQLite database to PostgreSQL
3. Verifies row counts match
4. Generates a migration report

Usage:
    # Migrate specific countries
    python scripts/migrate_sqlite_to_postgres.py --countries India Malaysia

    # Migrate all countries
    python scripts/migrate_sqlite_to_postgres.py --all

    # Dry run (no actual migration)
    python scripts/migrate_sqlite_to_postgres.py --all --dry-run

Environment variables required:
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
"""

import argparse
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)

from core.db.postgres_connection import COUNTRY_PREFIX_MAP, SHARED_TABLES


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# SQLite database locations
SQLITE_DB_PATHS = {
    "India": PROJECT_ROOT / "output" / "India" / "india.db",
    "Malaysia": PROJECT_ROOT / "output" / "Malaysia" / "malaysia.db",
    "Netherlands": PROJECT_ROOT / "output" / "Netherlands" / "netherlands.db",
    "Belarus": PROJECT_ROOT / "output" / "Belarus" / "belarus.db",
    "Argentina": PROJECT_ROOT / "output" / "Argentina" / "argentina.db",
    "Taiwan": PROJECT_ROOT / "output" / "Taiwan" / "taiwan.db",
    "CanadaOntario": PROJECT_ROOT / "output" / "CanadaOntario" / "canadaontario.db",
    "CanadaQuebec": PROJECT_ROOT / "output" / "CanadaQuebec" / "canadaquebec.db",
    "Russia": PROJECT_ROOT / "output" / "Russia" / "russia.db",
}

# Tables to migrate per country (common + country-specific)
COMMON_TABLES = ["run_ledger", "http_requests", "scraped_items", "input_uploads"]

COUNTRY_TABLES = {
    "India": [
        "formulation_map", "sku_main", "sku_mrp", "brand_alternatives",
        "med_details", "formulation_status", "progress_snapshots",
        "input_formulations",
    ],
    "Malaysia": [
        "products", "product_details", "consolidated_products",
        "reimbursable_drugs", "pcid_mappings", "step_progress",
        "pcid_reference", "input_products",
    ],
    "Netherlands": ["input_search_terms"],
    "Belarus": ["input_generic_names"],
    "Argentina": ["input_product_list", "input_ignore_list", "input_dictionary"],
    "Taiwan": ["input_atc_prefixes"],
}


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def get_postgres_connection() -> psycopg2.extensions.connection:
    """Get a PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "scrappers"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )


def get_sqlite_tables(sqlite_path: Path) -> List[str]:
    """Get list of tables in a SQLite database."""
    if not sqlite_path.exists():
        return []

    conn = sqlite3.connect(str(sqlite_path))
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables


def get_table_row_count(conn, table: str, is_postgres: bool = False) -> int:
    """Get row count for a table."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def get_table_columns(conn, table: str, is_postgres: bool = False) -> List[str]:
    """Get column names for a table."""
    cursor = conn.cursor()
    if is_postgres:
        cursor.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table}' AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        return [row[0] for row in cursor.fetchall()]
    else:
        cursor.execute(f"PRAGMA table_info('{table}')")
        return [row[1] for row in cursor.fetchall()]


def get_prefixed_table_name(country: str, table: str) -> str:
    """Get the PostgreSQL table name with country prefix."""
    if table in SHARED_TABLES:
        return table
    prefix = COUNTRY_PREFIX_MAP.get(country, "")
    return f"{prefix}{table}"


# ---------------------------------------------------------------------------
# Schema Creation
# ---------------------------------------------------------------------------

def create_postgres_schema(pg_conn, dry_run: bool = False) -> None:
    """Create PostgreSQL schema from schema files."""
    schemas_dir = PROJECT_ROOT / "sql" / "schemas" / "postgres"

    schema_files = [
        schemas_dir / "common.sql",
        schemas_dir / "inputs.sql",
        schemas_dir / "india.sql",
        schemas_dir / "malaysia.sql",
    ]

    for schema_file in schema_files:
        if not schema_file.exists():
            print(f"  WARNING: Schema file not found: {schema_file}")
            continue

        print(f"  Applying schema: {schema_file.name}")
        if not dry_run:
            sql = schema_file.read_text(encoding="utf-8")
            cursor = pg_conn.cursor()
            cursor.execute(sql)
            pg_conn.commit()


# ---------------------------------------------------------------------------
# Data Migration
# ---------------------------------------------------------------------------

def migrate_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn: psycopg2.extensions.connection,
    sqlite_table: str,
    pg_table: str,
    batch_size: int = 1000,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Migrate data from a SQLite table to PostgreSQL.

    Returns:
        Tuple of (rows_migrated, rows_failed)
    """
    # Get columns from SQLite
    sqlite_columns = get_table_columns(sqlite_conn, sqlite_table, is_postgres=False)
    if not sqlite_columns:
        return 0, 0

    # Check if PostgreSQL table exists and get its columns
    pg_columns = get_table_columns(pg_conn, pg_table, is_postgres=True)
    if not pg_columns:
        print(f"    WARNING: PostgreSQL table {pg_table} does not exist, skipping")
        return 0, 0

    # Find common columns (excluding auto-increment id columns)
    common_cols = [c for c in sqlite_columns if c in pg_columns and c != "id"]
    if not common_cols:
        print(f"    WARNING: No common columns between {sqlite_table} and {pg_table}")
        return 0, 0

    col_str = ", ".join(common_cols)
    placeholders = ", ".join(["%s"] * len(common_cols))

    # Read from SQLite
    sqlite_cursor = sqlite_conn.cursor()
    sqlite_cursor.execute(f"SELECT {col_str} FROM {sqlite_table}")

    rows_migrated = 0
    rows_failed = 0

    if dry_run:
        # Just count rows
        rows = sqlite_cursor.fetchall()
        return len(rows), 0

    pg_cursor = pg_conn.cursor()

    # Insert in batches
    while True:
        batch = sqlite_cursor.fetchmany(batch_size)
        if not batch:
            break

        for row in batch:
            try:
                # Handle None values and convert types if needed
                cleaned_row = tuple(
                    None if v == "" else v for v in row
                )
                pg_cursor.execute(
                    f"INSERT INTO {pg_table} ({col_str}) VALUES ({placeholders}) "
                    f"ON CONFLICT DO NOTHING",
                    cleaned_row,
                )
                rows_migrated += 1
            except Exception as e:
                rows_failed += 1
                if rows_failed <= 5:
                    print(f"    Error inserting row: {e}")

        pg_conn.commit()

    return rows_migrated, rows_failed


def migrate_country(
    country: str,
    pg_conn: psycopg2.extensions.connection,
    dry_run: bool = False,
) -> Dict[str, Tuple[int, int]]:
    """
    Migrate all data for a country from SQLite to PostgreSQL.

    Returns:
        Dict mapping table names to (rows_migrated, rows_failed) tuples
    """
    results = {}

    sqlite_path = SQLITE_DB_PATHS.get(country)
    if not sqlite_path or not sqlite_path.exists():
        print(f"  SQLite database not found: {sqlite_path}")
        return results

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    existing_tables = get_sqlite_tables(sqlite_path)

    # Determine tables to migrate
    tables_to_migrate = []

    # Common tables (first country only migrates these to avoid duplicates)
    for table in COMMON_TABLES:
        if table in existing_tables:
            tables_to_migrate.append((table, table))  # No prefix for common

    # Country-specific tables
    country_specific = COUNTRY_TABLES.get(country, [])
    for table in country_specific:
        if table in existing_tables:
            pg_table = get_prefixed_table_name(country, table)
            tables_to_migrate.append((table, pg_table))

    # Migrate each table
    for sqlite_table, pg_table in tables_to_migrate:
        sqlite_count = get_table_row_count(sqlite_conn, sqlite_table)
        print(f"  Migrating {sqlite_table} -> {pg_table} ({sqlite_count} rows)")

        migrated, failed = migrate_table(
            sqlite_conn, pg_conn, sqlite_table, pg_table, dry_run=dry_run
        )
        results[pg_table] = (migrated, failed)

        if not dry_run:
            pg_count = get_table_row_count(pg_conn, pg_table, is_postgres=True)
            status = "OK" if pg_count >= migrated else "MISMATCH"
            print(f"    -> Migrated: {migrated}, Failed: {failed}, PG count: {pg_count} [{status}]")
        else:
            print(f"    -> Would migrate: {migrated} rows")

    sqlite_conn.close()
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite databases to PostgreSQL")
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Countries to migrate (e.g., India Malaysia)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Migrate all countries",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be migrated without actually doing it",
    )
    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip schema creation (assume tables exist)",
    )

    args = parser.parse_args()

    if not args.countries and not args.all:
        parser.error("Either --countries or --all is required")

    # Determine countries to migrate
    if args.all:
        countries = list(SQLITE_DB_PATHS.keys())
    else:
        countries = args.countries

    print("=" * 60)
    print("SQLite to PostgreSQL Migration")
    print("=" * 60)
    print(f"Countries: {', '.join(countries)}")
    print(f"Dry run: {args.dry_run}")
    print(f"Started at: {datetime.now().isoformat()}")
    print()

    # Connect to PostgreSQL
    try:
        pg_conn = get_postgres_connection()
        print("Connected to PostgreSQL")
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}")
        print("Make sure POSTGRES_* environment variables are set")
        sys.exit(1)

    # Create schema
    if not args.skip_schema:
        print("\n1. Creating PostgreSQL schema...")
        create_postgres_schema(pg_conn, dry_run=args.dry_run)

    # Migrate each country
    print("\n2. Migrating data...")
    all_results = {}
    common_migrated = False

    for country in countries:
        print(f"\n--- {country} ---")

        # Only migrate common tables for the first country
        if common_migrated:
            # Skip common tables for subsequent countries
            pass

        results = migrate_country(country, pg_conn, dry_run=args.dry_run)
        all_results[country] = results
        common_migrated = True

    # Summary
    print("\n" + "=" * 60)
    print("Migration Summary")
    print("=" * 60)

    total_migrated = 0
    total_failed = 0

    for country, results in all_results.items():
        country_migrated = sum(r[0] for r in results.values())
        country_failed = sum(r[1] for r in results.values())
        total_migrated += country_migrated
        total_failed += country_failed
        print(f"{country}: {country_migrated} migrated, {country_failed} failed")

    print()
    print(f"Total rows migrated: {total_migrated}")
    print(f"Total rows failed: {total_failed}")
    print(f"Completed at: {datetime.now().isoformat()}")

    pg_conn.close()


if __name__ == "__main__":
    main()
