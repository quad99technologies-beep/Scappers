# PostgreSQL database layer
"""
Provides PostgreSQL-backed working store for scrapers.
Single database with country-prefixed tables (e.g., in_sku_main, my_products).

Modules:
- postgres_connection: PostgresDB class with connection pooling
- schema_registry: Versioned migration runner
- models: Standard DDL for run_ledger, http_requests, scraped_items
- upsert: UPSERT helpers, bulk insert, hash-based dedup
"""

from core.db.postgres_connection import PostgresDB, get_db, CountryDB
from core.db.models import apply_common_schema
from core.db.schema_registry import SchemaRegistry
from core.db.upsert import upsert_items, bulk_insert, compute_item_hash

__all__ = [
    "PostgresDB",
    "CountryDB",  # Alias for backward compatibility
    "get_db",
    "SchemaRegistry",
    "apply_common_schema",
    "upsert_items",
    "bulk_insert",
    "compute_item_hash",
]
