#!/usr/bin/env python3
"""
UPSERT helpers, bulk insert, and hash-based deduplication for PostgreSQL.

Usage:
    from core.db.upsert import upsert_items, bulk_insert, compute_item_hash

    # Bulk insert with batching
    bulk_insert(conn, "products", items_list, batch_size=500)

    # Upsert on conflict columns
    upsert_items(conn, "products", items_list,
                 conflict_columns=["registration_no"],
                 update_columns=["product_name", "price"])
"""

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# PERFORMANCE: execute_values is 5-10x faster than executemany for PostgreSQL
# (sends all rows in a single statement instead of row-by-row)
try:
    from psycopg2.extras import execute_values
    _HAS_EXECUTE_VALUES = True
except ImportError:
    _HAS_EXECUTE_VALUES = False


def compute_item_hash(item: Dict[str, Any], keys: Optional[List[str]] = None) -> str:
    """
    SHA-256 hash of item content for deduplication.

    Args:
        item: Dict to hash.
        keys: If provided, hash only these keys. Otherwise hash all.

    Returns:
        Hex digest string.
    """
    if keys:
        subset = {k: item.get(k) for k in sorted(keys)}
    else:
        subset = dict(sorted(item.items()))
    raw = json.dumps(subset, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def bulk_insert(
    conn: Any,
    table: str,
    items: List[Dict[str, Any]],
    batch_size: int = 500,
) -> int:
    """
    Insert items in batches.

    Uses psycopg2.extras.execute_values when available (5-10x faster)
    for sending all rows in a single statement instead of row-by-row.

    Args:
        conn: PostgreSQL database connection.
        table: Target table name.
        items: List of dicts (all must have same keys).
        batch_size: Rows per batch commit.

    Returns:
        Total rows inserted.
    """
    if not items:
        return 0

    columns = list(items[0].keys())
    col_str = ", ".join(columns)

    total = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        rows = [tuple(item.get(c) for c in columns) for item in batch]

        if _HAS_EXECUTE_VALUES:
            raw_conn = getattr(conn, '_conn', conn)
            cur = raw_conn.cursor()
            try:
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({col_str}) VALUES %s",
                    rows,
                    page_size=batch_size,
                )
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                cur.close()
        else:
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
            conn.executemany(sql, rows)
            conn.commit()

        total += len(batch)
        logger.debug("Inserted batch %d-%d into %s", i, i + len(batch), table)

    return total


def upsert_items(
    conn: Any,
    table: str,
    items: List[Dict[str, Any]],
    conflict_columns: List[str],
    update_columns: Optional[List[str]] = None,
    batch_size: int = 500,
) -> int:
    """
    INSERT OR UPDATE (upsert) items based on conflict columns.

    Uses PostgreSQL's ON CONFLICT ... DO UPDATE SET syntax.

    Args:
        conn: PostgreSQL database connection.
        table: Target table name.
        items: List of dicts.
        conflict_columns: Columns that form the unique constraint.
        update_columns: Columns to update on conflict. If None, updates all non-conflict columns.
        batch_size: Rows per batch.

    Returns:
        Total rows processed.
    """
    if not items:
        return 0

    columns = list(items[0].keys())

    if update_columns is None:
        update_columns = [c for c in columns if c not in conflict_columns]

    col_str = ", ".join(columns)
    conflict_str = ", ".join(conflict_columns)
    update_str = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    total = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        rows = [tuple(item.get(c) for c in columns) for item in batch]

        if _HAS_EXECUTE_VALUES:
            raw_conn = getattr(conn, '_conn', conn)
            cur = raw_conn.cursor()
            try:
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({col_str}) VALUES %s "
                    f"ON CONFLICT({conflict_str}) DO UPDATE SET {update_str}",
                    rows,
                    page_size=batch_size,
                )
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                cur.close()
        else:
            placeholders = ", ".join(["%s"] * len(columns))
            sql = (
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict_str}) DO UPDATE SET {update_str}"
            )
            conn.executemany(sql, rows)
            conn.commit()

        total += len(batch)

    logger.info("Upserted %d rows into %s", total, table)
    return total


def insert_ignore(
    conn: Any,
    table: str,
    items: List[Dict[str, Any]],
    conflict_columns: List[str],
    batch_size: int = 500,
) -> int:
    """
    INSERT ... ON CONFLICT DO NOTHING (insert if not exists).

    Args:
        conn: PostgreSQL database connection.
        table: Target table name.
        items: List of dicts.
        conflict_columns: Columns that form the unique constraint.
        batch_size: Rows per batch.

    Returns:
        Total rows processed.
    """
    if not items:
        return 0

    columns = list(items[0].keys())
    col_str = ", ".join(columns)
    conflict_str = ", ".join(conflict_columns)

    total = 0
    for i in range(0, len(items), batch_size):
        batch = items[i : i + batch_size]
        rows = [tuple(item.get(c) for c in columns) for item in batch]

        if _HAS_EXECUTE_VALUES:
            raw_conn = getattr(conn, '_conn', conn)
            cur = raw_conn.cursor()
            try:
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({col_str}) VALUES %s "
                    f"ON CONFLICT({conflict_str}) DO NOTHING",
                    rows,
                    page_size=batch_size,
                )
                raw_conn.commit()
            except Exception:
                raw_conn.rollback()
                raise
            finally:
                cur.close()
        else:
            placeholders = ", ".join(["%s"] * len(columns))
            sql = (
                f"INSERT INTO {table} ({col_str}) VALUES ({placeholders}) "
                f"ON CONFLICT({conflict_str}) DO NOTHING"
            )
            conn.executemany(sql, rows)
            conn.commit()

        total += len(batch)

    logger.info("Insert-ignored %d rows into %s", total, table)
    return total
