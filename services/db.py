#!/usr/bin/env python3
"""
Centralized Database Access Layer for the Scraping Platform.

This module provides:
- Connection management with pooling
- Pipeline run operations (create, claim, update, heartbeat)
- URL registry operations
- Entity/attribute operations
- Fetch and error logging
- File storage tracking

All database operations go through this module.
"""

import os
import sys
import uuid
import json
import hashlib
import socket
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

# Add parent directories to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    import psycopg2
    from psycopg2 import pool, sql, errors
    from psycopg2.extras import RealDictCursor, execute_values
except ImportError:
    psycopg2 = None
    pool = None
    sql = None
    errors = None
    RealDictCursor = None
    execute_values = None

# Try to import from core
try:
    from core.db.postgres_connection import PostgresDB, get_db, COUNTRY_PREFIX_MAP, SHARED_TABLES
except ImportError:
    PostgresDB = None
    get_db = None
    COUNTRY_PREFIX_MAP = {}
    SHARED_TABLES = frozenset()


# =============================================================================
# Connection Pool Singleton
# =============================================================================

_pool_lock = threading.Lock()
_connection_pool: Optional["pool.ThreadedConnectionPool"] = None


def _get_connection_pool() -> "pool.ThreadedConnectionPool":
    """Get or create the global connection pool (singleton)."""
    global _connection_pool

    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                if psycopg2 is None:
                    raise ImportError(
                        "psycopg2 is required. Install with: pip install psycopg2-binary"
                    )

                _connection_pool = pool.ThreadedConnectionPool(
                    minconn=int(os.getenv("POSTGRES_POOL_MIN", "2")),
                    maxconn=int(os.getenv("POSTGRES_POOL_MAX", "10")),
                    host=os.getenv("POSTGRES_HOST", "localhost"),
                    port=int(os.getenv("POSTGRES_PORT", "5432")),
                    database=os.getenv("POSTGRES_DB", "scrappers"),
                    user=os.getenv("POSTGRES_USER", "postgres"),
                    password=os.getenv("POSTGRES_PASSWORD", ""),
                )

    return _connection_pool


def close_connection_pool() -> None:
    """Close the global connection pool."""
    global _connection_pool

    with _pool_lock:
        if _connection_pool is not None:
            _connection_pool.closeall()
            _connection_pool = None


@contextmanager
def get_connection() -> Iterator[Any]:
    """Get a connection from the pool as a context manager."""
    pool_obj = _get_connection_pool()
    conn = pool_obj.getconn()
    try:
        yield conn
    finally:
        pool_obj.putconn(conn)


@contextmanager
def get_cursor(dict_cursor: bool = False) -> Iterator[Any]:
    """Get a cursor with auto-commit/rollback."""
    with get_connection() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


# =============================================================================
# Database Instance Helpers
# =============================================================================

def get_platform_db():
    """Get a platform-level database connection (no country prefix)."""
    if PostgresDB:
        return PostgresDB("")  # Empty country = no prefix
    raise ImportError("core.db.postgres_connection not available")


def get_country_db(country: str):
    """Get a country-specific database connection."""
    if get_db:
        return get_db(country)
    if PostgresDB:
        return PostgresDB(country)
    raise ImportError("core.db.postgres_connection not available")


# =============================================================================
# Pipeline Run Operations
# =============================================================================

def generate_worker_id() -> str:
    """Generate a unique worker ID based on hostname and process."""
    hostname = socket.gethostname()[:20]
    pid = os.getpid()
    short_uuid = uuid.uuid4().hex[:8]
    return f"{hostname}-{pid}-{short_uuid}"


def create_pipeline_run(
    country: str,
    total_steps: Optional[int] = None,
    priority: int = 0,
    metadata: Optional[Dict] = None
) -> str:
    """
    Create a new pipeline run in the queue.
    
    Args:
        country: Country name
        total_steps: Total number of steps in the pipeline
        priority: Priority (higher = higher priority)
        metadata: Optional metadata dict
        
    Returns:
        run_id (UUID string)
    """
    run_id = str(uuid.uuid4())
    
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs 
            (run_id, country, status, total_steps, priority, metadata_json, created_at, updated_at)
            VALUES (%s, %s, 'queued', %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (run_id, country, total_steps, priority, json.dumps(metadata or {})))
    
    return run_id


def claim_next_run(
    worker_id: str,
    countries: Optional[List[str]] = None
) -> Optional[Dict]:
    """
    Atomically claim the next available run from the queue.
    
    Uses SELECT ... FOR UPDATE SKIP LOCKED for safe concurrent claiming.
    
    Args:
        worker_id: ID of the worker claiming the job
        countries: Optional list of countries this worker can handle
        
    Returns:
        Dict with run details if claimed, None if no jobs available
    """
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        try:
            # Build country filter
            country_filter = ""
            params = []
            if countries:
                placeholders = ",".join(["%s"] * len(countries))
                country_filter = f"AND country IN ({placeholders})"
                params = countries
            
            # Claim the highest priority, oldest job
            cur.execute(f"""
                SELECT run_id, country, current_step, current_step_num, total_steps, metadata_json
                FROM pipeline_runs
                WHERE status = 'queued'
                {country_filter}
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, params)
            
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            
            run_id = row['run_id']
            
            # Update the run to 'running' and assign worker
            cur.execute("""
                UPDATE pipeline_runs
                SET status = 'running',
                    worker_id = %s,
                    started_at = CURRENT_TIMESTAMP,
                    last_heartbeat = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE run_id = %s
            """, (worker_id, run_id))
            
            conn.commit()
            
            return dict(row)
            
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def update_run_status(
    run_id: str,
    status: str,
    error_message: Optional[str] = None
) -> bool:
    """
    Update the status of a pipeline run.
    
    Args:
        run_id: Run UUID
        status: New status (queued, running, stopped, completed, failed, cancelled)
        error_message: Optional error message for failed runs
        
    Returns:
        True if updated, False if run not found
    """
    with get_cursor() as cur:
        ended_at = "CURRENT_TIMESTAMP" if status in ('completed', 'failed', 'stopped', 'cancelled') else "NULL"
        
        cur.execute(f"""
            UPDATE pipeline_runs
            SET status = %s,
                error_message = %s,
                ended_at = {ended_at},
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s
        """, (status, error_message, run_id))
        
        return cur.rowcount > 0


def update_run_step(
    run_id: str,
    step_num: int,
    step_name: Optional[str] = None
) -> bool:
    """
    Update the current step of a pipeline run.
    
    Args:
        run_id: Run UUID
        step_num: Current step number
        step_name: Optional step name
        
    Returns:
        True if updated, False if run not found
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET current_step_num = %s,
                current_step = %s,
                last_heartbeat = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s
        """, (step_num, step_name, run_id))
        
        return cur.rowcount > 0


def heartbeat(run_id: str) -> bool:
    """
    Update the heartbeat timestamp for a running job.
    
    Args:
        run_id: Run UUID
        
    Returns:
        True if updated, False if run not found
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET last_heartbeat = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE run_id = %s AND status = 'running'
        """, (run_id,))
        
        return cur.rowcount > 0


def get_latest_command(run_id: str) -> Optional[Dict]:
    """
    Get the latest unacknowledged command for a run.
    
    Args:
        run_id: Run UUID
        
    Returns:
        Dict with command details or None
    """
    with get_cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT id, command, issued_by, created_at
            FROM pipeline_commands
            WHERE run_id = %s AND acknowledged_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
        """, (run_id,))
        
        row = cur.fetchone()
        return dict(row) if row else None


def acknowledge_command(command_id: int) -> bool:
    """
    Acknowledge a command (mark as processed).
    
    Args:
        command_id: Command ID
        
    Returns:
        True if acknowledged, False if not found
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_commands
            SET acknowledged_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (command_id,))
        
        return cur.rowcount > 0


def issue_command(run_id: str, command: str, issued_by: str = "system") -> int:
    """
    Issue a command to a running pipeline.
    
    Args:
        run_id: Run UUID
        command: Command (stop, resume, cancel, pause)
        issued_by: Who issued the command
        
    Returns:
        Command ID
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_commands (run_id, command, issued_by, created_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (run_id, command, issued_by))
        
        return cur.fetchone()[0]


def get_stale_runs(timeout_seconds: int = 600) -> List[Dict]:
    """
    Get runs that have stale heartbeats (likely crashed).
    
    Args:
        timeout_seconds: Heartbeat timeout in seconds
        
    Returns:
        List of stale run dicts
    """
    with get_cursor(dict_cursor=True) as cur:
        cur.execute("""
            SELECT run_id, country, worker_id, current_step, started_at, last_heartbeat
            FROM pipeline_runs
            WHERE status = 'running'
            AND last_heartbeat < CURRENT_TIMESTAMP - INTERVAL '%s seconds'
        """, (timeout_seconds,))
        
        return [dict(row) for row in cur.fetchall()]


def requeue_stale_runs(timeout_seconds: int = 600) -> int:
    """
    Requeue runs with stale heartbeats.
    
    Args:
        timeout_seconds: Heartbeat timeout in seconds
        
    Returns:
        Number of runs requeued
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET status = 'queued',
                worker_id = NULL,
                retry_count = retry_count + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE status = 'running'
            AND last_heartbeat < CURRENT_TIMESTAMP - INTERVAL '%s seconds'
            AND retry_count < max_retries
        """, (timeout_seconds,))
        
        return cur.rowcount


# =============================================================================
# Worker Registry Operations
# =============================================================================

def register_worker(
    worker_id: str,
    hostname: Optional[str] = None,
    capabilities: Optional[List[str]] = None
) -> None:
    """Register a worker in the registry."""
    hostname = hostname or socket.gethostname()
    pid = os.getpid()
    
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO workers (worker_id, hostname, pid, capabilities, started_at, last_heartbeat)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (worker_id) DO UPDATE
            SET hostname = EXCLUDED.hostname,
                pid = EXCLUDED.pid,
                capabilities = EXCLUDED.capabilities,
                last_heartbeat = CURRENT_TIMESTAMP,
                status = 'active'
        """, (worker_id, hostname, pid, json.dumps(capabilities or [])))


def update_worker_heartbeat(worker_id: str, status: str = "active", current_run_id: Optional[str] = None) -> None:
    """Update worker heartbeat and status."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE workers
            SET last_heartbeat = CURRENT_TIMESTAMP,
                status = %s,
                current_run_id = %s
            WHERE worker_id = %s
        """, (status, current_run_id, worker_id))


def unregister_worker(worker_id: str) -> None:
    """Mark worker as offline."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE workers
            SET status = 'offline',
                current_run_id = NULL
            WHERE worker_id = %s
        """, (worker_id,))


# =============================================================================
# URL Registry Operations
# =============================================================================

def register_url(
    url: str,
    country: str,
    source: Optional[str] = None,
    entity_type: Optional[str] = None,
    priority: int = 0,
    depth: int = 0,
    metadata: Optional[Dict] = None
) -> int:
    """
    Register a URL in the registry (upsert).
    
    Args:
        url: URL to register
        country: Country name
        source: Where this URL was discovered
        entity_type: Type of entity expected
        priority: Fetch priority
        depth: Crawl depth from seed
        metadata: Optional metadata
        
    Returns:
        URL ID
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO urls (url, country, source, entity_type, priority, depth, metadata_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url_hash, country) DO UPDATE
            SET updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (url, country, source, entity_type, priority, depth, json.dumps(metadata or {})))
        
        return cur.fetchone()[0]


def upsert_url(
    url: str,
    country: str,
    source: Optional[str] = None,
    entity_type: Optional[str] = None,
    priority: int = 0,
    depth: int = 0,
    metadata: Optional[Dict] = None
) -> int:
    """
    Upsert a URL in the registry (alias for register_url).

    Args:
        url: URL to register
        country: Country name
        source: Where this URL was discovered
        entity_type: Type of entity expected
        priority: Fetch priority
        depth: Crawl depth from seed
        metadata: Optional metadata

    Returns:
        URL ID
    """
    return register_url(
        url=url,
        country=country,
        source=source,
        entity_type=entity_type,
        priority=priority,
        depth=depth,
        metadata=metadata,
    )


def get_url_id(url: str, country: str) -> Optional[int]:
    """Get the ID of a registered URL."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT id FROM urls WHERE url = %s AND country = %s
        """, (url, country))
        
        row = cur.fetchone()
        return row[0] if row else None


def update_url_status(
    url_id: int,
    status: str,
    content_hash: Optional[str] = None,
    error: Optional[str] = None
) -> bool:
    """Update URL fetch status."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE urls
            SET status = %s,
                last_fetch_at = CURRENT_TIMESTAMP,
                fetch_count = fetch_count + 1,
                content_hash = COALESCE(%s, content_hash),
                last_error = %s,
                error_count = CASE WHEN %s IS NOT NULL THEN error_count + 1 ELSE error_count END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (status, content_hash, error, error, url_id))
        
        return cur.rowcount > 0


def get_pending_urls(
    country: str,
    limit: int = 100,
    entity_type: Optional[str] = None
) -> List[Dict]:
    """Get pending URLs for a country."""
    with get_cursor(dict_cursor=True) as cur:
        type_filter = "AND entity_type = %s" if entity_type else ""
        params = [country, entity_type, limit] if entity_type else [country, limit]
        
        cur.execute(f"""
            SELECT id, url, source, entity_type, priority, depth, metadata_json
            FROM urls
            WHERE country = %s AND status = 'pending'
            {type_filter}
            ORDER BY priority DESC, created_at ASC
            LIMIT %s
        """, params if entity_type else [country, limit])
        
        return [dict(row) for row in cur.fetchall()]


# =============================================================================
# Entity Operations
# =============================================================================

def compute_entity_hash(entity_type: str, country: str, data: Dict) -> str:
    """Compute a hash for entity deduplication."""
    content = f"{entity_type}:{country}:{json.dumps(data, sort_keys=True)}"
    return hashlib.sha256(content.encode()).hexdigest()[:32]


def insert_entity(
    entity_type: str,
    country: str,
    source_url_id: Optional[int] = None,
    run_id: Optional[str] = None,
    external_id: Optional[str] = None,
    data: Optional[Dict] = None
) -> int:
    """
    Insert a new entity.
    
    Args:
        entity_type: Type of entity (product, tender, drug, etc.)
        country: Country name
        source_url_id: URL this entity was extracted from
        run_id: Pipeline run ID
        external_id: External identifier
        data: Data dict for hash computation
        
    Returns:
        Entity ID
    """
    entity_hash = compute_entity_hash(entity_type, country, data or {})
    
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO entities (entity_type, country, source_url_id, run_id, external_id, entity_hash)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (entity_type, country, entity_hash) DO UPDATE
            SET updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (entity_type, country, source_url_id, run_id, external_id, entity_hash))
        
        return cur.fetchone()[0]


def insert_attributes(
    entity_id: int,
    attributes: Dict[str, Any],
    source: Optional[str] = None,
    language: str = "en"
) -> int:
    """
    Insert attributes for an entity.
    
    Args:
        entity_id: Entity ID
        attributes: Dict of field_name: field_value
        source: Source of the data
        language: Language code
        
    Returns:
        Number of attributes inserted
    """
    if not attributes:
        return 0
    
    rows = []
    for idx, (name, value) in enumerate(attributes.items()):
        # Determine field type
        if isinstance(value, bool):
            field_type = "boolean"
            field_value = str(value).lower()
        elif isinstance(value, (int, float)):
            field_type = "number"
            field_value = str(value)
        elif isinstance(value, (list, dict)):
            field_type = "json"
            field_value = json.dumps(value)
        else:
            field_type = "text"
            field_value = str(value) if value is not None else None
        
        rows.append((entity_id, name, field_value, field_type, idx, language, source))
    
    with get_cursor() as cur:
        execute_values(cur, """
            INSERT INTO entity_attributes 
            (entity_id, field_name, field_value, field_type, field_order, language, source)
            VALUES %s
        """, rows)
        
        return len(rows)


def insert_attribute(
    entity_id: int,
    field_name: str,
    field_value: Any,
    source: Optional[str] = None,
    language: str = "en"
) -> int:
    """
    Insert a single attribute for an entity.

    Args:
        entity_id: Entity ID
        field_name: Attribute field name
        field_value: Attribute value
        source: Source of the data
        language: Language code

    Returns:
        Number of attributes inserted (0 or 1)
    """
    return insert_attributes(entity_id, {field_name: field_value}, source=source, language=language)


def get_entity(entity_id: int) -> Optional[Dict]:
    """Get an entity with all its attributes."""
    with get_cursor(dict_cursor=True) as cur:
        # Get entity
        cur.execute("""
            SELECT id, entity_type, country, external_id, status, created_at, updated_at
            FROM entities WHERE id = %s
        """, (entity_id,))
        
        entity = cur.fetchone()
        if not entity:
            return None
        
        result = dict(entity)
        
        # Get attributes
        cur.execute("""
            SELECT field_name, field_value, field_type
            FROM entity_attributes
            WHERE entity_id = %s
            ORDER BY field_order
        """, (entity_id,))
        
        result['attributes'] = {row['field_name']: row['field_value'] for row in cur.fetchall()}
        
        return result


# =============================================================================
# Fetch Logging
# =============================================================================

def log_fetch(
    url: str,
    method: str,
    success: bool,
    url_id: Optional[int] = None,
    run_id: Optional[str] = None,
    status_code: Optional[int] = None,
    response_bytes: Optional[int] = None,
    latency_ms: Optional[int] = None,
    proxy_used: Optional[str] = None,
    user_agent: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    retry_count: int = 0,
    fallback_used: bool = False
) -> int:
    """
    Log a fetch operation.
    
    Returns:
        Fetch log ID
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO fetch_logs 
            (url_id, run_id, url, method, status_code, success, response_bytes, 
             latency_ms, proxy_used, user_agent, error_type, error_message, 
             retry_count, fallback_used, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (url_id, run_id, url, method, status_code, success, response_bytes,
              latency_ms, proxy_used, user_agent, error_type, error_message,
              retry_count, fallback_used))
        
        return cur.fetchone()[0]


# =============================================================================
# Error Logging
# =============================================================================

def log_error(
    country: str,
    error_type: str,
    error_message: str,
    run_id: Optional[str] = None,
    step: Optional[str] = None,
    url_id: Optional[int] = None,
    error_code: Optional[str] = None,
    stack_trace: Optional[str] = None,
    context: Optional[Dict] = None,
    severity: str = "error"
) -> int:
    """
    Log an error.
    
    Returns:
        Error ID
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO errors 
            (run_id, country, step, url_id, error_type, error_code, error_message,
             stack_trace, context_json, severity, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (run_id, country, step, url_id, error_type, error_code, error_message,
              stack_trace, json.dumps(context or {}), severity))
        
        return cur.fetchone()[0]


# =============================================================================
# File Storage
# =============================================================================

def register_file(
    file_path: str,
    file_name: str,
    file_type: str,
    url_id: Optional[int] = None,
    entity_id: Optional[int] = None,
    run_id: Optional[str] = None,
    file_size: Optional[int] = None,
    checksum: Optional[str] = None,
    mime_type: Optional[str] = None
) -> int:
    """
    Register a downloaded file.
    
    Returns:
        File ID
    """
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO files 
            (url_id, entity_id, run_id, file_type, file_path, file_name, 
             file_size, checksum, mime_type, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            RETURNING id
        """, (url_id, entity_id, run_id, file_type, file_path, file_name,
              file_size, checksum, mime_type))
        
        return cur.fetchone()[0]


def update_file_extraction(
    file_id: int,
    status: str,
    error: Optional[str] = None
) -> bool:
    """Update file extraction status."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE files
            SET extraction_status = %s,
                extraction_error = %s,
                extracted_at = CASE WHEN %s = 'completed' THEN CURRENT_TIMESTAMP ELSE NULL END
            WHERE id = %s
        """, (status, error, status, file_id))
        
        return cur.rowcount > 0


# =============================================================================
# Schema Management
# =============================================================================

def ensure_platform_schema() -> None:
    """Ensure platform tables exist."""
    schema_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        'sql', 'schemas', 'postgres', 'platform.sql'
    )
    
    if not os.path.exists(schema_path):
        print(f"[WARNING] Platform schema not found at {schema_path}")
        return
    
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_sql = f.read()
    
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(schema_sql)
            conn.commit()
            print("[DB] Platform schema applied successfully")
        except Exception as e:
            conn.rollback()
            # Ignore "already exists" errors
            if "already exists" not in str(e).lower():
                print(f"[WARNING] Schema error (may be benign): {e}")
        finally:
            cur.close()


# =============================================================================
# Convenience Exports
# =============================================================================

# Run this on import to ensure tables exist
def _init():
    """Initialize database on first import."""
    try:
        ensure_platform_schema()
    except Exception as e:
        print(f"[WARNING] Could not initialize platform schema: {e}")


# Don't auto-init on import - let it be called explicitly
# _init()
