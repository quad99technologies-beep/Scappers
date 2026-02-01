#!/usr/bin/env python3
"""
PostgreSQL database connection with connection pooling.

Provides PostgresDB class that matches the CountryDB interface but uses
psycopg2 with ThreadedConnectionPool for PostgreSQL connections.

Tables are prefixed with country codes (e.g., in_sku_main, my_products).
"""

import os
import threading
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

try:
    import psycopg2
    from psycopg2 import pool, sql
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    pool = None
    sql = None
    RealDictCursor = None


# ---------------------------------------------------------------------------
# Country Prefix Mapping
# ---------------------------------------------------------------------------

COUNTRY_PREFIX_MAP: Dict[str, str] = {
    "India": "in_",
    "Malaysia": "my_",
    "Netherlands": "nl_",
    "Belarus": "by_",
    "Argentina": "ar_",
    "Taiwan": "tw_",
    "Tender_Chile": "cl_",
    "Tender-Chile": "cl_",
    "CanadaOntario": "ca_on_",
    "Canada Ontario": "ca_on_",
    "CanadaQuebec": "ca_qc_",
    "Canada Quebec": "ca_qc_",
    "Russia": "ru_",
    "North_Macedonia": "mk_",
    "North Macedonia": "mk_",
}

# Tables that are shared across all countries (no prefix)
SHARED_TABLES = frozenset({
    "_schema_versions",
    "run_ledger",
    "http_requests",
    "scraped_items",
    "input_uploads",
    "pcid_mapping",
})


# ---------------------------------------------------------------------------
# PostgreSQL Session Settings (equivalent to SQLite PRAGMAs)
# ---------------------------------------------------------------------------

_PG_SESSION_SETTINGS = [
    ("lock_timeout", "'5s'"),
    ("statement_timeout", "'300s'"),
    ("idle_in_transaction_session_timeout", "'60s'"),
]


# ---------------------------------------------------------------------------
# Connection Pool Singleton
# ---------------------------------------------------------------------------

_pool_lock = threading.Lock()
_connection_pool: Optional["pool.ThreadedConnectionPool"] = None
_env_loaded = False


def _load_env_if_needed():
    """Load .env file if POSTGRES_PASSWORD not set in environment."""
    global _env_loaded
    if _env_loaded:
        return
    _env_loaded = True
    
    # If password already set, no need to load
    if os.getenv("POSTGRES_PASSWORD"):
        return
    
    def _parse_env_file(env_path):
        try:
            content = env_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return
        for line in content.splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, value = raw.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            if key and key not in os.environ:
                os.environ[key] = value

    # Try to load from .env files
    try:
        from dotenv import load_dotenv
        from pathlib import Path

        # Try repo root .env
        repo_root = Path(__file__).resolve().parents[2]
        env_file = repo_root / ".env"
        if env_file.exists():
            load_dotenv(env_file, override=False)
            return

        # Try config/platform.env
        platform_env = repo_root / "config" / "platform.env"
        if platform_env.exists():
            load_dotenv(platform_env, override=False)
            return
    except ImportError:
        # dotenv not available: fall back to a simple parser
        try:
            from pathlib import Path
            repo_root = Path(__file__).resolve().parents[2]
            env_file = repo_root / ".env"
            if env_file.exists():
                _parse_env_file(env_file)
                return
            platform_env = repo_root / "config" / "platform.env"
            if platform_env.exists():
                _parse_env_file(platform_env)
                return
        except Exception:
            pass
    except Exception:
        pass  # Silently continue


def _get_connection_pool() -> "pool.ThreadedConnectionPool":
    """Get or create the global connection pool (singleton)."""
    global _connection_pool

    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                if psycopg2 is None:
                    raise ImportError(
                        "psycopg2 is required for PostgreSQL support. "
                        "Install with: pip install psycopg2-binary"
                    )

                # Load .env if needed
                _load_env_if_needed()

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


# ---------------------------------------------------------------------------
# PostgresDB Class
# ---------------------------------------------------------------------------

class PostgresDB:
    """
    PostgreSQL database wrapper with connection pooling.

    Matches the CountryDB interface for drop-in replacement.
    Uses country prefixes for table names (e.g., in_sku_main, my_products).

    Usage:
        db = PostgresDB("India")
        with db.cursor() as cur:
            cur.execute("SELECT * FROM %s" % db.table_name("sku_main"))
    """

    def __init__(self, country: str):
        """
        Initialize PostgresDB for a specific country.

        Args:
            country: Country name (e.g., "India", "Malaysia")
        """
        self._country = country
        self._prefix = COUNTRY_PREFIX_MAP.get(country, "")
        self._conn: Optional[Any] = None
        self._owns_connection = False

    @property
    def country(self) -> str:
        """Return the country name."""
        return self._country

    @property
    def prefix(self) -> str:
        """Return the table prefix for this country."""
        return self._prefix

    def table_name(self, base_name: str) -> str:
        """
        Get the full table name with country prefix.

        Args:
            base_name: Base table name (e.g., "sku_main", "products")

        Returns:
            Prefixed table name (e.g., "in_sku_main", "my_products")
            or base name if it's a shared table
        """
        if base_name in SHARED_TABLES:
            return base_name
        return f"{self._prefix}{base_name}"

    def connect(self) -> Any:
        """
        Get a connection from the pool.

        Returns:
            psycopg2 connection object
        """
        if self._conn is None:
            pool = _get_connection_pool()
            self._conn = pool.getconn()
            self._owns_connection = True
            self._apply_session_settings()
        return self._conn

    def _apply_session_settings(self) -> None:
        """Apply session-level settings to the connection."""
        if self._conn is None:
            return

        with self._conn.cursor() as cur:
            for setting, value in _PG_SESSION_SETTINGS:
                cur.execute(f"SET {setting} = {value}")
        self._conn.commit()

    @contextmanager
    def cursor(self, dict_cursor: bool = False) -> Iterator[Any]:
        """
        Context manager that yields a cursor with auto-commit/rollback.

        Args:
            dict_cursor: If True, use RealDictCursor for dict-like row access

        Yields:
            psycopg2 cursor object
        """
        conn = self.connect()
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

    @contextmanager
    def transaction(self, isolation_level: str = "SERIALIZABLE") -> Iterator[Any]:
        """
        Context manager for explicit transaction with specified isolation level.

        Args:
            isolation_level: Transaction isolation level
                            (SERIALIZABLE, REPEATABLE READ, READ COMMITTED)

        Yields:
            psycopg2 connection object
        """
        conn = self.connect()
        old_isolation = conn.isolation_level

        try:
            # Set isolation level
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
                if isolation_level == "SERIALIZABLE"
                else psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED
            )
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.set_isolation_level(old_isolation)

    def execute(
        self,
        sql_str: str,
        params: Optional[Union[Tuple, Dict]] = None
    ) -> Any:
        """
        Execute a single SQL statement with auto-commit.

        Args:
            sql_str: SQL statement with %s placeholders
            params: Parameters for the SQL statement

        Returns:
            Cursor after execution
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.execute(sql_str, params)
            conn.commit()
            return cur
        except Exception:
            conn.rollback()
            raise

    def executemany(
        self,
        sql_str: str,
        params_list: List[Union[Tuple, Dict]]
    ) -> Any:
        """
        Execute a SQL statement with multiple parameter sets.

        Args:
            sql_str: SQL statement with %s placeholders
            params_list: List of parameter tuples/dicts

        Returns:
            Cursor after execution
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.executemany(sql_str, params_list)
            conn.commit()
            return cur
        except Exception:
            conn.rollback()
            raise

    def executescript(self, sql_script: str) -> None:
        """
        Execute multiple SQL statements separated by semicolons.

        Note: PostgreSQL doesn't have native executescript like SQLite,
        so we split and execute statements individually.

        Args:
            sql_script: Multiple SQL statements separated by semicolons
        """
        conn = self.connect()
        cur = conn.cursor()
        try:
            # Split by semicolon, but be careful with strings containing semicolons
            # For simplicity, we use a basic split - complex scripts should use
            # separate execute() calls
            statements = [s.strip() for s in sql_script.split(';') if s.strip()]
            for stmt in statements:
                # Skip comment-only statements to avoid "can't execute an empty query"
                non_comment_lines = []
                for line in stmt.splitlines():
                    stripped = line.strip()
                    if not stripped or stripped.startswith("--"):
                        continue
                    non_comment_lines.append(stripped)
                if not non_comment_lines:
                    continue
                cur.execute(stmt)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def commit(self) -> None:
        """
        Commit the current transaction.
        
        Note: The cursor() context manager auto-commits, so this is mainly
        for compatibility with code that expects explicit commit calls.
        """
        if self._conn is not None:
            self._conn.commit()

    def rollback(self) -> None:
        """
        Rollback the current transaction.
        """
        if self._conn is not None:
            self._conn.rollback()

    def fetchone(self, sql_str: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """
        Execute SQL and fetch one row.

        Args:
            sql_str: SQL statement
            params: Parameters

        Returns:
            Single row tuple or None
        """
        with self.cursor() as cur:
            cur.execute(sql_str, params)
            return cur.fetchone()

    def fetchall(self, sql_str: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute SQL and fetch all rows.

        Args:
            sql_str: SQL statement
            params: Parameters

        Returns:
            List of row tuples
        """
        with self.cursor() as cur:
            cur.execute(sql_str, params)
            return cur.fetchall()

    def close(self) -> None:
        """Return connection to the pool."""
        if self._conn is not None and self._owns_connection:
            pool = _get_connection_pool()
            pool.putconn(self._conn)
            self._conn = None
            self._owns_connection = False

    def __enter__(self) -> "PostgresDB":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Context manager exit - return connection to pool."""
        self.close()
        return False


# ---------------------------------------------------------------------------
# Factory Function
# ---------------------------------------------------------------------------

def get_db(country: str) -> "PostgresDB":
    """
    Get a PostgreSQL database instance for the specified country.

    Args:
        country: Country name (e.g., "India", "Malaysia")

    Returns:
        PostgresDB instance
    """
    return PostgresDB(country)


# Alias for backward compatibility
CountryDB = PostgresDB
