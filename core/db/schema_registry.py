#!/usr/bin/env python3
"""
Schema migration registry. Tracks applied migrations in _schema_versions table
and applies pending SQL files in version order.

Migration files must be named: NNN_description.sql (e.g. 001_init.sql)
"""

import logging
import re
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

_VERSION_PATTERN = re.compile(r"^(\d+)_.*\.sql$")


class SchemaRegistry:
    """Tracks and applies versioned SQL migrations for PostgreSQL."""

    def __init__(self, db):
        """
        Initialize schema registry.

        Args:
            db: PostgresDB instance
        """
        self.db = db
        self._ensure_version_table()

    def _ensure_version_table(self):
        """Create _schema_versions table if it doesn't exist."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS _schema_versions (
                version INTEGER PRIMARY KEY,
                filename TEXT NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def current_version(self) -> int:
        """Return highest applied migration version, or 0 if none."""
        with self.db.cursor() as cur:
            cur.execute("SELECT MAX(version) FROM _schema_versions")
            row = cur.fetchone()
            return row[0] if row[0] is not None else 0

    def applied_versions(self) -> List[int]:
        """Return list of all applied migration versions."""
        with self.db.cursor() as cur:
            cur.execute("SELECT version FROM _schema_versions ORDER BY version")
            return [row[0] for row in cur.fetchall()]

    def apply_pending(self, migrations_dir: Path) -> List[int]:
        """
        Apply all SQL files in migrations_dir with version > current.

        Args:
            migrations_dir: Directory containing NNN_*.sql files
                           If postgres/ subdirectory exists, it will be used.

        Returns:
            List of newly applied version numbers.
        """
        # Check if there's a postgres subdirectory
        actual_dir = migrations_dir
        pg_dir = migrations_dir / "postgres"
        if pg_dir.exists():
            actual_dir = pg_dir

        if not actual_dir.exists():
            logger.warning("Migrations dir not found: %s", actual_dir)
            return []

        current = self.current_version()
        applied = []

        migration_files = sorted(actual_dir.glob("*.sql"))
        for sql_file in migration_files:
            match = _VERSION_PATTERN.match(sql_file.name)
            if not match:
                continue
            version = int(match.group(1))
            if version <= current:
                continue

            sql = sql_file.read_text(encoding="utf-8")

            logger.info("Applying migration %03d: %s", version, sql_file.name)
            self.db.executescript(sql)
            self.db.execute(
                "INSERT INTO _schema_versions (version, filename) VALUES (%s, %s)",
                (version, sql_file.name),
            )
            applied.append(version)

        if applied:
            logger.info("Applied %d migration(s): %s", len(applied), applied)
        return applied

    def apply_schema(self, schema_sql_path: Path) -> None:
        """
        Apply a schema SQL file (idempotent — uses IF NOT EXISTS).

        Args:
            schema_sql_path: Path to .sql file with CREATE TABLE IF NOT EXISTS statements.
                            If postgres/ subdirectory contains a version, that will be used.
        """
        # Check for postgres/ subdirectory version
        actual_path = schema_sql_path
        pg_path = schema_sql_path.parent / "postgres" / schema_sql_path.name
        if pg_path.exists():
            actual_path = pg_path

        if not actual_path.exists():
            raise FileNotFoundError(f"Schema file not found: {actual_path}")

        sql = actual_path.read_text(encoding="utf-8")
        self.db.executescript(sql)
        logger.info("Applied schema: %s", actual_path.name)
