#!/usr/bin/env python3
"""
Standard DDL for common tables present in the PostgreSQL database.

Tables:
- run_ledger: tracks each pipeline run
- http_requests: logs every HTTP request made during a run
- scraped_items: generic item storage (countries extend with specific tables)
"""

import uuid
from datetime import datetime, timezone
from typing import Optional, Tuple


# ---------------------------------------------------------------------------
# DDL Strings - PostgreSQL
# ---------------------------------------------------------------------------

RUN_LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS run_ledger (
    run_id TEXT PRIMARY KEY,
    scraper_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'running'
        CHECK(status IN ('running', 'completed', 'failed', 'cancelled', 'partial', 'resume', 'stopped')),
    step_count INTEGER DEFAULT 0,
    items_scraped INTEGER DEFAULT 0,
    items_exported INTEGER DEFAULT 0,
    error_message TEXT,
    git_commit TEXT,
    config_hash TEXT,
    metadata_json TEXT,
    mode TEXT DEFAULT 'fresh',
    thread_count INTEGER,
    totals_json TEXT
);
"""

RUN_LEDGER_COLUMNS = [
    "run_id",
    "scraper_name",
    "started_at",
    "ended_at",
    "status",
    "step_count",
    "items_scraped",
    "items_exported",
    "error_message",
    "git_commit",
    "config_hash",
    "metadata_json",
    "mode",
    "thread_count",
    "totals_json",
]

HTTP_REQUESTS_DDL = """
CREATE TABLE IF NOT EXISTS http_requests (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    url TEXT NOT NULL,
    method TEXT DEFAULT 'GET',
    status_code INTEGER,
    response_bytes INTEGER,
    elapsed_ms REAL,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_req_run ON http_requests(run_id);
CREATE INDEX IF NOT EXISTS idx_req_url ON http_requests(url);
"""

SCRAPED_ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS scraped_items (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
    source_url TEXT,
    item_json TEXT NOT NULL,
    item_hash TEXT,
    parse_status TEXT DEFAULT 'ok' CHECK(parse_status IN ('ok', 'partial', 'error')),
    error_reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_items_run ON scraped_items(run_id);
CREATE INDEX IF NOT EXISTS idx_items_hash ON scraped_items(item_hash);
"""

INPUT_UPLOADS_DDL = """
CREATE TABLE IF NOT EXISTS input_uploads (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    replaced_previous INTEGER DEFAULT 0,
    uploaded_by TEXT DEFAULT 'gui',
    source_country TEXT
);
"""

INPUT_UPLOADS_MIGRATE_DDL = """
ALTER TABLE input_uploads
    ADD COLUMN IF NOT EXISTS source_country TEXT;
"""

ALL_COMMON_DDL = [
    RUN_LEDGER_DDL,
    HTTP_REQUESTS_DDL,
    SCRAPED_ITEMS_DDL,
    INPUT_UPLOADS_DDL,
    INPUT_UPLOADS_MIGRATE_DDL,
]


def get_common_ddl() -> list:
    """Get the common DDL list."""
    return ALL_COMMON_DDL


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def generate_run_id() -> str:
    """Generate a unique run ID: timestamp + short uuid."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    short = uuid.uuid4().hex[:8]
    return f"{ts}_{short}"


def apply_common_schema(db) -> None:
    """
    Apply all common DDL to a database instance.

    Args:
        db: PostgresDB instance
    """
    for ddl in ALL_COMMON_DDL:
        db.executescript(ddl)


def run_ledger_start(run_id: str, scraper_name: str,
                     git_commit: Optional[str] = None,
                     config_hash: Optional[str] = None,
                     mode: str = "fresh",
                     thread_count: Optional[int] = None) -> Tuple[str, tuple]:
    """Return (sql, params) for starting a new run."""
    sql = """
        INSERT INTO run_ledger (run_id, scraper_name, started_at, status, git_commit, config_hash, mode, thread_count)
        VALUES (%s, %s, CURRENT_TIMESTAMP, 'running', %s, %s, %s, %s)
    """
    return sql, (run_id, scraper_name, git_commit, config_hash, mode, thread_count)


def run_ledger_finish(run_id: str, status: str,
                      items_scraped: int = 0, items_exported: int = 0,
                      error_message: Optional[str] = None,
                      totals_json: Optional[str] = None) -> Tuple[str, tuple]:
    """Return (sql, params) for finishing a run."""
    sql = """
        UPDATE run_ledger
        SET ended_at = CURRENT_TIMESTAMP,
            status = %s,
            items_scraped = %s,
            items_exported = %s,
            error_message = %s,
            totals_json = %s
        WHERE run_id = %s
    """
    return sql, (status, items_scraped, items_exported, error_message, totals_json, run_id)


def run_ledger_resume(run_id: str) -> Tuple[str, tuple]:
    """Return (sql, params) for resuming a run (set status back to running)."""
    sql = """
        UPDATE run_ledger
        SET status = 'running', mode = 'resume'
        WHERE run_id = %s
    """
    return sql, (run_id,)


def run_ledger_mark_resumable(run_id: str) -> Tuple[str, tuple]:
    """Return (sql, params) for marking a run as resumable (can be resumed later)."""
    sql = """
        UPDATE run_ledger
        SET status = 'resume'
        WHERE run_id = %s
    """
    return sql, (run_id,)


def run_ledger_mark_stopped(run_id: str) -> Tuple[str, tuple]:
    """Return (sql, params) for marking a run as stopped (cannot be resumed)."""
    sql = """
        UPDATE run_ledger
        SET status = 'stopped', ended_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
    """
    return sql, (run_id,)


def _migrate_run_ledger_schema(db) -> bool:
    """
    Migrate run_ledger table to support new status values ('resume', 'stopped').

    PostgreSQL can drop and recreate constraints.

    Returns True if migration was performed.
    """
    # Get the actual connection
    try:
        if hasattr(db, 'connect'):
            conn = db.connect()
        elif hasattr(db, 'executescript'):
            conn = db
        else:
            return False
    except Exception:
        return False

    # psycopg2 connections use cursor(); SQLite uses conn.execute()
    def _execute(connection, sql, params=None):
        cur = connection.cursor()
        try:
            cur.execute(sql, params)
            return cur
        except Exception:
            cur.close()
            raise

    try:
        cur = _execute(conn, """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'run_ledger'
        """)
        if cur.fetchone()[0] == 0:
            cur.close()
            return False  # Table doesn't exist
        cur.close()

        cur = _execute(conn, """
            SELECT pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid = 'run_ledger'::regclass
              AND contype = 'c'
              AND conname LIKE '%status%'
        """)
        row = cur.fetchone()
        cur.close()
        if row and "'resume'" in str(row[0]) and "'stopped'" in str(row[0]):
            return False  # No migration needed

        print("[MIGRATION] Migrating run_ledger schema for PostgreSQL...")

        cur = _execute(conn, """
            SELECT conname FROM pg_constraint
            WHERE conrelid = 'run_ledger'::regclass
              AND contype = 'c'
              AND conname LIKE '%status%'
        """)
        row = cur.fetchone()
        cur.close()
        if row:
            constraint_name = row[0]
            cur = conn.cursor()
            cur.execute(f"ALTER TABLE run_ledger DROP CONSTRAINT {constraint_name}")
            cur.close()

        cur = conn.cursor()
        cur.execute("""
            ALTER TABLE run_ledger ADD CONSTRAINT run_ledger_status_check
            CHECK(status IN ('running', 'completed', 'failed', 'cancelled', 'partial', 'resume', 'stopped'))
        """)
        cur.close()
        conn.commit()

        print("[MIGRATION] Successfully migrated run_ledger schema (PostgreSQL)")
        return True
    except Exception as e:
        print(f"[MIGRATION] PostgreSQL migration warning: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False


def recover_stale_db_runs(db, scraper_name: Optional[str] = None) -> dict:
    """
    Recover stale 'running' runs in the database on app startup.

    Logic:
    - Find all runs with status='running' for the given scraper (or all)
    - Mark the LATEST 'running' run as 'resume' (can be resumed)
    - Mark all OTHER 'running' runs as 'stopped' (cannot be resumed)

    Args:
        db: PostgresDB instance or raw connection
        scraper_name: Optional scraper name filter

    Returns:
        Dict with 'resumed' and 'stopped' lists of run_ids
    """
    result = {"resumed": [], "stopped": [], "migrated": False}

    # Get the actual connection - handle both PostgresDB and raw connection
    try:
        if hasattr(db, 'connect'):
            conn = db.connect()
        else:
            conn = db  # Raw connection
    except Exception:
        return result

    # Ensure schema supports resume/stopped statuses before updates
    try:
        if _migrate_run_ledger_schema(db):
            result["migrated"] = True
            # Refresh connection after migration (schema might have been recreated)
            if hasattr(db, "connect"):
                conn = db.connect()
    except Exception as e:
        print(f"[DB RECOVERY] Migration warning: {e}")

    # Find all running runs, ordered by started_at DESC (newest first)
    # psycopg2: use conn.cursor() then cur.execute(); conn.execute() is SQLite-only
    try:
        cur = conn.cursor()
        if scraper_name:
            sql = """
                SELECT run_id, scraper_name, started_at
                FROM run_ledger
                WHERE status IN ('running', 'resume') AND scraper_name = %s
                ORDER BY started_at DESC
            """
            cur.execute(sql, (scraper_name,))
        else:
            sql = """
                SELECT run_id, scraper_name, started_at
                FROM run_ledger
                WHERE status IN ('running', 'resume')
                ORDER BY scraper_name, started_at DESC
            """
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        # Table might not exist
        print(f"[DB RECOVERY] Query failed: {e}")
        return result

    if not rows:
        return result

    print(f"[DB RECOVERY] Found {len(rows)} running/resume run(s) to recover")

    # Group by scraper_name
    by_scraper = {}
    for row in rows:
        run_id, scraper, started_at = row[0], row[1], row[2]
        if scraper not in by_scraper:
            by_scraper[scraper] = []
        by_scraper[scraper].append(run_id)

    # For each scraper: first (newest) becomes 'resume', rest become 'stopped'
    for scraper, run_ids in by_scraper.items():
        if not run_ids:
            continue

        # First (newest) one becomes 'resume'
        latest_run_id = run_ids[0]
        try:
            sql, params = run_ledger_mark_resumable(latest_run_id)
            cur = conn.cursor()
            cur.execute(sql, params)
            cur.close()
            conn.commit()
            result["resumed"].append(latest_run_id)
            print(f"[DB RECOVERY] Marked {latest_run_id} as 'resume'")
        except Exception as e:
            print(f"[DB RECOVERY] Failed to mark {latest_run_id} as resume: {e}")
            # CHECK constraint might be failing - try migration
            if "CHECK constraint" in str(e) or "constraint" in str(e).lower():
                if _migrate_run_ledger_schema(db):
                    result["migrated"] = True
                    # Retry after migration
                    try:
                        sql, params = run_ledger_mark_resumable(latest_run_id)
                        cur = conn.cursor()
                        cur.execute(sql, params)
                        cur.close()
                        conn.commit()
                        result["resumed"].append(latest_run_id)
                        print(f"[DB RECOVERY] Marked {latest_run_id} as 'resume' (after migration)")
                    except Exception as e2:
                        print(f"[DB RECOVERY] Still failed after migration: {e2}")

        # All others become 'stopped'
        for run_id in run_ids[1:]:
            try:
                sql, params = run_ledger_mark_stopped(run_id)
                cur = conn.cursor()
                cur.execute(sql, params)
                cur.close()
                conn.commit()
                result["stopped"].append(run_id)
                print(f"[DB RECOVERY] Marked {run_id} as 'stopped'")
            except Exception as e:
                print(f"[DB RECOVERY] Failed to mark {run_id} as stopped: {e}")

    return result
