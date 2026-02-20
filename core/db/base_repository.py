"""
Base repository providing shared DB access patterns.
All country-specific repositories inherit from this.
Does NOT change any business logic, scraper behavior, or output schema.
"""

from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class BaseRepository:
    """
    Base class for all country-specific repositories.

    Subclasses MUST set:
        SCRAPER_NAME = "Argentina"     # Used in run_ledger
        TABLE_PREFIX = "ar"            # Used in _table()

    Subclasses MAY set:
        _STEP_TABLE_MAP = {1: ("urls",), 2: ("details",)}  # For clear_step_data()
    """

    SCRAPER_NAME: str = ""
    TABLE_PREFIX: str = ""
    _STEP_TABLE_MAP: Dict[int, Tuple[str, ...]] = {}

    def __init__(self, db, run_id: str):
        self.db = db
        self.run_id = run_id

    # ── Helpers ──────────────────────────────────────────────

    def _table(self, name: str) -> str:
        """Return fully-prefixed table name."""
        return f"{self.TABLE_PREFIX}_{name}"

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel and logging."""
        try:
            logger.info(f"[DB] {message}")
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    @contextmanager
    def transaction(self):
        """Context manager for explicit transaction control."""
        try:
            yield
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ── Run Lifecycle ────────────────────────────────────────

    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in the ledger."""
        from core.db.models import run_ledger_start
        sql, params = run_ledger_start(self.run_id, self.SCRAPER_NAME, mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger start | run_id={self.run_id} mode={mode}")

    def finish_run(self, status: str, items_scraped: int = 0,
                   items_exported: int = 0, error_message: str = None) -> None:
        """Mark run as finished in the ledger."""
        from core.db.models import run_ledger_finish
        sql, params = run_ledger_finish(
            self.run_id, status,
            items_scraped=items_scraped,
            items_exported=items_exported,
            error_message=error_message,
        )
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"FINISH | run_ledger updated | status={status} items={items_scraped}")

    def resume_run(self) -> None:
        """Mark existing run as resumed."""
        from core.db.models import run_ledger_resume
        sql, params = run_ledger_resume(self.run_id)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"RESUME | run_ledger updated | run_id={self.run_id}")

    def ensure_run_in_ledger(self, mode: str = "resume") -> None:
        """Ensure run exists in ledger (insert if missing)."""
        from core.db.models import run_ledger_ensure_exists
        sql, params = run_ledger_ensure_exists(
            self.run_id, self.SCRAPER_NAME, mode=mode
        )
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger ensure | run_id={self.run_id}")

    # Alias for backward compatibility (Russia uses this name)
    ensure_run_exists = ensure_run_in_ledger

    # ── Step Management ──────────────────────────────────────

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """Delete data for a step (and optionally downstream steps)."""
        if step not in self._STEP_TABLE_MAP:
            raise ValueError(
                f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}"
            )
        steps = [
            s for s in sorted(self._STEP_TABLE_MAP)
            if s == step or (include_downstream and s >= step)
        ]
        deleted: Dict[str, int] = {}
        with self.db.cursor() as cur:
            for s in steps:
                for short_name in self._STEP_TABLE_MAP[s]:
                    table = self._table(short_name)
                    cur.execute(
                        f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,)
                    )
                    deleted[table] = cur.rowcount
        try:
            self.db.commit()
        except Exception:
            pass
        self._db_log(
            f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}"
        )
        return deleted

    # ── Step Progress ────────────────────────────────────────

    def mark_progress(self, step_number: int, step_name: str,
                      progress_key: str, status: str,
                      error_message: str = None) -> None:
        """Mark a sub-step progress item."""
        now = datetime.now()
        table = self._table("step_progress")

        sql = f"""
            INSERT INTO {table}
            (run_id, step_number, step_name, progress_key, status,
             error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, step_number, progress_key) DO UPDATE SET
                step_name = EXCLUDED.step_name,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                started_at = CASE
                    WHEN EXCLUDED.status = 'in_progress' THEN EXCLUDED.started_at
                    WHEN {table}.started_at IS NULL THEN EXCLUDED.started_at
                    ELSE {table}.started_at
                END,
                completed_at = CASE
                    WHEN EXCLUDED.status IN ('completed', 'failed', 'skipped') THEN EXCLUDED.completed_at
                    WHEN EXCLUDED.status = 'in_progress' THEN NULL
                    ELSE {table}.completed_at
                END
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id, step_number, step_name, progress_key, status,
                error_message,
                now,
                now if status in ("completed", "failed", "skipped") else None,
            ))
        self.db.commit()

        # Mirror into shared per-step statistics table (best-effort).
        try:
            started_at = None
            completed_at = None
            duration_seconds = None
            try:
                with self.db.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT started_at, completed_at
                        FROM {table}
                        WHERE run_id = %s AND step_number = %s AND progress_key = %s
                        """,
                        (self.run_id, step_number, progress_key),
                    )
                    row = cur.fetchone()
                    if row:
                        started_at, completed_at = row[0], row[1]
                        if started_at and completed_at:
                            duration_seconds = (completed_at - started_at).total_seconds()
            except Exception:
                pass

            from core.statistics.step_statistics import upsert_step_statistics

            upsert_step_statistics(
                self.db,
                scraper_name=self.SCRAPER_NAME,
                run_id=self.run_id,
                step_number=step_number,
                step_name=step_name,
                status=status,
                error_message=error_message,
                stats={
                    "progress_key": progress_key,
                    "duration_seconds": duration_seconds,
                    "started_at": started_at.isoformat() if started_at else None,
                    "completed_at": completed_at.isoformat() if completed_at else None,
                },
            )
        except Exception:
            pass

    def is_progress_completed(self, step_number: int, progress_key: str) -> bool:
        """Check if a sub-step item is completed."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT status FROM {table}
                WHERE run_id = %s AND step_number = %s AND progress_key = %s
            """, (self.run_id, step_number, progress_key))
            row = cur.fetchone()
            if row is None:
                return False
            # Handle both tuple and dict cursor
            status = row[0] if isinstance(row, tuple) else row.get("status")
            return status == "completed"

    def get_completed_keys(self, step_number: int) -> List[str]:
        """Get all completed progress keys for a step."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT progress_key FROM {table}
                WHERE run_id = %s AND step_number = %s AND status = 'completed'
            """, (self.run_id, step_number))
            rows = cur.fetchall()
            return [row[0] if isinstance(row, tuple) else row.get("progress_key") for row in rows]
