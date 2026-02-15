#!/usr/bin/env python3
"""
Canada Quebec database repository - all DB access in one place.

Provides methods for:
- Inserting/querying annexe drug pricing data (IV.1, IV.2, V)
- Sub-step progress tracking
- Run lifecycle management
- Export report tracking
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class CanadaQuebecRepository:
    """All database operations for Canada Quebec scraper (PostgreSQL backend)."""

    def __init__(self, db, run_id: str):
        """
        Initialize repository.

        Args:
            db: PostgresDB instance
            run_id: Current run ID
        """
        self.db = db
        self.run_id = run_id

    def _table(self, name: str) -> str:
        """Get table name with Canada Quebec prefix."""
        return f"cq_{name}"

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel."""
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Utility: clear step data
    # ------------------------------------------------------------------

    _STEP_TABLE_MAP = {
        1: ("annexe_data",),
        2: ("annexe_data",),
        3: ("annexe_data",),
        4: ("annexe_data",),
        5: ("annexe_data",),
        6: ("annexe_data",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.

        Args:
            step: Pipeline step number (1-6)
            include_downstream: If True, also clear tables for all later steps.

        Returns:
            Dict mapping full table name -> rows deleted.
        """
        if step not in self._STEP_TABLE_MAP:
            raise ValueError(f"Unsupported step {step}; valid steps: {sorted(self._STEP_TABLE_MAP)}")

        steps = [s for s in sorted(self._STEP_TABLE_MAP) if s == step or (include_downstream and s >= step)]
        deleted: Dict[str, int] = {}
        with self.db.cursor() as cur:
            for s in steps:
                for short_name in self._STEP_TABLE_MAP[s]:
                    table = self._table(short_name)
                    cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))
                    deleted[table] = cur.rowcount
        try:
            self.db.commit()
        except Exception:
            pass

        self._db_log(f"CLEAR | steps={steps} tables={','.join(deleted)} run_id={self.run_id}")
        return deleted

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, mode: str = "fresh") -> None:
        """Register a new run in run_ledger."""
        from core.db.models import run_ledger_start
        sql, params = run_ledger_start(self.run_id, "CanadaQuebec", mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger start | run_id={self.run_id} mode={mode}")

    def finish_run(self, status: str, items_scraped: int = 0,
                   items_exported: int = 0, error_message: str = None) -> None:
        """Mark run as finished."""
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

    def ensure_run_in_ledger(self, mode: str = "resume") -> None:
        """Ensure this run_id exists in run_ledger (insert if missing)."""
        from core.db.models import run_ledger_ensure_exists
        sql, params = run_ledger_ensure_exists(self.run_id, "CanadaQuebec", mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger ensure | run_id={self.run_id}")

    def resume_run(self) -> None:
        """Mark existing run as resumed."""
        from core.db.models import run_ledger_resume
        sql, params = run_ledger_resume(self.run_id)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"RESUME | run_ledger updated | run_id={self.run_id}")

    # ------------------------------------------------------------------
    # Step progress (sub-step resume)
    # ------------------------------------------------------------------

    def mark_progress(self, step_number: int, step_name: str,
                      progress_key: str, status: str,
                      error_message: str = None) -> None:
        """Mark a sub-step progress item."""
        now = datetime.now().isoformat()
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
                    ELSE COALESCE({table}.started_at, EXCLUDED.started_at)
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
                now if status == "in_progress" else None,
                now if status in ("completed", "failed", "skipped") else None,
            ))

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
            status = row[0] if isinstance(row, tuple) else row["status"]
            return status == "completed"

    def get_completed_keys(self, step_number: int) -> Set[str]:
        """Get all completed progress keys for a step."""
        table = self._table("step_progress")

        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT progress_key FROM {table}
                WHERE run_id = %s AND step_number = %s AND status = 'completed'
            """, (self.run_id, step_number))
            rows = cur.fetchall()
            return {row[0] if isinstance(row, tuple) else row["progress_key"] for row in rows}

    # ------------------------------------------------------------------
    # Annexe Data (Steps 1-6)
    # ------------------------------------------------------------------

    def insert_annexe_data(self, data: List[Dict]) -> int:
        """Bulk insert annexe drug pricing data."""
        if not data:
            return 0

        table = self._table("annexe_data")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, annexe_type, generic_name, formulation, strength,
             fill_size, din, brand, manufacturer, price, price_type,
             currency, local_pack_code, local_pack_description, source_page)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, din, annexe_type) DO UPDATE SET
                generic_name = EXCLUDED.generic_name,
                formulation = EXCLUDED.formulation,
                strength = EXCLUDED.strength,
                fill_size = EXCLUDED.fill_size,
                brand = EXCLUDED.brand,
                manufacturer = EXCLUDED.manufacturer,
                price = EXCLUDED.price,
                price_type = EXCLUDED.price_type,
                currency = EXCLUDED.currency,
                local_pack_code = EXCLUDED.local_pack_code,
                local_pack_description = EXCLUDED.local_pack_description,
                source_page = EXCLUDED.source_page,
                scraped_at = CURRENT_TIMESTAMP
        """

        with self.db.cursor() as cur:
            for row in data:
                cur.execute(sql, (
                    self.run_id,
                    row.get("annexe_type"),
                    row.get("generic_name"),
                    row.get("formulation"),
                    row.get("strength"),
                    row.get("fill_size"),
                    row.get("din"),
                    row.get("brand"),
                    row.get("manufacturer"),
                    row.get("price"),
                    row.get("price_type"),
                    row.get("currency", "CAD"),
                    row.get("local_pack_code"),
                    row.get("local_pack_description"),
                    row.get("source_page"),
                ))
                count += 1

        logger.info("Inserted %d annexe data entries", count)
        self._db_log(f"OK | cq_annexe_data inserted={count} | run_id={self.run_id}")
        return count

    def get_annexe_data_count(self) -> int:
        """Get total annexe data entries for this run."""
        table = self._table("annexe_data")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_annexe_data(self) -> List[Dict]:
        """Get all annexe data entries as list of dicts."""
        table = self._table("annexe_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_annexe_data_by_type(self, annexe_type: str) -> List[Dict]:
        """Get annexe data filtered by annexe type (e.g. 'IV.1', 'IV.2', 'V')."""
        table = self._table("annexe_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND annexe_type = %s",
                       (self.run_id, annexe_type))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Export report tracking
    # ------------------------------------------------------------------

    def log_export_report(self, report_type: str, row_count: int = None,
                          export_format: str = "db") -> None:
        """Track an export/report for this run (DB-only, no file path)."""
        table = self._table("export_reports")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, report_type, row_count, export_format)
                VALUES (%s, %s, %s, %s)
            """, (self.run_id, report_type, row_count, export_format))

    # ------------------------------------------------------------------
    # Stats / reporting helpers
    # ------------------------------------------------------------------

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run."""
        table = self._table("annexe_data")
        stats = {
            "annexe_data_total": self.get_annexe_data_count(),
        }

        # Break down by annexe type
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT annexe_type, COUNT(*) as cnt
                FROM {table}
                WHERE run_id = %s
                GROUP BY annexe_type
                ORDER BY annexe_type
            """, (self.run_id,))
            rows = cur.fetchall()
            for row in rows:
                atype = row[0] if isinstance(row, tuple) else row["annexe_type"]
                cnt = row[1] if isinstance(row, tuple) else row["cnt"]
                stats[f"annexe_{atype}"] = cnt

        return stats
