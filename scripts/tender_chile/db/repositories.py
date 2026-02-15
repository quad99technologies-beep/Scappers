#!/usr/bin/env python3
"""
Tender Chile database repository - all DB access in one place.

Provides methods for:
- Inserting/querying tender redirects, details, and awards
- Final output generation and retrieval (EVERSANA format)
- Sub-step progress tracking
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class ChileRepository:
    """All database operations for Tender Chile scraper (PostgreSQL backend)."""

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
        """Get table name with Chile prefix."""
        return f"tc_{name}"

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
        1: ("tender_redirects",),
        2: ("tender_details",),
        3: ("tender_awards",),
        4: ("final_output",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.

        Args:
            step: Pipeline step number (1-4)
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
        sql, params = run_ledger_start(self.run_id, "Tender_Chile", mode=mode)
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
        sql, params = run_ledger_ensure_exists(self.run_id, "Tender_Chile", mode=mode)
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
    # Tender Redirects (Step 1)
    # ------------------------------------------------------------------

    def insert_tender_redirect(self, tender_id: str, redirect_url: str, source_url: str = None) -> None:
        """Insert a single tender redirect."""
        table = self._table("tender_redirects")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, tender_id, redirect_url, source_url)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id, tender_id) DO UPDATE SET
                    redirect_url = EXCLUDED.redirect_url,
                    source_url = EXCLUDED.source_url
            """, (self.run_id, tender_id, redirect_url, source_url))

    def insert_tender_redirects_bulk(self, redirects: List[Dict]) -> int:
        """Bulk insert tender redirects."""
        if not redirects:
            return 0
        if not self.run_id:
            raise ValueError("run_id is required for insert_tender_redirects_bulk")
        
        table = self._table("tender_redirects")
        count = 0
        
        # Use cursor context manager which auto-commits
        with self.db.cursor() as cur:
            for r in redirects:
                tender_id = r.get("tender_id", "").strip() if r.get("tender_id") else ""
                redirect_url = r.get("redirect_url", "").strip() if r.get("redirect_url") else ""
                source_url = r.get("source_url", "").strip() if r.get("source_url") else ""
                
                if not tender_id:
                    logger.warning("Skipping redirect with empty tender_id")
                    continue
                
                try:
                    cur.execute(f"""
                        INSERT INTO {table}
                        (run_id, tender_id, redirect_url, source_url)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, tender_id) DO UPDATE SET
                            redirect_url = EXCLUDED.redirect_url,
                            source_url = EXCLUDED.source_url
                    """, (
                        self.run_id,
                        tender_id,
                        redirect_url,
                        source_url,
                    ))
                    count += 1
                except Exception as e:
                    logger.error(f"Failed to insert redirect for tender_id={tender_id}: {e}")
                    raise  # Re-raise to fail the bulk insert
        
        # Explicit commit to ensure data is persisted (cursor context manager should do this, but be explicit)
        self.db.commit()
        
        logger.info("Inserted %d tender redirects", count)
        self._db_log(f"OK | tc_tender_redirects inserted={count} | run_id={self.run_id}")
        return count

    def get_tender_redirects_count(self) -> int:
        """Get total tender redirects for this run."""
        table = self._table("tender_redirects")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_tender_redirects(self) -> List[Dict]:
        """Get all tender redirects as list of dicts."""
        table = self._table("tender_redirects")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Tender Details (Step 2)
    # ------------------------------------------------------------------

    def insert_tender_detail(self, tender_id: str, details: Dict) -> None:
        """Insert a single tender detail."""
        table = self._table("tender_details")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, tender_id, tender_name, tender_status, publication_date,
                 closing_date, organization, province, contact_info, description,
                 currency, estimated_amount, source_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, tender_id) DO UPDATE SET
                    tender_name = EXCLUDED.tender_name,
                    tender_status = EXCLUDED.tender_status,
                    publication_date = EXCLUDED.publication_date,
                    closing_date = EXCLUDED.closing_date,
                    organization = EXCLUDED.organization,
                    province = EXCLUDED.province,
                    contact_info = EXCLUDED.contact_info,
                    description = EXCLUDED.description,
                    currency = EXCLUDED.currency,
                    estimated_amount = EXCLUDED.estimated_amount,
                    source_url = EXCLUDED.source_url
            """, (
                self.run_id, tender_id,
                details.get("tender_name"),
                details.get("tender_status"),
                details.get("publication_date"),
                details.get("closing_date"),
                details.get("organization"),
                details.get("province"),
                details.get("contact_info"),
                details.get("description"),
                details.get("currency", "CLP"),
                details.get("estimated_amount"),
                details.get("source_url"),
            ))

    def insert_tender_details_bulk(self, details: List[Dict]) -> int:
        """Bulk insert tender details."""
        if not details:
            return 0
        table = self._table("tender_details")
        count = 0
        with self.db.cursor() as cur:
            for d in details:
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, tender_id, tender_name, tender_status, publication_date,
                     closing_date, organization, province, contact_info, description,
                     currency, estimated_amount, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id, tender_id) DO UPDATE SET
                        tender_name = EXCLUDED.tender_name,
                        tender_status = EXCLUDED.tender_status,
                        publication_date = EXCLUDED.publication_date,
                        closing_date = EXCLUDED.closing_date,
                        organization = EXCLUDED.organization,
                        province = EXCLUDED.province,
                        contact_info = EXCLUDED.contact_info,
                        description = EXCLUDED.description,
                        currency = EXCLUDED.currency,
                        estimated_amount = EXCLUDED.estimated_amount,
                        source_url = EXCLUDED.source_url
                """, (
                    self.run_id,
                    d.get("tender_id"),
                    d.get("tender_name"),
                    d.get("tender_status"),
                    d.get("publication_date"),
                    d.get("closing_date"),
                    d.get("organization"),
                    d.get("province"),
                    d.get("contact_info"),
                    d.get("description"),
                    d.get("currency", "CLP"),
                    d.get("estimated_amount"),
                    d.get("source_url"),
                ))
                count += 1
        logger.info("Inserted %d tender details", count)
        self._db_log(f"OK | tc_tender_details inserted={count} | run_id={self.run_id}")
        return count

    def get_tender_details_count(self) -> int:
        """Get total tender details for this run."""
        table = self._table("tender_details")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_tender_details(self) -> List[Dict]:
        """Get all tender details as list of dicts."""
        table = self._table("tender_details")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Tender Awards (Step 3)
    # ------------------------------------------------------------------

    def insert_tender_award(self, award: Dict) -> None:
        """Insert a single tender award (includes all bidders, not just winners)."""
        table = self._table("tender_awards")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, tender_id, lot_number, lot_title, un_classification_code, buyer_specifications,
                 lot_quantity, supplier_name, supplier_rut, supplier_specifications, unit_price_offer,
                 awarded_quantity, total_net_awarded, award_amount, award_date, award_status, is_awarded,
                 awarded_unit_price, source_url, source_tender_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self.run_id,
                award.get("tender_id"),
                award.get("lot_number"),
                award.get("lot_title"),
                award.get("un_classification_code"),
                award.get("buyer_specifications"),
                award.get("lot_quantity"),
                award.get("supplier_name"),
                award.get("supplier_rut"),
                award.get("supplier_specifications"),
                award.get("unit_price_offer"),
                award.get("awarded_quantity"),
                award.get("total_net_awarded"),
                award.get("award_amount"),
                award.get("award_date"),
                award.get("award_status"),
                award.get("is_awarded"),
                award.get("awarded_unit_price"),
                award.get("source_url"),
                award.get("source_tender_url"),
            ))

    def insert_tender_awards_bulk(self, awards: List[Dict]) -> int:
        """Bulk insert tender awards (includes all bidders, not just winners)."""
        if not awards:
            return 0
        table = self._table("tender_awards")
        count = 0
        with self.db.cursor() as cur:
            for a in awards:
                cur.execute(f"""
                    INSERT INTO {table}
                    (run_id, tender_id, lot_number, lot_title, un_classification_code, buyer_specifications,
                     lot_quantity, supplier_name, supplier_rut, supplier_specifications, unit_price_offer,
                     awarded_quantity, total_net_awarded, award_amount, award_date, award_status, is_awarded,
                     awarded_unit_price, source_url, source_tender_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.run_id,
                    a.get("tender_id"),
                    a.get("lot_number"),
                    a.get("lot_title"),
                    a.get("un_classification_code"),
                    a.get("buyer_specifications"),
                    a.get("lot_quantity"),
                    a.get("supplier_name"),
                    a.get("supplier_rut"),
                    a.get("supplier_specifications"),
                    a.get("unit_price_offer"),
                    a.get("awarded_quantity"),
                    a.get("total_net_awarded"),
                    a.get("award_amount"),
                    a.get("award_date"),
                    a.get("award_status"),
                    a.get("is_awarded"),
                    a.get("awarded_unit_price"),
                    a.get("source_url"),
                    a.get("source_tender_url"),
                ))
                count += 1
        logger.info("Inserted %d tender awards", count)
        self._db_log(f"OK | tc_tender_awards inserted={count} | run_id={self.run_id}")
        return count

    def get_tender_awards_count(self) -> int:
        """Get total tender awards for this run."""
        table = self._table("tender_awards")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_tender_awards(self) -> List[Dict]:
        """Get all tender awards as list of dicts."""
        table = self._table("tender_awards")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Final Output (Step 4 - EVERSANA format)
    # ------------------------------------------------------------------

    def insert_final_output(self, outputs: List[Dict]) -> int:
        """Bulk insert final output data (EVERSANA format)."""
        if not outputs:
            return 0

        table = self._table("final_output")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, tender_id, tender_name, tender_status, organization,
             contact_info, lot_number, lot_title, supplier_name, supplier_rut,
             currency, estimated_amount, award_amount, publication_date,
             closing_date, award_date, description, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, tender_id, lot_number, supplier_name) DO UPDATE SET
                tender_name = EXCLUDED.tender_name,
                tender_status = EXCLUDED.tender_status,
                organization = EXCLUDED.organization,
                lot_title = EXCLUDED.lot_title,
                supplier_rut = EXCLUDED.supplier_rut,
                award_amount = EXCLUDED.award_amount,
                award_date = EXCLUDED.award_date
        """

        with self.db.cursor() as cur:
            for out in outputs:
                cur.execute(sql, (
                    self.run_id,
                    out.get("tender_id"),
                    out.get("tender_name"),
                    out.get("tender_status"),
                    out.get("organization"),
                    out.get("contact_info"),
                    out.get("lot_number"),
                    out.get("lot_title"),
                    out.get("supplier_name"),
                    out.get("supplier_rut"),
                    out.get("currency", "CLP"),
                    out.get("estimated_amount"),
                    out.get("award_amount"),
                    out.get("publication_date"),
                    out.get("closing_date"),
                    out.get("award_date"),
                    out.get("description"),
                    out.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d final output entries", count)
        self._db_log(f"OK | tc_final_output inserted={count} | run_id={self.run_id}")
        return count

    def get_final_output_count(self) -> int:
        """Get total final output entries for this run."""
        table = self._table("final_output")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_final_output(self) -> List[Dict]:
        """Get all final output entries as list of dicts."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s ORDER BY id", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_final_output_by_tender(self, tender_id: str) -> List[Dict]:
        """Get final output entries by tender ID."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND tender_id = %s", (self.run_id, tender_id))
            return [dict(row) for row in cur.fetchall()]

    def get_final_output_by_supplier(self, supplier_name: str) -> List[Dict]:
        """Get final output entries by supplier name."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND supplier_name ILIKE %s", 
                       (self.run_id, f"%{supplier_name}%"))
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
        # Check if run exists in run_ledger
        run_exists = False
        try:
            with self.db.cursor() as cur:
                cur.execute("SELECT 1 FROM run_ledger WHERE run_id = %s", (self.run_id,))
                run_exists = cur.fetchone() is not None
        except Exception:
            pass
        
        return {
            "run_exists": run_exists,
            "tender_redirects_count": self.get_tender_redirects_count(),
            "tender_details_count": self.get_tender_details_count(),
            "tender_awards_count": self.get_tender_awards_count(),
            "final_output_count": self.get_final_output_count(),
        }
