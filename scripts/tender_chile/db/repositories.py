#!/usr/bin/env python3
"""
Tender Chile database repository - all DB access in one place.

Provides methods for:
- Inserting/querying tender redirects, details, and awards
- Final output generation and retrieval (EVERSANA format)
- Sub-step progress tracking
- Run lifecycle management
"""

import sys
from pathlib import Path

# Add repo root to path for core imports (MUST be before any core imports)
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import logging
from typing import Dict, List, Optional

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class ChileRepository(BaseRepository):
    """All database operations for Tender Chile scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Tender_Chile"
    TABLE_PREFIX = "tc"

    _STEP_TABLE_MAP = {
        1: ("tender_redirects",),
        2: ("tender_details",),
        3: ("tender_awards",),
        4: ("final_output",),
    }

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # Progress, lifecycle, and clear_step_data methods inherited from BaseRepository

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
