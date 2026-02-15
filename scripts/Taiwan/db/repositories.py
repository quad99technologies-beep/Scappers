#!/usr/bin/env python3
"""
Taiwan database repository - all DB access in one place.

Provides methods for:
- Inserting/querying NHI drug code data
- Drug detail management
- Sub-step progress tracking
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class TaiwanRepository:
    """All database operations for Taiwan scraper (PostgreSQL backend)."""

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
        """Get table name with Taiwan prefix."""
        return f"tw_{name}"

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
        1: ("drug_codes",),
        2: ("drug_details",),
    }

    def clear_step_data(self, step: int, include_downstream: bool = False) -> Dict[str, int]:
        """
        Delete data for the given step (and optionally downstream steps) for this run_id.

        Args:
            step: Pipeline step number (1-2)
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
        sql, params = run_ledger_start(self.run_id, "Taiwan", mode=mode)
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
        sql, params = run_ledger_ensure_exists(self.run_id, "Taiwan", mode=mode)
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
    # Drug Codes (Step 1)
    # ------------------------------------------------------------------

    def insert_drug_codes(self, drugs: List[Dict]) -> int:
        """Bulk insert NHI drug code data."""
        if not drugs:
            return 0

        table = self._table("drug_codes")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, drug_code, drug_code_url, lic_id, name_en, name_zh,
             ingredient_content, gauge_quantity, single_compound, price,
             effective_date, effective_start_date, effective_end_date,
             pharmacists, dosage_form, classification, taxonomy_group,
             atc_code, page_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, drug_code) DO UPDATE SET
                drug_code_url = EXCLUDED.drug_code_url,
                lic_id = EXCLUDED.lic_id,
                name_en = EXCLUDED.name_en,
                name_zh = EXCLUDED.name_zh,
                ingredient_content = EXCLUDED.ingredient_content,
                gauge_quantity = EXCLUDED.gauge_quantity,
                single_compound = EXCLUDED.single_compound,
                price = EXCLUDED.price,
                effective_date = EXCLUDED.effective_date,
                effective_start_date = EXCLUDED.effective_start_date,
                effective_end_date = EXCLUDED.effective_end_date,
                pharmacists = EXCLUDED.pharmacists,
                dosage_form = EXCLUDED.dosage_form,
                classification = EXCLUDED.classification,
                taxonomy_group = EXCLUDED.taxonomy_group,
                atc_code = EXCLUDED.atc_code,
                page_number = EXCLUDED.page_number
        """

        with self.db.cursor() as cur:
            for drug in drugs:
                cur.execute(sql, (
                    self.run_id,
                    drug.get("drug_code"),
                    drug.get("drug_code_url"),
                    drug.get("lic_id"),
                    drug.get("name_en"),
                    drug.get("name_zh"),
                    drug.get("ingredient_content"),
                    drug.get("gauge_quantity"),
                    drug.get("single_compound"),
                    drug.get("price"),
                    drug.get("effective_date"),
                    drug.get("effective_start_date"),
                    drug.get("effective_end_date"),
                    drug.get("pharmacists"),
                    drug.get("dosage_form"),
                    drug.get("classification"),
                    drug.get("taxonomy_group"),
                    drug.get("atc_code"),
                    drug.get("page_number"),
                ))
                count += 1

        logger.info("Inserted %d drug code entries", count)
        self._db_log(f"OK | tw_drug_codes inserted={count} | run_id={self.run_id}")
        return count

    def get_drug_codes_count(self) -> int:
        """Get total drug code entries for this run."""
        table = self._table("drug_codes")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_drug_codes(self) -> List[Dict]:
        """Get all drug code entries as list of dicts."""
        table = self._table("drug_codes")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Drug Details (Step 2)
    # ------------------------------------------------------------------

    def insert_drug_details(self, details: List[Dict]) -> int:
        """Bulk insert drug detail/license data."""
        if not details:
            return 0

        table = self._table("drug_details")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, drug_code, lic_id_url, valid_date_roc, valid_date_ad,
             original_certificate_date, license_type, customs_doc_number,
             chinese_product_name, english_product_name, indications,
             dosage_form, package, drug_category, atc_code,
             principal_components, restricted_items, drug_company_name,
             drugstore_address, manufacturer_code, factory,
             manufacturer_name, manufacturing_plant_address,
             manufacturing_plant_company_address, country_of_manufacture,
             process_description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, drug_code) DO UPDATE SET
                lic_id_url = EXCLUDED.lic_id_url,
                valid_date_roc = EXCLUDED.valid_date_roc,
                valid_date_ad = EXCLUDED.valid_date_ad,
                original_certificate_date = EXCLUDED.original_certificate_date,
                license_type = EXCLUDED.license_type,
                customs_doc_number = EXCLUDED.customs_doc_number,
                chinese_product_name = EXCLUDED.chinese_product_name,
                english_product_name = EXCLUDED.english_product_name,
                indications = EXCLUDED.indications,
                dosage_form = EXCLUDED.dosage_form,
                package = EXCLUDED.package,
                drug_category = EXCLUDED.drug_category,
                atc_code = EXCLUDED.atc_code,
                principal_components = EXCLUDED.principal_components,
                restricted_items = EXCLUDED.restricted_items,
                drug_company_name = EXCLUDED.drug_company_name,
                drugstore_address = EXCLUDED.drugstore_address,
                manufacturer_code = EXCLUDED.manufacturer_code,
                factory = EXCLUDED.factory,
                manufacturer_name = EXCLUDED.manufacturer_name,
                manufacturing_plant_address = EXCLUDED.manufacturing_plant_address,
                manufacturing_plant_company_address = EXCLUDED.manufacturing_plant_company_address,
                country_of_manufacture = EXCLUDED.country_of_manufacture,
                process_description = EXCLUDED.process_description
        """

        with self.db.cursor() as cur:
            for detail in details:
                cur.execute(sql, (
                    self.run_id,
                    detail.get("drug_code"),
                    detail.get("lic_id_url"),
                    detail.get("valid_date_roc"),
                    detail.get("valid_date_ad"),
                    detail.get("original_certificate_date"),
                    detail.get("license_type"),
                    detail.get("customs_doc_number"),
                    detail.get("chinese_product_name"),
                    detail.get("english_product_name"),
                    detail.get("indications"),
                    detail.get("dosage_form"),
                    detail.get("package"),
                    detail.get("drug_category"),
                    detail.get("atc_code"),
                    detail.get("principal_components"),
                    detail.get("restricted_items"),
                    detail.get("drug_company_name"),
                    detail.get("drugstore_address"),
                    detail.get("manufacturer_code"),
                    detail.get("factory"),
                    detail.get("manufacturer_name"),
                    detail.get("manufacturing_plant_address"),
                    detail.get("manufacturing_plant_company_address"),
                    detail.get("country_of_manufacture"),
                    detail.get("process_description"),
                ))
                count += 1

        logger.info("Inserted %d drug detail entries", count)
        self._db_log(f"OK | tw_drug_details inserted={count} | run_id={self.run_id}")
        return count

    def get_drug_details_count(self) -> int:
        """Get total drug detail entries for this run."""
        table = self._table("drug_details")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_drug_details(self) -> List[Dict]:
        """Get all drug detail entries as list of dicts."""
        table = self._table("drug_details")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
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
        return {
            "drug_codes": self.get_drug_codes_count(),
            "drug_details": self.get_drug_details_count(),
        }
