#!/usr/bin/env python3
"""
North Macedonia database repository - all DB access in one place.

Provides methods for:
- URL collection (Step 1)
- Drug register data (Step 2)
- PCID mapping (Step 3)
- Validation and quality checks
- Statistics and reporting
- Chrome instance tracking
- Error logging
- Export functions
"""

import json
import logging
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Set, Any, Tuple
from datetime import datetime
from contextlib import contextmanager

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class NorthMacedoniaRepository(BaseRepository):
    """All database operations for North Macedonia scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "NorthMacedonia"
    TABLE_PREFIX = "nm"

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    # Progress methods (mark_progress, is_progress_completed, get_completed_keys)
    # are inherited from BaseRepository.

    # ==================================================================
    # URLS (Step 1) - replaces north_macedonia_detail_urls.csv
    # ==================================================================

    def insert_urls(self, urls: List[Dict], batch_size: int = 500) -> int:
        """
        Bulk insert collected URLs.

        Args:
            urls: List of URL records with keys: detail_url, page_num
            batch_size: Number of records per batch

        Returns:
            Number of rows inserted
        """
        if not urls:
            return 0

        table = self._table("urls")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, detail_url, page_num, status)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (run_id, detail_url) DO UPDATE SET
                page_num = EXCLUDED.page_num,
                status = CASE
                    WHEN {table}.status = 'scraped' THEN {table}.status
                    ELSE 'pending'
                END
        """

        with self.db.cursor() as cur:
            for url_rec in urls:
                cur.execute(sql, (
                    self.run_id,
                    url_rec.get("detail_url", ""),
                    url_rec.get("page_num", 0),
                    url_rec.get("status", "pending"),
                ))
                count += 1

                # Commit in batches
                if count % batch_size == 0:
                    self.db.commit()

        self.db.commit()
        self._db_log(f"OK | nm_urls inserted={count} | run_id={self.run_id}")
        return count

    def get_url_count(self) -> int:
        """Get total URLs for this run."""
        table = self._table("urls")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_pending_urls(self, limit: int = 100) -> List[Dict]:
        """Get URLs that need scraping (pending status)."""
        table = self._table("urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, detail_url, page_num
                FROM {table}
                WHERE run_id = %s AND status = 'pending'
                ORDER BY id
                LIMIT %s
            """, (self.run_id, limit))
            rows = cur.fetchall()
            return [
                {
                    "id": row[0],
                    "detail_url": row[1],
                    "page_num": row[2],
                }
                for row in rows
            ]

    def get_scraped_url_count(self) -> int:
        """Get count of successfully scraped URLs."""
        table = self._table("urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(*) FROM {table}
                WHERE run_id = %s AND status = 'scraped'
            """, (self.run_id,))
            row = cur.fetchone()
            return row[0] if row else 0

    def mark_url_scraped(self, url_id: int, status: str = 'scraped',
                         error: str = None) -> None:
        """
        Mark a URL as scraped with given status.

        Args:
            url_id: The ID of the URL record
            status: 'scraped', 'failed', or 'skipped'
            error: Error message if status is 'failed'
        """
        table = self._table("urls")
        now = datetime.now()

        with self.db.cursor() as cur:
            cur.execute(f"""
                UPDATE {table}
                SET status = %s,
                    error_message = %s,
                    scraped_at = %s,
                    retry_count = retry_count + 1
                WHERE id = %s
            """, (status, (error or "")[:500], now, url_id))
        self.db.commit()

    def get_failed_urls(self, max_retries: int = 3) -> List[Dict]:
        """Get failed URLs that haven't exceeded max retries."""
        table = self._table("urls")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT id, detail_url, retry_count, error_message
                FROM {table}
                WHERE run_id = %s AND status = 'failed' AND retry_count < %s
                ORDER BY id
            """, (self.run_id, max_retries))
            rows = cur.fetchall()
            return [
                {
                    "id": row[0],
                    "detail_url": row[1],
                    "retry_count": row[2],
                    "error_message": row[3],
                }
                for row in rows
            ]

    # ==================================================================
    # DRUG REGISTER (Step 2) - replaces north_macedonia_drug_register.csv
    # ==================================================================

    def insert_drug_register(self, data: Dict, url_id: int = None) -> int:
        """
        Insert drug registration data.

        Args:
            data: Dictionary with drug registration fields
            url_id: Optional URL ID reference

        Returns:
            Inserted record ID
        """
        table = self._table("drug_register")

        sql = f"""
            INSERT INTO {table}
            (run_id, url_id, detail_url, local_product_name, local_pack_code,
             generic_name, who_atc_code, formulation, strength_size, fill_size,
             customized_1, marketing_authority_company_name, effective_start_date,
             effective_end_date, public_with_vat_price, pharmacy_purchase_price,
             local_pack_description, reimbursable_status, reimbursable_rate,
             reimbursable_notes, copayment_value, copayment_percent, margin_rule,
             vat_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, detail_url) DO UPDATE SET
                local_product_name = EXCLUDED.local_product_name,
                local_pack_code = EXCLUDED.local_pack_code,
                generic_name = EXCLUDED.generic_name,
                who_atc_code = EXCLUDED.who_atc_code,
                formulation = EXCLUDED.formulation,
                strength_size = EXCLUDED.strength_size,
                fill_size = EXCLUDED.fill_size,
                customized_1 = EXCLUDED.customized_1,
                marketing_authority_company_name = EXCLUDED.marketing_authority_company_name,
                effective_start_date = EXCLUDED.effective_start_date,
                effective_end_date = EXCLUDED.effective_end_date,
                public_with_vat_price = EXCLUDED.public_with_vat_price,
                pharmacy_purchase_price = EXCLUDED.pharmacy_purchase_price,
                local_pack_description = EXCLUDED.local_pack_description
            RETURNING id
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                url_id,
                data.get("detail_url"),
                data.get("Local Product Name"),
                data.get("Local Pack Code"),
                data.get("Generic Name"),
                data.get("WHO ATC Code"),
                data.get("Formulation"),
                data.get("Strength Size"),
                data.get("Fill Size"),
                data.get("Customized 1"),
                data.get("Marketing Authority / Company Name"),
                data.get("Effective Start Date"),
                data.get("Effective End Date"),
                data.get("Public with VAT Price"),
                data.get("Pharmacy Purchase Price"),
                data.get("Local Pack Description"),
                data.get("Reimbursable Status", "PARTIALLY REIMBURSABLE"),
                data.get("Reimbursable Rate", "80.00%"),
                data.get("Reimbursable Notes", ""),
                data.get("Copayment Value", ""),
                data.get("Copayment Percent", "20.00%"),
                data.get("Margin Rule", "650 PPP & PPI Listed"),
                data.get("VAT Percent", "5"),
            ))
            row = cur.fetchone()
            record_id = row[0] if row else None

        self.db.commit()
        self._db_log(f"OK | nm_drug_register inserted | id={record_id}")
        return record_id

    def insert_drug_register_batch(self, records: List[Dict], batch_size: int = 500) -> int:
        """
        Bulk insert drug registration data.

        Args:
            records: List of dictionaries with drug registration fields
            batch_size: Number of records per batch

        Returns:
            Number of rows inserted
        """
        if not records:
            return 0

        table = self._table("drug_register")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, url_id, detail_url, local_product_name, local_pack_code,
             generic_name, who_atc_code, formulation, strength_size, fill_size,
             customized_1, marketing_authority_company_name, effective_start_date,
             effective_end_date, public_with_vat_price, pharmacy_purchase_price,
             local_pack_description, reimbursable_status, reimbursable_rate,
             reimbursable_notes, copayment_value, copayment_percent, margin_rule,
             vat_percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, detail_url) DO UPDATE SET
                local_product_name = EXCLUDED.local_product_name,
                local_pack_code = EXCLUDED.local_pack_code,
                generic_name = EXCLUDED.generic_name,
                who_atc_code = EXCLUDED.who_atc_code,
                formulation = EXCLUDED.formulation,
                strength_size = EXCLUDED.strength_size,
                fill_size = EXCLUDED.fill_size,
                customized_1 = EXCLUDED.customized_1,
                marketing_authority_company_name = EXCLUDED.marketing_authority_company_name,
                effective_start_date = EXCLUDED.effective_start_date,
                effective_end_date = EXCLUDED.effective_end_date,
                public_with_vat_price = EXCLUDED.public_with_vat_price,
                pharmacy_purchase_price = EXCLUDED.pharmacy_purchase_price,
                local_pack_description = EXCLUDED.local_pack_description
        """

        with self.db.cursor() as cur:
            for rec in records:
                cur.execute(sql, (
                    self.run_id,
                    rec.get("url_id"),
                    rec.get("source_url") or rec.get("detail_url"),
                    rec.get("product_name") or rec.get("product_name_en"),
                    rec.get("registration_number"),
                    rec.get("generic_name") or rec.get("generic_name_en"),
                    rec.get("atc_code"),
                    rec.get("dosage_form"),
                    rec.get("strength"),
                    rec.get("pack_size"),
                    rec.get("composition"),
                    rec.get("manufacturer") or rec.get("marketing_authorisation_holder"),
                    rec.get("effective_start_date"),
                    rec.get("effective_end_date"),
                    rec.get("public_price"),
                    rec.get("pharmacy_price"),
                    rec.get("description"),
                    rec.get("reimbursable_status", "PARTIALLY REIMBURSABLE"),
                    rec.get("reimbursable_rate", "80.00%"),
                    rec.get("reimbursable_notes", ""),
                    rec.get("copayment_value", ""),
                    rec.get("copayment_percent", "20.00%"),
                    rec.get("margin_rule", "650 PPP & PPI Listed"),
                    rec.get("vat_percent", "5"),
                ))
                count += 1

                # Commit in batches
                if count % batch_size == 0:
                    self.db.commit()

        self.db.commit()
        self._db_log(f"OK | nm_drug_register batch inserted={count} | run_id={self.run_id}")
        return count

    def get_drug_register_count(self) -> int:
        """Get total drug register entries for this run."""
        table = self._table("drug_register")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row.get("count", 0)

    def get_all_drug_register(self) -> List[Dict]:
        """Get all drug register entries as list of dicts."""
        table = self._table("drug_register")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s ORDER BY id", (self.run_id,))
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # PCID MAPPING
    # ==================================================================

    def insert_pcid_mapping(self, drug_register_id: int, pcid: str,
                           match_type: str, match_score: float,
                           product_data: Dict) -> int:
        """
        Insert PCID mapping result.

        Args:
            drug_register_id: Reference to drug register record
            pcid: PCID value (or None if not found)
            match_type: 'exact', 'fuzzy', 'manual', 'not_found'
            match_score: Match confidence score (0-1)
            product_data: Product details dict

        Returns:
            Inserted record ID
        """
        table = self._table("pcid_mappings")

        sql = f"""
            INSERT INTO {table}
            (run_id, drug_register_id, pcid, match_type, match_score,
             local_product_name, generic_name, manufacturer, local_pack_code,
             local_pack_description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, drug_register_id) DO UPDATE SET
                pcid = EXCLUDED.pcid,
                match_type = EXCLUDED.match_type,
                match_score = EXCLUDED.match_score
            RETURNING id
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                drug_register_id,
                pcid,
                match_type,
                match_score,
                product_data.get("local_product_name"),
                product_data.get("generic_name"),
                product_data.get("manufacturer"),
                product_data.get("local_pack_code"),
                product_data.get("local_pack_description"),
            ))
            row = cur.fetchone()
            record_id = row[0] if row else None

        self.db.commit()
        self._db_log(f"OK | nm_pcid_mappings inserted | id={record_id} pcid={pcid}")
        return record_id

    def get_pcid_mapping_stats(self) -> Dict:
        """Get PCID mapping statistics."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) as total,
                    COUNT(CASE WHEN match_type = 'exact' THEN 1 END) as exact_matches,
                    COUNT(CASE WHEN match_type = 'fuzzy' THEN 1 END) as fuzzy_matches,
                    COUNT(CASE WHEN match_type = 'not_found' THEN 1 END) as not_found,
                    AVG(CASE WHEN match_score IS NOT NULL THEN match_score END) as avg_score
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            row = cur.fetchone()
            if row:
                return {
                    "total": row[0] or 0,
                    "exact_matches": row[1] or 0,
                    "fuzzy_matches": row[2] or 0,
                    "not_found": row[3] or 0,
                    "avg_score": float(row[4]) if row[4] else 0.0,
                }
        return {}

    # ==================================================================
    # FINAL OUTPUT (EVERSANA format)
    # ==================================================================

    def insert_final_output(self, drug_register_id: int, pcid_mapping_id: int,
                           data: Dict) -> int:
        """Insert final output record in EVERSANA format."""
        table = self._table("final_output")

        sql = f"""
            INSERT INTO {table}
            (run_id, drug_register_id, pcid_mapping_id, pcid, country, company,
             local_product_name, generic_name, description, strength, dosage_form,
             pack_size, public_price, pharmacy_price, currency, effective_start_date,
             effective_end_date, local_pack_code, atc_code, reimbursable_status,
             reimbursable_rate, copayment_percent, margin_rule, vat_percent,
             marketing_authorisation_holder, source_url, source_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, drug_register_id) DO UPDATE SET
                pcid = EXCLUDED.pcid,
                company = EXCLUDED.company,
                local_product_name = EXCLUDED.local_product_name,
                generic_name = EXCLUDED.generic_name,
                description = EXCLUDED.description,
                strength = EXCLUDED.strength,
                dosage_form = EXCLUDED.dosage_form,
                pack_size = EXCLUDED.pack_size,
                public_price = EXCLUDED.public_price,
                pharmacy_price = EXCLUDED.pharmacy_price
            RETURNING id
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                drug_register_id,
                pcid_mapping_id,
                data.get("pcid"),
                data.get("country", "NORTH MACEDONIA"),
                data.get("company"),
                data.get("local_product_name"),
                data.get("generic_name"),
                data.get("description"),
                data.get("strength"),
                data.get("dosage_form"),
                data.get("pack_size"),
                data.get("public_price"),
                data.get("pharmacy_price"),
                data.get("currency", "MKD"),
                data.get("effective_start_date"),
                data.get("effective_end_date"),
                data.get("local_pack_code"),
                data.get("atc_code"),
                data.get("reimbursable_status"),
                data.get("reimbursable_rate"),
                data.get("copayment_percent"),
                data.get("margin_rule"),
                data.get("vat_percent"),
                data.get("marketing_authorisation_holder"),
                data.get("source_url"),
                data.get("source_type", "drug_register"),
            ))
            row = cur.fetchone()
            record_id = row[0] if row else None

        self.db.commit()
        return record_id

    def get_final_output_count(self) -> int:
        """Get total final output records."""
        table = self._table("final_output")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if row else 0

    # ==================================================================
    # VALIDATION (NEW)
    # ==================================================================

    def insert_validation_result(self, validation_type: str, table_name: str,
                                 record_id: int, field_name: str,
                                 validation_rule: str, status: str,
                                 message: str, severity: str = "medium") -> None:
        """
        Insert validation result.

        Args:
            validation_type: Type of validation (e.g., 'required_field', 'format', 'range')
            table_name: Table being validated
            record_id: Record ID
            field_name: Field name
            validation_rule: Rule description
            status: 'pass', 'fail', 'warning'
            message: Validation message
            severity: 'critical', 'high', 'medium', 'low', 'info'
        """
        table = self._table("validation_results")

        sql = f"""
            INSERT INTO {table}
            (run_id, validation_type, table_name, record_id, field_name,
             validation_rule, status, message, severity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id, validation_type, table_name, record_id, field_name,
                validation_rule, status, message, severity
            ))
        self.db.commit()

    def get_validation_summary(self) -> Dict:
        """Get validation summary statistics."""
        table = self._table("validation_results")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_validations,
                    COUNT(CASE WHEN status = 'pass' THEN 1 END) as passed,
                    COUNT(CASE WHEN status = 'fail' THEN 1 END) as failed,
                    COUNT(CASE WHEN status = 'warning' THEN 1 END) as warnings,
                    COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical,
                    COUNT(CASE WHEN severity = 'high' THEN 1 END) as high,
                    COUNT(CASE WHEN severity = 'medium' THEN 1 END) as medium,
                    COUNT(CASE WHEN severity = 'low' THEN 1 END) as low
                FROM {table}
                WHERE run_id = %s
            """, (self.run_id,))
            row = cur.fetchone()
            if row:
                return {
                    "total_validations": row[0] or 0,
                    "passed": row[1] or 0,
                    "failed": row[2] or 0,
                    "warnings": row[3] or 0,
                    "critical": row[4] or 0,
                    "high": row[5] or 0,
                    "medium": row[6] or 0,
                    "low": row[7] or 0,
                }
        return {}

    def get_validation_failures(self, severity: str = None) -> List[Dict]:
        """Get validation failures, optionally filtered by severity."""
        table = self._table("validation_results")
        
        sql = f"""
            SELECT validation_type, table_name, record_id, field_name,
                   validation_rule, message, severity
            FROM {table}
            WHERE run_id = %s AND status = 'fail'
        """
        params = [self.run_id]
        
        if severity:
            sql += " AND severity = %s"
            params.append(severity)
        
        sql += " ORDER BY severity DESC, created_at"
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # STATISTICS (NEW)
    # ==================================================================

    def insert_statistic(self, step_number: int, metric_name: str,
                        metric_value: float, metric_type: str,
                        category: str = None, description: str = None) -> None:
        """
        Insert a statistic metric.

        Args:
            step_number: Pipeline step number
            metric_name: Metric name (e.g., 'urls_collected', 'scrape_success_rate')
            metric_value: Numeric value
            metric_type: 'count', 'percentage', 'duration', 'rate', 'size'
            category: Optional category grouping
            description: Optional description
        """
        table = self._table("statistics")

        sql = f"""
            INSERT INTO {table}
            (run_id, step_number, metric_name, metric_value, metric_type, category, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id, step_number, metric_name, metric_value,
                metric_type, category, description
            ))
        self.db.commit()

    def get_statistics(self, step_number: int = None) -> List[Dict]:
        """Get statistics, optionally filtered by step."""
        table = self._table("statistics")
        
        sql = f"""
            SELECT step_number, metric_name, metric_value, metric_type,
                   category, description, created_at
            FROM {table}
            WHERE run_id = %s
        """
        params = [self.run_id]
        
        if step_number is not None:
            sql += " AND step_number = %s"
            params.append(step_number)
        
        sql += " ORDER BY step_number, metric_name"
        
        with self.db.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]

    # ==================================================================
    # ERROR LOGGING
    # ==================================================================

    def log_error(self, error_type: str, error_message: str,
                  step_number: int = None, step_name: str = None,
                  url: str = None, context: Dict = None,
                  include_traceback: bool = True) -> None:
        """
        Log an error to the database.

        Args:
            error_type: Type of error (e.g., 'network', 'parsing', 'validation')
            error_message: Error message
            step_number: Optional step number
            step_name: Optional step name
            url: Optional URL where error occurred
            context: Optional context dict
            include_traceback: Whether to include traceback
        """
        table = self._table("errors")
        tb = traceback.format_exc() if include_traceback else None

        sql = f"""
            INSERT INTO {table}
            (run_id, error_type, error_message, context, step_number,
             step_name, url, traceback)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                error_type,
                error_message[:1000],
                json.dumps(context) if context else None,
                step_number,
                step_name,
                url,
                tb[:5000] if tb else None,
            ))
        self.db.commit()

    def get_error_count(self) -> int:
        """Get total error count for this run."""
        table = self._table("errors")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if row else 0

    def get_errors_by_type(self) -> Dict[str, int]:
        """Get error counts grouped by type."""
        table = self._table("errors")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT error_type, COUNT(*) as count
                FROM {table}
                WHERE run_id = %s
                GROUP BY error_type
                ORDER BY count DESC
            """, (self.run_id,))
            rows = cur.fetchall()
            return {row[0]: row[1] for row in rows}

    # ==================================================================
    # EXPORT REPORTING
    # ==================================================================

    def log_export_report(self, report_type: str, row_count: int = None,
                         file_path: str = None, export_format: str = "db") -> None:
        """Track an export/report for this run."""
        table = self._table("export_reports")
        with self.db.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {table}
                (run_id, report_type, row_count, file_path, export_format)
                VALUES (%s, %s, %s, %s, %s)
            """, (self.run_id, report_type, row_count, file_path, export_format))
        self.db.commit()

    # ==================================================================
    # COMPREHENSIVE RUN STATISTICS
    # ==================================================================

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run (single query for all counts)."""
        empty = {
            'urls_total': 0, 'urls_scraped': 0, 'urls_failed': 0, 'urls_pending': 0,
            'drug_register_total': 0, 'pcid_mappings_total': 0,
            'final_output_total': 0, 'error_count': 0, 'validation_passed': 0,
            'validation_failed': 0, 'validation_warnings': 0, 'run_exists': False,
        }

        try:
            sql = """
                SELECT
                    (SELECT COUNT(*) FROM nm_urls WHERE run_id = %s) as urls_total,
                    (SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'scraped') as urls_scraped,
                    (SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'failed') as urls_failed,
                    (SELECT COUNT(*) FROM nm_urls WHERE run_id = %s AND status = 'pending') as urls_pending,
                    (SELECT COUNT(*) FROM nm_drug_register WHERE run_id = %s) as drug_register_total,
                    (SELECT COUNT(*) FROM nm_pcid_mappings WHERE run_id = %s) as pcid_mappings_total,
                    (SELECT COUNT(*) FROM nm_final_output WHERE run_id = %s) as final_output_total,
                    (SELECT COUNT(*) FROM nm_errors WHERE run_id = %s) as error_count,
                    (SELECT COUNT(*) FROM nm_validation_results WHERE run_id = %s AND status = 'pass') as validation_passed,
                    (SELECT COUNT(*) FROM nm_validation_results WHERE run_id = %s AND status = 'fail') as validation_failed,
                    (SELECT COUNT(*) FROM nm_validation_results WHERE run_id = %s AND status = 'warning') as validation_warnings,
                    (SELECT 1 FROM run_ledger WHERE run_id = %s LIMIT 1) as run_exists
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (self.run_id,) * 12)
                row = cur.fetchone()
                if row is None:
                    return empty
                return {
                    'urls_total': row[0] or 0,
                    'urls_scraped': row[1] or 0,
                    'urls_failed': row[2] or 0,
                    'urls_pending': row[3] or 0,
                    'drug_register_total': row[4] or 0,
                    'pcid_mappings_total': row[5] or 0,
                    'final_output_total': row[6] or 0,
                    'error_count': row[7] or 0,
                    'validation_passed': row[8] or 0,
                    'validation_failed': row[9] or 0,
                    'validation_warnings': row[10] or 0,
                    'run_exists': row[11] is not None,
                }
        except Exception as e:
            self._db_log(f"WARN | get_run_stats failed: {e}")
            return empty

    @staticmethod
    def get_latest_incomplete_run(db) -> Optional[str]:
        """
        Find the best run_id to resume from database.

        Returns run_id if found, None otherwise.
        """
        try:
            with db.cursor() as cur:
                cur.execute("""
                    SELECT run_id
                    FROM run_ledger
                    WHERE scraper_name = 'NorthMacedonia'
                      AND status IN ('running', 'partial', 'resume', 'stopped')
                    ORDER BY COALESCE(items_scraped, 0) DESC NULLS LAST, started_at DESC
                    LIMIT 1
                """)
                row = cur.fetchone()
                return row[0] if row else None
        except Exception as e:
            print(f"[DB] Could not check for incomplete runs: {e}")
            return None

    # ------------------------------------------------------------------
    # Translation Cache (replaces JSON file cache)
    # ------------------------------------------------------------------

    def get_translation_cache(self, source_lang: str = 'mk', target_lang: str = 'en') -> Dict[str, str]:
        """Load all translation cache entries from DB."""
        cache = {}
        try:
            sql = """
                SELECT source_text, translated_text
                FROM nm_translation_cache
                WHERE source_language = %s AND target_language = %s
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_lang, target_lang))
                for row in cur.fetchall():
                    cache[row[0]] = row[1]
        except Exception as e:
            print(f"[WARNING] Failed to load translation cache from DB: {e}")
        return cache

    def save_translation_cache(self, cache: Dict[str, str], source_lang: str = 'mk', target_lang: str = 'en') -> None:
        """Save translation cache entries to DB (upsert)."""
        if not cache:
            return
        try:
            sql = """
                INSERT INTO nm_translation_cache (source_text, translated_text, source_language, target_language)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_text) DO UPDATE SET
                    translated_text = EXCLUDED.translated_text,
                    updated_at = CURRENT_TIMESTAMP
            """
            with self.db.cursor() as cur:
                for source_text, translated_text in cache.items():
                    cur.execute(sql, (source_text, translated_text, source_lang, target_lang))
            self.db.commit()
        except Exception as e:
            print(f"[WARNING] Failed to save translation cache to DB: {e}")

    def get_cached_translation(self, source_text: str, source_lang: str = 'mk', target_lang: str = 'en') -> Optional[str]:
        """Get a single cached translation."""
        try:
            sql = """
                SELECT translated_text
                FROM nm_translation_cache
                WHERE source_text = %s AND source_language = %s AND target_language = %s
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_text, source_lang, target_lang))
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    def save_single_translation(self, source_text: str, translated_text: str, source_lang: str = 'mk', target_lang: str = 'en') -> None:
        """Save a single translation to cache."""
        try:
            sql = """
                INSERT INTO nm_translation_cache (source_text, translated_text, source_language, target_language)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_text) DO UPDATE SET
                    translated_text = EXCLUDED.translated_text,
                    updated_at = CURRENT_TIMESTAMP
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_text, translated_text, source_lang, target_lang))
            self.db.commit()
        except Exception as e:
            print(f"[WARNING] Failed to save translation: {e}")

    # ==================================================================
    # HTTP REQUEST LOGGING
    # ==================================================================

    def log_request(self, url: str, method: str = "GET",
                    status_code: int = None, response_bytes: int = None,
                    elapsed_ms: float = None, error: str = None) -> None:
        """Log an HTTP request to the shared http_requests table."""
        try:
            sql = """
                INSERT INTO http_requests
                (run_id, url, method, status_code, response_bytes, elapsed_ms, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (
                    self.run_id, url, method, status_code, response_bytes,
                    elapsed_ms, error
                ))
            self.db.commit()
        except Exception:
            pass

    # ==================================================================
    # CLEAR DATA (for fresh runs)
    # ==================================================================

    def clear_all_data(self) -> Dict[str, int]:
        """Delete all data for this run_id. Returns dict of table -> rows deleted."""
        tables = [
            "urls", "drug_register", "pcid_mappings", "final_output",
            "step_progress", "export_reports", "errors",
            "validation_results", "statistics"
        ]

        deleted = {}
        with self.db.cursor() as cur:
            for short_name in tables:
                table = self._table(short_name)
                try:
                    cur.execute(f"DELETE FROM {table} WHERE run_id = %s", (self.run_id,))
                    deleted[table] = cur.rowcount
                except Exception as e:
                    self._db_log(f"WARN | Could not clear {table}: {e}")
                    deleted[table] = 0

        self.db.commit()
        self._db_log(f"CLEAR | Deleted data for run_id={self.run_id}")
        return deleted
