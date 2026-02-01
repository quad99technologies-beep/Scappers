#!/usr/bin/env python3
"""
Argentina database repository (PostgreSQL backend).

Centralises all DB access for the Argentina scraper so the pipeline can move
away from CSV inputs/progress files. Mirrors the Malaysia repository pattern.
"""

from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence

import logging
import hashlib

logger = logging.getLogger(__name__)


class ArgentinaRepository:
    """All Argentina-specific DB operations."""

    def __init__(self, db, run_id: str):
        self.db = db
        self.run_id = run_id

    def _db_log(self, message: str) -> None:
        """Emit a [DB] activity log line for GUI activity panel."""
        try:
            print(f"[DB] {message}", flush=True)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Run lifecycle                                                      #
    # ------------------------------------------------------------------ #
    def start_run(self, mode: str = "fresh") -> None:
        from core.db.models import run_ledger_start

        sql, params = run_ledger_start(self.run_id, "Argentina", mode=mode)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"OK | run_ledger start | run_id={self.run_id} mode={mode}")

    def finish_run(
        self,
        status: str,
        items_scraped: int = 0,
        items_exported: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        from core.db.models import run_ledger_finish

        sql, params = run_ledger_finish(
            self.run_id,
            status,
            items_scraped=items_scraped,
            items_exported=items_exported,
            error_message=error_message,
        )
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"FINISH | run_ledger updated | status={status} items={items_scraped}")

    def resume_run(self) -> None:
        from core.db.models import run_ledger_resume

        sql, params = run_ledger_resume(self.run_id)
        with self.db.cursor() as cur:
            cur.execute(sql, params)
        self._db_log(f"RESUME | run_ledger updated | run_id={self.run_id}")

    # ------------------------------------------------------------------ #
    # Product index / queue (prepared URLs replacement)                  #
    # ------------------------------------------------------------------ #
    def upsert_product_index(self, rows: Sequence[Dict]) -> int:
        """
        Bulk upsert product/company pairs into ar_product_index.
        Each row dict must have keys: product, company, url (optional).
        """
        if not rows:
            return 0
        inserted = 0
        sql = """
            INSERT INTO ar_product_index
            (run_id, product, company, url, loop_count, total_records, status)
            VALUES (%s, %s, %s, %s, COALESCE(%s,0), COALESCE(%s,0), %s)
            ON CONFLICT (run_id, company, product) DO UPDATE SET
                url = EXCLUDED.url,
                loop_count = EXCLUDED.loop_count,
                total_records = EXCLUDED.total_records,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        self.run_id,
                        r.get("product", ""),
                        r.get("company", ""),
                        r.get("url"),
                        r.get("loop_count", 0),
                        r.get("total_records", 0),
                        r.get("status", "pending"),
                    ),
                )
                inserted += 1
        self._db_log(f"OK | ar_product_index upserted={inserted} | run_id={self.run_id}")
        return inserted

    def set_urls(self, rows: Sequence[Dict]) -> int:
        """
        Update URLs for products in product_index (keeps loop counters).
        rows: [{product, company, url}]
        """
        if not rows:
            return 0
        sql = """
            UPDATE ar_product_index
               SET url = %s,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        updated = 0
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(sql, (r.get("url"), self.run_id, r.get("company"), r.get("product")))
                updated += cur.rowcount
        self._db_log(f"OK | ar_product_index urls_updated={updated} | run_id={self.run_id}")
        return updated

    def get_all_product_index(self) -> List[Dict]:
        """Return all product index rows for this run."""
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(
                """
                SELECT id, product, company, url, loop_count, total_records, status
                  FROM ar_product_index
                 WHERE run_id = %s
                 ORDER BY id
                """,
                (self.run_id,),
            )
            return [dict(row) for row in cur.fetchall()]

    def get_product_index_count(self) -> int:
        with self.db.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_urls_prepared_count(self) -> int:
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s AND url IS NOT NULL AND url <> ''",
                (self.run_id,),
            )
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_pending_products(self, max_loop: int = 5, limit: int = 500) -> List[Dict]:
        """
        Fetch products that still need scraping (Total Records = 0, loop_count < max_loop).
        """
        sql = """
            SELECT id, product, company, url, loop_count, total_records
            FROM ar_product_index
            WHERE run_id = %s
              AND total_records = 0
              AND loop_count < %s
              AND status IN ('pending','failed','in_progress')
            ORDER BY loop_count ASC, id ASC
            LIMIT %s
        """
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(sql, (self.run_id, max_loop, limit))
            return [dict(row) for row in cur.fetchall()]

    def mark_attempt(
        self,
        product_id: int,
        loop_count: int,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ) -> None:
        sql = """
            UPDATE ar_product_index
            SET loop_count = %s,
                total_records = %s,
                status = %s,
                last_attempt_at = CURRENT_TIMESTAMP,
                last_attempt_source = COALESCE(%s, last_attempt_source),
                error_message = %s,
                scraped_by_selenium = CASE WHEN %s = 'selenium' THEN TRUE ELSE scraped_by_selenium END,
                scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = %s AND run_id = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    loop_count,
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    product_id,
                    self.run_id,
                ),
            )

    def mark_attempt_by_name(
        self,
        company: str,
        product: str,
        loop_count: int,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ) -> None:
        """Convenience when product_index id is not available."""
        sql = """
            UPDATE ar_product_index
               SET loop_count = %s,
                   total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = COALESCE(%s, last_attempt_source),
                   error_message = %s,
                   scraped_by_selenium = CASE WHEN %s = 'selenium' THEN TRUE ELSE scraped_by_selenium END,
                   scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    loop_count,
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    self.run_id,
                    company,
                    product,
                ),
            )

    def bump_attempt(
        self,
        company: str,
        product: str,
        total_records: int = 0,
        status: str = "in_progress",
        source: str = None,
        error_message: str = None,
    ):
        """Increment loop_count by 1 for company/product."""
        sql = """
            UPDATE ar_product_index
               SET loop_count = COALESCE(loop_count,0) + 1,
                   total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = COALESCE(%s, last_attempt_source),
                   error_message = %s,
                   scraped_by_selenium = CASE WHEN %s = 'selenium' THEN TRUE ELSE scraped_by_selenium END,
                   scraped_by_api = CASE WHEN %s = 'api' THEN TRUE ELSE scraped_by_api END,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    total_records,
                    status,
                    source,
                    error_message,
                    source,
                    source,
                    self.run_id,
                    company,
                    product,
                ),
            )

    def mark_api_result(
        self,
        company: str,
        product: str,
        total_records: int,
        status: str = "completed",
        error_message: str = None,
    ) -> None:
        """Update product_index for API results without altering loop_count."""
        sql = """
            UPDATE ar_product_index
               SET total_records = %s,
                   status = %s,
                   last_attempt_at = CURRENT_TIMESTAMP,
                   last_attempt_source = 'api',
                   error_message = %s,
                   scraped_by_api = TRUE,
                   updated_at = CURRENT_TIMESTAMP
             WHERE run_id = %s AND company = %s AND product = %s
        """
        with self.db.cursor() as cur:
            cur.execute(
                sql,
                (
                    total_records,
                    status,
                    error_message,
                    self.run_id,
                    company,
                    product,
                ),
            )

    # ------------------------------------------------------------------ #
    # Scraped product rows                                              #
    # ------------------------------------------------------------------ #
    def insert_products(self, rows: Sequence[Dict], source: str = "selenium") -> int:
        """
        Insert scraped rows into ar_products.
        Expected keys: input_company, input_product_name, company, product_name,
                       active_ingredient, therapeutic_class, description,
                       price_ars/price_raw, date, sifar_detail, pami_af, pami_os,
                       ioma_detail, ioma_af, ioma_os, import_status, coverage_json
        """
        if not rows:
            return 0

        def _val(row: Dict, *keys: str):
            """Fetch value from row using multiple possible keys (case variants)."""
            for k in keys:
                if k in row:
                    return row.get(k)
            # try common upper/lower variants
            for k in keys:
                if k.lower() in row:
                    return row.get(k.lower())
                if k.upper() in row:
                    return row.get(k.upper())
            return None

        def _hash_for_row(row: Dict) -> str:
            parts = [
                str(source or ""),
                str(_val(row, "input_company", "Company") or ""),
                str(_val(row, "input_product_name", "Product", "product") or ""),
                str(_val(row, "company", "Company") or ""),
                str(_val(row, "product_name", "Product Name", "product_name") or ""),
                str(_val(row, "active_ingredient", "Active Ingredient", "active") or ""),
                str(_val(row, "therapeutic_class", "Therapeutic Class", "therapeutic") or ""),
                str(_val(row, "description", "Description") or ""),
                str(_val(row, "price_raw", "price_ars_raw", "price") or ""),
                str(_val(row, "date", "Date") or ""),
                str(_val(row, "sifar_detail", "SIFAR_detail") or ""),
                str(_val(row, "pami_af", "PAMI_AF") or ""),
                str(_val(row, "pami_os", "PAMI_OS") or ""),
                str(_val(row, "ioma_detail", "IOMA_detail") or ""),
                str(_val(row, "ioma_af", "IOMA_AF") or ""),
                str(_val(row, "ioma_os", "IOMA_OS") or ""),
                str(_val(row, "import_status", "Import_Status", "import") or ""),
                str(_val(row, "coverage_json", "coverage") or ""),
            ]
            stable = "|".join(p.replace("\r", " ").replace("\n", " ").strip() for p in parts)
            return hashlib.sha1(stable.encode("utf-8", errors="ignore")).hexdigest()

        sql = """
            INSERT INTO ar_products
            (run_id, record_hash, input_company, input_product_name, company, product_name,
             active_ingredient, therapeutic_class, description, price_ars, price_raw,
             date, scraped_at, sifar_detail, pami_af, pami_os, ioma_detail,
             ioma_af, ioma_os, import_status, coverage_json, source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id, record_hash) DO UPDATE SET
                input_company = EXCLUDED.input_company,
                input_product_name = EXCLUDED.input_product_name,
                company = EXCLUDED.company,
                product_name = EXCLUDED.product_name,
                active_ingredient = EXCLUDED.active_ingredient,
                therapeutic_class = EXCLUDED.therapeutic_class,
                description = EXCLUDED.description,
                price_ars = EXCLUDED.price_ars,
                price_raw = EXCLUDED.price_raw,
                date = EXCLUDED.date,
                sifar_detail = EXCLUDED.sifar_detail,
                pami_af = EXCLUDED.pami_af,
                pami_os = EXCLUDED.pami_os,
                ioma_detail = EXCLUDED.ioma_detail,
                ioma_af = EXCLUDED.ioma_af,
                ioma_os = EXCLUDED.ioma_os,
                import_status = EXCLUDED.import_status,
                coverage_json = EXCLUDED.coverage_json,
                source = EXCLUDED.source,
                scraped_at = CURRENT_TIMESTAMP
        """
        inserted = 0
        with self.db.cursor() as cur:
            for r in rows:
                price_ars = _val(r, "price_ars", "price_ARS", "Price_ARS", "price")
                # allow raw string for price to store separately
                price_raw = _val(r, "price_raw", "price_ars_raw", "price")
                try:
                    price_ars = float(price_ars) if price_ars not in (None, "", "nan") else None
                except Exception:
                    price_ars = None
                    if price_raw is None:
                        price_raw = _val(r, "price_ars")
                cur.execute(
                    sql,
                    (
                        self.run_id,
                        _hash_for_row(r),
                        _val(r, "input_company", "Company"),
                        _val(r, "input_product_name", "Product", "product"),
                        _val(r, "company", "Company"),
                        _val(r, "product_name", "Product Name", "product_name"),
                        _val(r, "active_ingredient", "Active Ingredient", "active"),
                        _val(r, "therapeutic_class", "Therapeutic Class", "therapeutic"),
                        _val(r, "description", "Description"),
                        price_ars,
                        price_raw,
                        _val(r, "date", "Date"),
                        _val(r, "sifar_detail", "SIFAR_detail"),
                        _val(r, "pami_af", "PAMI_AF"),
                        _val(r, "pami_os", "PAMI_OS"),
                        _val(r, "ioma_detail", "IOMA_detail"),
                        _val(r, "ioma_af", "IOMA_AF"),
                        _val(r, "ioma_os", "IOMA_OS"),
                        _val(r, "import_status", "Import_Status", "import"),
                        _val(r, "coverage_json", "coverage"),
                        source,
                    ),
                )
                inserted += 1
        logger.info("Inserted/updated %d product rows (source=%s)", inserted, source)
        self._db_log(f"OK | ar_products upserted={inserted} source={source} | run_id={self.run_id}")
        return inserted

    def log_error(self, company: str, product: str, message: str) -> None:
        sql = """
            INSERT INTO ar_errors (run_id, input_company, input_product_name, error_message)
            VALUES (%s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, company, product, message))

    # ------------------------------------------------------------------ #
    # Translation (ar_products_translated)                               #
    # ------------------------------------------------------------------ #
    def clear_translated(self) -> None:
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM ar_products_translated WHERE run_id = %s", (self.run_id,))
        self._db_log(f"RESET | ar_products_translated cleared | run_id={self.run_id}")

    def insert_translated(self, rows: Sequence[Dict]) -> int:
        """Insert translated rows into ar_products_translated."""
        if not rows:
            return 0
        sql = """
            INSERT INTO ar_products_translated
            (run_id, product_id, company, product_name, active_ingredient,
             therapeutic_class, description, price_ars, date,
             sifar_detail, pami_af, pami_os, ioma_detail, ioma_af, ioma_os,
             import_status, coverage_json, translation_source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id, product_id) DO UPDATE SET
                company = EXCLUDED.company,
                product_name = EXCLUDED.product_name,
                active_ingredient = EXCLUDED.active_ingredient,
                therapeutic_class = EXCLUDED.therapeutic_class,
                description = EXCLUDED.description,
                price_ars = EXCLUDED.price_ars,
                date = EXCLUDED.date,
                sifar_detail = EXCLUDED.sifar_detail,
                pami_af = EXCLUDED.pami_af,
                pami_os = EXCLUDED.pami_os,
                ioma_detail = EXCLUDED.ioma_detail,
                ioma_af = EXCLUDED.ioma_af,
                ioma_os = EXCLUDED.ioma_os,
                import_status = EXCLUDED.import_status,
                coverage_json = EXCLUDED.coverage_json,
                translation_source = EXCLUDED.translation_source,
                translated_at = CURRENT_TIMESTAMP
        """
        inserted = 0
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        self.run_id,
                        r.get("product_id"),
                        r.get("company"),
                        r.get("product_name"),
                        r.get("active_ingredient"),
                        r.get("therapeutic_class"),
                        r.get("description"),
                        r.get("price_ars"),
                        r.get("date"),
                        r.get("sifar_detail"),
                        r.get("pami_af"),
                        r.get("pami_os"),
                        r.get("ioma_detail"),
                        r.get("ioma_af"),
                        r.get("ioma_os"),
                        r.get("import_status"),
                        r.get("coverage_json"),
                        r.get("translation_source"),
                    ),
                )
                inserted += 1
        self._db_log(f"OK | ar_products_translated upserted={inserted} | run_id={self.run_id}")
        return inserted

    # ------------------------------------------------------------------ #
    # Dictionary + PCID                                                 #
    # ------------------------------------------------------------------ #
    def replace_dictionary(self, entries: Iterable[Dict[str, str]]) -> int:
        """Replace ar_dictionary with provided ES->EN entries."""
        with self.db.cursor() as cur:
            cur.execute("TRUNCATE ar_dictionary")
        inserted = 0
        sql = """
            INSERT INTO ar_dictionary (es, en, source)
            VALUES (%s, %s, %s)
            ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            for e in entries:
                cur.execute(sql, (e.get("es"), e.get("en"), e.get("source", "file")))
                inserted += 1
        self._db_log(f"OK | ar_dictionary replaced={inserted}")
        return inserted

    def upsert_dictionary_entries(self, entries: Iterable[Dict[str, str]]) -> int:
        """Upsert ES->EN dictionary entries without truncating the table."""
        inserted = 0
        sql = """
            INSERT INTO ar_dictionary (es, en, source)
            VALUES (%s, %s, %s)
            ON CONFLICT (es) DO UPDATE SET en = EXCLUDED.en, source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            for e in entries:
                cur.execute(sql, (e.get("es"), e.get("en"), e.get("source", "manual")))
                inserted += 1
        self._db_log(f"OK | ar_dictionary upserted={inserted}")
        return inserted

    def replace_pcid_reference(self, rows: Iterable[Dict[str, str]]) -> int:
        """Replace PCID reference table."""
        with self.db.cursor() as cur:
            cur.execute("TRUNCATE ar_pcid_reference")
        inserted = 0
        sql = """
            INSERT INTO ar_pcid_reference
            (pcid, company, local_product_name, generic_name, local_pack_description)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (company, local_product_name, generic_name, local_pack_description)
                DO UPDATE SET pcid = EXCLUDED.pcid
        """
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        r.get("pcid") or r.get("PCID"),
                        r.get("company") or r.get("Company"),
                        r.get("local_product_name") or r.get("Local Product Name"),
                        r.get("generic_name") or r.get("Generic Name"),
                        r.get("local_pack_description") or r.get("Local Pack Description"),
                    ),
                )
                inserted += 1
        self._db_log(f"OK | ar_pcid_reference replaced={inserted}")
        return inserted

    # ------------------------------------------------------------------ #
    # Ignore list                                                       #
    # ------------------------------------------------------------------ #
    def replace_ignore_list(self, rows: Iterable[Dict[str, str]]) -> int:
        """Replace ignore list entries."""
        with self.db.cursor() as cur:
            cur.execute("TRUNCATE ar_ignore_list")
        inserted = 0
        sql = """
            INSERT INTO ar_ignore_list (company, product)
            VALUES (%s, %s)
            ON CONFLICT (company, product) DO NOTHING
        """
        with self.db.cursor() as cur:
            for r in rows:
                company = r.get("company") or r.get("Company") or ""
                product = r.get("product") or r.get("Product") or ""
                if company and product:
                    cur.execute(sql, (company, product))
                    inserted += 1
        self._db_log(f"OK | ar_ignore_list replaced={inserted}")
        return inserted

    def get_ignore_list(self) -> List[Dict]:
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute("SELECT company, product FROM ar_ignore_list")
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------ #
    # PCID mappings (export source)                                      #
    # ------------------------------------------------------------------ #
    def clear_pcid_mappings(self) -> None:
        with self.db.cursor() as cur:
            cur.execute("DELETE FROM ar_pcid_mappings WHERE run_id = %s", (self.run_id,))
        self._db_log(f"RESET | ar_pcid_mappings cleared | run_id={self.run_id}")

    def insert_pcid_mappings(self, rows: Sequence[Dict]) -> int:
        """Insert PCID mapped rows."""
        if not rows:
            return 0
        sql = """
            INSERT INTO ar_pcid_mappings
            (run_id, pcid, company, local_product_name, generic_name, local_pack_description, price_ars, source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id, company, local_product_name, generic_name, local_pack_description)
            DO UPDATE SET
                pcid = EXCLUDED.pcid,
                price_ars = EXCLUDED.price_ars,
                source = EXCLUDED.source,
                mapped_at = CURRENT_TIMESTAMP
        """
        inserted = 0
        with self.db.cursor() as cur:
            for r in rows:
                cur.execute(
                    sql,
                    (
                        self.run_id,
                        r.get("pcid"),
                        r.get("company"),
                        r.get("local_product_name"),
                        r.get("generic_name"),
                        r.get("local_pack_description"),
                        r.get("price_ars"),
                        r.get("source", "PRICENTRIC"),
                    ),
                )
                inserted += 1
        self._db_log(f"OK | ar_pcid_mappings upserted={inserted} | run_id={self.run_id}")
        return inserted

    def log_export_report(self, report_type: str, file_path: str, row_count: Optional[int]) -> None:
        sql = """
            INSERT INTO ar_export_reports (run_id, report_type, file_path, row_count)
            VALUES (%s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, report_type, file_path, row_count))
        self._db_log(f"OK | ar_export_reports type={report_type} rows={row_count}")

    def log_input_upload(
        self,
        table_name: str,
        source_file: str,
        row_count: int,
        replaced_previous: int = 0,
        uploaded_by: str = "pipeline",
    ) -> None:
        """Log input file loads (dictionary/PCID) into common input_uploads table."""
        sql = """
            INSERT INTO input_uploads
            (table_name, source_file, row_count, replaced_previous, uploaded_by)
            VALUES (%s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (table_name, source_file, row_count, replaced_previous, uploaded_by))
        self._db_log(f"OK | input_uploads table={table_name} rows={row_count}")

    def get_latest_input_upload(self, table_name: str) -> Optional[Dict]:
        sql = """
            SELECT table_name, source_file, row_count, uploaded_at, replaced_previous, uploaded_by
              FROM input_uploads
             WHERE table_name = %s
             ORDER BY uploaded_at DESC
             LIMIT 1
        """
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(sql, (table_name,))
            row = cur.fetchone()
            return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # Stats                                                             #
    # ------------------------------------------------------------------ #
    def get_stats(self) -> Dict:
        with self.db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ar_product_index WHERE run_id = %s", (self.run_id,)
            )
            product_index = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ar_products WHERE run_id = %s", (self.run_id,)
            )
            products = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ar_products_translated WHERE run_id = %s", (self.run_id,)
            )
            translated = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*) FROM ar_pcid_mappings
                 WHERE run_id = %s AND pcid IS NOT NULL AND pcid <> ''
                """,
                (self.run_id,),
            )
            mapped = cur.fetchone()[0]
            cur.execute(
                """
                SELECT COUNT(*) FROM ar_pcid_mappings
                 WHERE run_id = %s AND (pcid IS NULL OR pcid = '')
                """,
                (self.run_id,),
            )
            not_mapped = cur.fetchone()[0]
        return {
            "product_index": product_index,
            "products": products,
            "translated": translated,
            "pcid_mapped": mapped,
            "pcid_not_mapped": not_mapped,
        }
