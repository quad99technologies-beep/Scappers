#!/usr/bin/env python3
"""
Belarus database repository - all DB access in one place.

Provides methods for:
- Inserting/querying RCETH drug price data
- PCID mapping management
- Final output generation and retrieval (EVERSANA format)
- Sub-step progress tracking
- Run lifecycle management
"""

import logging
from typing import Dict, List, Optional, Set
from datetime import datetime

from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class BelarusRepository(BaseRepository):
    """All database operations for Belarus scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Belarus"
    TABLE_PREFIX = "by"

    _STEP_TABLE_MAP = {
        1: ("rceth_data",),
        2: ("pcid_mappings", "final_output"),
        3: ("translated_data",),  # Translation step
        4: (),  # Format for export - no DB tables, only CSV output
    }

    def __init__(self, db, run_id: str):
        """
        Initialize repository.

        Args:
            db: PostgresDB instance
            run_id: Current run ID
        """
        super().__init__(db, run_id)

    # Progress methods (is_progress_completed, get_completed_keys, mark_progress) 
    # are inherited from BaseRepository.

    # ------------------------------------------------------------------
    # RCETH Data (Step 1)
    # ------------------------------------------------------------------

    def insert_rceth_data(self, drugs: List[Dict]) -> int:
        """Bulk insert RCETH drug price data."""
        if not drugs:
            return 0

        table = self._table("rceth_data")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, inn, inn_en, trade_name, trade_name_en, manufacturer,
             manufacturer_country, dosage_form, dosage_form_en, strength,
             pack_size, local_pack_description, registration_number,
             registration_date, registration_valid_to, producer_price,
             producer_price_vat, wholesale_price, wholesale_price_vat,
             retail_price, retail_price_vat, import_price, import_price_currency,
             currency, atc_code, who_atc_code,
             pharmacotherapeutic_group, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, registration_number, trade_name, pack_size) DO UPDATE SET
                inn = EXCLUDED.inn,
                inn_en = EXCLUDED.inn_en,
                trade_name_en = EXCLUDED.trade_name_en,
                manufacturer = EXCLUDED.manufacturer,
                manufacturer_country = EXCLUDED.manufacturer_country,
                dosage_form = EXCLUDED.dosage_form,
                dosage_form_en = EXCLUDED.dosage_form_en,
                strength = EXCLUDED.strength,
                pack_size = EXCLUDED.pack_size,
                local_pack_description = EXCLUDED.local_pack_description,
                registration_date = EXCLUDED.registration_date,
                registration_valid_to = EXCLUDED.registration_valid_to,
                producer_price = EXCLUDED.producer_price,
                producer_price_vat = EXCLUDED.producer_price_vat,
                wholesale_price = EXCLUDED.wholesale_price,
                wholesale_price_vat = EXCLUDED.wholesale_price_vat,
                retail_price = EXCLUDED.retail_price,
                retail_price_vat = EXCLUDED.retail_price_vat,
                import_price = EXCLUDED.import_price,
                import_price_currency = EXCLUDED.import_price_currency,
                atc_code = EXCLUDED.atc_code,
                who_atc_code = EXCLUDED.who_atc_code,
                pharmacotherapeutic_group = EXCLUDED.pharmacotherapeutic_group,
                source_url = EXCLUDED.source_url
        """

        with self.db.cursor() as cur:
            for drug in drugs:
                cur.execute(sql, (
                    self.run_id,
                    drug.get("inn"),
                    drug.get("inn_en"),
                    drug.get("trade_name"),
                    drug.get("trade_name_en"),
                    drug.get("manufacturer"),
                    drug.get("manufacturer_country"),
                    drug.get("dosage_form"),
                    drug.get("dosage_form_en"),
                    drug.get("strength"),
                    drug.get("pack_size"),
                    drug.get("local_pack_description"),
                    drug.get("registration_number"),
                    drug.get("registration_date"),
                    drug.get("registration_valid_to"),
                    drug.get("producer_price"),
                    drug.get("producer_price_vat"),
                    drug.get("wholesale_price"),
                    drug.get("wholesale_price_vat"),
                    drug.get("retail_price"),
                    drug.get("retail_price_vat"),
                    drug.get("import_price"),
                    drug.get("import_price_currency"),
                    drug.get("currency", "BYN"),
                    drug.get("atc_code"),
                    drug.get("who_atc_code"),
                    drug.get("pharmacotherapeutic_group"),
                    drug.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d RCETH drug entries", count)
        self._db_log(f"OK | by_rceth_data inserted={count} | run_id={self.run_id}")
        return count

    def get_rceth_data_count(self) -> int:
        """Get total RCETH drug entries for this run."""
        table = self._table("rceth_data")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_rceth_data(self, run_id_override: str = None) -> List[Dict]:
        """Get all RCETH drug entries as list of dicts.

        Args:
            run_id_override: Optional run_id to query instead of self.run_id.
        """
        table = self._table("rceth_data")
        rid = run_id_override or self.run_id
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s ORDER BY id", (rid,))
            return [dict(row) for row in cur.fetchall()]

    def get_rceth_data_by_atc(self, atc_code: str) -> List[Dict]:
        """Get RCETH data by ATC code."""
        table = self._table("rceth_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND atc_code = %s",
                       (self.run_id, atc_code))
            return [dict(row) for row in cur.fetchall()]

    def get_best_rceth_run_id(self) -> Optional[str]:
        """Return run_id with the most by_rceth_data rows (for fallback when current run has none)."""
        table = self._table("rceth_data")
        with self.db.cursor() as cur:
            cur.execute(f"""
                SELECT run_id FROM {table}
                GROUP BY run_id ORDER BY COUNT(*) DESC LIMIT 1
            """)
            row = cur.fetchone()
            return (row[0] if isinstance(row, tuple) else row["run_id"]) if row else None

    # ------------------------------------------------------------------
    # PCID Mappings (Step 2)
    # ------------------------------------------------------------------

    def insert_pcid_mappings(self, mappings: List[Dict]) -> int:
        """Bulk insert PCID mappings."""
        if not mappings:
            return 0

        table = self._table("pcid_mappings")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, pcid, local_pack_code, presentation, inn, inn_en,
             trade_name, trade_name_en, manufacturer, manufacturer_country,
             atc_code, who_atc_code, retail_price, retail_price_vat,
             currency, country, region, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, pcid, trade_name, local_pack_code) DO UPDATE SET
                local_pack_code = EXCLUDED.local_pack_code,
                presentation = EXCLUDED.presentation,
                inn = EXCLUDED.inn,
                inn_en = EXCLUDED.inn_en,
                trade_name_en = EXCLUDED.trade_name_en,
                manufacturer = EXCLUDED.manufacturer,
                manufacturer_country = EXCLUDED.manufacturer_country,
                atc_code = EXCLUDED.atc_code,
                who_atc_code = EXCLUDED.who_atc_code,
                retail_price = EXCLUDED.retail_price,
                retail_price_vat = EXCLUDED.retail_price_vat
        """

        with self.db.cursor() as cur:
            for m in mappings:
                cur.execute(sql, (
                    self.run_id,
                    m.get("pcid"),
                    m.get("local_pack_code"),
                    m.get("presentation"),
                    m.get("inn"),
                    m.get("inn_en"),
                    m.get("trade_name"),
                    m.get("trade_name_en"),
                    m.get("manufacturer"),
                    m.get("manufacturer_country"),
                    m.get("atc_code"),
                    m.get("who_atc_code"),
                    m.get("retail_price"),
                    m.get("retail_price_vat"),
                    m.get("currency", "BYN"),
                    m.get("country", "BELARUS"),
                    m.get("region", "EUROPE"),
                    m.get("source", "PRICENTRIC"),
                ))
                count += 1

        logger.info("Inserted %d PCID mappings", count)
        self._db_log(f"OK | by_pcid_mappings inserted={count} | run_id={self.run_id}")
        return count

    def get_pcid_mappings_count(self) -> int:
        """Get total PCID mappings for this run."""
        table = self._table("pcid_mappings")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    def get_all_pcid_mappings(self) -> List[Dict]:
        """Get all PCID mappings as list of dicts."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_pcid_mappings_by_atc(self, atc_code: str) -> List[Dict]:
        """Get PCID mappings by ATC code."""
        table = self._table("pcid_mappings")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND atc_code = %s",
                       (self.run_id, atc_code))
            return [dict(row) for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # Final Output (EVERSANA format)
    # ------------------------------------------------------------------

    def insert_final_output(self, outputs: List[Dict]) -> int:
        """Bulk insert final output data (EVERSANA format)."""
        if not outputs:
            return 0

        table = self._table("final_output")
        count = 0

        sql = f"""
            INSERT INTO {table}
            (run_id, pcid, country, region, company, local_product_name,
             generic_name, generic_name_en, dosage_form, dosage_form_en,
             strength, pack_size, local_pack_description, producer_price,
             producer_price_vat, wholesale_price, wholesale_price_vat,
             retail_price, retail_price_vat, currency, atc_code, who_atc_code,
             pharmacotherapeutic_group, registration_number, registration_date,
             registration_valid_to, source_type, source_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, registration_number, trade_name, pack_size) DO UPDATE SET
                pcid = EXCLUDED.pcid,
                company = EXCLUDED.company,
                retail_price = EXCLUDED.retail_price,
                retail_price_vat = EXCLUDED.retail_price_vat,
                source_type = EXCLUDED.source_type
        """

        with self.db.cursor() as cur:
            for out in outputs:
                cur.execute(sql, (
                    self.run_id,
                    out.get("pcid"),
                    out.get("country", "BELARUS"),
                    out.get("region", "EUROPE"),
                    out.get("company"),
                    out.get("local_product_name"),
                    out.get("generic_name"),
                    out.get("generic_name_en"),
                    out.get("dosage_form"),
                    out.get("dosage_form_en"),
                    out.get("strength"),
                    out.get("pack_size"),
                    out.get("local_pack_description"),
                    out.get("producer_price"),
                    out.get("producer_price_vat"),
                    out.get("wholesale_price"),
                    out.get("wholesale_price_vat"),
                    out.get("retail_price"),
                    out.get("retail_price_vat"),
                    out.get("currency", "BYN"),
                    out.get("atc_code"),
                    out.get("who_atc_code"),
                    out.get("pharmacotherapeutic_group"),
                    out.get("registration_number"),
                    out.get("registration_date"),
                    out.get("registration_valid_to"),
                    out.get("source_type", "rceth"),
                    out.get("source_url"),
                ))
                count += 1

        logger.info("Inserted %d final output entries", count)
        self._db_log(f"OK | by_final_output inserted={count} | run_id={self.run_id}")
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

    def get_final_output_by_pcid(self, pcid: str) -> List[Dict]:
        """Get final output entries by PCID."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND pcid = %s", (self.run_id, pcid))
            return [dict(row) for row in cur.fetchall()]

    def get_final_output_by_atc(self, atc_code: str) -> List[Dict]:
        """Get final output entries by ATC code."""
        table = self._table("final_output")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s AND atc_code = %s", (self.run_id, atc_code))
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
    # Input tables (by_input_dictionary for translation)
    # ------------------------------------------------------------------

    def get_translation_dictionary_rows(self) -> List[tuple]:
        """
        Load translation dictionary from by_input_dictionary table.
        Returns list of (source_term, translated_term) for RU->EN lookup.
        """
        try:
            sql = """
                SELECT source_term, translated_term
                FROM by_input_dictionary
                WHERE source_term IS NOT NULL AND source_term != ''
                  AND translated_term IS NOT NULL AND translated_term != ''
            """
            with self.db.cursor() as cur:
                cur.execute(sql)
                return [(row[0], row[1]) for row in cur.fetchall()]
        except Exception as e:
            logger.warning("by_input_dictionary not available: %s", e)
            return []

    # ------------------------------------------------------------------
    # Translated Data (Step 3)
    # ------------------------------------------------------------------

    def insert_translated_data(self, translated: Dict) -> None:
        """Insert or update translated data entry."""
        table = self._table("translated_data")
        sql = f"""
            INSERT INTO {table}
            (run_id, rceth_data_id, inn_ru, trade_name_ru, dosage_form_ru,
             manufacturer_ru, manufacturer_country_ru, pharmacotherapeutic_group_ru,
             inn_en, trade_name_en, dosage_form_en,
             manufacturer_en, manufacturer_country_en, pharmacotherapeutic_group_en,
             translation_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, rceth_data_id) DO UPDATE SET
                inn_en = EXCLUDED.inn_en,
                trade_name_en = EXCLUDED.trade_name_en,
                dosage_form_en = EXCLUDED.dosage_form_en,
                manufacturer_en = EXCLUDED.manufacturer_en,
                manufacturer_country_en = EXCLUDED.manufacturer_country_en,
                pharmacotherapeutic_group_en = EXCLUDED.pharmacotherapeutic_group_en,
                translation_method = EXCLUDED.translation_method,
                translated_at = CURRENT_TIMESTAMP
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (
                self.run_id,
                translated.get("rceth_data_id"),
                translated.get("inn_ru"),
                translated.get("trade_name_ru"),
                translated.get("dosage_form_ru"),
                translated.get("manufacturer_ru"),
                translated.get("manufacturer_country_ru"),
                translated.get("pharmacotherapeutic_group_ru"),
                translated.get("inn_en"),
                translated.get("trade_name_en"),
                translated.get("dosage_form_en"),
                translated.get("manufacturer_en"),
                translated.get("manufacturer_country_en"),
                translated.get("pharmacotherapeutic_group_en"),
                translated.get("translation_method", "none"),
            ))

    def get_translated_data(self) -> List[Dict]:
        """Get all translated data entries as list of dicts."""
        table = self._table("translated_data")
        with self.db.cursor(dict_cursor=True) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE run_id = %s ORDER BY id", (self.run_id,))
            return [dict(row) for row in cur.fetchall()]

    def get_translated_data_count(self) -> int:
        """Get total translated data entries for this run."""
        table = self._table("translated_data")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE run_id = %s", (self.run_id,))
            row = cur.fetchone()
            return row[0] if isinstance(row, tuple) else row["count"]

    # ------------------------------------------------------------------
    # Input tables (by_input_generic_names)
    # ------------------------------------------------------------------

    def get_unique_inns(self) -> List[str]:
        """
        Get unique INNs from by_input_generic_names table.
        This table is typically populated before the run.
        """
        try:
            # We don't use self._table here because by_input_generic_names is a shared input table
            sql = "SELECT DISTINCT generic_name FROM by_input_generic_names WHERE generic_name IS NOT NULL ORDER BY generic_name"
            with self.db.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                return [row[0] if isinstance(row, tuple) else row["generic_name"] for row in rows]
        except Exception as e:
            logger.warning("by_input_generic_names table not available: %s", e)
            return []

    # ------------------------------------------------------------------
    # Translation Cache (delegates to core.translation)
    # ------------------------------------------------------------------
    # Note: These methods now delegate to core.translation.TranslationCache
    # for unified caching across all scrapers.
    
    def _get_translation_cache(self):
        """Lazy initialization of unified translation cache."""
        if not hasattr(self, '_translation_cache'):
            import sys
            from pathlib import Path
            repo_root = Path(__file__).resolve().parents[3]
            if str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))
            from core.translation import get_cache
            self._translation_cache = get_cache("belarus")
        return self._translation_cache

    def get_translation_cache(self, source_lang: str = 'ru', target_lang: str = 'en') -> Dict[str, str]:
        """Load all translation cache entries from DB.
        
        DEPRECATED: Use get_cached_translation() for individual lookups.
        """
        cache = {}
        try:
            sql = """
                SELECT source_text, translated_text
                FROM by_translation_cache
                WHERE source_language = %s AND target_language = %s
            """
            with self.db.cursor() as cur:
                cur.execute(sql, (source_lang, target_lang))
                for row in cur.fetchall():
                    cache[row[0]] = row[1]
        except Exception as e:
            print(f"[WARNING] Failed to load translation cache from DB: {e}")
        return cache

    def save_translation_cache(self, cache: Dict[str, str], source_lang: str = 'ru', target_lang: str = 'en') -> None:
        """Save translation cache entries to DB (upsert).
        
        DEPRECATED: Use save_single_translation() or unified cache.
        """
        if not cache:
            return
        tcache = self._get_translation_cache()
        count = 0
        for source_text, translated_text in cache.items():
            if tcache.set(source_text, translated_text, source_lang, target_lang):
                count += 1
        print(f"[OK] Saved {count}/{len(cache)} translations to cache")

    def get_cached_translation(self, source_text: str, source_lang: str = 'ru', target_lang: str = 'en') -> Optional[str]:
        """Get a single cached translation using unified cache."""
        return self._get_translation_cache().get(source_text, source_lang, target_lang)

    def save_single_translation(self, source_text: str, translated_text: str, source_lang: str = 'ru', target_lang: str = 'en') -> None:
        """Save a single translation to cache using unified cache."""
        self._get_translation_cache().set(source_text, translated_text, source_lang, target_lang)

    # ------------------------------------------------------------------
    # Stats / reporting helpers
    # ------------------------------------------------------------------

    def get_run_stats(self) -> Dict:
        """Get comprehensive stats for this run."""
        return {
            "rceth_data": self.get_rceth_data_count(),
            "pcid_mappings": self.get_pcid_mappings_count(),
            "translated_data": self.get_translated_data_count(),
            "final_output": self.get_final_output_count(),
        }
