#!/usr/bin/env python3
"""
Tender Brazil database repository.
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime

# Add repo root to path for core imports
_repo_root = Path(__file__).resolve().parents[3]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import logging
from core.db.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class BrazilRepository(BaseRepository):
    """All database operations for Tender Brazil scraper (PostgreSQL backend)."""

    SCRAPER_NAME = "Tender_Brazil"
    TABLE_PREFIX = "br"

    def __init__(self, db, run_id: str):
        super().__init__(db, run_id)

    def insert_tender_list(self, cn_numbers: List[str]) -> None:
        table = self._table("tender_list")
        with self.db.cursor() as cur:
            for cn in cn_numbers:
                cur.execute(f"INSERT INTO {table} (run_id, cn_number) VALUES (%s, %s) ON CONFLICT DO NOTHING", (self.run_id, cn))

    def get_tender_list(self) -> List[str]:
        table = self._table("tender_list")
        with self.db.cursor() as cur:
            cur.execute(f"SELECT cn_number FROM {table} WHERE run_id = %s", (self.run_id,))
            return [row[0] for row in cur.fetchall()]

    def insert_tender_details(self, details: Dict) -> None:
        table = self._table("tender_details")
        sql = f"""
            INSERT INTO {table} (run_id, cn_number, source_tender_id, tender_title, province, authority, 
                               purchasing_unit, status, publication_date, deadline_date, currency, 
                               contract_type, legal_basis, budget_source, notice_link)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id, cn_number) DO UPDATE SET
                source_tender_id = EXCLUDED.source_tender_id,
                tender_title = EXCLUDED.tender_title,
                province = EXCLUDED.province,
                authority = EXCLUDED.authority,
                purchasing_unit = EXCLUDED.purchasing_unit,
                status = EXCLUDED.status,
                publication_date = EXCLUDED.publication_date,
                deadline_date = EXCLUDED.deadline_date,
                contract_type = EXCLUDED.contract_type,
                legal_basis = EXCLUDED.legal_basis,
                budget_source = EXCLUDED.budget_source,
                notice_link = EXCLUDED.notice_link
        """
        with self.db.cursor() as cur:
            cur.execute(sql, (self.run_id, details['cn_number'], details.get('source_tender_id'), details.get('tender_title'),
                            details.get('province'), details.get('authority'), details.get('purchasing_unit'), 
                            details.get('status'), details.get('publication_date'), details.get('deadline_date'),
                            details.get('currency', 'BRL'), details.get('contract_type'), details.get('legal_basis'),
                            details.get('budget_source'), details.get('notice_link')))

    def insert_tender_awards_bulk(self, awards: List[Dict]) -> None:
        table = self._table("tender_awards")
        sql = f"""
            INSERT INTO {table} (run_id, cn_number, item_no, lot_number, lot_title, awarded_lot_title, 
                               est_lot_value_local, ceiling_price_mg_iu, ceiling_unit_price, meat, 
                               price_eval_ratio, quality_eval_ratio, other_eval_ratio, award_date, 
                               bidder, bidder_id, bid_status, awarded_qty, awarded_unit_price, 
                               lot_award_value_local, ranking_order, price_eval, quality_eval, 
                               other_eval, basic_productive_incentive)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self.db.cursor() as cur:
            for a in awards:
                # Convert values to correct types if needed (e.g. empty string to None for REAL)
                def to_float(v):
                    if v is None or v == "": return None
                    try: return float(str(v).replace(",", "."))
                    except: return None

                cur.execute(sql, (self.run_id, a['cn_number'], a.get('item_no'), a.get('lot_number'), a.get('lot_title'),
                                a.get('awarded_lot_title'), to_float(a.get('est_lot_value_local')), 
                                to_float(a.get('ceiling_price_mg_iu')), to_float(a.get('ceiling_unit_price')),
                                a.get('meat'), to_float(a.get('price_eval_ratio')), to_float(a.get('quality_eval_ratio')),
                                to_float(a.get('other_eval_ratio')), a.get('award_date'), a.get('bidder'),
                                a.get('bidder_id'), a.get('bid_status'), to_float(a.get('awarded_qty')),
                                to_float(a.get('awarded_unit_price')), to_float(a.get('lot_award_value_local')),
                                a.get('ranking_order'), to_float(a.get('price_eval')), to_float(a.get('quality_eval')),
                                to_float(a.get('other_eval')), a.get('basic_productive_incentive')))
