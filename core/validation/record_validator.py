"""
Standardized passive record validator.
Does NOT block scraping, but tags records for downstream quality checks.
Replaces 700+ lines of duplicate validation logic across repositories.
"""

import logging
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class RecordValidator:
    """Passive validation layer for scraped drug records."""

    def __init__(self):
        self._stats = {
            "total_validated": 0,
            "pass_count": 0,
            "fail_count": 0,
            "violations": {}
        }

    def validate_record(self, record: Dict[str, Any], country: str = "Generic") -> bool:
        """
        Validate a single record.
        Returns True if valid, False if it has critical violations.
        Populates record['validation_flags'] and record['validation_errors'].
        """
        self._stats["total_validated"] += 1
        errors = []
        flags = []

        # 1. Price validation
        price = record.get("price_ars") or record.get("retail_price") or record.get("producer_price")
        if price is None:
            flags.append("MISSING_PRICE")
        elif not isinstance(price, (int, float)) or price < 0:
            errors.append(f"INVALID_PRICE: {price}")

        # 2. Name validation
        pname = record.get("product_name") or record.get("trade_name") or record.get("local_product_name")
        if not pname:
            errors.append("MISSING_NAME")
        elif len(str(pname)) < 2:
            flags.append("SHORT_NAME")

        # 3. Company validation
        company = record.get("company") or record.get("manufacturer")
        if not company:
            flags.append("MISSING_COMPANY")

        # 4. Critical checks
        is_valid = len(errors) == 0

        # Update record
        record["validation_flags"] = flags
        record["validation_errors"] = errors
        record["is_valid"] = is_valid

        # Update stats
        if is_valid:
            self._stats["pass_count"] += 1
        else:
            self._stats["fail_count"] += 1
            for err in errors:
                err_type = err.split(":")[0]
                self._stats["violations"][err_type] = self._stats["violations"].get(err_type, 0) + 1

        return is_valid

    def get_stats(self) -> Dict[str, Any]:
        """Return cumulative validation stats."""
        return self._stats

    def reset_stats(self):
        """Reset internal counters."""
        self._stats = {
            "total_validated": 0,
            "pass_count": 0,
            "fail_count": 0,
            "violations": {}
        }
