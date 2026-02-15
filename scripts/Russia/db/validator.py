#!/usr/bin/env python3
"""
Data validation module for Russia scraper.
Validates scraped data against business rules and quality standards.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates scraped data and logs results to database."""

    def __init__(self, repository):
        """
        Initialize validator.

        Args:
            repository: RussiaRepository instance
        """
        self.repo = repository

    def validate_ved_product(self, record: Dict, record_id: int) -> Tuple[bool, List[str]]:
        """
        Validate a VED product record.

        Args:
            record: VED product data dict
            record_id: Database record ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        is_valid = True
        errors = []

        # Required fields validation
        required_fields = {
            "tn": "Trade Name",
            "inn": "INN (Generic Name)",
            "manufacturer_country": "Manufacturer Country",
        }

        for field_key, display_name in required_fields.items():
            value = record.get(field_key, "").strip()
            if not value:
                is_valid = False
                errors.append(f"Missing required field: {display_name}")
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="ru_ved_products",
                    record_id=record_id,
                    field_name=field_key,
                    validation_rule="Field must not be empty",
                    status="fail",
                    message=f"Required field '{display_name}' is empty",
                    severity="critical"
                )
            else:
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="ru_ved_products",
                    record_id=record_id,
                    field_name=field_key,
                    validation_rule="Field must not be empty",
                    status="pass",
                    message=f"Required field '{display_name}' is present",
                    severity="info"
                )

        # EAN validation
        ean = record.get("ean", "").strip()
        if ean:
            # EAN should be 8-14 digits
            if not re.match(r'^\d{8,14}$', ean):
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="ru_ved_products",
                    record_id=record_id,
                    field_name="ean",
                    validation_rule="EAN should be 8-14 digits",
                    status="warning",
                    message=f"EAN '{ean}' does not match expected format",
                    severity="medium"
                )
        else:
            self.repo.insert_validation_result(
                validation_type="required_field",
                table_name="ru_ved_products",
                record_id=record_id,
                field_name="ean",
                validation_rule="EAN should be present",
                status="warning",
                message="EAN is missing",
                severity="medium"
            )

        # Price validation
        price = record.get("registered_price_rub", "").strip()
        if price:
            # Check if price is numeric
            price_clean = re.sub(r'[^\d.]', '', price)
            try:
                price_val = float(price_clean)
                if price_val <= 0:
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="ru_ved_products",
                        record_id=record_id,
                        field_name="registered_price_rub",
                        validation_rule="Price must be greater than 0",
                        status="fail",
                        message=f"Price {price_val} is not positive",
                        severity="high"
                    )
                    is_valid = False
                    errors.append(f"Invalid price: {price}")
                elif price_val > 10000000:  # 10M RUB seems unreasonable
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="ru_ved_products",
                        record_id=record_id,
                        field_name="registered_price_rub",
                        validation_rule="Price should be reasonable",
                        status="warning",
                        message=f"Price {price_val} seems unusually high",
                        severity="medium"
                    )
                else:
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="ru_ved_products",
                        record_id=record_id,
                        field_name="registered_price_rub",
                        validation_rule="Price must be positive and reasonable",
                        status="pass",
                        message=f"Price {price_val} is valid",
                        severity="info"
                    )
            except ValueError:
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="ru_ved_products",
                    record_id=record_id,
                    field_name="registered_price_rub",
                    validation_rule="Price must be numeric",
                    status="fail",
                    message=f"Price '{price}' is not numeric",
                    severity="high"
                )
                is_valid = False
                errors.append(f"Non-numeric price: {price}")

        # Date validation
        start_date = record.get("start_date_text", "").strip()
        if start_date:
            if not self._is_valid_date(start_date):
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="ru_ved_products",
                    record_id=record_id,
                    field_name="start_date_text",
                    validation_rule="Date must be in valid format",
                    status="warning",
                    message=f"Start date '{start_date}' may not be in standard format",
                    severity="low"
                )

        # Trade name length validation
        tn = record.get("tn", "").strip()
        if tn and len(tn) < 2:
            self.repo.insert_validation_result(
                validation_type="format",
                table_name="ru_ved_products",
                record_id=record_id,
                field_name="tn",
                validation_rule="Trade name should be at least 2 characters",
                status="warning",
                message=f"Trade name '{tn}' seems too short",
                severity="medium"
            )

        return is_valid, errors

    def validate_excluded_product(self, record: Dict, record_id: int) -> Tuple[bool, List[str]]:
        """
        Validate an excluded product record.

        Args:
            record: Excluded product data dict
            record_id: Database record ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        is_valid = True
        errors = []

        # Required fields validation
        required_fields = {
            "tn": "Trade Name",
            "inn": "INN (Generic Name)",
        }

        for field_key, display_name in required_fields.items():
            value = record.get(field_key, "").strip()
            if not value:
                is_valid = False
                errors.append(f"Missing required field: {display_name}")
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="ru_excluded_products",
                    record_id=record_id,
                    field_name=field_key,
                    validation_rule="Field must not be empty",
                    status="fail",
                    message=f"Required field '{display_name}' is empty",
                    severity="critical"
                )

        return is_valid, errors

    def validate_translated_product(self, record_id: int, pcid: str, match_type: str,
                                   match_score: float) -> bool:
        """
        Validate PCID mapping result.

        Args:
            record_id: Database record ID
            pcid: PCID value
            match_type: Match type
            match_score: Match score

        Returns:
            True if valid, False otherwise
        """
        is_valid = True

        # Check if PCID was found
        if not pcid or match_type == 'not_found':
            self.repo.insert_validation_result(
                validation_type="pcid_mapping",
                table_name="ru_translated_products",
                record_id=record_id,
                field_name="pcid",
                validation_rule="PCID should be found",
                status="warning",
                message="PCID not found for this product",
                severity="high"
            )
            is_valid = False
        else:
            # Check match score for fuzzy matches
            if match_type == 'fuzzy' and match_score < 0.8:
                self.repo.insert_validation_result(
                    validation_type="pcid_mapping",
                    table_name="ru_translated_products",
                    record_id=record_id,
                    field_name="match_score",
                    validation_rule="Fuzzy match score should be >= 0.8",
                    status="warning",
                    message=f"Low match score: {match_score:.2f}",
                    severity="medium"
                )
            else:
                self.repo.insert_validation_result(
                    validation_type="pcid_mapping",
                    table_name="ru_translated_products",
                    record_id=record_id,
                    field_name="pcid",
                    validation_rule="PCID mapping should be valid",
                    status="pass",
                    message=f"PCID mapped successfully ({match_type}, score: {match_score:.2f})",
                    severity="info"
                )

        return is_valid

    def validate_export_ready(self, output_id: int, data: Dict) -> Tuple[bool, List[str]]:
        """
        Validate export-ready record.

        Args:
            output_id: Database record ID
            data: Export-ready data dict

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        is_valid = True
        errors = []

        # Required EVERSANA fields
        required_fields = {
            "country": "Country",
            "local_product_name": "Local Product Name",
            "generic_name": "Generic Name",
            "company": "Company",
        }

        for field_key, display_name in required_fields.items():
            value = data.get(field_key, "")
            if not value or str(value).strip() == "":
                is_valid = False
                errors.append(f"Missing required EVERSANA field: {display_name}")
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="ru_export_ready",
                    record_id=output_id,
                    field_name=field_key,
                    validation_rule="EVERSANA required field",
                    status="fail",
                    message=f"Required field '{display_name}' is empty",
                    severity="critical"
                )
            else:
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="ru_export_ready",
                    record_id=output_id,
                    field_name=field_key,
                    validation_rule="EVERSANA required field",
                    status="pass",
                    message=f"Required field '{display_name}' is present",
                    severity="info"
                )

        # PCID validation
        pcid = data.get("pcid")
        if not pcid:
            self.repo.insert_validation_result(
                validation_type="required_field",
                table_name="ru_export_ready",
                record_id=output_id,
                field_name="pcid",
                validation_rule="PCID should be present",
                status="warning",
                message="PCID is missing in export-ready output",
                severity="high"
            )

        return is_valid, errors

    def _is_valid_date(self, date_str: str) -> bool:
        """Check if string is a valid date."""
        # Try common date formats (Russia uses DD.MM.YYYY and DD/MM/YYYY)
        formats = [
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%Y/%m/%d",
        ]

        for fmt in formats:
            try:
                datetime.strptime(date_str, fmt)
                return True
            except ValueError:
                continue

        return False

    def get_validation_report(self) -> Dict:
        """
        Generate comprehensive validation report.

        Returns:
            Dict with validation statistics and details
        """
        summary = self.repo.get_validation_summary()
        failures = self.repo.get_validation_failures()
        critical_failures = self.repo.get_validation_failures(severity="critical")

        return {
            "summary": summary,
            "all_failures": failures,
            "critical_failures": critical_failures,
            "validation_rate": (
                summary.get("passed", 0) / summary.get("total_validations", 1) * 100
                if summary.get("total_validations", 0) > 0 else 0
            ),
        }
