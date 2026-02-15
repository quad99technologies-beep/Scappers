#!/usr/bin/env python3
"""
Data validation module for North Macedonia scraper.
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
            repository: NorthMacedoniaRepository instance
        """
        self.repo = repository

    def validate_drug_register_record(self, record: Dict, record_id: int) -> Tuple[bool, List[str]]:
        """
        Validate a drug register record.

        Args:
            record: Drug register data dict
            record_id: Database record ID

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        is_valid = True
        errors = []

        # Required fields validation
        required_fields = {
            "Local Product Name": "local_product_name",
            "Generic Name": "generic_name",
            "Marketing Authority / Company Name": "marketing_authority_company_name",
        }

        for display_name, field_key in required_fields.items():
            value = record.get(display_name, "").strip()
            if not value:
                is_valid = False
                errors.append(f"Missing required field: {display_name}")
                self.repo.insert_validation_result(
                    validation_type="required_field",
                    table_name="nm_drug_register",
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
                    table_name="nm_drug_register",
                    record_id=record_id,
                    field_name=field_key,
                    validation_rule="Field must not be empty",
                    status="pass",
                    message=f"Required field '{display_name}' is present",
                    severity="info"
                )

        # ATC code format validation
        atc_code = record.get("WHO ATC Code", "").strip()
        if atc_code:
            # ATC codes should be alphanumeric, typically 7 characters (e.g., A10BA02)
            if not re.match(r'^[A-Z][0-9]{2}[A-Z]{2}[0-9]{2}$', atc_code):
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="nm_drug_register",
                    record_id=record_id,
                    field_name="who_atc_code",
                    validation_rule="ATC code should match pattern: A10BA02",
                    status="warning",
                    message=f"ATC code '{atc_code}' may not follow standard format",
                    severity="low"
                )
        else:
            self.repo.insert_validation_result(
                validation_type="required_field",
                table_name="nm_drug_register",
                record_id=record_id,
                field_name="who_atc_code",
                validation_rule="ATC code should be present",
                status="warning",
                message="ATC code is missing",
                severity="medium"
            )

        # Price validation
        public_price = record.get("Public with VAT Price", "").strip()
        pharmacy_price = record.get("Pharmacy Purchase Price", "").strip()

        if public_price:
            # Check if price is numeric
            price_clean = re.sub(r'[^\d.]', '', public_price)
            try:
                price_val = float(price_clean)
                if price_val <= 0:
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="nm_drug_register",
                        record_id=record_id,
                        field_name="public_with_vat_price",
                        validation_rule="Price must be greater than 0",
                        status="fail",
                        message=f"Public price {price_val} is not positive",
                        severity="high"
                    )
                    is_valid = False
                    errors.append(f"Invalid public price: {public_price}")
                elif price_val > 1000000:
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="nm_drug_register",
                        record_id=record_id,
                        field_name="public_with_vat_price",
                        validation_rule="Price should be reasonable",
                        status="warning",
                        message=f"Public price {price_val} seems unusually high",
                        severity="medium"
                    )
                else:
                    self.repo.insert_validation_result(
                        validation_type="range",
                        table_name="nm_drug_register",
                        record_id=record_id,
                        field_name="public_with_vat_price",
                        validation_rule="Price must be positive and reasonable",
                        status="pass",
                        message=f"Public price {price_val} is valid",
                        severity="info"
                    )
            except ValueError:
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="nm_drug_register",
                    record_id=record_id,
                    field_name="public_with_vat_price",
                    validation_rule="Price must be numeric",
                    status="fail",
                    message=f"Public price '{public_price}' is not numeric",
                    severity="high"
                )
                is_valid = False
                errors.append(f"Non-numeric public price: {public_price}")

        # Date validation
        start_date = record.get("Effective Start Date", "").strip()
        end_date = record.get("Effective End Date", "").strip()

        if start_date:
            if not self._is_valid_date(start_date):
                self.repo.insert_validation_result(
                    validation_type="format",
                    table_name="nm_drug_register",
                    record_id=record_id,
                    field_name="effective_start_date",
                    validation_rule="Date must be in valid format",
                    status="warning",
                    message=f"Start date '{start_date}' may not be in standard format",
                    severity="low"
                )

        # Product name length validation
        product_name = record.get("Local Product Name", "").strip()
        if product_name and len(product_name) < 3:
            self.repo.insert_validation_result(
                validation_type="format",
                table_name="nm_drug_register",
                record_id=record_id,
                field_name="local_product_name",
                validation_rule="Product name should be at least 3 characters",
                status="warning",
                message=f"Product name '{product_name}' seems too short",
                severity="medium"
            )

        return is_valid, errors

    def validate_pcid_mapping(self, mapping_id: int, pcid: str, match_type: str,
                             match_score: float) -> bool:
        """
        Validate PCID mapping result.

        Args:
            mapping_id: Database record ID
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
                table_name="nm_pcid_mappings",
                record_id=mapping_id,
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
                    table_name="nm_pcid_mappings",
                    record_id=mapping_id,
                    field_name="match_score",
                    validation_rule="Fuzzy match score should be >= 0.8",
                    status="warning",
                    message=f"Low match score: {match_score:.2f}",
                    severity="medium"
                )
            else:
                self.repo.insert_validation_result(
                    validation_type="pcid_mapping",
                    table_name="nm_pcid_mappings",
                    record_id=mapping_id,
                    field_name="pcid",
                    validation_rule="PCID mapping should be valid",
                    status="pass",
                    message=f"PCID mapped successfully ({match_type}, score: {match_score:.2f})",
                    severity="info"
                )

        return is_valid

    def validate_final_output(self, output_id: int, data: Dict) -> Tuple[bool, List[str]]:
        """
        Validate final output record.

        Args:
            output_id: Database record ID
            data: Final output data dict

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
                    table_name="nm_final_output",
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
                    table_name="nm_final_output",
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
                table_name="nm_final_output",
                record_id=output_id,
                field_name="pcid",
                validation_rule="PCID should be present",
                status="warning",
                message="PCID is missing in final output",
                severity="high"
            )

        return is_valid, errors

    def _is_valid_date(self, date_str: str) -> bool:
        """Check if string is a valid date."""
        # Try common date formats
        formats = [
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d/%m/%Y",
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
