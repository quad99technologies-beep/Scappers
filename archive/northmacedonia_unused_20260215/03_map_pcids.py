#!/usr/bin/env python3
"""
Step 3: PCID Mapping for North Macedonia

Maps drug register records to PCID codes using exact and fuzzy matching.
Generates final output in EVERSANA format.
"""

import sys
import os
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz, process

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.db import get_db
from scripts.north_macedonia.db import (
    NorthMacedoniaRepository,
    DataValidator,
    StatisticsCollector,
    apply_schema
)
from scripts.north_macedonia.config_loader import load_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"logs/NorthMacedonia/03_map_pcids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PCIDMapper:
    """PCID mapping engine for North Macedonia drug register."""

    def __init__(self, repository, config):
        """
        Initialize PCID mapper.

        Args:
            repository: NorthMacedoniaRepository instance
            config: Configuration dict
        """
        self.repo = repository
        self.config = config
        self.pcid_df = None
        self.fuzzy_threshold = float(config.get("SCRIPT_03_FUZZY_MATCH_THRESHOLD", 0.85))

    def load_pcid_mapping_file(self) -> pd.DataFrame:
        """Load PCID mapping file from Excel."""
        mapping_file = self.config.get("SCRIPT_03_PCID_MAPPING_FILE")
        
        if not mapping_file:
            raise ValueError("PCID mapping file not configured (SCRIPT_03_PCID_MAPPING_FILE)")
        
        # Try multiple possible locations
        possible_paths = [
            Path(mapping_file),
            Path("input") / mapping_file,
            Path("config") / mapping_file,
            Path.cwd() / mapping_file,
        ]
        
        for path in possible_paths:
            if path.exists():
                logger.info(f"Loading PCID mapping from: {path}")
                df = pd.read_excel(path)
                logger.info(f"Loaded {len(df)} PCID mappings")
                self.pcid_df = df
                return df
        
        raise FileNotFoundError(f"PCID mapping file not found: {mapping_file}")

    def normalize_text(self, text: str) -> str:
        """Normalize text for comparison."""
        if not text or pd.isna(text):
            return ""
        return str(text).strip().lower()

    def exact_match(self, drug: Dict) -> Optional[Tuple[str, float]]:
        """
        Try exact matching on product name + company + generic.

        Args:
            drug: Drug register record dict

        Returns:
            Tuple of (pcid, score) or None if no match
        """
        product_name = self.normalize_text(drug.get("local_product_name"))
        company = self.normalize_text(drug.get("marketing_authority_company_name"))
        generic = self.normalize_text(drug.get("generic_name"))

        if not product_name:
            return None

        # Try matching on multiple field combinations
        for _, row in self.pcid_df.iterrows():
            pcid_product = self.normalize_text(row.get("local_product_name") or row.get("product_name"))
            pcid_company = self.normalize_text(row.get("company") or row.get("manufacturer"))
            pcid_generic = self.normalize_text(row.get("generic_name"))

            # Exact match on product name
            if product_name == pcid_product:
                # Check if company and generic also match (if available)
                company_match = (not company or not pcid_company or company == pcid_company)
                generic_match = (not generic or not pcid_generic or generic == pcid_generic)

                if company_match and generic_match:
                    pcid = row.get("pcid") or row.get("PCID")
                    return (pcid, 1.0)

        return None

    def fuzzy_match(self, drug: Dict) -> Optional[Tuple[str, float]]:
        """
        Try fuzzy matching on product name and generic.

        Args:
            drug: Drug register record dict

        Returns:
            Tuple of (pcid, score) or None if no match
        """
        product_name = self.normalize_text(drug.get("local_product_name"))
        generic = self.normalize_text(drug.get("generic_name"))

        if not product_name:
            return None

        # Create search strings from PCID mapping
        pcid_products = []
        pcid_map = {}

        for idx, row in self.pcid_df.iterrows():
            pcid_product = self.normalize_text(row.get("local_product_name") or row.get("product_name"))
            pcid_generic = self.normalize_text(row.get("generic_name"))
            pcid = row.get("pcid") or row.get("PCID")

            if pcid_product:
                # Combine product + generic for better matching
                search_key = f"{pcid_product} {pcid_generic}".strip()
                pcid_products.append(search_key)
                pcid_map[search_key] = pcid

        if not pcid_products:
            return None

        # Fuzzy match on product name + generic
        search_query = f"{product_name} {generic}".strip()

        # Use rapidfuzz to find best match
        result = process.extractOne(
            search_query,
            pcid_products,
            scorer=fuzz.token_sort_ratio
        )

        if result:
            match_text, score, _ = result
            normalized_score = score / 100.0

            if normalized_score >= self.fuzzy_threshold:
                pcid = pcid_map.get(match_text)
                return (pcid, normalized_score)

        return None

    def match_pcid(self, drug: Dict) -> Tuple[Optional[str], str, float]:
        """
        Match drug to PCID using exact and fuzzy matching.

        Args:
            drug: Drug register record dict

        Returns:
            Tuple of (pcid, match_type, score)
        """
        # Try exact match first
        exact_result = self.exact_match(drug)
        if exact_result:
            pcid, score = exact_result
            return (pcid, "exact", score)

        # Try fuzzy match
        fuzzy_result = self.fuzzy_match(drug)
        if fuzzy_result:
            pcid, score = fuzzy_result
            return (pcid, "fuzzy", score)

        # No match found
        return (None, "not_found", 0.0)

    def create_final_output_record(self, drug: Dict, pcid: Optional[str]) -> Dict:
        """
        Create final output record in EVERSANA format.

        Args:
            drug: Drug register record dict
            pcid: PCID value (or None)

        Returns:
            Final output dict
        """
        # Parse prices
        public_price_str = drug.get("public_with_vat_price", "")
        pharmacy_price_str = drug.get("pharmacy_purchase_price", "")

        try:
            public_price = float(public_price_str.replace(",", "").strip()) if public_price_str else None
        except (ValueError, AttributeError):
            public_price = None

        try:
            pharmacy_price = float(pharmacy_price_str.replace(",", "").strip()) if pharmacy_price_str else None
        except (ValueError, AttributeError):
            pharmacy_price = None

        return {
            "pcid": pcid,
            "country": "NORTH MACEDONIA",
            "company": drug.get("marketing_authority_company_name"),
            "local_product_name": drug.get("local_product_name"),
            "generic_name": drug.get("generic_name"),
            "description": drug.get("local_pack_description"),
            "strength": drug.get("strength_size"),
            "dosage_form": drug.get("formulation"),
            "pack_size": drug.get("fill_size"),
            "public_price": public_price,
            "pharmacy_price": pharmacy_price,
            "currency": "MKD",
            "effective_start_date": drug.get("effective_start_date"),
            "effective_end_date": drug.get("effective_end_date"),
            "local_pack_code": drug.get("local_pack_code"),
            "atc_code": drug.get("who_atc_code"),
            "reimbursable_status": drug.get("reimbursable_status", "PARTIALLY REIMBURSABLE"),
            "reimbursable_rate": drug.get("reimbursable_rate", "80.00%"),
            "copayment_percent": drug.get("copayment_percent", "20.00%"),
            "margin_rule": drug.get("margin_rule", "650 PPP & PPI Listed"),
            "vat_percent": drug.get("vat_percent", "5"),
            "marketing_authorisation_holder": drug.get("marketing_authority_company_name"),
            "source_url": drug.get("detail_url"),
            "source_type": "drug_register",
        }


def main():
    """Main execution function."""
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("NORTH MACEDONIA - STEP 3: PCID MAPPING")
    logger.info("="*80)

    # Load configuration
    config = load_config()
    logger.info(f"Configuration loaded")

    # Get run_id from command line or use latest
    run_id = sys.argv[1] if len(sys.argv) > 1 else None

    # Initialize database
    db = get_db()
    apply_schema(db)

    if not run_id:
        run_id = NorthMacedoniaRepository.get_latest_incomplete_run(db)
        if not run_id:
            logger.error("No run_id provided and no incomplete runs found")
            sys.exit(1)
        logger.info(f"Resuming run: {run_id}")
    else:
        logger.info(f"Using run_id: {run_id}")

    # Initialize repository, validator, and statistics
    repo = NorthMacedoniaRepository(db, run_id)
    validator = DataValidator(repo)
    stats_collector = StatisticsCollector(repo)

    # Ensure run exists in ledger
    repo.ensure_run_in_ledger(mode="resume")

    # Initialize PCID mapper
    mapper = PCIDMapper(repo, config)

    try:
        # Load PCID mapping file
        logger.info("Loading PCID mapping file...")
        mapper.load_pcid_mapping_file()

        # Get all drug register records
        logger.info("Fetching drug register records...")
        drugs = repo.get_all_drug_register()
        total_drugs = len(drugs)
        logger.info(f"Found {total_drugs} drug register records to process")

        if total_drugs == 0:
            logger.warning("No drug register records found. Run Step 2 first.")
            sys.exit(1)

        # Process each drug record
        processed = 0
        exact_matches = 0
        fuzzy_matches = 0
        not_found = 0
        errors = 0

        for i, drug in enumerate(drugs, 1):
            try:
                drug_id = drug.get("id")
                product_name = drug.get("local_product_name", "Unknown")

                # Match PCID
                pcid, match_type, score = mapper.match_pcid(drug)

                # Track match types
                if match_type == "exact":
                    exact_matches += 1
                elif match_type == "fuzzy":
                    fuzzy_matches += 1
                else:
                    not_found += 1

                # Insert PCID mapping
                mapping_id = repo.insert_pcid_mapping(
                    drug_register_id=drug_id,
                    pcid=pcid,
                    match_type=match_type,
                    match_score=score,
                    product_data={
                        "local_product_name": drug.get("local_product_name"),
                        "generic_name": drug.get("generic_name"),
                        "manufacturer": drug.get("marketing_authority_company_name"),
                        "local_pack_code": drug.get("local_pack_code"),
                        "local_pack_description": drug.get("local_pack_description"),
                    }
                )

                # Validate mapping
                if config.get("ENABLE_VALIDATION", True):
                    validator.validate_pcid_mapping(mapping_id, pcid, match_type, score)

                # Create final output record
                final_data = mapper.create_final_output_record(drug, pcid)

                # Insert to final output
                output_id = repo.insert_final_output(drug_id, mapping_id, final_data)

                # Validate final output
                if config.get("ENABLE_VALIDATION", True):
                    validator.validate_final_output(output_id, final_data)

                processed += 1

                # Progress logging
                if processed % 100 == 0 or processed == total_drugs:
                    logger.info(f"Progress: {processed}/{total_drugs} ({processed/total_drugs*100:.1f}%) - "
                               f"Exact: {exact_matches}, Fuzzy: {fuzzy_matches}, Not Found: {not_found}")

            except Exception as e:
                errors += 1
                logger.error(f"Error processing drug {drug_id}: {e}")
                repo.log_error(
                    error_type="pcid_mapping",
                    error_message=str(e),
                    step_number=3,
                    step_name="pcid_mapping",
                    url=drug.get("detail_url"),
                    context={"drug_id": drug_id, "product_name": product_name}
                )

        # Collect statistics
        end_time = datetime.now()
        if config.get("ENABLE_STATISTICS", True):
            stats_collector.collect_step_statistics(
                step_number=3,
                step_name="pcid_mapping",
                start_time=start_time,
                end_time=end_time,
                items_processed=processed,
                items_failed=errors
            )

        # Log summary
        logger.info("="*80)
        logger.info("PCID MAPPING COMPLETE")
        logger.info("="*80)
        logger.info(f"Total Processed: {processed}")
        logger.info(f"Exact Matches: {exact_matches} ({exact_matches/processed*100:.1f}%)")
        logger.info(f"Fuzzy Matches: {fuzzy_matches} ({fuzzy_matches/processed*100:.1f}%)")
        logger.info(f"Not Found: {not_found} ({not_found/processed*100:.1f}%)")
        logger.info(f"Errors: {errors}")
        logger.info(f"Duration: {(end_time - start_time).total_seconds():.1f}s")
        logger.info("="*80)

        # Generate final report
        if config.get("REPORT_GENERATE_CONSOLE", True):
            logger.info("\nGenerating final report...")
            report = stats_collector.generate_final_report()
            stats_collector.print_report(report)

            # Export report to file
            if config.get("REPORT_GENERATE_JSON", True):
                report_dir = Path(config.get("REPORT_EXPORT_DIR", "reports"))
                report_path = stats_collector.export_report_to_file(report, report_dir)
                logger.info(f"Report exported to: {report_path}")

        # Mark step as complete
        repo.mark_progress(
            step_number=3,
            step_name="pcid_mapping",
            progress_key="complete",
            status="completed"
        )

        logger.info("Step 3 completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Fatal error in PCID mapping: {e}", exc_info=True)
        repo.log_error(
            error_type="fatal",
            error_message=str(e),
            step_number=3,
            step_name="pcid_mapping"
        )
        repo.mark_progress(
            step_number=3,
            step_name="pcid_mapping",
            progress_key="complete",
            status="failed",
            error_message=str(e)
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
