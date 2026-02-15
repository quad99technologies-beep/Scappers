#!/usr/bin/env python3
"""
Statistics and reporting module for North Macedonia scraper.
Generates comprehensive reports and metrics.
"""

import logging
from typing import Dict, List
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class StatisticsCollector:
    """Collects and reports statistics for scraper runs."""

    def __init__(self, repository):
        """
        Initialize statistics collector.

        Args:
            repository: NorthMacedoniaRepository instance
        """
        self.repo = repository

    def collect_step_statistics(self, step_number: int, step_name: str,
                                start_time: datetime, end_time: datetime,
                                items_processed: int, items_failed: int = 0) -> None:
        """
        Collect statistics for a pipeline step.

        Args:
            step_number: Step number
            step_name: Step name
            start_time: Step start time
            end_time: Step end time
            items_processed: Number of items processed
            items_failed: Number of items that failed
        """
        duration_seconds = (end_time - start_time).total_seconds()
        success_count = items_processed - items_failed
        success_rate = (success_count / items_processed * 100) if items_processed > 0 else 0
        items_per_second = items_processed / duration_seconds if duration_seconds > 0 else 0

        # Insert statistics
        self.repo.insert_statistic(
            step_number=step_number,
            metric_name=f"{step_name}_duration",
            metric_value=duration_seconds,
            metric_type="duration",
            category="performance",
            description=f"Duration of {step_name} in seconds"
        )

        self.repo.insert_statistic(
            step_number=step_number,
            metric_name=f"{step_name}_items_processed",
            metric_value=items_processed,
            metric_type="count",
            category="volume",
            description=f"Total items processed in {step_name}"
        )

        self.repo.insert_statistic(
            step_number=step_number,
            metric_name=f"{step_name}_items_failed",
            metric_value=items_failed,
            metric_type="count",
            category="quality",
            description=f"Failed items in {step_name}"
        )

        self.repo.insert_statistic(
            step_number=step_number,
            metric_name=f"{step_name}_success_rate",
            metric_value=success_rate,
            metric_type="percentage",
            category="quality",
            description=f"Success rate for {step_name}"
        )

        self.repo.insert_statistic(
            step_number=step_number,
            metric_name=f"{step_name}_throughput",
            metric_value=items_per_second,
            metric_type="rate",
            category="performance",
            description=f"Items per second in {step_name}"
        )

        logger.info(f"[STATS] {step_name}: {items_processed} items, {success_rate:.1f}% success, "
                   f"{duration_seconds:.1f}s, {items_per_second:.2f} items/s")

    def generate_final_report(self) -> Dict:
        """
        Generate comprehensive final report.

        Returns:
            Dict with complete run statistics and metrics
        """
        stats = self.repo.get_run_stats()
        validation_summary = self.repo.get_validation_summary()
        pcid_stats = self.repo.get_pcid_mapping_stats()
        error_counts = self.repo.get_errors_by_type()
        all_statistics = self.repo.get_statistics()

        # Calculate derived metrics
        total_urls = stats.get("urls_total", 0)
        scraped_urls = stats.get("urls_scraped", 0)
        failed_urls = stats.get("urls_failed", 0)
        
        scrape_success_rate = (scraped_urls / total_urls * 100) if total_urls > 0 else 0
        
        drug_register_count = stats.get("drug_register_total", 0)
        pcid_mapped_count = stats.get("pcid_mappings_total", 0)
        pcid_mapping_rate = (pcid_mapped_count / drug_register_count * 100) if drug_register_count > 0 else 0
        
        validation_total = validation_summary.get("total_validations", 0)
        validation_passed = validation_summary.get("passed", 0)
        validation_rate = (validation_passed / validation_total * 100) if validation_total > 0 else 0

        report = {
            "run_id": self.repo.run_id,
            "generated_at": datetime.now().isoformat(),
            
            # URL Collection (Step 1)
            "step_1_url_collection": {
                "total_urls_collected": total_urls,
                "urls_scraped": scraped_urls,
                "urls_failed": failed_urls,
                "urls_pending": stats.get("urls_pending", 0),
                "success_rate": round(scrape_success_rate, 2),
            },
            
            # Drug Register (Step 2)
            "step_2_drug_register": {
                "total_records": drug_register_count,
                "records_with_prices": self._count_records_with_prices(),
                "records_with_atc": self._count_records_with_atc(),
            },
            
            # PCID Mapping (Step 3)
            "step_3_pcid_mapping": {
                "total_mappings": pcid_mapped_count,
                "exact_matches": pcid_stats.get("exact_matches", 0),
                "fuzzy_matches": pcid_stats.get("fuzzy_matches", 0),
                "not_found": pcid_stats.get("not_found", 0),
                "average_match_score": round(pcid_stats.get("avg_score", 0), 3),
                "mapping_rate": round(pcid_mapping_rate, 2),
            },
            
            # Final Output
            "final_output": {
                "total_records": stats.get("final_output_total", 0),
                "export_ready": stats.get("final_output_total", 0),
            },
            
            # Data Quality
            "data_quality": {
                "validation_total": validation_total,
                "validation_passed": validation_passed,
                "validation_failed": validation_summary.get("failed", 0),
                "validation_warnings": validation_summary.get("warnings", 0),
                "validation_rate": round(validation_rate, 2),
                "critical_issues": validation_summary.get("critical", 0),
                "high_issues": validation_summary.get("high", 0),
            },
            
            # Errors
            "errors": {
                "total_errors": stats.get("error_count", 0),
                "errors_by_type": error_counts,
            },
            
            # Performance Metrics
            "performance": self._extract_performance_metrics(all_statistics),
            
            # Overall Summary
            "summary": {
                "status": self._determine_run_status(stats, validation_summary),
                "data_completeness": self._calculate_completeness(stats),
                "data_quality_score": self._calculate_quality_score(validation_summary, pcid_stats),
            }
        }

        return report

    def _count_records_with_prices(self) -> int:
        """Count drug register records with price data."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM nm_drug_register
                    WHERE run_id = %s
                      AND (public_with_vat_price IS NOT NULL AND public_with_vat_price != '')
                """, (self.repo.run_id,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def _count_records_with_atc(self) -> int:
        """Count drug register records with ATC code."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM nm_drug_register
                    WHERE run_id = %s
                      AND (who_atc_code IS NOT NULL AND who_atc_code != '')
                """, (self.repo.run_id,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def _extract_performance_metrics(self, statistics: List[Dict]) -> Dict:
        """Extract performance metrics from statistics."""
        metrics = {}
        
        for stat in statistics:
            if stat.get("metric_type") in ["duration", "rate"]:
                step = stat.get("step_number")
                metric_name = stat.get("metric_name")
                value = stat.get("metric_value")
                
                if step not in metrics:
                    metrics[f"step_{step}"] = {}
                
                metrics[f"step_{step}"][metric_name] = round(float(value), 2)
        
        return metrics

    def _determine_run_status(self, stats: Dict, validation_summary: Dict) -> str:
        """Determine overall run status."""
        critical_issues = validation_summary.get("critical", 0)
        failed_validations = validation_summary.get("failed", 0)
        urls_failed = stats.get("urls_failed", 0)
        urls_total = stats.get("urls_total", 0)
        
        if critical_issues > 0:
            return "completed_with_critical_issues"
        elif failed_validations > 0:
            return "completed_with_warnings"
        elif urls_failed > urls_total * 0.1:  # More than 10% failed
            return "completed_with_errors"
        else:
            return "success"

    def _calculate_completeness(self, stats: Dict) -> float:
        """Calculate data completeness score (0-100)."""
        urls_total = stats.get("urls_total", 0)
        drug_register_total = stats.get("drug_register_total", 0)
        final_output_total = stats.get("final_output_total", 0)
        
        if urls_total == 0:
            return 0.0
        
        # Completeness = (final_output / urls_collected) * 100
        completeness = (final_output_total / urls_total * 100) if urls_total > 0 else 0
        return round(min(completeness, 100), 2)

    def _calculate_quality_score(self, validation_summary: Dict, pcid_stats: Dict) -> float:
        """Calculate data quality score (0-100)."""
        # Validation quality (50% weight)
        validation_total = validation_summary.get("total_validations", 0)
        validation_passed = validation_summary.get("passed", 0)
        validation_score = (validation_passed / validation_total * 50) if validation_total > 0 else 0
        
        # PCID mapping quality (50% weight)
        pcid_total = pcid_stats.get("total", 0)
        pcid_found = pcid_total - pcid_stats.get("not_found", 0)
        pcid_score = (pcid_found / pcid_total * 50) if pcid_total > 0 else 0
        
        total_score = validation_score + pcid_score
        return round(total_score, 2)

    def print_report(self, report: Dict) -> None:
        """Print formatted report to console."""
        print("\n" + "="*80)
        print("NORTH MACEDONIA SCRAPER - FINAL REPORT")
        print("="*80)
        print(f"Run ID: {report['run_id']}")
        print(f"Generated: {report['generated_at']}")
        print()
        
        # Step 1
        print("STEP 1: URL COLLECTION")
        print("-" * 80)
        step1 = report["step_1_url_collection"]
        print(f"  Total URLs Collected: {step1['total_urls_collected']}")
        print(f"  URLs Scraped: {step1['urls_scraped']}")
        print(f"  URLs Failed: {step1['urls_failed']}")
        print(f"  URLs Pending: {step1['urls_pending']}")
        print(f"  Success Rate: {step1['success_rate']}%")
        print()
        
        # Step 2
        print("STEP 2: DRUG REGISTER")
        print("-" * 80)
        step2 = report["step_2_drug_register"]
        print(f"  Total Records: {step2['total_records']}")
        print(f"  Records with Prices: {step2['records_with_prices']}")
        print(f"  Records with ATC: {step2['records_with_atc']}")
        print()
        
        # Step 3
        print("STEP 3: PCID MAPPING")
        print("-" * 80)
        step3 = report["step_3_pcid_mapping"]
        print(f"  Total Mappings: {step3['total_mappings']}")
        print(f"  Exact Matches: {step3['exact_matches']}")
        print(f"  Fuzzy Matches: {step3['fuzzy_matches']}")
        print(f"  Not Found: {step3['not_found']}")
        print(f"  Average Match Score: {step3['average_match_score']}")
        print(f"  Mapping Rate: {step3['mapping_rate']}%")
        print()
        
        # Data Quality
        print("DATA QUALITY")
        print("-" * 80)
        quality = report["data_quality"]
        print(f"  Total Validations: {quality['validation_total']}")
        print(f"  Passed: {quality['validation_passed']}")
        print(f"  Failed: {quality['validation_failed']}")
        print(f"  Warnings: {quality['validation_warnings']}")
        print(f"  Validation Rate: {quality['validation_rate']}%")
        print(f"  Critical Issues: {quality['critical_issues']}")
        print(f"  High Issues: {quality['high_issues']}")
        print()
        
        # Errors
        print("ERRORS")
        print("-" * 80)
        errors = report["errors"]
        print(f"  Total Errors: {errors['total_errors']}")
        if errors['errors_by_type']:
            print("  Errors by Type:")
            for error_type, count in errors['errors_by_type'].items():
                print(f"    - {error_type}: {count}")
        print()
        
        # Summary
        print("OVERALL SUMMARY")
        print("-" * 80)
        summary = report["summary"]
        print(f"  Status: {summary['status'].upper()}")
        print(f"  Data Completeness: {summary['data_completeness']}%")
        print(f"  Data Quality Score: {summary['data_quality_score']}/100")
        print()
        
        print("="*80)

    def export_report_to_file(self, report: Dict, output_dir: Path) -> Path:
        """
        Export report to JSON file.

        Args:
            report: Report dict
            output_dir: Output directory

        Returns:
            Path to exported file
        """
        import json
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"north_macedonia_report_{self.repo.run_id}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Report exported to: {filepath}")
        return filepath
