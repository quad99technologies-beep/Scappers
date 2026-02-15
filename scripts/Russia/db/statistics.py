#!/usr/bin/env python3
"""
Statistics and reporting module for Russia scraper.
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
            repository: RussiaRepository instance
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
        ved_total = stats.get("ved_products_total", 0)
        excluded_total = stats.get("excluded_products_total", 0)
        translated_total = stats.get("translated_products_total", 0)
        export_ready_total = stats.get("export_ready_total", 0)
        
        translation_rate = (translated_total / ved_total * 100) if ved_total > 0 else 0
        export_rate = (export_ready_total / translated_total * 100) if translated_total > 0 else 0
        
        validation_total = validation_summary.get("total_validations", 0)
        validation_passed = validation_summary.get("passed", 0)
        validation_rate = (validation_passed / validation_total * 100) if validation_total > 0 else 0

        report = {
            "run_id": self.repo.run_id,
            "generated_at": datetime.now().isoformat(),
            
            # Step 1: VED Products Scraping
            "step_1_ved_scraping": {
                "total_products": ved_total,
                "products_with_ean": self._count_ved_with_ean(),
                "products_with_prices": self._count_ved_with_prices(),
                "pages_scraped": self._count_completed_pages(1),
            },
            
            # Step 2: Excluded Products Scraping
            "step_2_excluded_scraping": {
                "total_products": excluded_total,
                "products_with_ean": self._count_excluded_with_ean(),
                "pages_scraped": self._count_completed_pages(2),
            },
            
            # Step 3: Translation
            "step_3_translation": {
                "total_translated": translated_total,
                "translation_rate": round(translation_rate, 2),
            },
            
            # Step 4: PCID Mapping
            "step_4_pcid_mapping": {
                "total_mappings": pcid_stats.get("total", 0),
                "exact_matches": pcid_stats.get("exact_matches", 0),
                "fuzzy_matches": pcid_stats.get("fuzzy_matches", 0),
                "not_found": pcid_stats.get("not_found", 0),
                "average_match_score": round(pcid_stats.get("avg_score", 0), 3),
            },
            
            # Step 5: Export Preparation
            "step_5_export_preparation": {
                "total_records": export_ready_total,
                "export_rate": round(export_rate, 2),
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

    def _count_ved_with_ean(self) -> int:
        """Count VED products with EAN."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM ru_ved_products
                    WHERE run_id = %s
                      AND (ean IS NOT NULL AND ean != '')
                """, (self.repo.run_id,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def _count_ved_with_prices(self) -> int:
        """Count VED products with price data."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM ru_ved_products
                    WHERE run_id = %s
                      AND (registered_price_rub IS NOT NULL AND registered_price_rub != '')
                """, (self.repo.run_id,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def _count_excluded_with_ean(self) -> int:
        """Count excluded products with EAN."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM ru_excluded_products
                    WHERE run_id = %s
                      AND (ean IS NOT NULL AND ean != '')
                """, (self.repo.run_id,))
                row = cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    def _count_completed_pages(self, step_number: int) -> int:
        """Count completed pages for a step."""
        try:
            with self.repo.db.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM ru_step_progress
                    WHERE run_id = %s
                      AND step_number = %s
                      AND status = 'completed'
                """, (self.repo.run_id, step_number))
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
        error_count = stats.get("error_count", 0)
        
        if critical_issues > 0:
            return "completed_with_critical_issues"
        elif failed_validations > 0:
            return "completed_with_warnings"
        elif error_count > 10:  # More than 10 errors
            return "completed_with_errors"
        else:
            return "success"

    def _calculate_completeness(self, stats: Dict) -> float:
        """Calculate data completeness score (0-100)."""
        ved_total = stats.get("ved_products_total", 0)
        export_ready_total = stats.get("export_ready_total", 0)
        
        if ved_total == 0:
            return 0.0
        
        # Completeness = (export_ready / ved_products) * 100
        completeness = (export_ready_total / ved_total * 100) if ved_total > 0 else 0
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
        print("RUSSIA SCRAPER - FINAL REPORT")
        print("="*80)
        print(f"Run ID: {report['run_id']}")
        print(f"Generated: {report['generated_at']}")
        print()
        
        # Step 1
        print("STEP 1: VED PRODUCTS SCRAPING")
        print("-" * 80)
        step1 = report["step_1_ved_scraping"]
        print(f"  Total Products: {step1['total_products']}")
        print(f"  Products with EAN: {step1['products_with_ean']}")
        print(f"  Products with Prices: {step1['products_with_prices']}")
        print(f"  Pages Scraped: {step1['pages_scraped']}")
        print()
        
        # Step 2
        print("STEP 2: EXCLUDED PRODUCTS SCRAPING")
        print("-" * 80)
        step2 = report["step_2_excluded_scraping"]
        print(f"  Total Products: {step2['total_products']}")
        print(f"  Products with EAN: {step2['products_with_ean']}")
        print(f"  Pages Scraped: {step2['pages_scraped']}")
        print()
        
        # Step 3
        print("STEP 3: TRANSLATION")
        print("-" * 80)
        step3 = report["step_3_translation"]
        print(f"  Total Translated: {step3['total_translated']}")
        print(f"  Translation Rate: {step3['translation_rate']}%")
        print()
        
        # Step 4
        print("STEP 4: PCID MAPPING")
        print("-" * 80)
        step4 = report["step_4_pcid_mapping"]
        print(f"  Total Mappings: {step4['total_mappings']}")
        print(f"  Exact Matches: {step4['exact_matches']}")
        print(f"  Fuzzy Matches: {step4['fuzzy_matches']}")
        print(f"  Not Found: {step4['not_found']}")
        print(f"  Average Match Score: {step4['average_match_score']}")
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
        filename = f"russia_report_{self.repo.run_id}_{timestamp}.json"
        filepath = output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Report exported to: {filepath}")
        return filepath
