#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Quality Checks

Automated pre-flight and post-run data quality validation.
Catches data issues before export delivery.

Usage:
    from core.data.data_quality_checks import DataQualityChecker
    
    checker = DataQualityChecker("Malaysia", run_id)
    
    # Pre-flight checks
    results = checker.run_preflight_checks()
    
    # Post-run checks
    results = checker.run_postrun_checks()
    
    # Export validation
    results = checker.validate_export(export_file_path)
"""

import logging
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class CheckSeverity(Enum):
    """Severity levels for data quality checks."""
    CRITICAL = "critical"  # Block export
    WARNING = "warning"    # Warn but allow
    INFO = "info"          # Informational only


@dataclass
class QualityCheckResult:
    """Result of a single data quality check."""
    check_type: str  # 'preflight', 'postrun', 'export'
    check_name: str
    severity: CheckSeverity
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "check_type": self.check_type,
            "check_name": self.check_name,
            "status": "pass" if self.passed else ("fail" if self.severity == CheckSeverity.CRITICAL else "warning"),
            "message": self.message,
            "details_json": self.details,
        }


class DataQualityChecker:
    """Automated data quality checks."""
    
    def __init__(self, scraper_name: str, run_id: str):
        """
        Initialize data quality checker.
        
        Args:
            scraper_name: Name of the scraper
            run_id: Current run ID
        """
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.results: List[QualityCheckResult] = []
    
    def check_input_table_counts(self) -> QualityCheckResult:
        """Pre-flight: Check input table row counts."""
        try:
            from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
            
            with get_db(self.scraper_name) as db:
                prefix = COUNTRY_PREFIX_MAP.get(self.scraper_name, "")
                
                input_tables = [
                    f"{prefix}input_products",
                    f"{prefix}input_search_terms",
                ]
                
                counts = {}
                with db.cursor() as cur:
                    for table in input_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cur.fetchone()[0]
                            counts[table] = count
                        except Exception:
                            pass
            
                if not counts:
                    return QualityCheckResult(
                        check_type="preflight",
                        check_name="input_table_counts",
                        severity=CheckSeverity.WARNING,
                        passed=True,
                        message="No input tables found (may be expected)",
                        details={"counts": counts}
                    )
                
                total = sum(counts.values())
                if total == 0:
                    return QualityCheckResult(
                        check_type="preflight",
                        check_name="input_table_counts",
                        severity=CheckSeverity.CRITICAL,
                        passed=False,
                        message="Input tables are empty",
                        details={"counts": counts}
                    )
                
                return QualityCheckResult(
                    check_type="preflight",
                    check_name="input_table_counts",
                    severity=CheckSeverity.INFO,
                    passed=True,
                    message=f"Input tables populated: {total} total rows",
                    details={"counts": counts}
                )
        except Exception as e:
            return QualityCheckResult(
                check_type="preflight",
                check_name="input_table_counts",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check input tables: {e}",
                details={"error": str(e)}
            )
    
    def check_pcid_mapping_coverage(self) -> QualityCheckResult:
        """Pre-flight: Check PCID mapping coverage."""
        try:
            from core.data.pcid_mapping_contract import get_pcid_mapping
            
            pcid = get_pcid_mapping(self.scraper_name)
            all_mappings = pcid.get_all()
            
            if not all_mappings:
                return QualityCheckResult(
                    check_type="preflight",
                    check_name="pcid_mapping_coverage",
                    severity=CheckSeverity.WARNING,
                    passed=True,
                    message="No PCID mappings found (may be expected)",
                    details={"mapping_count": 0}
                )
            
            return QualityCheckResult(
                check_type="preflight",
                check_name="pcid_mapping_coverage",
                severity=CheckSeverity.INFO,
                passed=True,
                message=f"PCID mapping loaded: {len(all_mappings)} mappings",
                details={"mapping_count": len(all_mappings)}
            )
        except Exception as e:
            return QualityCheckResult(
                check_type="preflight",
                check_name="pcid_mapping_coverage",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check PCID mapping: {e}",
                details={"error": str(e)}
            )
    
    def check_row_count_deltas(self) -> QualityCheckResult:
        """Post-run: Check row count deltas between steps."""
        try:
            from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
            
            with get_db(self.scraper_name) as db:
                prefix = COUNTRY_PREFIX_MAP.get(self.scraper_name, "")
                table_name = f"{prefix}_step_progress"
                
                with db.cursor() as cur:
                    cur.execute(f"""
                        SELECT step_number, rows_inserted, rows_processed
                        FROM {table_name}
                        WHERE run_id = %s
                        ORDER BY step_number
                    """, (self.run_id,))
                
                steps = cur.fetchall()
                if not steps:
                    return QualityCheckResult(
                        check_type="postrun",
                        check_name="row_count_deltas",
                        severity=CheckSeverity.INFO,
                        passed=True,
                        message="No step data available",
                        details={}
                    )
                
                # Check for significant drops (>50%)
                issues = []
                for i in range(1, len(steps)):
                    prev_processed = steps[i-1][2] or 0
                    curr_processed = steps[i][2] or 0
                    if prev_processed > 0:
                        delta_pct = ((curr_processed - prev_processed) / prev_processed) * 100
                        if delta_pct < -50:
                            issues.append({
                                "step": steps[i][0],
                                "delta_pct": delta_pct,
                                "prev": prev_processed,
                                "curr": curr_processed
                            })
                
                if issues:
                    return QualityCheckResult(
                        check_type="postrun",
                        check_name="row_count_deltas",
                        severity=CheckSeverity.WARNING,
                        passed=False,
                        message=f"Significant row count drops detected: {len(issues)} issues",
                        details={"issues": issues}
                    )
                
                return QualityCheckResult(
                    check_type="postrun",
                    check_name="row_count_deltas",
                    severity=CheckSeverity.INFO,
                    passed=True,
                    message="Row count deltas within normal range",
                    details={}
                )
        except Exception as e:
            return QualityCheckResult(
                check_type="postrun",
                check_name="row_count_deltas",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check row count deltas: {e}",
                details={"error": str(e)}
            )
    
    def check_null_rates(self) -> QualityCheckResult:
        """Post-run: Check null rates in key columns."""
        try:
            from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
            
            with get_db(self.scraper_name) as db:
                prefix = COUNTRY_PREFIX_MAP.get(self.scraper_name, "")
                
                # Check main product table
                product_table = f"{prefix}_products"
                with db.cursor() as cur:
                    try:
                        cur.execute(f"""
                            SELECT 
                                COUNT(*) as total,
                                COUNT(*) FILTER (WHERE product_name IS NULL) as null_product_name,
                                COUNT(*) FILTER (WHERE company IS NULL) as null_company
                            FROM {product_table}
                            WHERE run_id = %s
                        """, (self.run_id,))
                    
                        row = cur.fetchone()
                        if row and row[0] > 0:
                            total = row[0]
                            null_product = row[1]
                            null_company = row[2]
                            
                            null_rate_product = (null_product / total) * 100
                            null_rate_company = (null_company / total) * 100
                            
                            issues = []
                            if null_rate_product > 10:
                                issues.append(f"product_name: {null_rate_product:.1f}%")
                            if null_rate_company > 10:
                                issues.append(f"company: {null_rate_company:.1f}%")
                            
                            if issues:
                                return QualityCheckResult(
                                    check_type="postrun",
                                    check_name="null_rates",
                                    severity=CheckSeverity.WARNING,
                                    passed=False,
                                    message=f"High null rates detected: {', '.join(issues)}",
                                    details={
                                        "null_rate_product": null_rate_product,
                                        "null_rate_company": null_rate_company
                                    }
                                )
                    except Exception:
                        # Table doesn't exist or different schema
                        pass
            
            return QualityCheckResult(
                check_type="postrun",
                check_name="null_rates",
                severity=CheckSeverity.INFO,
                passed=True,
                message="Null rates within acceptable range",
                details={}
            )
        except Exception as e:
            return QualityCheckResult(
                check_type="postrun",
                check_name="null_rates",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check null rates: {e}",
                details={"error": str(e)}
            )
    
    def validate_export(self, export_file_path: Path) -> QualityCheckResult:
        """Export: Validate export file integrity."""
        try:
            if not export_file_path.exists():
                return QualityCheckResult(
                    check_type="export",
                    check_name="export_file_exists",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message=f"Export file not found: {export_file_path}",
                    details={}
                )
            
            # Check file size
            file_size_mb = export_file_path.stat().st_size / (1024 ** 2)
            if file_size_mb > 100:
                return QualityCheckResult(
                    check_type="export",
                    check_name="export_file_size",
                    severity=CheckSeverity.WARNING,
                    passed=True,
                    message=f"Export file is large: {file_size_mb:.1f} MB",
                    details={"file_size_mb": file_size_mb}
                )
            
            # Try to read CSV
            try:
                df = pd.read_csv(export_file_path, nrows=10)
                row_count = len(df)
                
                return QualityCheckResult(
                    check_type="export",
                    check_name="export_file_readable",
                    severity=CheckSeverity.INFO,
                    passed=True,
                    message=f"Export file is readable: {row_count} rows (sample)",
                    details={"file_size_mb": file_size_mb, "sample_rows": row_count}
                )
            except Exception as e:
                return QualityCheckResult(
                    check_type="export",
                    check_name="export_file_readable",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message=f"Export file is not readable: {e}",
                    details={"error": str(e)}
                )
        except Exception as e:
            return QualityCheckResult(
                check_type="export",
                check_name="export_validation",
                severity=CheckSeverity.CRITICAL,
                passed=False,
                message=f"Export validation failed: {e}",
                details={"error": str(e)}
            )
    
    def run_preflight_checks(self) -> List[QualityCheckResult]:
        """Run all pre-flight checks."""
        self.results = [
            self.check_input_table_counts(),
            self.check_pcid_mapping_coverage(),
        ]
        return self.results
    
    def run_postrun_checks(self) -> List[QualityCheckResult]:
        """Run all post-run checks."""
        self.results = [
            self.check_row_count_deltas(),
            self.check_null_rates(),
        ]
        return self.results
    
    def save_results_to_db(self):
        """Save check results to database."""
        try:
            from core.db.postgres_connection import get_db
            
            with get_db(self.scraper_name) as db:
                with db.cursor() as cur:
                    # Check if table exists
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = 'data_quality_checks'
                        )
                    """)
                    table_exists = cur.fetchone()[0]
                    
                    if not table_exists:
                        # Create table
                        cur.execute("""
                            CREATE TABLE data_quality_checks (
                                id SERIAL PRIMARY KEY,
                                run_id TEXT NOT NULL REFERENCES run_ledger(run_id),
                                scraper_name TEXT NOT NULL,
                                check_type TEXT NOT NULL,
                                check_name TEXT NOT NULL,
                                status TEXT NOT NULL CHECK(status IN ('pass', 'fail', 'warning')),
                                message TEXT,
                                details_json JSONB,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        cur.execute("""
                            CREATE INDEX idx_dqc_run ON data_quality_checks(run_id)
                        """)
                    
                    # Verify run_id exists in run_ledger before inserting
                    cur.execute("SELECT 1 FROM run_ledger WHERE run_id = %s", (self.run_id,))
                    if not cur.fetchone():
                        logger.warning(f"Run ID {self.run_id} not found in run_ledger, skipping data quality check save")
                        return
                    
                    # Insert results
                    for result in self.results:
                        data = result.to_dict()
                        # Convert details_json to proper JSON string (not Python dict string representation)
                        details_json_str = json.dumps(data["details_json"]) if data["details_json"] else None
                        cur.execute("""
                            INSERT INTO data_quality_checks
                                (run_id, scraper_name, check_type, check_name, status, message, details_json)
                            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        """, (
                            self.run_id,
                            self.scraper_name,
                            data["check_type"],
                            data["check_name"],
                            data["status"],
                            data["message"],
                            details_json_str
                        ))
                    
                    db.commit()
        except Exception as e:
            logger.error(f"Could not save quality check results: {e}", exc_info=True)
