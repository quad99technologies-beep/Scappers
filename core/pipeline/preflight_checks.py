#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Preflight Health Checks Contract

Standardized preflight health checks that block bad runs before they start,
preventing wasted time and resources.

Usage:
    from core.pipeline.preflight_checks import PreflightChecker, CheckSeverity
    
    checker = PreflightChecker("Malaysia", run_id)
    results = checker.run_all_checks()
    
    if checker.has_critical_failures():
        print("Pipeline blocked due to critical failures")
        sys.exit(1)
"""

import os
import sys
import logging
import shutil
import psutil
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class CheckSeverity(Enum):
    """Severity levels for health checks."""
    CRITICAL = "critical"  # Block run
    WARNING = "warning"    # Warn but allow
    INFO = "info"          # Informational only


@dataclass
class HealthCheckResult:
    """Result of a single health check."""
    name: str
    severity: CheckSeverity
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        # Use ASCII-safe status indicators for Windows console compatibility
        status = "[OK]" if self.passed else "[FAIL]"
        return f"{status} {self.name}: {self.message}"


class PreflightChecker:
    """Standardized preflight health checks."""
    
    def __init__(self, scraper_name: str, run_id: str, repo_root: Optional[Path] = None):
        """
        Initialize preflight checker.
        
        Args:
            scraper_name: Name of the scraper (e.g., "Malaysia")
            run_id: Current run ID
            repo_root: Optional repository root path (auto-detected if None)
        """
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.repo_root = repo_root or self._detect_repo_root()
        self.results: List[HealthCheckResult] = []
    
    def _detect_repo_root(self) -> Path:
        """Detect repository root path."""
        current = Path(__file__).resolve()
        # Look for .git or core/ directory
        while current.parent != current:
            if (current / ".git").exists() or (current / "core").exists():
                return current
            current = current.parent
        return Path.cwd()
    
    def check_database_connectivity(self) -> HealthCheckResult:
        """Check: Database is accessible."""
        try:
            from core.db.postgres_connection import get_db
            
            db = get_db(self.scraper_name)
            with db.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            
            return HealthCheckResult(
                name="database_connectivity",
                severity=CheckSeverity.CRITICAL,
                passed=True,
                message="Database connection successful",
                details={"scraper_name": self.scraper_name}
            )
        except Exception as e:
            return HealthCheckResult(
                name="database_connectivity",
                severity=CheckSeverity.CRITICAL,
                passed=False,
                message=f"Database connection failed: {e}",
                details={"error": str(e)}
            )
    
    def check_disk_space(self, min_gb: float = 10.0) -> HealthCheckResult:
        """Check: Sufficient disk space available."""
        try:
            output_dir = self.repo_root / "output" / self.scraper_name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            stat = shutil.disk_usage(output_dir)
            free_gb = stat.free / (1024 ** 3)
            
            if free_gb >= min_gb:
                return HealthCheckResult(
                    name="disk_space",
                    severity=CheckSeverity.CRITICAL,
                    passed=True,
                    message=f"Sufficient disk space: {free_gb:.1f} GB free",
                    details={"free_gb": free_gb, "required_gb": min_gb}
                )
            else:
                return HealthCheckResult(
                    name="disk_space",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message=f"Insufficient disk space: {free_gb:.1f} GB free (need {min_gb} GB)",
                    details={"free_gb": free_gb, "required_gb": min_gb}
                )
        except Exception as e:
            return HealthCheckResult(
                name="disk_space",
                severity=CheckSeverity.CRITICAL,
                passed=False,
                message=f"Could not check disk space: {e}",
                details={"error": str(e)}
            )
    
    def check_memory_available(self, min_gb: float = 4.0) -> HealthCheckResult:
        """Check: Sufficient memory available."""
        try:
            if not psutil:
                return HealthCheckResult(
                    name="memory_available",
                    severity=CheckSeverity.INFO,
                    passed=True,
                    message="Memory check skipped (psutil not available)",
                    details={}
                )
            
            mem = psutil.virtual_memory()
            available_gb = mem.available / (1024 ** 3)
            
            if available_gb >= min_gb:
                return HealthCheckResult(
                    name="memory_available",
                    severity=CheckSeverity.CRITICAL,
                    passed=True,
                    message=f"Sufficient memory: {available_gb:.1f} GB available",
                    details={"available_gb": available_gb, "required_gb": min_gb}
                )
            else:
                return HealthCheckResult(
                    name="memory_available",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message=f"Insufficient memory: {available_gb:.1f} GB available (need {min_gb} GB)",
                    details={"available_gb": available_gb, "required_gb": min_gb}
                )
        except Exception as e:
            return HealthCheckResult(
                name="memory_available",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check memory: {e}",
                details={"error": str(e)}
            )
    
    def check_browser_executable(self) -> HealthCheckResult:
        """Check: Browser executable is available."""
        try:
            # Check for Chrome/Chromium
            chrome_paths = [
                shutil.which("chrome"),
                shutil.which("chromium"),
                shutil.which("google-chrome"),
                shutil.which("chromium-browser"),
            ]
            
            # On Windows, check common locations
            if sys.platform == "win32":
                common_paths = [
                    Path("C:/Program Files/Google/Chrome/Application/chrome.exe"),
                    Path("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"),
                ]
                chrome_paths.extend([str(p) for p in common_paths if p.exists()])
            
            chrome_found = any(chrome_paths)
            
            # Check for Playwright
            playwright_available = False
            try:
                from playwright.sync_api import sync_playwright
                playwright_available = True
            except ImportError:
                pass
            
            if chrome_found or playwright_available:
                return HealthCheckResult(
                    name="browser_executable",
                    severity=CheckSeverity.CRITICAL,
                    passed=True,
                    message="Browser executable found",
                    details={
                        "chrome_found": chrome_found,
                        "playwright_available": playwright_available
                    }
                )
            else:
                return HealthCheckResult(
                    name="browser_executable",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message="Browser executable not found (Chrome or Playwright required)",
                    details={}
                )
        except Exception as e:
            return HealthCheckResult(
                name="browser_executable",
                severity=CheckSeverity.CRITICAL,
                passed=False,
                message=f"Could not check browser: {e}",
                details={"error": str(e)}
            )
    
    def check_input_tables_populated(self) -> HealthCheckResult:
        """Check: Input tables have data."""
        try:
            from core.db.postgres_connection import get_db, COUNTRY_PREFIX_MAP
            
            db = get_db(self.scraper_name)
            prefix = COUNTRY_PREFIX_MAP.get(self.scraper_name, "")
            
            # Check for common input table patterns
            input_tables = [
                f"{prefix}input_products",
                f"{prefix}input_search_terms",
                f"input_uploads",  # Shared table
            ]
            
            has_data = False
            checked_tables = []
            
            with db.cursor() as cur:
                for table in input_tables:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cur.fetchone()[0]
                        checked_tables.append({"table": table, "count": count})
                        if count > 0:
                            has_data = True
                    except Exception:
                        # Table doesn't exist, skip
                        pass
            
            if has_data or not checked_tables:
                return HealthCheckResult(
                    name="input_tables_populated",
                    severity=CheckSeverity.CRITICAL,
                    passed=True,
                    message="Input tables populated",
                    details={"checked_tables": checked_tables}
                )
            else:
                return HealthCheckResult(
                    name="input_tables_populated",
                    severity=CheckSeverity.CRITICAL,
                    passed=False,
                    message="Input tables are empty",
                    details={"checked_tables": checked_tables}
                )
        except Exception as e:
            return HealthCheckResult(
                name="input_tables_populated",
                severity=CheckSeverity.WARNING,
                passed=False,
                message=f"Could not check input tables: {e}",
                details={"error": str(e)}
            )
    
    def check_stale_run_detection(self, max_age_hours: int = 24) -> HealthCheckResult:
        """Check: Previous run is not stale (prevents accidental resume of old runs)."""
        try:
            from core.db.postgres_connection import get_db
            
            db = get_db(self.scraper_name)
            with db.cursor() as cur:
                cur.execute("""
                    SELECT run_id, started_at, status
                    FROM run_ledger
                    WHERE scraper_name = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                """, (self.scraper_name,))
                
                row = cur.fetchone()
                if not row:
                    return HealthCheckResult(
                        name="stale_run_detection",
                        severity=CheckSeverity.INFO,
                        passed=True,
                        message="No previous runs found",
                        details={}
                    )
                
                run_id, started_at, status = row
                if not started_at:
                    return HealthCheckResult(
                        name="stale_run_detection",
                        severity=CheckSeverity.INFO,
                        passed=True,
                        message="Previous run has no start time",
                        details={}
                    )
                
                from datetime import datetime, timezone
                now_utc = datetime.now(timezone.utc)
                if getattr(started_at, "tzinfo", None) is None or started_at.tzinfo.utcoffset(started_at) is None:
                    # DB may return naive timestamps depending on driver/schema; treat as UTC.
                    started_at_utc = started_at.replace(tzinfo=timezone.utc)
                else:
                    started_at_utc = started_at.astimezone(timezone.utc)
                age_hours = (now_utc - started_at_utc).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    return HealthCheckResult(
                        name="stale_run_detection",
                        severity=CheckSeverity.WARNING,
                        passed=True,  # Don't block, just warn
                        message=f"Previous run is {age_hours:.1f} hours old (may be stale)",
                        details={
                            "previous_run_id": run_id,
                            "age_hours": age_hours,
                            "status": status
                        }
                    )
                else:
                    return HealthCheckResult(
                        name="stale_run_detection",
                        severity=CheckSeverity.INFO,
                        passed=True,
                        message=f"Previous run is {age_hours:.1f} hours old",
                        details={
                            "previous_run_id": run_id,
                            "age_hours": age_hours,
                            "status": status
                        }
                    )
        except Exception as e:
            return HealthCheckResult(
                name="stale_run_detection",
                severity=CheckSeverity.INFO,
                passed=True,  # Don't block on check failure
                message=f"Could not check stale runs: {e}",
                details={"error": str(e)}
            )
    
    def run_all_checks(self) -> List[HealthCheckResult]:
        """Run all preflight checks."""
        self.results = [
            self.check_database_connectivity(),
            self.check_disk_space(),
            self.check_memory_available(),
            self.check_browser_executable(),
            self.check_input_tables_populated(),
            self.check_stale_run_detection(),
        ]
        return self.results
    
    def has_critical_failures(self) -> bool:
        """Check if any critical checks failed."""
        return any(
            r.severity == CheckSeverity.CRITICAL and not r.passed
            for r in self.results
        )
    
    def get_failure_summary(self) -> str:
        """Get human-readable summary of failures."""
        failures = [r for r in self.results if not r.passed]
        if not failures:
            return "All checks passed"
        return "\n".join(f"- {r.name}: {r.message}" for r in failures)
    
    def get_summary(self) -> str:
        """Get human-readable summary of all checks."""
        lines = ["Preflight Health Checks:"]
        for result in self.results:
            lines.append(f"  {result}")
        return "\n".join(lines)
