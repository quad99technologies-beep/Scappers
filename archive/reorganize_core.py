#!/usr/bin/env python3
"""
Reorganize core/ directory structure

Moves files into logical sub-packages and creates backward-compatible imports.
"""

import os
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
CORE_DIR = REPO_ROOT / "core"

# File mapping: source -> destination
FILE_MAPPING = {
    # Scraping adapters
    "base_scraper.py": "scraping/base.py",
    "hybrid_scraper.py": "scraping/hybrid.py",
    "human_actions.py": "scraping/human_actions.py",
    "stealth_profile.py": "scraping/stealth.py",
    "selector_healer.py": "scraping/selector_healer.py",
    "smart_retry.py": "scraping/smart_retry.py",
    "retry_config.py": "scraping/retry_config.py",
    "frontier.py": "scraping/frontier.py",
    "tor_httpx.py": "scraping/tor_httpx.py",
    
    # Browser management
    "chrome_manager.py": "browser/chrome_manager.py",
    "chrome_pid_tracker.py": "browser/chrome_pid_tracker.py",
    "chrome_instance_tracker.py": "browser/chrome_tracker.py",
    "firefox_pid_tracker.py": "browser/firefox_tracker.py",
    "browser_observer.py": "browser/observer.py",
    "memory_leak_detector.py": "browser/memory_monitor.py",
    
    # Infrastructure
    "config_manager.py": "infrastructure/config.py",
    "logger.py": "infrastructure/logger.py",
    "proxy_pool.py": "infrastructure/proxy.py",
    "ip_rotation.py": "infrastructure/ip_rotation.py",
    "rate_limiter.py": "infrastructure/rate_limiter.py",
    "geo_router.py": "infrastructure/geo_router.py",
    "network_info.py": "infrastructure/network.py",
    "cache_manager.py": "infrastructure/cache.py",
    "shared_utils.py": "infrastructure/utils.py",
    
    # Workflow
    "pipeline_checkpoint.py": "workflow/checkpoint.py",
    "pipeline_start_lock.py": "workflow/start_lock.py",
    "standalone_checkpoint.py": "workflow/standalone_checkpoint.py",
    "run_ledger.py": "workflow/ledger.py",
    "run_rollback.py": "workflow/rollback.py",
    "step_hooks.py": "workflow/step_hooks.py",
    "step_progress_logger.py": "workflow/progress_logger.py",
    "progress_tracker.py": "workflow/progress.py",
    "rich_progress.py": "workflow/rich_progress.py",
    "preflight_checks.py": "workflow/preflight.py",
    
    # Quality/Monitoring
    "data_quality_checks.py": "quality/data_checks.py",
    "data_validator.py": "quality/validator.py",
    "schema_inference.py": "quality/schema_inference.py",
    "deduplicator.py": "quality/deduplicator.py",
    "anomaly_detection.py": "quality/anomaly_detection.py",
    "anomaly_detector.py": "quality/anomaly_detector.py",
    "health_monitor.py": "quality/health.py",
    "error_tracker.py": "quality/error_tracker.py",
    "data_diff.py": "quality/diff.py",
    "run_comparison.py": "quality/comparison.py",
    
    # Reporting/Export
    "report_generator.py": "infrastructure/report_generator.py",
    "export_delivery_tracking.py": "infrastructure/export_tracking.py",
    "dashboard.py": "infrastructure/dashboard.py",
    "benchmarking.py": "infrastructure/benchmarking.py",
    "cost_tracking.py": "infrastructure/cost_tracking.py",
    "prometheus_exporter.py": "infrastructure/prometheus.py",
    "diagnostics_exporter.py": "infrastructure/diagnostics.py",
    "resource_monitor.py": "infrastructure/resource_monitor.py",
    "run_metrics_tracker.py": "infrastructure/metrics_tracker.py",
    "run_metrics_integration.py": "infrastructure/metrics_integration.py",
    "trend_analysis.py": "infrastructure/trend_analysis.py",
    
    # Integration
    "integration_helpers.py": "infrastructure/integration_helpers.py",
    "integration_example.py": "infrastructure/integration_example.py",
    "alerting_integration.py": "infrastructure/alerting.py",
    "alerting_contract.py": "infrastructure/alerting_contract.py",
    "pcid_mapping.py": "infrastructure/pcid_mapping.py",
    "pcid_mapping_contract.py": "infrastructure/pcid_contract.py",
    "telegram_notifier.py": "infrastructure/telegram.py",
    "audit_logger.py": "infrastructure/audit_logger.py",
    "hybrid_auditor.py": "infrastructure/hybrid_auditor.py",
}

# Files to keep at root (commonly used)
KEEP_AT_ROOT = {
    "__init__.py",
    "translation",  # Already organized
    "observability",  # Already organized
    "transform",  # Already organized
    "db",  # Keep as-is for now
}


def create_backward_compat():
    """Create backward-compatible import shims at root level"""
    
    compat_files = {
        "base_scraper.py": """# Backward compatibility - use core.scraping.base instead
from core.scraping.base import BaseScraper
__all__ = ['BaseScraper']
""",
        "config_manager.py": """# Backward compatibility - use core.infrastructure.config instead
from core.infrastructure.config import ConfigManager, get_config_manager
__all__ = ['ConfigManager', 'get_config_manager']
""",
        "logger.py": """# Backward compatibility - use core.infrastructure.logger instead
from core.infrastructure.logger import get_logger, Logger
__all__ = ['get_logger', 'Logger']
""",
        "proxy_pool.py": """# Backward compatibility - use core.infrastructure.proxy instead
from core.infrastructure.proxy import ProxyPool, Proxy, get_proxy_for_scraper
__all__ = ['ProxyPool', 'Proxy', 'get_proxy_for_scraper']
""",
    }
    
    for filename, content in compat_files.items():
        filepath = CORE_DIR / filename
        if not filepath.exists():  # Only create if original was moved
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[COMPAT] Created {filename}")


def reorganize():
    """Move files to new locations"""
    print("=" * 70)
    print("REORGANIZING core/ DIRECTORY")
    print("=" * 70)
    
    moved = []
    errors = []
    skipped = []
    
    for source_name, dest_path in FILE_MAPPING.items():
        source = CORE_DIR / source_name
        dest = CORE_DIR / dest_path
        
        if not source.exists():
            skipped.append(source_name)
            continue
        
        if dest.exists():
            skipped.append(f"{source_name} -> {dest_path} (dest exists)")
            continue
        
        try:
            # Ensure parent directory exists
            dest.parent.mkdir(parents=True, exist_ok=True)
            
            # Move file
            shutil.move(str(source), str(dest))
            moved.append(f"{source_name} -> {dest_path}")
            print(f"[MOVED] {source_name} -> {dest_path}")
        except Exception as e:
            errors.append((source_name, str(e)))
            print(f"[ERROR] {source_name}: {e}")
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Moved: {len(moved)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Errors: {len(errors)}")
    
    if errors:
        print("\nErrors:")
        for src, err in errors:
            print(f"  - {src}: {err}")
    
    # Create backward compatibility shims
    print("\n" + "=" * 70)
    print("CREATING BACKWARD COMPATIBILITY SHIMS")
    print("=" * 70)
    create_backward_compat()
    
    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)
    print("\nNew structure:")
    for subdir in sorted(CORE_DIR.iterdir()):
        if subdir.is_dir() and not subdir.name.startswith('_'):
            count = len(list(subdir.glob('*.py')))
            print(f"  {subdir.name}/: {count} files")


if __name__ == "__main__":
    reorganize()
