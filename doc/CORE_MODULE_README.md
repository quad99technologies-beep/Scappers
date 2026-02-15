# Core Module Organization

## ✅ Reorganized Structure (v3.0.0)

The core module has been reorganized into logical subdirectories for better maintainability.

**Backward Compatibility**: All modules can still be imported from their original paths (e.g., `from core.config_manager import ConfigManager`) thanks to stub files in the root `core/` directory.

## Directory Map

```
core/
├── browser/            # Browser automation & tracking
│   ├── chrome_manager.py
│   ├── chrome_instance_tracker.py
│   ├── firefox_pid_tracker.py
│   ├── stealth_profile.py
│   ├── selector_healer.py
│   └── human_actions.py
│
├── config/             # Configuration
│   ├── config_manager.py
│   └── retry_config.py
│
├── data/               # Data processing & quality
│   ├── data_validator.py
│   ├── data_quality_checks.py
│   ├── deduplicator.py
│   ├── schema_inference.py
│   └── pcid_mapping.py
│
├── db/                 # Database (PostgreSQL)
│   ├── postgres_connection.py
│   ├── models.py
│   └── connection.py (CountryDB)
│
├── monitoring/         # Health & Metrics
│   ├── health_monitor.py
│   ├── alert_manager.py
│   ├── dashboard.py
│   ├── resources.py
│   └── prometheus_exporter.py
│
├── network/            # Networking & Proxies
│   ├── proxy_pool.py
│   ├── tor_manager.py
│   ├── geo_router.py
│   └── ip_rotation.py
│
├── pipeline/           # Pipeline Orchestration
│   ├── base_scraper.py
│   ├── scraper_orchestrator.py
│   ├── pipeline_checkpoint.py
│   ├── frontier.py
│   └── preflight_checks.py
│
├── progress/           # Progress Tracking
│   ├── progress_tracker.py
│   ├── rich_progress.py
│   ├── run_ledger.py
│   └── report_generator.py
│
├── reliability/        # Resilience
│   ├── smart_retry.py
│   └── rate_limiter.py
│
├── utils/              # General Utilities
│   ├── logger.py
│   ├── shared_utils.py
│   ├── cache_manager.py
│   └── telegram_notifier.py
│
└── observability/      # OpenTelemetry (existing)
└── transform/          # Transformations (existing)
└── translation/        # Translation (existing)
```

## Migration Guide

### New Code
Use the new import paths for better clarity:
```python
from core.config.config_manager import ConfigManager
from core.browser.chrome_manager import ChromeManager
from core.utils.logger import get_logger
```

### Existing Code
Existing code works without changes:
```python
from core.config_manager import ConfigManager  # Works via stub
from core.logger import get_logger             # Works via stub
```

## Maintenance

When adding a new core module:
1. Place it in the appropriate subdirectory (e.g., `core/network/new_proxy.py`)
2. (Optional) If backward compatibility is needed, create a stub in `core/` that imports from the new location.

## Key Modules

### Most Used
- `core/config/config_manager.py` - Single source of truth for config
- `core/utils/logger.py` - Standardized logging
- `core/pipeline/base_scraper.py` - Base class for all scrapers
- `core/network/proxy_pool.py` - Smart proxy management

