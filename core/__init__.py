# Core modules for Scraper Platform
"""
Core utilities and intelligence modules for the Scraper Platform.

ORGANIZED STRUCTURE:
- core/browser/     : Browser & automation (Chrome, Firefox, stealth)
- core/config/      : Configuration management
- core/data/        : Data processing, validation & quality
- core/db/          : Database connections & models
- core/monitoring/  : Alerting, health checks, metrics
- core/network/     : Proxies, Tor, IP rotation
- core/pipeline/    : Pipeline orchestration & checkpoints
- core/progress/    : Progress tracking & reporting
- core/reliability/ : Rate limiting & smart retries
- core/utils/       : Logging, caching, helpers

The old module locations (e.g., core.config_manager) continue to work
via backward-compatibility stubs.
"""

__version__ = "3.0.0"

# Explicitly export common components for convenience if needed,
# but usually users just import what they need.
