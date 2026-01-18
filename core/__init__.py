# Core modules for Scraper Platform
"""
Core utilities and intelligence modules for the Scraper Platform.

EXISTING MODULES (unchanged):
- config_manager: Configuration and path management
- chrome_manager: Chrome WebDriver instance tracking
- chrome_pid_tracker: Chrome process ID tracking
- firefox_pid_tracker: Firefox process ID tracking
- logger: Logging utilities
- pipeline_checkpoint: Checkpoint/resume functionality
- retry_config: Retry configuration
- shared_utils: Shared utility functions
- run_ledger: Run metadata tracking
- diagnostics_exporter: Diagnostics bundle export

NEW INTELLIGENCE MODULES (no business logic changes):
- data_validator: Post-processing data validation with pandera
- deduplicator: Fuzzy deduplication with rapidfuzz
- anomaly_detector: Price anomaly detection with scikit-learn
- smart_retry: Intelligent retry wrappers with tenacity
- rate_limiter: Rate limiting decorators
- cache_manager: Disk-based caching layer
- rich_progress: Enhanced progress display with rich
- report_generator: Automated report generation
- health_monitor: Website health monitoring
- data_diff: Change detection between runs

All new modules gracefully degrade if their dependencies are not installed.
Existing scraping logic and business rules remain UNCHANGED.
"""

__version__ = "2.0.0"
