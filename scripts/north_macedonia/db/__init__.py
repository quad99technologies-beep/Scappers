# North Macedonia database layer
"""
Provides PostgreSQL-backed storage for North Macedonia scraper.
All data operations go through the repository pattern.

Modules:
- repositories: NorthMacedoniaRepository class with all DB operations
- schema: DDL for nm_* tables
- validator: DataValidator for quality checks
- statistics: StatisticsCollector for metrics and reporting
"""

from .repositories import NorthMacedoniaRepository
from .schema import apply_schema, apply_north_macedonia_schema
from .validator import DataValidator
from .statistics import StatisticsCollector

__all__ = [
    "NorthMacedoniaRepository",
    "apply_schema",
    "apply_north_macedonia_schema",  # Backward compatibility
    "DataValidator",
    "StatisticsCollector",
]
