"""
Statistics module - passive observer for scraper metrics.
Hooks into StepHookRegistry without touching scraper logic.
Tracks counts per scraper, emits summary at pipeline end.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class StepStats:
    step_number: int
    step_name: str
    records_extracted: int = 0
    records_valid: int = 0
    records_rejected: int = 0
    duplicates: int = 0
    request_count: int = 0
    error_count: int = 0
    duration_seconds: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        total = self.records_extracted
        if total == 0:
            return 0.0
        return round(self.records_valid / total * 100, 1)


@dataclass
class ScraperRunStats:
    scraper_name: str
    run_id: str
    steps: Dict[int, StepStats] = field(default_factory=dict)
    pipeline_started_at: Optional[datetime] = None
    pipeline_completed_at: Optional[datetime] = None

    @property
    def total_records_extracted(self) -> int:
        return sum(s.records_extracted for s in self.steps.values())

    @property
    def total_records_valid(self) -> int:
        return sum(s.records_valid for s in self.steps.values())

    @property
    def total_records_rejected(self) -> int:
        return sum(s.records_rejected for s in self.steps.values())

    @property
    def total_duplicates(self) -> int:
        return sum(s.duplicates for s in self.steps.values())

    @property
    def total_requests(self) -> int:
        return sum(s.request_count for s in self.steps.values())

    @property
    def total_errors(self) -> int:
        return sum(s.error_count for s in self.steps.values())

    @property
    def overall_success_rate(self) -> float:
        total = self.total_records_extracted
        if total == 0:
            return 0.0
        return round(self.total_records_valid / total * 100, 1)

    @property
    def total_duration_seconds(self) -> float:
        if self.pipeline_started_at and self.pipeline_completed_at:
            return (
                self.pipeline_completed_at - self.pipeline_started_at
            ).total_seconds()
        return sum(s.duration_seconds for s in self.steps.values())

    def summary(self) -> Dict[str, Any]:
        return {
            "scraper": self.scraper_name,
            "run_id": self.run_id,
            "total_extracted": self.total_records_extracted,
            "total_valid": self.total_records_valid,
            "total_rejected": self.total_records_rejected,
            "total_duplicates": self.total_duplicates,
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "success_rate_pct": self.overall_success_rate,
            "duration_seconds": round(self.total_duration_seconds, 1),
            "steps_completed": len(
                [s for s in self.steps.values() if s.completed_at]
            ),
            "steps_total": len(self.steps),
        }


class ScraperStatsCollector:
    """
    Passive observer that collects statistics via StepHookRegistry.

    Usage:
        collector = ScraperStatsCollector("Argentina", run_id)
        collector.register_hooks()

        # ... pipeline runs normally ...

        # At end:
        stats = collector.get_stats()
        print(stats.summary())
        collector.save_to_db(db)
    """

    def __init__(self, scraper_name: str, run_id: str):
        self._stats = ScraperRunStats(
            scraper_name=scraper_name,
            run_id=run_id,
        )
        self._step_start_times: Dict[int, float] = {}

    def register_hooks(self) -> None:
        """Register as observer on StepHookRegistry. No-op if unavailable."""
        try:
            from core.pipeline.step_hooks import StepHookRegistry
            StepHookRegistry.register_start_hook(self._on_step_start)
            StepHookRegistry.register_end_hook(self._on_step_end)
            StepHookRegistry.register_error_hook(self._on_step_error)
            logger.debug("ScraperStatsCollector hooks registered")
        except ImportError:
            logger.debug("StepHookRegistry not available, stats disabled")

    def _on_step_start(self, metrics) -> None:
        """Called when a step starts."""
        step_num = metrics.step_number
        self._step_start_times[step_num] = time.monotonic()
        if step_num not in self._stats.steps:
            self._stats.steps[step_num] = StepStats(
                step_number=step_num,
                step_name=metrics.step_name,
                started_at=datetime.now(),
            )
        if self._stats.pipeline_started_at is None:
            self._stats.pipeline_started_at = datetime.now()

    def _on_step_end(self, metrics) -> None:
        """Called when a step completes."""
        step_num = metrics.step_number
        step = self._stats.steps.get(step_num)
        if step:
            step.completed_at = datetime.now()
            start_time = self._step_start_times.get(step_num)
            if start_time:
                step.duration_seconds = time.monotonic() - start_time
            step.records_extracted = metrics.rows_read or 0
            step.records_valid = metrics.rows_inserted or 0
            step.records_rejected = metrics.rows_rejected or 0
        self._stats.pipeline_completed_at = datetime.now()

    def _on_step_error(self, metrics, error) -> None:
        """Called when a step errors."""
        step_num = metrics.step_number
        step = self._stats.steps.get(step_num)
        if step:
            step.error_count += 1
            start_time = self._step_start_times.get(step_num)
            if start_time:
                step.duration_seconds = time.monotonic() - start_time

    # ── Manual recording (for scrapers not using hooks) ──

    def record_extraction(self, step: int, extracted: int = 0,
                          valid: int = 0, rejected: int = 0,
                          duplicates: int = 0) -> None:
        """Manually record extraction counts for a step."""
        if step not in self._stats.steps:
            self._stats.steps[step] = StepStats(
                step_number=step, step_name=f"Step {step}"
            )
        s = self._stats.steps[step]
        s.records_extracted += extracted
        s.records_valid += valid
        s.records_rejected += rejected
        s.duplicates += duplicates

    def record_request(self, step: int, count: int = 1) -> None:
        """Record HTTP request count."""
        if step not in self._stats.steps:
            self._stats.steps[step] = StepStats(
                step_number=step, step_name=f"Step {step}"
            )
        self._stats.steps[step].request_count += count

    def record_error(self, step: int, count: int = 1) -> None:
        """Record error count."""
        if step not in self._stats.steps:
            self._stats.steps[step] = StepStats(
                step_number=step, step_name=f"Step {step}"
            )
        self._stats.steps[step].error_count += count

    # ── Output ──────────────────────────────────────────────

    def get_stats(self) -> ScraperRunStats:
        return self._stats

    def save_to_db(self, db) -> None:
        """Persist statistics to DB. Creates table if needed."""
        import json
        try:
            db.execute("""
                CREATE TABLE IF NOT EXISTS scraper_run_statistics (
                    id SERIAL PRIMARY KEY,
                    scraper_name TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    stats_json JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(scraper_name, run_id)
                )
            """)
            db.execute("""
                INSERT INTO scraper_run_statistics
                    (scraper_name, run_id, stats_json)
                VALUES (%s, %s, %s)
                ON CONFLICT (scraper_name, run_id)
                DO UPDATE SET stats_json = EXCLUDED.stats_json,
                              created_at = CURRENT_TIMESTAMP
            """, (
                self._stats.scraper_name,
                self._stats.run_id,
                json.dumps(self._stats.summary(), default=str),
            ))
            logger.info(
                f"Stats saved for {self._stats.scraper_name} "
                f"run {self._stats.run_id}"
            )
        except Exception as e:
            logger.warning(f"Could not save stats to DB: {e}")

    def print_summary(self) -> None:
        """Print human-readable summary."""
        s = self._stats.summary()
        lines = [
            f"{'='*50}",
            f"  SCRAPER STATISTICS: {s['scraper']}",
            f"  Run ID: {s['run_id']}",
            f"{'='*50}",
            f"  Records extracted:  {s['total_extracted']}",
            f"  Records valid:      {s['total_valid']}",
            f"  Records rejected:   {s['total_rejected']}",
            f"  Duplicates:         {s['total_duplicates']}",
            f"  Requests:           {s['total_requests']}",
            f"  Errors:             {s['total_errors']}",
            f"  Success rate:       {s['success_rate_pct']}%",
            f"  Duration:           {s['duration_seconds']}s",
            f"  Steps completed:    {s['steps_completed']}/{s['steps_total']}",
            f"{'='*50}",
        ]
        for line in lines:
            logger.info(line)
