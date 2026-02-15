# Run Metrics Tracker - Implementation Summary

## Overview
A module to track network consumption and active execution time for each scraper run. Only counts when the script is actively running (excludes idle/paused periods).

## Files Created

### Core Module
- **`core/run_metrics_tracker.py`** - Main tracking module
  - `RunMetrics` dataclass - Stores metrics for a single run
  - `RunMetricsTracker` class - Tracks metrics with pause/resume support
  - Thread-safe implementation
  - Automatic disk persistence

### Integration Module
- **`core/run_metrics_integration.py`** - Integration with WorkflowRunner
  - `WorkflowMetricsIntegration` class - Hooks into workflow lifecycle
  - `MetricsContextManager` - Context manager for easy usage
  - Automatic pause on pipeline stop, resume on restart

### CLI Tool
- **`scripts/view_run_metrics.py`** - Command-line interface
  - List runs with metrics
  - Filter by scraper
  - Show detailed run info
  - Export to CSV
  - Summary statistics

### Tests
- **`testing/test_run_metrics.py`** - Test suite
  - Basic tracking test
  - Pause/resume test
  - Multiple runs test
  - Summary statistics test
  - Integration test

### Documentation
- **`doc/run_metrics_usage.md`** - Usage guide

## Integration with WorkflowRunner

The `shared_workflow_runner.py` has been updated to:
1. Import the metrics integration module
2. Initialize `metrics_integration` in `WorkflowRunner.__init__`
3. Start metrics tracking when a run starts
4. Complete metrics tracking when a run ends (success/failure)
5. Pause metrics when `stop_pipeline()` is called

## Usage Examples

### Automatic (via GUI)
Just run any scraper through the GUI - metrics are tracked automatically!

### Automatic (via WorkflowRunner)
```python
runner = WorkflowRunner("Malaysia", scraper_root, repo_root)
result = runner.run(my_scraper)  # Metrics tracked automatically
```

### Manual
```python
from core.run_metrics_tracker import RunMetricsTracker

tracker = RunMetricsTracker()
tracker.start_run("run_001", "Malaysia")
# ... work ...
tracker.pause_run("run_001")  # User paused
# ... idle ...
tracker.resume_run("run_001")  # User resumed
# ... more work ...
metrics = tracker.complete_run("run_001", "completed")
```

### Viewing Metrics

#### In the GUI
1. Open `scraper_gui.py`
2. Click the **"Run Metrics"** tab
3. Filter by scraper, view summary, export to CSV
4. Double-click a row for detailed metrics

#### Command Line
```bash
# List all runs
python scripts/view_run_metrics.py

# Summary
python scripts/view_run_metrics.py --summary

# Export to CSV
python scripts/view_run_metrics.py --export metrics.csv
```

## Data Stored Per Run

```json
{
  "run_id": "Malaysia_20240115_120000",
  "scraper_name": "Malaysia",
  "started_at": "2024-01-15T12:00:00+00:00",
  "ended_at": "2024-01-15T12:30:00+00:00",
  "status": "completed",
  "active_duration_seconds": 1800.0,
  "network_sent_bytes": 10485760,
  "network_received_bytes": 52428800,
  "network_total_gb": 0.058,
  "network_sent_mb": 10.0,
  "network_received_mb": 50.0
}
```

## Key Features

1. **Active Time Only**: Only counts time when script is running (not paused)
2. **Pause/Resume**: Handles pipeline stops and restarts correctly
3. **Network Tracking**: Uses psutil to track bytes sent/received
4. **One Row Per Run**: Single record per run_id with aggregated data
5. **Thread-Safe**: Safe for concurrent access
6. **Persistent**: Saved to disk on each state change
7. **Automatic**: Works transparently with WorkflowRunner

## Storage

Metrics stored at: `cache/run_metrics/<run_id>.json`

## Dependencies

- `psutil` - For network I/O tracking
- Standard library: `json`, `time`, `threading`, `dataclasses`, `pathlib`
