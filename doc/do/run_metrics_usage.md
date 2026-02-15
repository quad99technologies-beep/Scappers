# Run Metrics Tracker Usage Guide

## Overview

The Run Metrics Tracker module tracks network consumption and active execution time for each scraper run. It only counts time and network usage when the script is actively running (excludes paused/idle periods).

## Features

- **Active Time Tracking**: Measures only actual execution time, not idle time
- **Network Consumption**: Tracks bytes sent and received during active execution
- **Pause/Resume Support**: Handles pipeline stops and resumes correctly
- **One Row Per Run**: Each run_id has a single record with aggregated metrics
- **Automatic Integration**: Works seamlessly with WorkflowRunner

## Files

- `core/run_metrics_tracker.py` - Core tracking functionality
- `core/run_metrics_integration.py` - Integration with WorkflowRunner
- `scripts/view_run_metrics.py` - CLI tool to view metrics

## Automatic Usage (Recommended)

### Via GUI (Scraper GUI)

When running scrapers through the **Scraper GUI**, metrics tracking is **automatic**:

1. Open `scraper_gui.py`
2. Select a scraper and click "Run Fresh Pipeline"
3. Metrics are automatically tracked for the entire pipeline run
4. If you click "Stop Pipeline", metrics are paused
5. When you resume, metrics continue from where they left off
6. View results in the **"Run Metrics"** tab

### Via WorkflowRunner (Code)

When using `WorkflowRunner` directly in code:

```python
from shared_workflow_runner import WorkflowRunner

runner = WorkflowRunner("Malaysia", scraper_root, repo_root)
result = runner.run(my_scraper)

# Metrics are automatically tracked and saved
```

When a run is stopped via `stop_pipeline()`, metrics are paused. When the run resumes, metrics continue from where they left off.

## Manual Usage

For custom scenarios, you can use the metrics tracker directly:

```python
from core.run_metrics_tracker import RunMetricsTracker

tracker = RunMetricsTracker()

# Start tracking
metrics = tracker.start_run("my_run_001", "Malaysia")

# ... do work ...

# Pause (e.g., user paused)
tracker.pause_run("my_run_001")

# Resume
tracker.resume_run("my_run_001")

# ... do more work ...

# Complete
final_metrics = tracker.complete_run("my_run_001", "completed")

print(f"Duration: {final_metrics.active_duration_seconds}s")
print(f"Network: {final_metrics.network_total_gb} GB")
```

## Viewing Metrics

### In the GUI

The **"Run Metrics"** tab in the Scraper GUI provides a visual interface to view all run metrics:

1. **Open the Scraper GUI**: `python scraper_gui.py`
2. **Click the "Run Metrics" tab** (next to Documentation)
3. **Features**:
   - **Filter by Scraper**: Select a specific scraper or view "All"
   - **Summary Statistics**: Shows total runs, duration, and network usage at the top
   - **Run History Table**: Lists all runs with:
     - Run ID
     - Scraper name
     - Status (completed, failed, etc.)
     - Active Duration (actual running time, excludes pauses)
     - Network Usage in GB
     - Start time
   - **Double-click a row** to see detailed metrics for that run
   - **Export to CSV**: Save all metrics to a CSV file
   - **Refresh**: Reload the latest metrics

### Command Line

```bash
# List all runs
python scripts/view_run_metrics.py

# Filter by scraper
python scripts/view_run_metrics.py --scraper Malaysia

# Show specific run
python scripts/view_run_metrics.py --run-id Malaysia_20240115_120000

# Show summary statistics
python scripts/view_run_metrics.py --summary

# Show summary for specific scraper
python scripts/view_run_metrics.py --summary --scraper Argentina

# Export to CSV
python scripts/view_run_metrics.py --export metrics.csv
```

### Programmatic Access

```python
from core.run_metrics_tracker import RunMetricsTracker

tracker = RunMetricsTracker()

# Get metrics for a specific run
metrics = tracker.get_metrics("Malaysia_20240115_120000")
if metrics:
    print(f"Duration: {metrics.active_duration_seconds}s")
    print(f"Network: {metrics.network_total_gb} GB")

# List all runs for a scraper
runs = tracker.list_metrics(scraper_name="Malaysia", limit=10)
for run in runs:
    print(f"{run.run_id}: {run.active_duration_seconds}s, {run.network_total_gb} GB")

# Get summary statistics
summary = tracker.get_summary(scraper_name="Malaysia")
print(f"Total runs: {summary['total_runs']}")
print(f"Total duration: {summary['total_duration_seconds']}s")
print(f"Total network: {summary['total_network_gb']} GB")
```

## Metrics Data Structure

Each run's metrics include:

| Field | Type | Description |
|-------|------|-------------|
| run_id | str | Unique run identifier |
| scraper_name | str | Name of the scraper |
| started_at | str | ISO timestamp when run started |
| ended_at | str | ISO timestamp when run ended |
| status | str | completed, failed, stopped, etc. |
| active_duration_seconds | float | Actual execution time (excludes pauses) |
| network_sent_bytes | int | Bytes sent during execution |
| network_received_bytes | int | Bytes received during execution |
| network_total_gb | float | Total network usage in GB |
| network_sent_mb | float | Sent data in MB |
| network_received_mb | float | Received data in MB |

## Cost Estimation

The Run Metrics tab and CLI tool include automatic cost estimation based on network usage:

- **Rate**: $5 per GB (approximate cloud data transfer cost)
- **Calculation**: `Total Network (GB) × $5 = Estimated Cost`
- **Display Locations**:
  - GUI: Summary section in Run Metrics tab
  - GUI: Run details dialog (double-click a row)
  - CLI: `view_run_metrics.py --summary`

Example:
- If a run uses 2.5 GB of network data
- Estimated cost = 2.5 GB × $5/GB = **$12.50**

This helps you track and budget for cloud/data transfer costs associated with running scrapers.

## Storage Location

Metrics are stored in JSON files at:
```
cache/run_metrics/<run_id>.json
```

## Testing

Run the test suite:

```bash
python testing/test_run_metrics.py
```

## Notes

- Network tracking uses `psutil` to measure process-level I/O when available
- On Windows, process-level I/O tracking may require appropriate permissions
- If process-level tracking is unavailable, system-wide network stats are used
- Metrics are saved to disk on each state change (start, pause, resume, complete)
- The tracker is thread-safe for concurrent access
