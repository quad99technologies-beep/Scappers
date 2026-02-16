#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
View Run Metrics

CLI tool to view network consumption and execution time metrics for scraper runs.

Usage:
    python tools/view_run_metrics.py                    # List all runs
    python tools/view_run_metrics.py --scraper Malaysia # Filter by scraper
    python tools/view_run_metrics.py --run-id <id>      # Show specific run
    python tools/view_run_metrics.py --summary          # Show summary stats
    python tools/view_run_metrics.py --last 10          # Show last 10 runs
"""

import sys
import argparse
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from core.progress.run_metrics_tracker import RunMetricsTracker, RunMetrics, format_metrics_summary
    _METRICS_AVAILABLE = True
except ImportError:
    _METRICS_AVAILABLE = False
    print("Error: Run metrics tracker not available. Make sure psutil is installed.")
    print("Install with: pip install psutil")
    sys.exit(1)


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def format_network(gb: float) -> str:
    """Format network usage in human-readable format."""
    if gb < 0.001:
        mb = gb * 1024
        return f"{mb:.2f} MB"
    elif gb < 1:
        return f"{gb*1000:.2f} MB"
    else:
        return f"{gb:.3f} GB"


def list_runs(scraper_name: str = None, limit: int = 50):
    """List all runs with their metrics."""
    tracker = RunMetricsTracker()
    metrics_list = tracker.list_metrics(scraper_name=scraper_name, limit=limit)
    
    if not metrics_list:
        print("No runs found.")
        return
    
    # Header
    print("\n" + "=" * 120)
    print(f"{'Run ID':<35} {'Scraper':<15} {'Status':<12} {'Duration':<12} {'Network (GB)':<15} {'Started At':<25}")
    print("=" * 120)
    
    # Rows
    for m in metrics_list:
        run_id = m.run_id[:34] if len(m.run_id) > 35 else m.run_id
        duration = format_duration(m.active_duration_seconds)
        network = format_network(m.network_total_gb)
        started = m.started_at[:24] if m.started_at else "N/A"
        
        print(f"{run_id:<35} {m.scraper_name:<15} {m.status:<12} {duration:<12} {network:<15} {started:<25}")
    
    print("=" * 120)
    print(f"Total: {len(metrics_list)} runs\n")


def show_run(run_id: str):
    """Show detailed metrics for a specific run."""
    tracker = RunMetricsTracker()
    metrics = tracker.get_metrics(run_id)
    
    if not metrics:
        print(f"Run not found: {run_id}")
        return
    
    print("\n" + "=" * 60)
    print("RUN METRICS DETAILS")
    print("=" * 60)
    print(format_metrics_summary(metrics))
    print("=" * 60 + "\n")


def show_summary(scraper_name: str = None):
    """Show summary statistics."""
    tracker = RunMetricsTracker()
    summary = tracker.get_summary(scraper_name=scraper_name)
    
    # Calculate cost (approx $5 per GB)
    COST_PER_GB = 5.0
    total_cost = summary['total_network_gb'] * COST_PER_GB
    avg_cost = summary['avg_network_gb'] * COST_PER_GB
    
    print("\n" + "=" * 60)
    if scraper_name:
        print(f"SUMMARY FOR {scraper_name}")
    else:
        print("OVERALL SUMMARY")
    print("=" * 60)
    
    print(f"  Total Runs:           {summary['total_runs']}")
    print(f"  Total Duration:       {format_duration(summary['total_duration_seconds'])}")
    print(f"                      ({summary['total_duration_hours']:.2f} hours)")
    print(f"  Total Network:        {format_network(summary['total_network_gb'])}")
    print(f"  Avg Duration/Run:     {format_duration(summary['avg_duration_seconds'])}")
    print(f"  Avg Network/Run:      {format_network(summary['avg_network_gb'])}")
    print("-" * 60)
    print(f"  TOTAL NETWORK USED:   {format_network(summary['total_network_gb'])}")
    print(f"  ESTIMATED COST:       ${total_cost:.2f} (at ${COST_PER_GB}/GB)")
    print(f"  Avg Cost/Run:         ${avg_cost:.2f}")
    print("=" * 60 + "\n")


def export_csv(output_file: str, scraper_name: str = None):
    """Export metrics to CSV file."""
    import csv
    
    tracker = RunMetricsTracker()
    metrics_list = tracker.list_metrics(scraper_name=scraper_name, limit=10000)
    
    if not metrics_list:
        print("No runs to export.")
        return
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'run_id', 'scraper_name', 'status', 'started_at', 'ended_at',
            'active_duration_seconds', 'active_duration_minutes',
            'network_sent_bytes', 'network_received_bytes',
            'network_total_gb', 'network_sent_mb', 'network_received_mb'
        ])
        
        for m in metrics_list:
            writer.writerow([
                m.run_id,
                m.scraper_name,
                m.status,
                m.started_at,
                m.ended_at,
                m.active_duration_seconds,
                round(m.active_duration_seconds / 60, 2),
                m.network_sent_bytes,
                m.network_received_bytes,
                round(m.network_total_gb, 6),
                round(m.network_sent_mb, 2),
                round(m.network_received_mb, 2),
            ])
    
    print(f"Exported {len(metrics_list)} runs to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="View run metrics (network consumption and execution time)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # List all runs
  %(prog)s --scraper Malaysia                 # Filter by scraper
  %(prog)s --run-id Malaysia_20240115_120000  # Show specific run
  %(prog)s --summary                          # Show summary statistics
  %(prog)s --summary --scraper Argentina      # Summary for specific scraper
  %(prog)s --last 20                          # Show last 20 runs
  %(prog)s --export metrics.csv               # Export to CSV
        """
    )
    
    parser.add_argument("--scraper", "-s", help="Filter by scraper name")
    parser.add_argument("--run-id", "-r", help="Show specific run details")
    parser.add_argument("--summary", "-S", action="store_true", help="Show summary statistics")
    parser.add_argument("--last", "-n", type=int, default=50, help="Limit number of runs (default: 50)")
    parser.add_argument("--export", "-e", help="Export to CSV file")
    
    args = parser.parse_args()
    
    if args.run_id:
        show_run(args.run_id)
    elif args.summary:
        show_summary(args.scraper)
    elif args.export:
        export_csv(args.export, args.scraper)
    else:
        list_runs(args.scraper, args.last)


if __name__ == "__main__":
    main()
