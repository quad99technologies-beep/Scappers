#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test Run Metrics Tracker

Simple test script to verify the run metrics tracking functionality.
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

def test_basic_tracking():
    """Test basic metrics tracking."""
    print("\n" + "=" * 60)
    print("TEST 1: Basic Tracking")
    print("=" * 60)
    
    try:
        from core.progress.run_metrics_tracker import RunMetricsTracker
    except ImportError as e:
        print(f"FAIL: Could not import RunMetricsTracker: {e}")
        return False
    
    tracker = RunMetricsTracker()
    run_id = f"test_basic_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Start run
    print(f"Starting run: {run_id}")
    metrics = tracker.start_run(run_id, "TestScraper")
    print(f"  Started at: {metrics.started_at}")
    
    # Simulate work
    print("  Working for 2 seconds...")
    time.sleep(2)
    
    # Complete run
    print("  Completing run...")
    final = tracker.complete_run(run_id, "completed")
    
    print(f"  Duration: {final.active_duration_seconds:.2f}s")
    print(f"  Network: {final.network_total_gb:.6f} GB")
    
    # Verify
    if final.active_duration_seconds >= 1.5:  # Should be at least 1.5s
        print("  [OK] Duration tracking OK")
    else:
        print(f"  [FAIL] Duration tracking failed: {final.active_duration_seconds}s")
        return False
    
    print("  [OK] Test passed")
    return True


def test_pause_resume():
    """Test pause and resume functionality."""
    print("\n" + "=" * 60)
    print("TEST 2: Pause and Resume")
    print("=" * 60)
    
    from core.progress.run_metrics_tracker import RunMetricsTracker
    
    tracker = RunMetricsTracker()
    run_id = f"test_pause_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Start run
    print(f"Starting run: {run_id}")
    tracker.start_run(run_id, "TestScraper")
    
    # Work 1 second
    print("  Working for 1 second...")
    time.sleep(1)
    
    # Pause
    print("  Pausing...")
    paused = tracker.pause_run(run_id)
    paused_duration = paused.active_duration_seconds
    print(f"  Duration after pause: {paused_duration:.2f}s")
    
    # Idle 2 seconds (should not count)
    print("  Idling for 2 seconds (should not count)...")
    time.sleep(2)
    
    # Resume
    print("  Resuming...")
    tracker.resume_run(run_id)
    
    # Work 1 more second
    print("  Working for 1 more second...")
    time.sleep(1)
    
    # Complete
    print("  Completing...")
    final = tracker.complete_run(run_id, "completed")
    
    print(f"  Final duration: {final.active_duration_seconds:.2f}s")
    print(f"  Expected: ~2s (not ~4s)")
    
    # Verify - should be around 2s, not 4s
    if 1.5 <= final.active_duration_seconds <= 3.0:
        print("  [OK] Pause/Resume tracking OK")
    else:
        print(f"  [FAIL] Pause/Resume tracking failed: {final.active_duration_seconds}s")
        return False
    
    print("  [OK] Test passed")
    return True


def test_multiple_runs():
    """Test tracking multiple runs."""
    print("\n" + "=" * 60)
    print("TEST 3: Multiple Runs")
    print("=" * 60)
    
    from core.progress.run_metrics_tracker import RunMetricsTracker
    
    tracker = RunMetricsTracker()
    base_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create 3 runs
    for i in range(3):
        run_id = f"test_multi_{base_id}_{i}"
        print(f"  Run {i+1}: {run_id}")
        tracker.start_run(run_id, "TestScraper")
        time.sleep(0.5)
        tracker.complete_run(run_id, "completed")
    
    # List runs
    runs = tracker.list_metrics(scraper_name="TestScraper", limit=10)
    test_runs = [r for r in runs if r.run_id.startswith("test_multi_")]
    
    print(f"  Created {len(test_runs)} test runs")
    
    if len(test_runs) >= 3:
        print("  [OK] Multiple runs tracking OK")
    else:
        print(f"  [FAIL] Multiple runs tracking failed: found {len(test_runs)} runs")
        return False
    
    print("  [OK] Test passed")
    return True


def test_summary():
    """Test summary statistics."""
    print("\n" + "=" * 60)
    print("TEST 4: Summary Statistics")
    print("=" * 60)
    
    from core.progress.run_metrics_tracker import RunMetricsTracker
    
    tracker = RunMetricsTracker()
    summary = tracker.get_summary(scraper_name="TestScraper")
    
    print(f"  Total runs: {summary['total_runs']}")
    print(f"  Total duration: {summary['total_duration_seconds']:.2f}s")
    print(f"  Total network: {summary['total_network_gb']:.6f} GB")
    
    if summary['total_runs'] > 0:
        print("  [OK] Summary generation OK")
    else:
        print("  [FAIL] Summary generation failed")
        return False
    
    print("  [OK] Test passed")
    return True


def test_integration():
    """Test integration with WorkflowRunner."""
    print("\n" + "=" * 60)
    print("TEST 5: Integration Module")
    print("=" * 60)
    
    try:
        from core.run_metrics_integration import WorkflowMetricsIntegration
    except ImportError as e:
        print(f"FAIL: Could not import WorkflowMetricsIntegration: {e}")
        return False
    
    integration = WorkflowMetricsIntegration()
    
    if not integration.is_available():
        print("FAIL: Metrics integration not available")
        return False
    
    run_id = f"test_integration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Test start
    print(f"Starting run: {run_id}")
    metrics = integration.on_run_start(run_id, "TestScraper")
    if not metrics:
        print("FAIL: Could not start metrics tracking")
        return False
    
    time.sleep(1)
    
    # Test pause
    print("  Pausing...")
    paused = integration.on_run_pause(run_id)
    if not paused:
        print("FAIL: Could not pause metrics tracking")
        return False
    
    time.sleep(0.5)
    
    # Test resume
    print("  Resuming...")
    resumed = integration.on_run_resume(run_id)
    if not resumed:
        print("FAIL: Could not resume metrics tracking")
        return False
    
    time.sleep(1)
    
    # Test complete
    print("  Completing...")
    final = integration.on_run_complete(run_id, "completed")
    if not final:
        print("FAIL: Could not complete metrics tracking")
        return False
    
    print(f"  Final duration: {final.active_duration_seconds:.2f}s")
    print(f"  Final network: {final.network_total_gb:.6f} GB")
    
    if 1.5 <= final.active_duration_seconds <= 3.0:
        print("  [OK] Integration test OK")
    else:
        print(f"  [FAIL] Integration test failed: duration {final.active_duration_seconds}s")
        return False
    
    print("  [OK] Test passed")
    return True


def cleanup_test_data():
    """Clean up test data."""
    print("\n" + "=" * 60)
    print("CLEANUP: Removing test data")
    print("=" * 60)
    
    from core.progress.run_metrics_tracker import RunMetricsTracker
    
    tracker = RunMetricsTracker()
    runs = tracker.list_metrics(scraper_name="TestScraper", limit=100)
    
    deleted = 0
    for run in runs:
        if run.run_id.startswith("test_"):
            if tracker.delete_metrics(run.run_id):
                deleted += 1
    
    print(f"  Deleted {deleted} test runs")
    print("  [OK] Cleanup complete")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("RUN METRICS TRACKER TEST SUITE")
    print("=" * 60)
    
    results = []
    
    # Run tests
    results.append(("Basic Tracking", test_basic_tracking()))
    results.append(("Pause/Resume", test_pause_resume()))
    results.append(("Multiple Runs", test_multiple_runs()))
    results.append(("Summary", test_summary()))
    results.append(("Integration", test_integration()))
    
    # Cleanup
    cleanup_test_data()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status}: {name}")
    
    print("=" * 60)
    print(f"Result: {passed}/{total} tests passed")
    print("=" * 60 + "\n")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
