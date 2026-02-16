#!/usr/bin/env python3
"""
Watchdog for Stale Job Recovery.

This script monitors the pipeline_runs table and:
- Detects stale runs (heartbeat timeout)
- Requeues them for retry
- Marks exceeded-retry jobs as failed
- Cleans up offline workers

Run this as a cron job or separate process:
    python watchdog.py [--interval 120] [--timeout 600]

Arguments:
    --interval: Check interval in seconds (default: 120)
    --timeout: Heartbeat timeout in seconds (default: 600)
    --once: Run once and exit
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services.db import (
    get_cursor,
    get_connection,
    get_stale_runs,
    requeue_stale_runs,
    ensure_platform_schema,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [watchdog] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


def mark_exceeded_retries_failed(max_retries: int = 3) -> int:
    """
    Mark jobs that exceeded max retries as failed.
    
    Args:
        max_retries: Maximum retry count
        
    Returns:
        Number of jobs marked as failed
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET status = 'failed',
                error_message = 'Exceeded maximum retries',
                ended_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE status = 'queued'
            AND retry_count >= max_retries
        """)
        
        count = cur.rowcount
        if count > 0:
            log.warning(f"Marked {count} job(s) as failed (exceeded retries)")
        return count


def cleanup_offline_workers(timeout_seconds: int = 300) -> int:
    """
    Mark workers with stale heartbeats as offline.
    
    Args:
        timeout_seconds: Heartbeat timeout in seconds
        
    Returns:
        Number of workers marked offline
    """
    with get_cursor() as cur:
        cur.execute("""
            UPDATE workers
            SET status = 'offline',
                current_run_id = NULL
            WHERE status != 'offline'
            AND last_heartbeat < CURRENT_TIMESTAMP - INTERVAL '%s seconds'
        """, (timeout_seconds,))
        
        count = cur.rowcount
        if count > 0:
            log.info(f"Marked {count} worker(s) as offline")
        return count


def get_run_statistics() -> dict:
    """Get current run statistics."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'queued') AS queued,
                COUNT(*) FILTER (WHERE status = 'running') AS running,
                COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                COUNT(*) FILTER (WHERE status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE status = 'stopped') AS stopped
            FROM pipeline_runs
            WHERE created_at > CURRENT_DATE - INTERVAL '7 days'
        """)
        
        row = cur.fetchone()
        return {
            'queued': row[0],
            'running': row[1],
            'completed': row[2],
            'failed': row[3],
            'stopped': row[4]
        }


def get_worker_statistics() -> dict:
    """Get current worker statistics."""
    with get_cursor() as cur:
        cur.execute("""
            SELECT 
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status = 'active') AS active,
                COUNT(*) FILTER (WHERE status = 'busy') AS busy,
                COUNT(*) FILTER (WHERE status = 'idle') AS idle,
                COUNT(*) FILTER (WHERE status = 'offline') AS offline
            FROM workers
        """)
        
        row = cur.fetchone()
        return {
            'total': row[0],
            'active': row[1],
            'busy': row[2],
            'idle': row[3],
            'offline': row[4]
        }


def run_watchdog(
    heartbeat_timeout: int = 600,
    worker_timeout: int = 300,
    max_retries: int = 3,
    verbose: bool = False
) -> dict:
    """
    Run one watchdog check cycle.
    
    Args:
        heartbeat_timeout: Job heartbeat timeout in seconds
        worker_timeout: Worker heartbeat timeout in seconds
        max_retries: Maximum job retry count
        verbose: Log statistics
        
    Returns:
        Dict with action counts
    """
    results = {
        'stale_runs_found': 0,
        'runs_requeued': 0,
        'runs_failed': 0,
        'workers_offline': 0,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Check for stale runs
        stale_runs = get_stale_runs(heartbeat_timeout)
        results['stale_runs_found'] = len(stale_runs)
        
        if stale_runs:
            log.warning(f"Found {len(stale_runs)} stale run(s):")
            for run in stale_runs:
                log.warning(f"  - {run['run_id']} ({run['country']}) - worker: {run['worker_id']}")
        
        # Requeue stale runs
        requeued = requeue_stale_runs(heartbeat_timeout)
        results['runs_requeued'] = requeued
        if requeued > 0:
            log.info(f"Requeued {requeued} stale run(s)")
        
        # Mark exceeded retries as failed
        failed = mark_exceeded_retries_failed(max_retries)
        results['runs_failed'] = failed
        
        # Cleanup offline workers
        offline = cleanup_offline_workers(worker_timeout)
        results['workers_offline'] = offline
        
        # Log statistics if verbose
        if verbose:
            run_stats = get_run_statistics()
            worker_stats = get_worker_statistics()
            log.info(f"Runs: queued={run_stats['queued']}, running={run_stats['running']}, "
                    f"completed={run_stats['completed']}, failed={run_stats['failed']}")
            log.info(f"Workers: active={worker_stats['active']}, busy={worker_stats['busy']}, "
                    f"idle={worker_stats['idle']}, offline={worker_stats['offline']}")
        
    except Exception as e:
        log.error(f"Watchdog error: {e}")
        import traceback
        log.debug(traceback.format_exc())
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Watchdog for stale job recovery")
    parser.add_argument(
        "--interval",
        type=int,
        default=120,
        help="Check interval in seconds (default: 120)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Heartbeat timeout in seconds (default: 600)"
    )
    parser.add_argument(
        "--worker-timeout",
        type=int,
        default=300,
        help="Worker heartbeat timeout in seconds (default: 300)"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum job retries (default: 3)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Log statistics each cycle"
    )
    
    args = parser.parse_args()
    
    log.info(f"Watchdog starting: interval={args.interval}s, timeout={args.timeout}s")
    
    # Ensure schema exists
    try:
        ensure_platform_schema()
    except Exception as e:
        log.warning(f"Could not ensure schema: {e}")
    
    try:
        while True:
            results = run_watchdog(
                heartbeat_timeout=args.timeout,
                worker_timeout=args.worker_timeout,
                max_retries=args.max_retries,
                verbose=args.verbose
            )
            
            if args.once:
                log.info(f"Single run mode, results: {results}")
                break
            
            log.debug(f"Sleeping {args.interval}s until next check")
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        log.info("Watchdog stopped by user")


if __name__ == "__main__":
    main()
