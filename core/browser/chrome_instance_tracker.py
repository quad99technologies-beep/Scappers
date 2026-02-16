"""
Standardized Chrome Instance Tracking

Provides shared utilities for tracking browser instances across all scrapers.
Replaces country-specific implementations with a unified approach.

Usage:
    from core.browser.chrome_instance_tracker import ChromeInstanceTracker
    
    tracker = ChromeInstanceTracker(scraper_name, run_id, db)
    instance_id = tracker.register(step_number=1, thread_id=0, pid=12345)
    tracker.mark_terminated(instance_id, reason="cleanup")
"""

import json
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timedelta


class ChromeInstanceTracker:
    """
    Standardized Chrome instance tracking for all scrapers.
    
    Tracks browser instances per run/step/thread for cleanup and monitoring.
    """
    
    def __init__(self, scraper_name: str, run_id: str, db):
        """
        Initialize tracker.
        
        Args:
            scraper_name: Name of scraper (e.g., "Malaysia", "Argentina")
            run_id: Current run ID
            db: Database connection (PostgresDB or CountryDB)
        """
        self.scraper_name = scraper_name
        self.run_id = run_id
        self.db = db
    
    def register(
        self,
        step_number: int,
        pid: int,
        thread_id: Optional[int] = None,
        browser_type: str = "chrome",
        parent_pid: Optional[int] = None,
        user_data_dir: Optional[str] = None,
        child_pids: Optional[Set[int]] = None
    ) -> int:
        """
        Register a Chrome/browser instance.
        
        Args:
            step_number: Pipeline step number
            pid: Process ID of driver (chromedriver/geckodriver)
            thread_id: Worker thread ID (if multi-threaded)
            browser_type: 'chrome', 'chromium', or 'firefox'
            parent_pid: Parent process ID (chromedriver/playwright)
            user_data_dir: Path to user data directory
            child_pids: Full set of PIDs to kill (driver + browser children) for pipeline stop
        
        Returns:
            Instance ID
        """
        all_pids_list = list(child_pids) if child_pids else [pid]
        if pid not in all_pids_list:
            all_pids_list = [pid] + [p for p in all_pids_list if p != pid]
        all_pids_json = json.dumps(all_pids_list)
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    INSERT INTO chrome_instances
                    (run_id, scraper_name, step_number, thread_id, browser_type, pid, parent_pid, user_data_dir, all_pids)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                """, (
                    self.run_id,
                    self.scraper_name,
                    step_number,
                    thread_id,
                    browser_type,
                    pid,
                    parent_pid,
                    user_data_dir,
                    all_pids_json
                ))
                row = cur.fetchone()
                instance_id = row[0] if row else 0
                self.db.commit()
                return instance_id
        except Exception as e:
            # Non-blocking: tracking failure shouldn't break scraping
            return 0
    
    def mark_terminated(self, instance_id: int, reason: str = "cleanup") -> bool:
        """
        Mark a Chrome instance as terminated.
        
        Args:
            instance_id: Instance ID from register()
            reason: Termination reason (e.g., "cleanup", "error", "completed")
        
        Returns:
            True if successful
        """
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    UPDATE chrome_instances
                    SET terminated_at = CURRENT_TIMESTAMP, termination_reason = %s
                    WHERE id = %s AND run_id = %s AND scraper_name = %s
                """, (reason, instance_id, self.run_id, self.scraper_name))
                self.db.commit()
                return cur.rowcount > 0
        except Exception:
            return False
    
    def mark_terminated_by_pid(self, pid: int, reason: str = "cleanup") -> bool:
        """
        Mark a Chrome instance as terminated by PID.
        
        Args:
            pid: Process ID
            reason: Termination reason
        
        Returns:
            True if successful
        """
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    UPDATE chrome_instances
                    SET terminated_at = CURRENT_TIMESTAMP, termination_reason = %s
                    WHERE pid = %s AND run_id = %s AND scraper_name = %s
                    AND terminated_at IS NULL
                """, (reason, pid, self.run_id, self.scraper_name))
                self.db.commit()
                return cur.rowcount > 0
        except Exception:
            return False
    
    def get_active_instances(self) -> List[Dict[str, Any]]:
        """
        Get all active (not terminated) Chrome instances for this run.
        
        Returns:
            List of instance dicts
        """
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    SELECT id, step_number, thread_id, browser_type, pid, parent_pid,
                           started_at, user_data_dir
                    FROM chrome_instances
                    WHERE run_id = %s AND scraper_name = %s AND terminated_at IS NULL
                    ORDER BY started_at
                """, (self.run_id, self.scraper_name))
                
                rows = cur.fetchall()
                return [
                    {
                        "id": r[0],
                        "step_number": r[1],
                        "thread_id": r[2],
                        "browser_type": r[3],
                        "pid": r[4],
                        "parent_pid": r[5],
                        "started_at": r[6],
                        "user_data_dir": r[7]
                    }
                    for r in rows
                ]
        except Exception:
            return []
    
    def get_orphaned_instances(self, max_age_hours: int = 2) -> List[Dict[str, Any]]:
        """
        Get Chrome instances that have been running too long (likely orphaned).
        
        Args:
            max_age_hours: Maximum age in hours before considered orphaned
        
        Returns:
            List of orphaned instance dicts
        """
        try:
            cutoff = datetime.now() - timedelta(hours=max_age_hours)
            with self.db.cursor() as cur:
                cur.execute("""
                    SELECT id, step_number, thread_id, browser_type, pid, parent_pid,
                           started_at, user_data_dir
                    FROM chrome_instances
                    WHERE run_id = %s AND scraper_name = %s
                    AND terminated_at IS NULL
                    AND started_at < %s
                    ORDER BY started_at
                """, (self.run_id, self.scraper_name, cutoff))
                
                rows = cur.fetchall()
                return [
                    {
                        "id": r[0],
                        "step_number": r[1],
                        "thread_id": r[2],
                        "browser_type": r[3],
                        "pid": r[4],
                        "parent_pid": r[5],
                        "started_at": r[6],
                        "user_data_dir": r[7]
                    }
                    for r in rows
                ]
        except Exception:
            return []
    
    def terminate_all(self, reason: str = "pipeline_cleanup") -> int:
        """
        Mark all active Chrome instances for this run as terminated.
        
        Args:
            reason: Termination reason
        
        Returns:
            Number of instances terminated
        """
        try:
            with self.db.cursor() as cur:
                cur.execute("""
                    UPDATE chrome_instances
                    SET terminated_at = CURRENT_TIMESTAMP, termination_reason = %s
                    WHERE run_id = %s AND scraper_name = %s
                    AND terminated_at IS NULL
                """, (reason, self.run_id, self.scraper_name))
                count = cur.rowcount
                self.db.commit()
                return count
        except Exception:
            return 0
