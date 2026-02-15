"""
Distributed Scraping - URL Work Queue Manager

Handles atomic URL claiming and distribution across multiple worker nodes.
Supports horizontal scaling with shared run_id and independent Tor/browser per node.
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import hashlib
import logging

logger = logging.getLogger(__name__)


class URLWorkQueue:
    """Manages distributed URL work queue with atomic claiming"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize the URL work queue.
        
        Args:
            db_config: Database connection configuration
        """
        self.db_config = db_config
        self._ensure_tables()
    
    def _get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def _ensure_tables(self):
        """Create work queue tables if they don't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS url_work_queue (
            id SERIAL PRIMARY KEY,
            run_id VARCHAR(100) NOT NULL,
            scraper_name VARCHAR(100) NOT NULL,
            url TEXT NOT NULL,
            url_hash VARCHAR(64) NOT NULL,
            priority INT DEFAULT 0,
            status VARCHAR(20) DEFAULT 'pending',
            worker_id VARCHAR(100),
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP,
            retry_count INT DEFAULT 0,
            max_retries INT DEFAULT 3,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(run_id, url_hash)
        );
        
        CREATE INDEX IF NOT EXISTS idx_work_queue_status 
            ON url_work_queue(run_id, scraper_name, status, priority DESC);
        
        CREATE INDEX IF NOT EXISTS idx_work_queue_claimed 
            ON url_work_queue(worker_id, claimed_at) 
            WHERE status = 'claimed';
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
            conn.commit()
    
    def enqueue_urls(self, run_id: str, scraper_name: str, urls: List[str], priority: int = 0):
        """
        Add URLs to the work queue.
        
        Args:
            run_id: Shared run ID across all workers
            scraper_name: Name of the scraper
            urls: List of URLs to process
            priority: Priority level (higher = processed first)
            
        Returns:
            Number of URLs enqueued
        """
        if not urls:
            return 0
        
        # Prepare data with URL hashes for deduplication
        url_data = []
        for url in urls:
            url_hash = hashlib.sha256(url.encode()).hexdigest()
            url_data.append((run_id, scraper_name, url, url_hash, priority))
        
        insert_sql = """
        INSERT INTO url_work_queue (run_id, scraper_name, url, url_hash, priority)
        VALUES %s
        ON CONFLICT (run_id, url_hash) DO NOTHING
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, url_data)
                inserted = cur.rowcount
            conn.commit()
        
        logger.info(f"Enqueued {inserted} URLs for {scraper_name} run {run_id}")
        return inserted
    
    def claim_batch(self, worker_id: str, scraper_name: str, run_id: str, 
                    batch_size: int = 10, lease_seconds: int = 300) -> List[Dict[str, Any]]:
        """
        Atomically claim a batch of URLs for processing.
        
        Uses PostgreSQL FOR UPDATE SKIP LOCKED for atomic claiming.
        
        Args:
            worker_id: Unique worker identifier
            scraper_name: Name of the scraper
            run_id: Run ID to claim URLs from
            batch_size: Number of URLs to claim
            lease_seconds: Lease duration before URLs can be reclaimed
            
        Returns:
            List of claimed URL records
        """
        claim_sql = """
        UPDATE url_work_queue
        SET status = 'claimed',
            worker_id = %s,
            claimed_at = CURRENT_TIMESTAMP
        WHERE id IN (
            SELECT id FROM url_work_queue
            WHERE run_id = %s
              AND scraper_name = %s
              AND status = 'pending'
              AND retry_count < max_retries
            ORDER BY priority DESC, id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, url, url_hash, priority, retry_count
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(claim_sql, (worker_id, run_id, scraper_name, batch_size))
                claimed = cur.fetchall()
            conn.commit()
        
        results = [
            {
                'id': row[0],
                'url': row[1],
                'url_hash': row[2],
                'priority': row[3],
                'retry_count': row[4]
            }
            for row in claimed
        ]
        
        logger.info(f"Worker {worker_id} claimed {len(results)} URLs for {scraper_name}")
        return results
    
    def complete_url(self, work_id: int, success: bool = True, error_message: Optional[str] = None):
        """
        Mark a URL as completed or failed.
        
        Args:
            work_id: Work queue item ID
            success: Whether processing succeeded
            error_message: Error message if failed
        """
        if success:
            update_sql = """
            UPDATE url_work_queue
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP
            WHERE id = %s
            """
            params = (work_id,)
        else:
            update_sql = """
            UPDATE url_work_queue
            SET status = CASE 
                    WHEN retry_count + 1 >= max_retries THEN 'failed'
                    ELSE 'pending'
                END,
                retry_count = retry_count + 1,
                error_message = %s,
                worker_id = NULL,
                claimed_at = NULL
            WHERE id = %s
            """
            params = (error_message, work_id)
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(update_sql, params)
            conn.commit()
    
    def release_expired_leases(self, lease_seconds: int = 300):
        """
        Release URLs from workers that have stopped responding.
        
        Args:
            lease_seconds: Lease duration before expiry
            
        Returns:
            Number of URLs released
        """
        release_sql = """
        UPDATE url_work_queue
        SET status = 'pending',
            worker_id = NULL,
            claimed_at = NULL,
            retry_count = retry_count + 1
        WHERE status = 'claimed'
          AND claimed_at < CURRENT_TIMESTAMP - INTERVAL '%s seconds'
          AND retry_count < max_retries
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(release_sql, (lease_seconds,))
                released = cur.rowcount
            conn.commit()
        
        if released > 0:
            logger.warning(f"Released {released} expired URL leases")
        
        return released
    
    def get_queue_stats(self, run_id: str, scraper_name: str) -> Dict[str, int]:
        """
        Get queue statistics for a specific run.
        
        Args:
            run_id: Run ID
            scraper_name: Scraper name
            
        Returns:
            Dictionary with queue statistics
        """
        stats_sql = """
        SELECT 
            status,
            COUNT(*) as count
        FROM url_work_queue
        WHERE run_id = %s AND scraper_name = %s
        GROUP BY status
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(stats_sql, (run_id, scraper_name))
                rows = cur.fetchall()
        
        stats = {
            'pending': 0,
            'claimed': 0,
            'completed': 0,
            'failed': 0
        }
        
        for status, count in rows:
            stats[status] = count
        
        stats['total'] = sum(stats.values())
        stats['remaining'] = stats['pending'] + stats['claimed']
        
        return stats
