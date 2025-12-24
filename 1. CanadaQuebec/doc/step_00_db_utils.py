#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database Utilities

Provides database connection management, RunContext, and safe parameterized query execution.
All database operations can be disabled via DB_ENABLED environment variable.

Author: Enterprise PDF Processing Pipeline
License: Proprietary
"""

import os
import uuid
import logging
from typing import Optional, Dict, Any, List, Tuple
from contextlib import contextmanager
from datetime import datetime, timezone

try:
    import psycopg2
    from psycopg2 import pool, sql
    from psycopg2.extras import execute_batch, execute_values
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    psycopg2 = None
    pool = None
    sql = None
    execute_batch = None
    execute_values = None


class RunContext:
    """
    Context object for tracking scraper runs.
    Contains run_id (UUID) and scraper_id (from env).
    """
    
    def __init__(self, run_id: Optional[str] = None, scraper_id: Optional[str] = None):
        """
        Initialize RunContext.
        
        Args:
            run_id: UUID string. If None, generates a new UUID.
            scraper_id: Scraper identifier. If None, reads from SCRAPER_ID env var.
        """
        self.run_id = uuid.UUID(run_id) if run_id else uuid.uuid4()
        self.scraper_id = scraper_id or os.getenv('SCRAPER_ID', 'canada_quebec_ramq')
        self.created_at = datetime.now(timezone.utc)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert RunContext to dictionary for database operations."""
        return {
            'run_id': str(self.run_id),
            'scraper_id': self.scraper_id,
            'created_at': self.created_at
        }


class DatabaseManager:
    """
    Database connection manager with optional enable/disable.
    All operations are no-ops if DB_ENABLED=0.
    """
    
    def __init__(self):
        """Initialize database manager from environment variables."""
        self.enabled = self._check_enabled()
        self.connection_pool = None
        self.logger = logging.getLogger(__name__)
        
        if self.enabled:
            if not PSYCOPG2_AVAILABLE:
                self.logger.warning("psycopg2 not available. Database operations disabled.")
                self.enabled = False
                return
            
            try:
                self._init_connection_pool()
            except Exception as e:
                self.logger.error(f"Failed to initialize database connection pool: {e}")
                self.logger.warning("Database operations disabled due to initialization failure.")
                self.enabled = False
    
    def _check_enabled(self) -> bool:
        """Check if database is enabled via environment variable."""
        db_enabled = os.getenv('DB_ENABLED', '0').strip()
        return db_enabled in ('1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES')
    
    def _init_connection_pool(self):
        """Initialize PostgreSQL connection pool."""
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'scraper_db')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', '')
        
        # Validate required fields
        if not db_password:
            raise ValueError("DB_PASSWORD environment variable is required when DB_ENABLED=1")
        
        try:
            db_port_int = int(db_port)
        except ValueError:
            raise ValueError(f"Invalid DB_PORT value: {db_port}")
        
        # Create connection pool (min 1, max 5 connections)
        self.connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=db_host,
            port=db_port_int,
            database=db_name,
            user=db_user,
            password=db_password,
            connect_timeout=10
        )
        
        # Test connection
        test_conn = self.connection_pool.getconn()
        try:
            test_conn.cursor().execute("SELECT 1")
            test_conn.commit()
        finally:
            self.connection_pool.putconn(test_conn)
        
        self.logger.info(f"Database connection pool initialized: {db_host}:{db_port}/{db_name}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        Returns None if DB is disabled.
        
        Usage:
            with db_manager.get_connection() as conn:
                if conn:
                    # use conn
        """
        if not self.enabled or not self.connection_pool:
            yield None
            return
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database operation failed: {e}", exc_info=True)
            raise
        finally:
            if conn and self.connection_pool:
                self.connection_pool.putconn(conn)
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Tuple]]:
        """
        Execute a parameterized query safely.
        
        Args:
            query: SQL query with %s placeholders (NOT f-strings or string formatting)
            params: Tuple of parameters for the query
        
        Returns:
            List of result rows, or None if DB is disabled
        
        Example:
            results = db.execute_query(
                "SELECT * FROM products WHERE din = %s",
                ('12345678',)
            )
        """
        if not self.enabled:
            return None
        
        with self.get_connection() as conn:
            if not conn:
                return None
            
            cursor = conn.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                if cursor.description:
                    return cursor.fetchall()
                return []
            finally:
                cursor.close()
    
    def execute_batch_insert(self, query: str, data: List[Tuple], page_size: int = 100) -> Optional[int]:
        """
        Execute batched insert using execute_batch for better performance.
        
        Args:
            query: INSERT query with %s placeholders
            data: List of tuples, each tuple contains values for one row
            page_size: Number of rows to insert per batch
        
        Returns:
            Number of rows inserted, or None if DB is disabled
        
        Example:
            rows = [
                ('DIN123', 'Brand1', 'Manuf1', run_id, scraper_id),
                ('DIN456', 'Brand2', 'Manuf2', run_id, scraper_id),
            ]
            count = db.execute_batch_insert(
                "INSERT INTO products (din, brand, manufacturer, run_id, scraper_id) VALUES (%s, %s, %s, %s, %s)",
                rows
            )
        """
        if not self.enabled:
            return None
        
        if not data:
            return 0
        
        with self.get_connection() as conn:
            if not conn:
                return None
            
            cursor = conn.cursor()
            try:
                execute_batch(cursor, query, data, page_size=page_size)
                return len(data)
            finally:
                cursor.close()
    
    def execute_values_insert(self, query_template: str, data: List[Tuple], page_size: int = 100) -> Optional[int]:
        """
        Execute batched insert using execute_values for very large datasets.
        Faster than execute_batch for bulk inserts.
        
        Args:
            query_template: INSERT query template with VALUES (%s, %s, ...) placeholder
            data: List of tuples, each tuple contains values for one row
            page_size: Number of rows to insert per batch
        
        Returns:
            Number of rows inserted, or None if DB is disabled
        
        Example:
            rows = [
                ('DIN123', 'Brand1', 'Manuf1', run_id, scraper_id),
                ('DIN456', 'Brand2', 'Manuf2', run_id, scraper_id),
            ]
            count = db.execute_values_insert(
                "INSERT INTO products (din, brand, manufacturer, run_id, scraper_id) VALUES %s",
                rows
            )
        """
        if not self.enabled:
            return None
        
        if not data:
            return 0
        
        with self.get_connection() as conn:
            if not conn:
                return None
            
            cursor = conn.cursor()
            try:
                execute_values(cursor, query_template, data, page_size=page_size)
                return len(data)
            finally:
                cursor.close()
    
    def close(self):
        """Close all database connections."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
            self.logger.info("Database connection pool closed")


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """Get or create the global database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


def is_db_enabled() -> bool:
    """Check if database operations are enabled."""
    return get_db_manager().enabled


def safe_db_operation(operation_name: str):
    """
    Decorator to safely execute database operations.
    Catches all exceptions and logs them without crashing the scraper.
    
    Usage:
        @safe_db_operation("insert_products")
        def insert_products(data):
            # database code here
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            if not is_db_enabled():
                return None
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.error(f"Database operation '{operation_name}' failed: {e}", exc_info=True)
                return None
        return wrapper
    return decorator

