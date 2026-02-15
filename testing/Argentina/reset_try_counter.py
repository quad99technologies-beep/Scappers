#!/usr/bin/env python3
"""
Reset ar_product_index try counters back to 1 in PostgreSQL.

This is useful when:
1. Starting a new scraping session with round-robin retry mode
2. Previous run had high try counts and you want to start fresh
3. You want to retry products that previously failed

Usage:
    python reset_try_counter.py
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

def get_db_connection():
    """Get PostgreSQL connection from environment variables."""
    import os
    
    # Load .env if exists
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:
                        os.environ[key] = value
    
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    dbname = os.getenv('POSTGRES_DB', 'postgres')
    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', '')
    
    return psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )

def reset_try_counters():
    """Reset all try > 0 to try = 1 in ar_product_index table."""
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Get current stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN try = 0 THEN 1 ELSE 0 END) as try_0,
                SUM(CASE WHEN try = 1 THEN 1 ELSE 0 END) as try_1,
                SUM(CASE WHEN try > 1 THEN 1 ELSE 0 END) as try_above_1,
                MAX(try) as max_try
            FROM ar_product_index
        """)
        row = cursor.fetchone()
        
        print("=" * 60)
        print("BEFORE RESET:")
        print("=" * 60)
        print(f"Total products:     {row[0]}")
        print(f"Try = 0:            {row[1] or 0}")
        print(f"Try = 1:            {row[2] or 0}")
        print(f"Try > 1:            {row[3] or 0}")
        print(f"Max try value:      {row[4] or 0}")
        print()
        
        # Confirm before reset
        if row[3] == 0:
            print("No products with try > 1 found. Nothing to reset.")
            return
        
        confirm = input(f"Reset {row[3]} products with try > 1 back to try = 1? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Cancelled.")
            return
        
        # Perform reset
        cursor.execute("""
            UPDATE ar_product_index 
            SET try = 1 
            WHERE try > 0
        """)
        
        updated = cursor.rowcount
        conn.commit()
        
        # Get after stats
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN try = 0 THEN 1 ELSE 0 END) as try_0,
                SUM(CASE WHEN try = 1 THEN 1 ELSE 0 END) as try_1,
                SUM(CASE WHEN try > 1 THEN 1 ELSE 0 END) as try_above_1,
                MAX(try) as max_try
            FROM ar_product_index
        """)
        row = cursor.fetchone()
        
        print()
        print("=" * 60)
        print("AFTER RESET:")
        print("=" * 60)
        print(f"Total products:     {row[0]}")
        print(f"Try = 0:            {row[1] or 0}")
        print(f"Try = 1:            {row[2] or 0}")
        print(f"Try > 1:            {row[3] or 0}")
        print(f"Max try value:      {row[4] or 0}")
        print()
        print(f"[OK] Reset {updated} products successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    reset_try_counters()
