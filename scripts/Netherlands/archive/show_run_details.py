#!/usr/bin/env python3
"""
Netherlands Run Details
Shows all runs and their statistics
"""

import sys
import os
from datetime import datetime

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from core.db.postgres_connection import get_db

def show_run_details():
    """Show all Netherlands run details"""
    print("=" * 100)
    print("NETHERLANDS RUN DETAILS")
    print("=" * 100)
    print()
    
    db = get_db("Netherlands")
    
    # Get all Netherlands runs from run_ledger
    print("[1] RUN LEDGER - All Netherlands Runs:")
    print("-" * 100)
    
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT 
                    run_id,
                    mode,
                    status,
                    items_scraped,
                    items_exported,
                    started_at,
                    finished_at,
                    EXTRACT(EPOCH FROM (finished_at - started_at))/60 as duration_minutes
                FROM run_ledger
                WHERE run_id LIKE 'nl_%'
                ORDER BY started_at DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
            
            if rows:
                print(f"{'Run ID':<25} {'Mode':<20} {'Status':<12} {'Scraped':<10} {'Exported':<10} {'Duration':<10} {'Started':<20}")
                print("-" * 100)
                for row in rows:
                    run_id, mode, status, scraped, exported, started, finished, duration = row
                    duration_str = f"{duration:.1f}m" if duration else "N/A"
                    started_str = started.strftime("%Y-%m-%d %H:%M") if started else "N/A"
                    print(f"{run_id:<25} {mode or 'N/A':<20} {status or 'N/A':<12} {scraped or 0:<10} {exported or 0:<10} {duration_str:<10} {started_str:<20}")
                print()
                print(f"Total runs: {len(rows)}")
            else:
                print("No Netherlands runs found in run_ledger")
    except Exception as e:
        print(f"Error querying run_ledger: {e}")
    
    print()
    print("-" * 100)
    print()
    
    # Get data from active tables
    print("[2] ACTIVE TABLES - Current Data:")
    print("-" * 100)
    
    tables = [
        ('nl_collected_urls', 'Collected URLs'),
        ('nl_packs', 'Product Packs'),
        ('nl_consolidated', 'Consolidated Data'),
        ('nl_chrome_instances', 'Chrome Instances'),
        ('nl_errors', 'Errors'),
    ]
    
    for table, description in tables:
        try:
            with db.cursor() as cur:
                # Count total rows
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                total = cur.fetchone()[0]
                
                # Count by run_id if table has run_id column
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}' AND column_name = 'run_id'
                """)
                has_run_id = cur.fetchone() is not None
                
                if has_run_id and total > 0:
                    cur.execute(f"""
                        SELECT run_id, COUNT(*) 
                        FROM {table} 
                        GROUP BY run_id 
                        ORDER BY run_id DESC
                        LIMIT 5
                    """)
                    run_counts = cur.fetchall()
                    
                    print(f"\n{description} ({table}):")
                    print(f"  Total rows: {total:,}")
                    if run_counts:
                        print(f"  By run_id:")
                        for run_id, count in run_counts:
                            print(f"    - {run_id}: {count:,} rows")
                else:
                    print(f"\n{description} ({table}): {total:,} rows")
                    
        except Exception as e:
            print(f"\n{description} ({table}): Error - {e}")
    
    print()
    print("-" * 100)
    print()
    
    # Get latest run details if exists
    print("[3] LATEST RUN - Detailed Statistics:")
    print("-" * 100)
    
    try:
        with db.cursor() as cur:
            # Get latest run_id
            cur.execute("""
                SELECT run_id 
                FROM run_ledger 
                WHERE run_id LIKE 'nl_%' 
                ORDER BY started_at DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            
            if result:
                latest_run_id = result[0]
                print(f"\nLatest Run ID: {latest_run_id}")
                print()
                
                # URLs collected
                cur.execute(f"""
                    SELECT COUNT(*), 
                           COUNT(CASE WHEN packs_scraped = 'success' THEN 1 END) as scraped,
                           COUNT(CASE WHEN packs_scraped = 'failed' THEN 1 END) as failed,
                           COUNT(CASE WHEN packs_scraped = 'pending' THEN 1 END) as pending
                    FROM nl_collected_urls 
                    WHERE run_id = %s
                """, (latest_run_id,))
                url_stats = cur.fetchone()
                if url_stats:
                    total_urls, scraped, failed, pending = url_stats
                    print(f"URLs Collected: {total_urls:,}")
                    print(f"  - Scraped: {scraped:,}")
                    print(f"  - Failed: {failed:,}")
                    print(f"  - Pending: {pending:,}")
                
                # Packs scraped
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM nl_packs 
                    WHERE run_id = %s
                """, (latest_run_id,))
                pack_count = cur.fetchone()[0]
                print(f"\nProduct Packs: {pack_count:,}")
                
                # Sample PPP values
                cur.execute(f"""
                    SELECT local_pack_description, ppp_vat, ppp_ex_vat, unit_price
                    FROM nl_packs 
                    WHERE run_id = %s 
                    AND ppp_vat IS NOT NULL
                    LIMIT 5
                """, (latest_run_id,))
                ppp_samples = cur.fetchall()
                if ppp_samples:
                    print(f"\nSample PPP Values:")
                    for desc, ppp_vat, ppp_ex_vat, unit_price in ppp_samples:
                        desc_short = (desc[:50] + '...') if desc and len(desc) > 50 else desc
                        print(f"  - {desc_short}")
                        print(f"    PPP VAT: {ppp_vat}, PPP ex-VAT: {ppp_ex_vat}, Unit: {unit_price}")
                
                # Errors
                cur.execute(f"""
                    SELECT COUNT(*), error_type 
                    FROM nl_errors 
                    WHERE run_id = %s 
                    GROUP BY error_type
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                """, (latest_run_id,))
                error_stats = cur.fetchall()
                if error_stats:
                    print(f"\nErrors:")
                    for count, error_type in error_stats:
                        print(f"  - {error_type or 'Unknown'}: {count:,}")
                
            else:
                print("\nNo runs found")
                
    except Exception as e:
        print(f"Error getting latest run details: {e}")
    
    print()
    print("=" * 100)
    print()


if __name__ == "__main__":
    show_run_details()
