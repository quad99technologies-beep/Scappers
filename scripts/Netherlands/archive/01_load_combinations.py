# -*- coding: utf-8 -*-
"""
Combination Loader for Netherlands Scraper
Loads vorm/sterkte combinations extracted from dropdowns into the database.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

# Set console encoding to UTF-8 for Windows compatibility
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from url_builder import build_combination_url
from config_loader import getenv

# Database imports
try:
    from db.repositories import NetherlandsRepository
    from core.db.postgres_connection import get_db
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False
    print("ERROR: Database not available. Please install required dependencies.")
    sys.exit(1)


def load_combinations_from_extraction(vorm_values: List[str], sterkte_values: List[str], run_id: str) -> int:
    """
    Load all vorm/sterkte combinations into the database.
    
    Args:
        vorm_values: List of vorm (form) values
        sterkte_values: List of sterkte (strength) values
        run_id: Current run ID
    
    Returns:
        Number of combinations loaded
    """
    if not DB_AVAILABLE:
        print("ERROR: Database not available")
        return 0
    
    print("=" * 80)
    print("LOADING COMBINATIONS INTO DATABASE")
    print("=" * 80)
    print(f"Run ID: {run_id}")
    print(f"VORM values: {len(vorm_values)}")
    print(f"STERKTE values: {len(sterkte_values)}")
    print(f"Total combinations: {len(vorm_values) * len(sterkte_values):,}")
    print("=" * 80)
    
    # Connect to database
    db = get_db("Netherlands")
    repo = NetherlandsRepository(db, run_id)
    
    # Ensure run exists in run_ledger (required for foreign key constraint)
    print("\nRegistering run in database...")
    repo.ensure_run_in_ledger(mode="combination_loading")
    print(f"[OK] Run registered: {run_id}")
    
    # Generate single combination that covers ALL products:
    # "Alle vormen" + "Alle sterktes" = complete coverage (22,000+ products)
    print("\nGenerating combination...")
    
    # Find "Alle vormen" and "Alle sterktes" values
    alle_vormen = "Alle vormen"
    alle_sterktes = "Alle sterktes"
    
    # Try to find them in the extracted values (case-insensitive)
    for v in vorm_values:
        if v.lower() == "alle vormen":
            alle_vormen = v
            break
    
    for s in sterkte_values:
        if s.lower() == "alle sterktes":
            alle_sterktes = s
            break
    
    # Create single combination
    search_url = build_combination_url(alle_vormen, alle_sterktes)
    combinations = [{
        "vorm": alle_vormen,
        "sterkte": alle_sterktes,
        "search_url": search_url
    }]
    
    print(f"[OPTIMIZED] Using single combination: {alle_vormen} + {alle_sterktes}")
    print(f"[OPTIMIZED] This covers ALL products (~22,000+) in one search")
    print("Inserting into database...")
    
    # Insert in bulk
    count = repo.insert_combinations_bulk(combinations)
    
    print(f"[OK] Inserted {count:,} combinations")
    
    # Get stats
    stats = repo.get_combination_stats()
    print("\n" + "=" * 80)
    print("DATABASE STATS")
    print("=" * 80)
    print(f"Total combinations: {stats['total']:,}")
    print(f"Pending: {stats['pending']:,}")
    print(f"Completed: {stats['completed']:,}")
    print(f"Failed: {stats['failed']:,}")
    print("=" * 80)
    
    return count


def main():
    """Main entry point."""
    import argparse
    from datetime import datetime
    
    parser = argparse.ArgumentParser(description="Load vorm/sterkte combinations into database")
    parser.add_argument("--run-id", help="Run ID (default: auto-generated)")
    parser.add_argument("--vorm-file", help="File with vorm values (one per line)")
    parser.add_argument("--sterkte-file", help="File with sterkte values (one per line)")
    
    args = parser.parse_args()
    
    # Generate run ID if not provided
    run_id = args.run_id or f"nl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Check if we should extract from files or run extraction
    if args.vorm_file and args.sterkte_file:
        # Load from files
        print(f"Loading from files:")
        print(f"  VORM: {args.vorm_file}")
        print(f"  STERKTE: {args.sterkte_file}")
        
        with open(args.vorm_file, 'r', encoding='utf-8') as f:
            vorm_values = [line.strip() for line in f if line.strip()]
        
        with open(args.sterkte_file, 'r', encoding='utf-8') as f:
            sterkte_values = [line.strip() for line in f if line.strip()]
    else:
        # Run extraction if module is available, otherwise use defaults
        print("No files provided. Attempting dropdown extraction...")
        try:
            from extract_dropdown_values import extract_all_combinations
            vorm_values, sterkte_values = extract_all_combinations()
        except ImportError:
            print("[WARN] extract_dropdown_values module not found. Using default combinations.")
            print("[INFO] For complete coverage, you should extract actual dropdown values from the website.")
            
            # Default vorm values (common forms)
            vorm_values = [
                "TABLET",
                "CAPSULE", 
                "VLOEISTOF",
                "INJECTIEVLOEISTOF",
                "ZETPIL",
                "CREME",
                "ZALF",
                "Alle vormen"
            ]
            
            # Default sterkte values (common strengths)
            sterkte_values = [
                "Alle sterktes",
                "10mg",
                "20mg",
                "50mg",
                "100mg",
                "200mg",
                "500mg"
            ]
            
            print(f"[DEFAULT] Using {len(vorm_values)} vorm values and {len(sterkte_values)} sterkte values")
        
        if not vorm_values or not sterkte_values:
            print("ERROR: Failed to extract dropdown values")
            return 1
    
    # Load into database
    count = load_combinations_from_extraction(vorm_values, sterkte_values, run_id)
    
    if count > 0:
        print(f"\n[SUCCESS] Successfully loaded {count:,} combinations")
        print(f"\nNext step: Run the scraper with USE_DROPDOWN_COMBINATIONS=true")
        return 0
    else:
        print("\n[ERROR] Failed to load combinations")
        return 1


if __name__ == "__main__":
    sys.exit(main())
