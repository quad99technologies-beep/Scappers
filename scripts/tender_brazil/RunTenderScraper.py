#!/usr/bin/env python3
"""
Tender Brazil Master Runner
Handles both scenarios:
  1. User provides Input.csv → run GetData.py directly
  2. User wants to filter by date/keywords → run PreviouDayTender.py first

Usage:
  python RunTenderScraper.py                    # Interactive mode
  python RunTenderScraper.py --input            # Use existing Input.csv
  python RunTenderScraper.py --days 7 --all     # Fetch all from last 7 days
  python RunTenderScraper.py --days 7 --search  # Fetch with keywords
  python RunTenderScraper.py --ids MyIDs.csv    # Use specific IDs
"""

import os
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent


def print_header():
    print("=" * 70)
    print("  TENDER BRAZIL SCRAPER - Master Runner")
    print("=" * 70)
    print()


def check_input_csv():
    """Check if Input.csv exists and has data."""
    input_file = SCRIPT_DIR / "Input.csv"
    if not input_file.exists():
        return False, 0
    
    try:
        with open(input_file, "r", encoding="utf-8-sig") as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
            # Subtract header line
            data_lines = [l for l in lines if not l.startswith("CN")]
            return len(data_lines) > 0, len(data_lines)
    except Exception:
        return False, 0


def run_getdata():
    """Run GetData.py to process Input.csv."""
    getdata_path = SCRIPT_DIR / "GetData.py"
    
    print("\n" + "=" * 70)
    print("  Running GetData.py to fetch full tender details...")
    print("=" * 70 + "\n")
    
    try:
        result = subprocess.run(
            [sys.executable, "-u", str(getdata_path)],
            cwd=str(SCRIPT_DIR),
            check=True
        )
        print("\n" + "=" * 70)
        print("  ✓ GetData.py completed successfully!")
        print("  ✓ Output saved to: output.csv")
        print("=" * 70)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] GetData.py failed with exit code: {e.returncode}")
        return e.returncode
    except Exception as e:
        print(f"\n[ERROR] Failed to run GetData.py: {e}")
        return 1


def run_previoudaytender(args):
    """Run PreviouDayTender.py with given arguments."""
    prevday_path = SCRIPT_DIR / "PreviouDayTender.py"
    
    print("\n" + "=" * 70)
    print("  Running PreviouDayTender.py to collect tender IDs...")
    print("=" * 70 + "\n")
    
    cmd = [sys.executable, "-u", str(prevday_path)] + args
    
    try:
        result = subprocess.run(cmd, cwd=str(SCRIPT_DIR), check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] PreviouDayTender.py failed with exit code: {e.returncode}")
        return e.returncode
    except Exception as e:
        print(f"\n[ERROR] Failed to run PreviouDayTender.py: {e}")
        return 1


def interactive_mode():
    """Interactive mode - ask user what they want to do."""
    print_header()
    
    has_input, count = check_input_csv()
    
    print("Choose an option:")
    print()
    
    if has_input:
        print(f"  [1] Use existing Input.csv ({count} tender IDs)")
    else:
        print(f"  [1] Use existing Input.csv (not found or empty)")
    
    print("  [2] Fetch ALL tenders by date range")
    print("  [3] Fetch tenders with KEYWORD filter")
    print("  [4] Use specific TENDER IDs from file")
    print()
    
    choice = input("Enter your choice (1-4): ").strip()
    
    if choice == "1":
        if not has_input:
            print("\n[ERROR] Input.csv not found or empty!")
            create = input("Would you like to create one first? (y/n): ").strip().lower()
            if create == 'y':
                return interactive_create_input()
            return 1
        return run_getdata()
    
    elif choice == "2":
        return interactive_fetch_all()
    
    elif choice == "3":
        return interactive_fetch_search()
    
    elif choice == "4":
        return interactive_fetch_ids()
    
    else:
        print("\n[ERROR] Invalid choice!")
        return 1


def interactive_create_input():
    """Interactive mode to create Input.csv."""
    print("\n" + "=" * 70)
    print("  Create Input.csv")
    print("=" * 70)
    print()
    print("  [1] Fetch by date range (ALL tenders)")
    print("  [2] Fetch by date range + keywords")
    print("  [3] Use existing ID file")
    print()
    
    choice = input("Enter your choice (1-3): ").strip()
    
    if choice == "1":
        return interactive_fetch_all()
    elif choice == "2":
        return interactive_fetch_search()
    elif choice == "3":
        return interactive_fetch_ids()
    else:
        print("\n[ERROR] Invalid choice!")
        return 1


def interactive_fetch_all():
    """Interactive mode to fetch all tenders."""
    print("\n" + "=" * 70)
    print("  Fetch ALL Tenders")
    print("=" * 70)
    print()
    
    days = input("Enter number of days (default: 7): ").strip()
    days = days if days else "7"
    
    date_from = input("Enter start date YYYY-MM-DD (optional, press Enter to skip): ").strip()
    date_to = input("Enter end date YYYY-MM-DD (optional, press Enter to skip): ").strip()
    
    args = ["-m", "all", "-d", days, "-r"]
    
    if date_from:
        args.extend(["--from", date_from])
    if date_to:
        args.extend(["--to", date_to])
    
    return run_previoudaytender(args)


def interactive_fetch_search():
    """Interactive mode to fetch with keywords."""
    print("\n" + "=" * 70)
    print("  Fetch with KEYWORD Filter")
    print("=" * 70)
    print()
    
    days = input("Enter number of days (default: 7): ").strip()
    days = days if days else "7"
    
    search_file = input("Search terms file (default: SearchTerm.csv): ").strip()
    search_file = search_file if search_file else "SearchTerm.csv"
    
    date_from = input("Enter start date YYYY-MM-DD (optional): ").strip()
    date_to = input("Enter end date YYYY-MM-DD (optional): ").strip()
    
    args = ["-m", "search", "-d", days, "-s", search_file, "-r"]
    
    if date_from:
        args.extend(["--from", date_from])
    if date_to:
        args.extend(["--to", date_to])
    
    return run_previoudaytender(args)


def interactive_fetch_ids():
    """Interactive mode to use specific IDs."""
    print("\n" + "=" * 70)
    print("  Use Specific Tender IDs")
    print("=" * 70)
    print()
    
    id_file = input("Enter ID file path (default: TenderIDs.csv): ").strip()
    id_file = id_file if id_file else "TenderIDs.csv"
    
    args = ["-m", "ids", "--id-file", id_file, "-r"]
    
    return run_previoudaytender(args)


def print_usage():
    print("""
Usage: python RunTenderScraper.py [options]

OPTIONS:
  (no arguments)        Interactive mode
  
  --input, -i           Use existing Input.csv (run GetData.py directly)
  
  --all, -a             Fetch ALL tenders (requires --days or --from/--to)
  --search, -s          Fetch with keyword filter
  --ids FILE            Use specific tender IDs from file
  
  --days N              Number of days to fetch (default: 7)
  --from DATE           Start date (YYYY-MM-DD)
  --to DATE             End date (YYYY-MM-DD)
  
  --search-file FILE    Search terms file (default: SearchTerm.csv)
  --id-file FILE        Tender IDs file (default: TenderIDs.csv)
  
  --help, -h            Show this help

EXAMPLES:
  # Interactive mode
  python RunTenderScraper.py
  
  # Use existing Input.csv
  python RunTenderScraper.py --input
  
  # Fetch all from last 7 days
  python RunTenderScraper.py --all --days 7
  
  # Fetch with custom date range
  python RunTenderScraper.py --all --from 2025-01-01 --to 2025-01-31
  
  # Fetch with keywords
  python RunTenderScraper.py --search --days 7
  
  # Use specific IDs
  python RunTenderScraper.py --ids MyTenders.csv
""")


def parse_and_run():
    """Parse command line arguments and run."""
    args = sys.argv[1:]
    
    if not args:
        return interactive_mode()
    
    if "-h" in args or "--help" in args:
        print_usage()
        return 0
    
    if "--input" in args or "-i" in args:
        return run_getdata()
    
    # Build PreviouDayTender.py arguments
    prev_args = []
    mode = None
    
    i = 0
    while i < len(args):
        arg = args[i]
        
        if arg in ("--all", "-a"):
            mode = "all"
            prev_args.extend(["-m", "all"])
            i += 1
        elif arg in ("--search", "-s"):
            mode = "search"
            prev_args.extend(["-m", "search"])
            i += 1
        elif arg == "--ids" and i + 1 < len(args):
            mode = "ids"
            prev_args.extend(["-m", "ids", "--id-file", args[i + 1]])
            i += 2
        elif arg == "--days" and i + 1 < len(args):
            prev_args.extend(["-d", args[i + 1]])
            i += 2
        elif arg == "--from" and i + 1 < len(args):
            prev_args.extend(["--from", args[i + 1]])
            i += 2
        elif arg == "--to" and i + 1 < len(args):
            prev_args.extend(["--to", args[i + 1]])
            i += 2
        elif arg == "--search-file" and i + 1 < len(args):
            prev_args.extend(["-s", args[i + 1]])
            i += 2
        elif arg == "--id-file" and i + 1 < len(args):
            prev_args.extend(["--id-file", args[i + 1]])
            i += 2
        else:
            i += 1
    
    if not mode:
        print("[ERROR] Please specify mode: --all, --search, or --ids")
        print_usage()
        return 1
    
    # Always auto-run GetData.py
    prev_args.append("-r")
    
    return run_previoudaytender(prev_args)


if __name__ == "__main__":
    sys.exit(parse_and_run())
