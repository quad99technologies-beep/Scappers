#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Argentina - Selenium 3-Round Retry Wrapper

This script runs the Selenium scraper in 3 rounds:
- Round 1: Process all products marked for Selenium scraping
- Round 2: Retry products that returned 0 records in Round 1
- Round 3: Final retry for products that still have 0 records after Round 2

After 3 rounds, remaining failed products are marked for API scraping (if enabled).
"""

import sys
import subprocess
import csv
import time
import logging
from pathlib import Path
from typing import Set, Tuple

# Add script directory to path for config_loader import
_script_dir = Path(__file__).resolve().parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from config_loader import (
    get_output_dir,
    PREPARED_URLS_FILE, USE_API_STEPS,
    SELENIUM_ROUNDS, ROUND_PAUSE_SECONDS
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("3round_wrapper")

# Configuration
SELENIUM_SCRIPT = "03_alfabeta_selenium_scraper.py"

def strip_accents(s: str) -> str:
    """Remove accents from string."""
    import unicodedata
    return "".join(ch for ch in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(ch))

def nk(s: str) -> str:
    """Normalize string for comparison (lowercase, no accents, single spaces)."""
    if not s:
        return ""
    import re
    normalized = strip_accents(s.strip())
    return re.sub(r"\s+", " ", normalized).lower()

def count_products_needing_retry(round_num: int) -> int:
    """
    Count products that need to be retried in the given round.

    Round 1: Count all products with Scraped_By_Selenium=no
    Round 2: Count products with Selenium_Attempt=1 and Selenium_Records=0
    Round 3: Count products with Selenium_Attempt=2 and Selenium_Records=0
    """
    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE

    if not prepared_urls_path.exists():
        log.error(f"Prepared URLs file not found: {prepared_urls_path}")
        return 0

    count = 0
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]

    for encoding in encoding_attempts:
        try:
            with open(prepared_urls_path, encoding=encoding) as f:
                reader = csv.DictReader(f)
                headers = {nk(h): h for h in (reader.fieldnames or [])}

                # Get column names (case-insensitive)
                scraped_col = headers.get(nk("Scraped_By_Selenium"), "Scraped_By_Selenium")
                attempt_col = headers.get(nk("Selenium_Attempt"), "Selenium_Attempt")
                records_col = headers.get(nk("Selenium_Records"), "Selenium_Records")
                source_col = headers.get(nk("Source"), "Source")

                for row in reader:
                    source = (row.get(source_col, "") or "").strip().lower()
                    scraped = (row.get(scraped_col, "") or "").strip().lower()
                    attempt = (row.get(attempt_col, "") or "0").strip()
                    records = (row.get(records_col, "") or "0").strip()

                    try:
                        attempt_num = int(float(attempt))
                        records_num = int(float(records))
                    except (ValueError, TypeError):
                        attempt_num = 0
                        records_num = 0

                    # Round 1: All unscraped products
                    if round_num == 1:
                        if source == "selenium" and scraped == "no":
                            count += 1
                    # Round 2: Products that failed in Round 1
                    elif round_num == 2:
                        if source == "selenium" and attempt_num == 1 and records_num == 0:
                            count += 1
                    # Round 3: Products that failed in Round 2
                    elif round_num == 3:
                        if source == "selenium" and attempt_num == 2 and records_num == 0:
                            count += 1

            return count  # Success, return count
        except UnicodeDecodeError:
            continue  # Try next encoding
        except Exception as e:
            log.error(f"Error counting products: {e}")
            return 0

    log.error("Failed to read prepared URLs file with any encoding")
    return 0

def mark_failed_products_for_api():
    """
    After 3 rounds, mark products with Selenium_Attempt=3 and Selenium_Records=0 for API scraping.
    Only if USE_API_STEPS is enabled.
    """
    if not USE_API_STEPS:
        log.info("[API] API steps disabled, skipping marking failed products for API")
        return

    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE

    if not prepared_urls_path.exists():
        log.error(f"Prepared URLs file not found: {prepared_urls_path}")
        return

    log.info("[API] Marking products with 0 records after 3 Selenium attempts for API scraping...")

    # Read all rows
    rows = []
    fieldnames = None
    encoding_used = None
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]

    for encoding in encoding_attempts:
        try:
            with open(prepared_urls_path, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
                if not fieldnames:
                    return

                for row in reader:
                    rows.append(row)
                encoding_used = encoding
                break  # Success
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning(f"Error reading with {encoding}: {e}")
            continue

    if encoding_used is None:
        log.error("Failed to read file with any encoding")
        return

    # Get column names (case-insensitive)
    headers = {nk(h): h for h in fieldnames}
    source_col = headers.get(nk("Source"), "Source")
    attempt_col = headers.get(nk("Selenium_Attempt"), "Selenium_Attempt")
    records_col = headers.get(nk("Selenium_Records"), "Selenium_Records")
    scraped_api_col = headers.get(nk("Scraped_By_API"), "Scraped_By_API")

    # Update rows that meet criteria
    marked_count = 0
    for row in rows:
        attempt = (row.get(attempt_col, "") or "0").strip()
        records = (row.get(records_col, "") or "0").strip()

        try:
            attempt_num = int(float(attempt))
            records_num = int(float(records))
        except (ValueError, TypeError):
            continue

        # Mark for API if: 3 attempts, 0 records
        if attempt_num == 3 and records_num == 0:
            row[source_col] = "api"
            row[scraped_api_col] = "no"
            marked_count += 1

    # Write back
    try:
        with open(prepared_urls_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"[API] Marked {marked_count} products for API scraping")
    except Exception as e:
        log.error(f"Error writing prepared URLs file: {e}")

def update_attempt_numbers(round_num: int):
    """
    Update Selenium_Attempt and Last_Attempt_Records for all products processed in this round.
    Reads from progress.csv to determine which products were just processed and their results.
    """
    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE
    progress_file = output_dir / "alfabeta_progress.csv"

    if not prepared_urls_path.exists() or not progress_file.exists():
        return

    try:
        # Read progress file to get products processed in this round with their record counts
        processed_products = {}  # {(company, product): records_found}
        encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]

        for encoding in encoding_attempts:
            try:
                with open(progress_file, encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        company = (row.get("input_company", "") or "").strip()
                        product = (row.get("input_product_name", "") or "").strip()
                        records = (row.get("records_found", "") or "0").strip()

                        if company and product:
                            try:
                                records_num = int(float(records))
                            except (ValueError, TypeError):
                                records_num = 0
                            processed_products[(nk(company), nk(product))] = records_num
                break  # Success
            except UnicodeDecodeError:
                continue
            except Exception:
                continue

        if not processed_products:
            return

        # Update prepared URLs file
        rows = []
        fieldnames = None

        for encoding in encoding_attempts:
            try:
                with open(prepared_urls_path, "r", encoding=encoding, newline="") as f:
                    reader = csv.DictReader(f)
                    fieldnames = reader.fieldnames
                    if not fieldnames:
                        return

                    for row in reader:
                        rows.append(row)
                break  # Success
            except UnicodeDecodeError:
                continue
            except Exception:
                continue

        if not fieldnames or not rows:
            return

        # Update rows
        headers = {nk(h): h for h in fieldnames}
        attempt_col = headers.get(nk("Selenium_Attempt"), "Selenium_Attempt")
        last_records_col = headers.get(nk("Last_Attempt_Records"), "Last_Attempt_Records")

        updated_count = 0
        for row in rows:
            company = (row.get("Company", "") or "").strip()
            product = (row.get("Product", "") or "").strip()
            key = (nk(company), nk(product))

            if key in processed_products:
                row[attempt_col] = str(round_num)
                row[last_records_col] = str(processed_products[key])
                updated_count += 1

        # Write back
        with open(prepared_urls_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        log.info(f"[ROUND {round_num}] Updated attempt numbers for {updated_count} products")

    except Exception as e:
        log.warning(f"Error updating attempt numbers: {e}")

def prepare_round(round_num: int):
    """
    Prepare for a round by cleaning up progress file to remove failed entries from previous rounds.
    This allows the Selenium scraper to retry products that returned 0 records.
    """
    output_dir = get_output_dir()
    progress_file = output_dir / "alfabeta_progress.csv"

    if not progress_file.exists():
        return  # No progress file yet

    if round_num == 1:
        return  # Round 1 doesn't need cleanup

    # For Round 2+: Remove progress entries with 0 records so they can be retried
    try:
        rows_to_keep = []
        encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]

        for encoding in encoding_attempts:
            try:
                with open(progress_file, "r", encoding=encoding, newline="") as f:
                    reader = csv.DictReader(f)
                    fieldnames = reader.fieldnames

                    for row in reader:
                        records_found = (row.get("records_found", "") or "0").strip()
                        try:
                            records_num = int(float(records_found))
                        except (ValueError, TypeError):
                            records_num = 0

                        # Keep only entries with records > 0 (successful scrapes)
                        if records_num > 0:
                            rows_to_keep.append(row)

                # Write back only successful entries
                with open(progress_file, "w", encoding="utf-8-sig", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows_to_keep)

                removed_count = 0  # We don't track original count, but it's ok
                log.info(f"[ROUND {round_num}] Cleaned progress file - kept {len(rows_to_keep)} successful entries, removed failed entries")
                return

            except UnicodeDecodeError:
                continue
            except Exception as e:
                log.warning(f"Error preparing round {round_num}: {e}")
                return

        log.warning(f"Failed to read progress file with any encoding")
    except Exception as e:
        log.warning(f"Error preparing round {round_num}: {e}")

def run_selenium_round(round_num: int, max_rows: int = 0) -> bool:
    """
    Run a single Selenium scraping round.
    Returns True if successful, False otherwise.
    """
    # Prepare for this round (clean up progress file for retries)
    prepare_round(round_num)

    # Count products for this round
    products_count = count_products_needing_retry(round_num)

    print(f"\n{'='*80}")
    print(f"SELENIUM SCRAPING - ROUND {round_num} OF {SELENIUM_ROUNDS}")
    print(f"{'='*80}")

    if products_count == 0:
        print(f"[INFO] No products to process in Round {round_num}")
        print(f"[INFO] All products from previous round(s) were successfully scraped")
        print(f"{'='*80}\n")
        log.info(f"[ROUND {round_num}] No products to process - all previous attempts succeeded")
        return True

    # Describe what this round will do with context
    round_descriptions = {
        1: "Processing all products marked for Selenium scraping",
        2: "Retrying products that returned 0 records in Round 1 (temporary failures, rate limits, etc.)",
        3: "Final retry attempt for products that still have 0 records after Round 2"
    }

    print(f"[ROUND {round_num}] {round_descriptions.get(round_num, 'Processing products')}")
    print(f"[ROUND {round_num}] Products to scrape: {products_count:,}")

    if max_rows > 0 and max_rows < products_count:
        print(f"[ROUND {round_num}] Limited to {max_rows:,} products per round (MAX_ROWS setting)")

    print(f"{'='*80}\n")

    log.info(f"[ROUND {round_num}] Starting Round {round_num}/{SELENIUM_ROUNDS} with {products_count:,} products")

    # Prepare command
    script_path = _script_dir / SELENIUM_SCRIPT
    if not script_path.exists():
        log.error(f"Selenium script not found: {script_path}")
        print(f"ERROR: Selenium script not found: {script_path}")
        return False

    cmd = [sys.executable, "-u", str(script_path)]
    if max_rows > 0:
        cmd.extend(["--max-rows", str(max_rows)])

    # Run Selenium scraper
    start_time = time.time()
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False
        )

        duration = time.time() - start_time
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        if hours > 0:
            duration_str = f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            duration_str = f"{minutes}m {seconds}s"
        else:
            duration_str = f"{seconds}s"

        # Update Selenium_Attempt column for all products processed in this round
        update_attempt_numbers(round_num)

        # Count successes after this round
        products_remaining = count_products_needing_retry(round_num + 1) if round_num < SELENIUM_ROUNDS else 0
        products_succeeded = products_count - products_remaining

        print(f"\n{'='*80}")
        print(f"[ROUND {round_num}] COMPLETED")
        print(f"{'='*80}")
        print(f"[ROUND {round_num}] Duration: {duration_str}")
        print(f"[ROUND {round_num}] Products processed: {products_count:,}")
        print(f"[ROUND {round_num}] Successfully scraped: {products_succeeded:,}")
        print(f"[ROUND {round_num}] Still need retry: {products_remaining:,}")

        if products_succeeded > 0:
            success_rate = (products_succeeded / products_count) * 100
            print(f"[ROUND {round_num}] Success rate: {success_rate:.1f}%")

        print(f"{'='*80}\n")

        log.info(f"[ROUND {round_num}] Completed in {duration_str} - {products_succeeded:,}/{products_count:,} successful ({success_rate:.1f}%)")
        return True

    except subprocess.CalledProcessError as e:
        duration = time.time() - start_time
        print(f"\n{'='*80}")
        print(f"[ERROR] Round {round_num} failed with exit code {e.returncode}")
        print(f"[ERROR] Duration before failure: {duration:.1f}s")
        print(f"{'='*80}\n")
        log.error(f"[ROUND {round_num}] Failed with exit code {e.returncode} after {duration:.1f}s")
        return False
    except Exception as e:
        duration = time.time() - start_time
        print(f"\n{'='*80}")
        print(f"[ERROR] Round {round_num} failed: {e}")
        print(f"[ERROR] Duration before failure: {duration:.1f}s")
        print(f"{'='*80}\n")
        log.error(f"[ROUND {round_num}] Failed: {e} after {duration:.1f}s")
        return False

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Argentina Selenium 3-Round Retry Wrapper")
    ap.add_argument("--max-rows", type=int, default=0, help="Maximum number of rows to process per round (0 = unlimited)")
    args = ap.parse_args()

    print("\n" + "="*80)
    print("ARGENTINA SELENIUM SCRAPER - 3-ROUND RETRY MECHANISM")
    print("="*80)
    print("[INFO] This wrapper runs Selenium scraping in 3 rounds:")
    print("[INFO]   Round 1: Process all products")
    print("[INFO]   Round 2: Retry products with 0 records from Round 1")
    print("[INFO]   Round 3: Final retry for remaining failures")
    print()
    print("[CONFIG] Configuration:")
    print(f"[CONFIG]   Rounds to execute: {SELENIUM_ROUNDS}")
    print(f"[CONFIG]   Pause between rounds: {ROUND_PAUSE_SECONDS} seconds")
    print(f"[CONFIG]   API fallback enabled: {'Yes' if USE_API_STEPS else 'No'}")
    if args.max_rows > 0:
        print(f"[CONFIG]   Max products per round: {args.max_rows:,}")
    else:
        print(f"[CONFIG]   Max products per round: Unlimited")
    print("="*80 + "\n")

    log.info("="*80)
    log.info("Starting Argentina Selenium 3-Round Retry Wrapper")
    log.info(f"Rounds: {SELENIUM_ROUNDS}, Pause: {ROUND_PAUSE_SECONDS}s, API enabled: {USE_API_STEPS}")
    log.info("="*80)

    # Run 3 rounds
    for round_num in range(1, SELENIUM_ROUNDS + 1):
        success = run_selenium_round(round_num, args.max_rows)

        if not success:
            print(f"\nERROR: Round {round_num} failed. Stopping pipeline.")
            log.error(f"Round {round_num} failed. Stopping pipeline.")
            sys.exit(1)

        # Pause between rounds (except after last round)
        if round_num < SELENIUM_ROUNDS:
            products_next = count_products_needing_retry(round_num + 1)
            if products_next > 0:
                print(f"\n{'='*80}")
                print(f"[PAUSE] Break before Round {round_num + 1}")
                print(f"{'='*80}")
                print(f"[PAUSE] {products_next:,} products need retry in Round {round_num + 1}")
                print(f"[PAUSE] Waiting {ROUND_PAUSE_SECONDS} seconds to let system stabilize...")
                print(f"[PAUSE] (Helps avoid rate limiting and gives browser time to rest)")
                log.info(f"[PAUSE] Waiting {ROUND_PAUSE_SECONDS}s before Round {round_num + 1} ({products_next:,} products pending)")

                # Show countdown for long pauses
                if ROUND_PAUSE_SECONDS >= 10:
                    for remaining in range(ROUND_PAUSE_SECONDS, 0, -10):
                        time.sleep(min(10, remaining))
                        if remaining > 10:
                            print(f"[PAUSE] {remaining} seconds remaining...", flush=True)
                    if ROUND_PAUSE_SECONDS % 10 != 0:
                        time.sleep(ROUND_PAUSE_SECONDS % 10)
                else:
                    time.sleep(ROUND_PAUSE_SECONDS)

                print(f"[PAUSE] Resuming with Round {round_num + 1}")
                print(f"{'='*80}\n")
            else:
                print(f"\n{'='*80}")
                print(f"[SUCCESS] All products successfully scraped after Round {round_num}!")
                print(f"[SUCCESS] No need for Round {round_num + 1}")
                print(f"{'='*80}\n")
                log.info(f"[SUCCESS] All products scraped after Round {round_num}, skipping remaining rounds")

    # After 3 rounds, mark failed products for API
    if USE_API_STEPS:
        print(f"\n{'='*80}")
        print("MARKING FAILED PRODUCTS FOR API SCRAPING")
        print(f"{'='*80}\n")
        mark_failed_products_for_api()

    # Summary - collect detailed statistics
    print(f"\n{'='*80}")
    print("3-ROUND SELENIUM SCRAPING - FINAL SUMMARY")
    print(f"{'='*80}")

    output_dir = get_output_dir()
    prepared_urls_path = output_dir / PREPARED_URLS_FILE

    # Count products by attempt number and success
    round1_success = 0
    round2_success = 0
    round3_success = 0
    total_failed = 0
    total_processed = 0

    if prepared_urls_path.exists():
        try:
            with open(prepared_urls_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                headers = {nk(h): h for h in (reader.fieldnames or [])}

                attempt_col = headers.get(nk("Selenium_Attempt"), "Selenium_Attempt")
                records_col = headers.get(nk("Selenium_Records"), "Selenium_Records")
                source_col = headers.get(nk("Source"), "Source")

                for row in reader:
                    source = (row.get(source_col, "") or "").strip().lower()
                    if source not in ["selenium", "api"]:
                        continue

                    attempt = (row.get(attempt_col, "") or "0").strip()
                    records = (row.get(records_col, "") or "0").strip()

                    try:
                        attempt_num = int(float(attempt))
                        records_num = int(float(records))
                    except (ValueError, TypeError):
                        continue

                    if attempt_num > 0:
                        total_processed += 1
                        if records_num > 0:
                            if attempt_num == 1:
                                round1_success += 1
                            elif attempt_num == 2:
                                round2_success += 1
                            elif attempt_num == 3:
                                round3_success += 1
                        elif attempt_num == 3 and records_num == 0:
                            total_failed += 1
        except Exception as e:
            log.warning(f"Error reading final stats: {e}")

    total_success = round1_success + round2_success + round3_success

    print(f"\n[RESULTS] Products Processed: {total_processed:,}")
    print(f"[RESULTS] Total Successful: {total_success:,} ({(total_success/total_processed*100) if total_processed > 0 else 0:.1f}%)")
    print(f"[RESULTS]   ├─ Succeeded in Round 1: {round1_success:,}")
    print(f"[RESULTS]   ├─ Succeeded in Round 2: {round2_success:,} (recovered from Round 1 failures)")
    print(f"[RESULTS]   └─ Succeeded in Round 3: {round3_success:,} (recovered from Round 2 failures)")
    print(f"[RESULTS] Total Failed: {total_failed:,}")

    if total_failed > 0:
        if USE_API_STEPS:
            print(f"[RESULTS]   └─ Marked for API scraping: {total_failed:,}")
        else:
            print(f"[RESULTS]   └─ API scraping disabled - these products will not be retried")

    # Show retry effectiveness
    retries_recovered = round2_success + round3_success
    if retries_recovered > 0:
        print(f"\n[IMPACT] Retry Effectiveness:")
        print(f"[IMPACT]   {retries_recovered:,} products recovered through retries")
        print(f"[IMPACT]   Without retries, success rate would have been {(round1_success/total_processed*100) if total_processed > 0 else 0:.1f}%")
        print(f"[IMPACT]   With retries, success rate is {(total_success/total_processed*100) if total_processed > 0 else 0:.1f}%")
        improvement = ((total_success - round1_success) / total_processed * 100) if total_processed > 0 else 0
        print(f"[IMPACT]   Improvement: +{improvement:.1f} percentage points")

    print(f"\n{'='*80}")
    print("[SUCCESS] 3-Round Selenium scraping completed successfully!")
    print(f"{'='*80}\n")

    log.info("="*80)
    log.info(f"3-Round Selenium scraping completed")
    log.info(f"Total: {total_processed:,}, Success: {total_success:,} ({(total_success/total_processed*100) if total_processed > 0 else 0:.1f}%), Failed: {total_failed:,}")
    log.info(f"Round 1: {round1_success:,}, Round 2: {round2_success:,}, Round 3: {round3_success:,}")
    log.info("="*80)

if __name__ == "__main__":
    main()
