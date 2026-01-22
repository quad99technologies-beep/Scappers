import sys
import os
import logging

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

try:
    import undetected_chromedriver as uc
    UNDETECTED_CHROME_AVAILABLE = True
except ImportError:
    UNDETECTED_CHROME_AVAILABLE = False
    print("=" * 80)
    print("WARNING: undetected-chromedriver not installed.")
    print("Cloudflare bypass may not work properly.")
    print("=" * 80)
    print("To install: pip install undetected-chromedriver")
    print("=" * 80)

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from pathlib import Path
from config_loader import load_env_file, require_env, getenv, get_output_dir

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.browser_observer import observe_selenium, wait_until_idle
from core.stealth_profile import apply_selenium
from core.human_actions import pause
from core.standalone_checkpoint import run_with_checkpoint
from core.chrome_manager import get_chromedriver_path
from smart_locator import SmartLocator
from state_machine import NavigationStateMachine, NavigationState, StateCondition

# Import pandas with graceful error handling
try:
    import pandas as pd
except ImportError as e:
    print("=" * 80)
    print("ERROR: Required module 'pandas' is not installed.")
    print("=" * 80)
    print("Please install dependencies by running:")
    print("  pip install -r requirements.txt")
    print("")
    print("Or install pandas directly:")
    print("  pip install pandas")
    print("=" * 80)
    sys.exit(1)

# Load environment variables from .env file
load_env_file()

URL = require_env("SCRIPT_01_URL")

# Use ConfigManager output directory instead of local output folder
OUT_DIR = get_output_dir()

# Setup logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] [%(name)s] %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def get_chrome_major_version():
    """Get the major version of installed Chrome browser."""
    import subprocess
    import re

    try:
        # Try Windows registry first (faster and more reliable on Windows)
        if os.name == 'nt':
            try:
                result = subprocess.run(
                    ['reg', 'query', 'HKEY_CURRENT_USER\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                if result.returncode != 0:
                    result = subprocess.run(
                        ['reg', 'query', 'HKEY_LOCAL_MACHINE\\Software\\Google\\Chrome\\BLBeacon', '/v', 'version'],
                        capture_output=True,
                        text=True,
                        timeout=2
                    )

                version_match = re.search(r'version\s+REG_SZ\s+(\d+)\.\d+\.\d+\.\d+', result.stdout, re.IGNORECASE)
                if version_match:
                    return int(version_match.group(1))
            except Exception:
                pass

        # Fallback: Try chrome.exe --version (slower)
        chrome_paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"
        ]

        for chrome_path in chrome_paths:
            if os.path.exists(chrome_path):
                result = subprocess.run(
                    [chrome_path, "--version"],
                    capture_output=True,
                    text=True,
                    timeout=3,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
                )
                version_match = re.search(r'(\d+)\.\d+\.\d+\.\d+', result.stdout)
                if version_match:
                    return int(version_match.group(1))
    except Exception as e:
        print(f"Warning: Could not detect Chrome version automatically: {e}", flush=True)

    return None

def main():
    """Scrape all drug prices from MyPriMe website."""

    # Enable headless mode (hide browser)
    headless_str = getenv("SCRIPT_01_HEADLESS")
    headless = headless_str.lower() == "true" if headless_str else True
    
    # Use undetected-chromedriver if available (better Cloudflare bypass)
    if UNDETECTED_CHROME_AVAILABLE:
        print("Using undetected-chromedriver for Cloudflare bypass...", flush=True)
        options = uc.ChromeOptions()

        # IMPORTANT: Cloudflare detects true headless mode and blocks it
        # Solution: Run visible browser (Selenium requires visible window for text extraction anyway)
        if headless:
            print("NOTE: Headless=true in config, but running VISIBLE browser (Cloudflare requirement).", flush=True)
            print("Browser will run in background - you can continue working.", flush=True)
            # Run browser in normal visible mode (required for both Cloudflare AND Selenium text extraction)
            options.add_argument("--window-size=1920,1080")
        else:
            chrome_start_max = getenv("SCRIPT_01_CHROME_START_MAXIMIZED")
            if chrome_start_max:
                options.add_argument(chrome_start_max)

        # Additional options for undetected-chromedriver
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        # Stability improvements to prevent tab crashes
        options.add_argument("--disable-gpu")  # Prevent GPU crashes
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-features=VizDisplayCompositor")

        # Create undetected Chrome driver (automatically handles Cloudflare bypass)
        # Auto-detect Chrome version to prevent version mismatch errors
        chrome_version = get_chrome_major_version()
        if chrome_version:
            print(f"Detected Chrome version: {chrome_version}", flush=True)
            # NEVER use headless=True with Cloudflare - it always gets detected
            driver = uc.Chrome(
                options=options,
                version_main=chrome_version,
                driver_executable_path=None,
                use_subprocess=True
            )
        else:
            print("Chrome version auto-detection failed, using default", flush=True)
            driver = uc.Chrome(
                options=options,
                version_main=None,
                driver_executable_path=None,
                use_subprocess=True
            )
    else:
        # Fallback to regular Selenium with stealth options
        print("Using regular Selenium (undetected-chromedriver not available)...", flush=True)
        options = webdriver.ChromeOptions()
        apply_selenium(options)
        
        # Enhanced anti-detection options for Cloudflare bypass
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")
        
        # Use a realistic user agent
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        if headless:
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
        else:
            chrome_start_max = getenv("SCRIPT_01_CHROME_START_MAXIMIZED")
            if chrome_start_max:
                options.add_argument(chrome_start_max)
        chrome_disable_automation = getenv("SCRIPT_01_CHROME_DISABLE_AUTOMATION")
        if chrome_disable_automation:
            options.add_argument(chrome_disable_automation)

        driver = webdriver.Chrome(
            service=Service(get_chromedriver_path()),
            options=options
        )
        
        # Execute CDP commands to hide webdriver property and other automation indicators
        try:
            driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Override the plugins property to use a custom getter
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5]
                    });
                    
                    // Override the languages property
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en']
                    });
                    
                    // Override permissions
                    const originalQuery = window.navigator.permissions.query;
                    window.navigator.permissions.query = (parameters) => (
                        parameters.name === 'notifications' ?
                            Promise.resolve({ state: Notification.permission }) :
                            originalQuery(parameters)
                    );
                    
                    // Mock chrome object
                    window.chrome = {
                        runtime: {}
                    };
                '''
            })
        except Exception as e:
            logger.debug(f"Could not execute CDP commands for stealth: {e}")
    
    # Register Chrome instance for cleanup tracking
    try:
        from core.chrome_manager import register_chrome_driver
        register_chrome_driver(driver)
    except ImportError:
        pass  # Chrome manager not available, continue without registration
    
    # Track Chrome process IDs for this pipeline run
    try:
        from core.chrome_pid_tracker import get_chrome_pids_from_driver, save_chrome_pids
        from pathlib import Path
        
        # Get repo root (assuming script is in scripts/Malaysia/)
        repo_root = Path(__file__).resolve().parent.parent.parent
        scraper_name = "Malaysia"
        
        # Extract PIDs from driver
        pids = get_chrome_pids_from_driver(driver)
        if pids:
            save_chrome_pids(scraper_name, repo_root, pids)
    except Exception as e:
        pass  # PID tracking not critical

    try:
        # Initialize smart locator and state machine
        locator = SmartLocator(driver, logger=logger)
        state_machine = NavigationStateMachine(locator, logger=logger)
        
        # Navigate to page
        print("Opening MyPriMe website...", flush=True)
        driver.get(URL)
        
        # Additional stealth: Remove webdriver property after page load (only for regular Selenium)
        if not UNDETECTED_CHROME_AVAILABLE:
            try:
                driver.execute_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)
            except Exception as e:
                logger.debug(f"Could not remove webdriver property: {e}")
        
        state = observe_selenium(driver)
        wait_until_idle(state)
        
        # Check for Cloudflare/bot protection verification page and wait for it to complete
        print("Checking for verification page...", flush=True)
        max_verification_wait = 90  # Maximum seconds to wait for verification (increased)
        verification_start = time.time()
        
        # Check if we're on a verification page
        try:
            page_source = driver.page_source.lower()
            page_title = driver.title.lower()
            
            is_verification = (
                "please wait" in page_source or
                "verifying" in page_source or
                "one moment" in page_title or
                "checking your browser" in page_source or
                "just a moment" in page_source
            )
            
            if is_verification:
                print("  -> Verification page detected, waiting for target page to load (up to 90s)...", flush=True)
                # The verification page auto-reloads after ~5 seconds
                # Wait for the page to change from verification to actual content
                wait_interval = 2
                target_page_loaded = False
                
                while time.time() - verification_start < max_verification_wait:
                    time.sleep(wait_interval)
                    try:
                        current_source = driver.page_source.lower()
                        current_title = driver.title.lower()
                        
                        # Check if we're still on verification page
                        still_verifying = (
                            "please wait" in current_source or
                            "verifying" in current_source or
                            "one moment" in current_title
                        )
                        
                        # Positive check: verify we're on the actual target page
                        # Look for indicators that the MyPriMe page has loaded
                        has_target_content = (
                            "tablefooter" in current_source or
                            "tinytable" in current_source or
                            "sorter.showall" in current_source or
                            "drug-price" in driver.current_url.lower()
                        )
                        
                        if not still_verifying and has_target_content:
                            print("  -> Verification complete, target page loaded", flush=True)
                            # Don't close the instance - it would lose the Cloudflare session
                            # Just continue with the same instance
                            target_page_loaded = True
                            break
                        elif not still_verifying:
                            # Not verifying anymore, but check if target content appeared
                            if has_target_content:
                                print("  -> Verification complete, target page loaded", flush=True)
                                # Don't close the instance - it would lose the Cloudflare session
                                # Just continue with the same instance
                                target_page_loaded = True
                                break
                            else:
                                # Page changed but target content not yet loaded - wait a bit more
                                time.sleep(2)
                        
                        elapsed = int(time.time() - verification_start)
                        if elapsed % 10 == 0:  # Print every 10 seconds
                            print(f"  -> Still waiting for verification... ({elapsed}s)", flush=True)
                    except Exception as e:
                        logger.debug(f"Error checking verification status: {e}")
                        time.sleep(3)
                        # Try to check one more time
                        try:
                            if "tablefooter" in driver.page_source.lower() or "tinytable" in driver.page_source.lower():
                                target_page_loaded = True
                                print("  -> Target page detected after error recovery", flush=True)
                                break
                        except:
                            pass
                
                if not target_page_loaded:
                    # Final check before giving up
                    try:
                        final_check = driver.page_source.lower()
                        if "tablefooter" in final_check or "tinytable" in final_check:
                            print("  -> Target page detected on final check", flush=True)
                            target_page_loaded = True
                    except:
                        pass
                    
                    if not target_page_loaded:
                        print("  -> WARNING: Verification wait timed out, but target page content not detected", flush=True)
                        print("  -> Proceeding anyway - page may still be loading...", flush=True)
            else:
                print("  -> No verification page detected", flush=True)
                # Still verify we have target content
                try:
                    if "tablefooter" not in driver.page_source.lower() and "tinytable" not in driver.page_source.lower():
                        print("  -> WARNING: Target page content not yet detected, waiting...", flush=True)
                        # Wait a bit for content to load
                        for _ in range(5):
                            time.sleep(2)
                            if "tablefooter" in driver.page_source.lower() or "tinytable" in driver.page_source.lower():
                                print("  -> Target page content now detected", flush=True)
                                break
                except:
                    pass
        except Exception as e:
            logger.debug(f"Error checking for verification page: {e}")
        
        # Give page time to fully load after verification
        time.sleep(2)
        
        # Final verification: ensure we're on the target page before proceeding
        try:
            page_check = driver.page_source.lower()
            if "tablefooter" not in page_check and "tinytable" not in page_check:
                print("  -> WARNING: Target page indicators not found. Page may not have loaded correctly.", flush=True)
                print(f"  -> Current URL: {driver.current_url}", flush=True)
                print(f"  -> Page title: {driver.title}", flush=True)
        except Exception as e:
            logger.debug(f"Error in final page verification: {e}")
        
        # Transition to PAGE_LOADED state
        wait_timeout = float(require_env("SCRIPT_01_WAIT_TIMEOUT"))
        if not state_machine.transition_to(NavigationState.PAGE_LOADED, reload_on_failure=True):
            raise RuntimeError("Failed to reach PAGE_LOADED state")
        
        # Detect DOM changes
        locator.detect_dom_change("body", "main_page")
        
        # Wait a bit for dynamic content to load
        print("Waiting for page content to stabilize...", flush=True)
        time.sleep(3)  # Give page time for any dynamic content to load after verification
        
        # Verify we're on the correct page before proceeding
        print("Verifying page content...", flush=True)
        try:
            page_source_check = driver.page_source
            has_tablefooter = "tablefooter" in page_source_check.lower()
            has_tinytable = "tinytable" in page_source_check.lower()
            has_sorter = "sorter.showall" in page_source_check
            
            if not (has_tablefooter or has_tinytable or has_sorter):
                print("  -> WARNING: Expected page content not found!", flush=True)
                print(f"  -> Current URL: {driver.current_url}", flush=True)
                print(f"  -> Page title: {driver.title}", flush=True)
                # Save page source for debugging
                try:
                    debug_dir = Path("debug")
                    debug_dir.mkdir(exist_ok=True)
                    debug_file = debug_dir / f"page_source_before_viewall_{int(time.time())}.html"
                    with open(debug_file, "w", encoding="utf-8") as f:
                        f.write(page_source_check)
                    print(f"  -> Saved page source to {debug_file} for debugging", flush=True)
                except Exception as e:
                    logger.debug(f"Could not save debug page source: {e}")
            else:
                print("  -> Page content verified", flush=True)
        except Exception as e:
            logger.debug(f"Error verifying page content: {e}")
        
        # Find "View All" link using smart locator with fallback
        print("Waiting for 'View All' link...", flush=True)
        view_all_xpath = require_env("SCRIPT_01_VIEW_ALL_XPATH")
        view_all_text = getenv("SCRIPT_01_VIEW_ALL_TEXT", "view all")
        
        # The "View All" element is an <a> tag, not a button
        # Use text and XPath selectors (no role parameter since it's a link)
        view_all_btn = locator.find_element(
            text=view_all_text,
            xpath=view_all_xpath,
            timeout=wait_timeout,
            required=True
        )
        
        if not view_all_btn:
            raise RuntimeError("Could not find 'View All' button with any selector strategy")

        # Click "View All"
        print("Clicking 'View All' to load all products...", flush=True)
        driver.execute_script("arguments[0].click();", view_all_btn)
        pause()  # Human-paced pause after click
        state = observe_selenium(driver)
        wait_until_idle(state)
        
        # Wait for table to be ready using state machine
        table_selector = require_env("SCRIPT_01_TABLE_SELECTOR")
        
        # Define custom state for table ready
        from state_machine import StateDefinition
        table_ready_state = StateDefinition(
            state=NavigationState.TABLE_READY,
            required_conditions=[
                StateCondition(element_selector=table_selector, min_count=1, max_wait=wait_timeout),
                StateCondition(
                    custom_check=lambda d: _check_table_stable(d, table_selector),
                    max_wait=wait_timeout
                )
            ],
            description="Table is ready with stable row count",
            retry_on_failure=True,
            max_retries=3,
            retry_delay=2.0
        )
        state_machine.add_custom_state(table_ready_state)
        
        if not state_machine.transition_to(NavigationState.TABLE_READY):
            # Fallback: use dynamic wait for table stability
            print("State machine transition failed, using dynamic wait for table...", flush=True)
            _wait_for_table_stable(driver, table_selector, wait_timeout)
        
        # Locate table using smart locator
        print("Extracting table data...", flush=True)
        table = locator.find_element(
            css=table_selector,
            timeout=wait_timeout,
            required=True
        )
        
        # Check for anomalies
        anomalies = locator.detect_anomalies(
            table_selector=table_selector,
            error_text_patterns=["error", "not found", "failed"]
        )
        if anomalies:
            logger.warning(f"[ANOMALY] Detected anomalies: {anomalies}")
            # Continue anyway, but log the issue

        # Extract headers
        print("Extracting table headers...", flush=True)
        header_selector = require_env("SCRIPT_01_HEADER_SELECTOR")
        headers = [
            th.text.strip()
            for th in table.find_elements(By.CSS_SELECTOR, header_selector)
        ]
        print(f"  -> Found {len(headers)} columns: {', '.join(headers[:5])}{'...' if len(headers) > 5 else ''}", flush=True)
        
        if not headers:
            raise RuntimeError("No table headers found - table structure may have changed")

        # Extract rows with row count stability check
        print("Extracting table rows...", flush=True)
        data = []
        row_selector = require_env("SCRIPT_01_ROW_SELECTOR")
        
        # Wait for row count to stabilize
        previous_row_count = 0
        stable_checks = 0
        max_stable_checks = 3
        check_interval = 0.5
        
        for _ in range(int(wait_timeout / check_interval)):
            rows = table.find_elements(By.CSS_SELECTOR, row_selector)
            current_row_count = len(rows)
            
            if current_row_count == previous_row_count and current_row_count > 0:
                stable_checks += 1
                if stable_checks >= max_stable_checks:
                    print(f"  -> Row count stable at {current_row_count:,} rows", flush=True)
                    break
            else:
                stable_checks = 0
                if current_row_count > 0:
                    print(f"  -> Row count: {current_row_count:,} (waiting for stability)...", flush=True)
            
            previous_row_count = current_row_count
            time.sleep(check_interval)
        
        rows = table.find_elements(By.CSS_SELECTOR, row_selector)
        total_rows = len(rows)
        print(f"  -> Found {total_rows:,} rows to process", flush=True)
        
        # Anomaly check: empty table
        if total_rows == 0:
            logger.warning("[ANOMALY] Empty table detected - no rows found")
            # Capture HTML snapshot for debugging
            try:
                snapshot_path = OUT_DIR / "table_snapshot_error.html"
                with open(snapshot_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                logger.info(f"[ANOMALY] HTML snapshot saved to {snapshot_path}")
            except Exception as e:
                logger.debug(f"Could not save snapshot: {e}")
        
        cell_selector = require_env("SCRIPT_01_CELL_SELECTOR")
        processed_rows = 0
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, cell_selector)
            if len(cells) != len(headers):
                continue
            data.append([cell.text.strip() for cell in cells])
            processed_rows += 1
            if processed_rows % 100 == 0:
                percent = round((processed_rows / total_rows) * 100, 1) if total_rows > 0 else 0
                print(f"  -> Processed {processed_rows:,}/{total_rows:,} rows ({percent}%)...", flush=True)
                print(f"[PROGRESS] Extracting rows: {processed_rows}/{total_rows} ({percent}%)", flush=True)

        # Save to CSV
        print(f"\nSaving data to CSV...", flush=True)
        df = pd.DataFrame(data, columns=headers)
        output_filename = require_env("SCRIPT_01_OUTPUT_CSV")
        output_path = OUT_DIR / output_filename
        df.to_csv(output_path, index=False, encoding="utf-8")
        
        # Anomaly check: CSV size
        csv_size = output_path.stat().st_size if output_path.exists() else 0
        if csv_size < 100:  # Very small CSV
            logger.warning(f"[ANOMALY] CSV file is very small: {csv_size} bytes")

        print(f"[OK] Scraped {len(df):,} rows", flush=True)
        print(f"[OK] Saved to {output_path}", flush=True)
        print(f"[PROGRESS] Extracting rows: {len(df)}/{len(df)} (100%)", flush=True)
        
        # Log metrics
        metrics = locator.get_metrics()
        metrics_summary = metrics.get_summary()
        logger.info(f"[METRICS] Locator performance: {metrics_summary}")
        
        # Log state transitions
        state_history = state_machine.get_state_history()
        logger.info(f"[METRICS] State transitions: {len(state_history)} transitions")
        for state, timestamp, success in state_history:
            status = "SUCCESS" if success else "FAILED"
            logger.debug(f"[METRICS] State: {state.value} at {timestamp:.2f}s - {status}")

    except Exception as e:
        print(f"[ERROR] ERROR: {e}", flush=True)
        logger.exception("[ERROR] Exception details:")
        raise
    finally:
        driver.quit()


def _check_table_stable(driver, table_selector):
    """Check if table row count is stable (used in state machine)."""
    try:
        row_selector = require_env("SCRIPT_01_ROW_SELECTOR")
        table = driver.find_element(By.CSS_SELECTOR, table_selector)
        
        previous_count = 0
        stable_checks = 0
        min_stable_checks = 3
        check_interval = 0.5
        
        for _ in range(10):  # Check up to 10 times
            rows = table.find_elements(By.CSS_SELECTOR, row_selector)
            current_count = len(rows)
            
            if current_count == previous_count and current_count > 0:
                stable_checks += 1
                if stable_checks >= min_stable_checks:
                    return True
            else:
                stable_checks = 0
            
            previous_count = current_count
            time.sleep(check_interval)
        
        return False
    except Exception:
        return False


def _wait_for_table_stable(driver, table_selector, timeout):
    """Wait for table to have stable row count."""
    row_selector = require_env("SCRIPT_01_ROW_SELECTOR")
    start_time = time.time()
    previous_count = 0
    stable_checks = 0
    max_stable_checks = 3
    check_interval = 0.5
    
    while time.time() - start_time < timeout:
        try:
            table = driver.find_element(By.CSS_SELECTOR, table_selector)
            rows = table.find_elements(By.CSS_SELECTOR, row_selector)
            current_count = len(rows)
            
            if current_count == previous_count and current_count > 0:
                stable_checks += 1
                if stable_checks >= max_stable_checks:
                    return
            else:
                stable_checks = 0
            
            previous_count = current_count
        except Exception:
            pass
        
        time.sleep(check_interval)

if __name__ == "__main__":
    run_with_checkpoint(
        main,
        "Malaysia",
        1,
        "Product Registration Number",
        output_files=[OUT_DIR / "malaysia_drug_prices_view_all.csv"]
    )

