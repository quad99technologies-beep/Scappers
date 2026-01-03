import sys
import os

# Force unbuffered output for real-time console updates
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
os.environ.setdefault('PYTHONUNBUFFERED', '1')

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from pathlib import Path
from config_loader import load_env_file, require_env, getenv, get_output_dir

# Load environment variables from .env file
load_env_file()

URL = require_env("SCRIPT_01_URL")

# Use ConfigManager output directory instead of local output folder
OUT_DIR = get_output_dir()

def main():
    """Scrape all drug prices from MyPriMe website."""
    options = webdriver.ChromeOptions()
    # Enable headless mode (hide browser)
    headless_str = getenv("SCRIPT_01_HEADLESS")
    headless = headless_str.lower() == "true" if headless_str else True
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
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    # Register Chrome instance for cleanup tracking
    try:
        from core.chrome_manager import register_chrome_driver
        register_chrome_driver(driver)
    except ImportError:
        pass  # Chrome manager not available, continue without registration

    try:
        print("Opening MyPriMe website...", flush=True)
        driver.get(URL)

        wait_timeout = int(require_env("SCRIPT_01_WAIT_TIMEOUT"))
        wait = WebDriverWait(driver, wait_timeout)

        # Wait until "View All" is clickable
        print("Waiting for 'View All' button...", flush=True)
        view_all_xpath = require_env("SCRIPT_01_VIEW_ALL_XPATH")
        view_all_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, view_all_xpath))
        )

        # Click "View All"
        print("Clicking 'View All' to load all products...", flush=True)
        driver.execute_script("arguments[0].click();", view_all_btn)

        # Give JS time to render full table
        click_delay = float(require_env("SCRIPT_01_CLICK_DELAY"))
        print(f"Waiting {click_delay}s for table to load...", flush=True)
        time.sleep(click_delay)

        # Locate table
        print("Extracting table data...", flush=True)
        table_selector = require_env("SCRIPT_01_TABLE_SELECTOR")
        table = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, table_selector))
        )

        # Extract headers
        print("Extracting table headers...", flush=True)
        header_selector = require_env("SCRIPT_01_HEADER_SELECTOR")
        headers = [
            th.text.strip()
            for th in table.find_elements(By.CSS_SELECTOR, header_selector)
        ]
        print(f"  -> Found {len(headers)} columns: {', '.join(headers[:5])}{'...' if len(headers) > 5 else ''}", flush=True)

        # Extract rows
        print("Extracting table rows...", flush=True)
        data = []
        row_selector = require_env("SCRIPT_01_ROW_SELECTOR")
        rows = table.find_elements(By.CSS_SELECTOR, row_selector)
        total_rows = len(rows)
        print(f"  -> Found {total_rows:,} rows to process", flush=True)
        cell_selector = require_env("SCRIPT_01_CELL_SELECTOR")
        processed_rows = 0
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, cell_selector)
            if len(cells) != len(headers):
                continue
            data.append([cell.text.strip() for cell in cells])
            processed_rows += 1
            if processed_rows % 100 == 0:
                print(f"  -> Processed {processed_rows:,}/{total_rows:,} rows ({processed_rows*100//total_rows}%)...", flush=True)

        # Save to CSV
        print(f"\nSaving data to CSV...", flush=True)
        df = pd.DataFrame(data, columns=headers)
        output_filename = require_env("SCRIPT_01_OUTPUT_CSV")
        output_path = OUT_DIR / output_filename
        df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"[OK] Scraped {len(df):,} rows", flush=True)
        print(f"[OK] Saved to {output_path}", flush=True)

    except Exception as e:
        print(f"[ERROR] ERROR: {e}")
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

