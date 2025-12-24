from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
from pathlib import Path

URL = "https://pharmacy.moh.gov.my/ms/apps/drug-price"

OUT_DIR = Path("../Output")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    """Scrape all drug prices from MyPriMe website."""
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    try:
        print("Opening MyPriMe website...")
        driver.get(URL)

        wait = WebDriverWait(driver, 20)

        # Wait until "View All" is clickable
        print("Waiting for 'View All' button...")
        view_all_btn = wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//a[contains(., 'View All') or contains(., 'view all') or contains(., 'Lihat Semua')]"
            ))
        )

        # Click "View All"
        print("Clicking 'View All' to load all products...")
        driver.execute_script("arguments[0].click();", view_all_btn)

        # Give JS time to render full table
        time.sleep(5)

        # Locate table
        print("Extracting table data...")
        table = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.tinytable"))
        )

        # Extract headers
        headers = [
            th.text.strip()
            for th in table.find_elements(By.CSS_SELECTOR, "thead th")
        ]

        # Extract rows
        data = []
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) != len(headers):
                continue
            data.append([cell.text.strip() for cell in cells])

        # Save to CSV
        df = pd.DataFrame(data, columns=headers)
        output_path = OUT_DIR / "malaysia_drug_prices_view_all.csv"
        df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"✅ Scraped {len(df)} rows")
        print(f"✅ Saved to {output_path}")

    except Exception as e:
        print(f"❌ ERROR: {e}")
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    main()

