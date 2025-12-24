#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AlfaBeta – COMPANIES index dump (srvLab)

Flow:
1. Login to https://www.alfabeta.net/precio/srv
2. Submit BLANK 'patron' on form#srvLab (Índice de Laboratorios)
3. DO NOT auto-translate (keep original Spanish)
4. Extract ONLY <a> links inside <table class="estandar"> (company names)
5. Handle pagination until no 'Siguiente'
6. Save to ./input/Companylist.csv with header: Company
"""

import os, csv, time, logging
from pathlib import Path
from typing import List, Set

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# ====== CONFIG ======
HUB_URL = "https://www.alfabeta.net/precio/"
USERNAME = os.getenv("ALFABETA_USER", "your_email@example.com")   # replace or set env var
PASSWORD = os.getenv("ALFABETA_PASS", "your_password")

HEADLESS   = False
WAIT_SHORT = 5
WAIT_LONG  = 15
PAUSE      = 0.6

INPUT_DIR = Path("./input"); INPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_COMPANIES = INPUT_DIR / "Companylist.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("alfabeta-companies-dump")

# ====== DRIVER ======
def setup_driver(headless=True):
    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    # Explicitly DISABLE Chrome Translate
    # (and avoid English-only accept-languages which can trigger auto-translate prompts)
    prefs = {
        "translate": {"enabled": False},
        "translate_whitelists": {},  # no whitelists
        "intl.accept_languages": "es-ES,es,en-US,en"
    }
    opts.add_experimental_option("prefs", prefs)
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-features=Translate,TranslateUI")

    d = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    d.set_page_load_timeout(45)
    d.implicitly_wait(0)
    return d

def is_login_page(d):
    try:
        return d.find_elements(By.ID, "usuario") and d.find_elements(By.ID, "clave")
    except Exception:
        return False

def ensure_logged_in(d):
    if not is_login_page(d):
        return
    log.info("Logging in…")
    user = WebDriverWait(d, WAIT_LONG).until(EC.presence_of_element_located((By.ID, "usuario")))
    pwd  = WebDriverWait(d, WAIT_LONG).until(EC.presence_of_element_located((By.ID, "clave")))
    user.clear(); user.send_keys(USERNAME)
    pwd.clear(); pwd.send_keys(PASSWORD)

    try:
        WebDriverWait(d, WAIT_SHORT).until(EC.element_to_be_clickable((
            By.CSS_SELECTOR,
            "input[type='button'][value='Enviar'], input[value='Enviar'], button[value='Enviar']"
        ))).click()
    except Exception:
        try:
            d.execute_script("if (typeof enviar === 'function') enviar();")
        except Exception:
            pwd.send_keys(Keys.ENTER)

    # Dismiss potential alert
    try:
        WebDriverWait(d, 2).until(EC.alert_is_present()); Alert(d).accept()
    except TimeoutException:
        pass
    time.sleep(1)

def open_hub(d):
    d.get(HUB_URL)
    ensure_logged_in(d)
    if d.current_url != HUB_URL:
        d.get(HUB_URL)

def clean(s: str) -> str:
    return " ".join((s or "").split()).strip()

def submit_blank_companies(d):
    form = WebDriverWait(d, WAIT_LONG).until(EC.presence_of_element_located((By.CSS_SELECTOR, "form#srvLab")))
    box = form.find_element(By.CSS_SELECTOR, "input[name='patron']")
    box.clear()
    try:
        form.find_element(By.CSS_SELECTOR, "input[type='submit'], .mfsubmit").click()
    except:
        box.send_keys(Keys.ENTER)
    time.sleep(PAUSE)
    ensure_logged_in(d)

# ====== EXTRACTION ======
def extract_companies_page(d) -> List[str]:
    time.sleep(0.5)  # small rendering pause; NOT for translation
    anchors = d.find_elements(By.CSS_SELECTOR, "table.estandar td a")
    names = []
    for a in anchors:
        txt = clean(a.get_attribute("innerText"))
        if txt:
            names.append(txt)
    return names

def go_next(d) -> bool:
    # Spanish-first selectors
    for sel in [
        "a#siguiente",
        "a[rel='next']",
        "a[title*='Siguiente']",
        "a[title*='siguiente']",
        "a.paginacion_siguiente"
    ]:
        try:
            el = d.find_element(By.CSS_SELECTOR, sel)
            if el.is_displayed() and el.is_enabled():
                el.click(); time.sleep(PAUSE); return True
        except:
            continue
    return False

# ====== MAIN ======
def main():
    d = setup_driver(headless=HEADLESS)
    try:
        open_hub(d)
        submit_blank_companies(d)

        acc: Set[str] = set()
        while True:
            acc.update(extract_companies_page(d))
            log.info(f"Companies collected so far: {len(acc)}")
            if not go_next(d): break

        with open(OUT_COMPANIES, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Company"])
            for comp in sorted(acc, key=lambda x: x.lower()):
                w.writerow([comp])

        log.info(f"Saved {len(acc)} companies → {OUT_COMPANIES}")
    finally:
        d.quit(); log.info("Done.")

if __name__ == "__main__":
    main()
