#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scraper Registry — Single source of truth for all scraper configurations.

Used by both scraper_gui.py and api_server.py to ensure consistent behavior.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional

# Repo root (two levels up from services/)
REPO_ROOT = Path(__file__).resolve().parents[2]


def _p(relative: str) -> Path:
    """Resolve a path relative to REPO_ROOT."""
    return REPO_ROOT / relative


# ---------------------------------------------------------------------------
# Scraper configuration registry
# ---------------------------------------------------------------------------
SCRAPER_CONFIGS: Dict[str, Dict[str, Any]] = {
    "CanadaQuebec": {
        "display_name": "Canada Quebec",
        "path": "scripts/canada_quebec",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Split PDF into Annexes", "script": "01_split_pdf_into_annexes.py", "desc": "Split PDF into annexes (IV.1, IV.2, V)"},
            {"name": "02 - Validate PDF Structure", "script": "02_validate_pdf_structure.py", "desc": "Validate PDF structure (optional)"},
            {"name": "03 - Extract Annexe IV.1", "script": "03_extract_annexe_iv1.py", "desc": "Extract Annexe IV.1 data"},
            {"name": "04 - Extract Annexe IV.2", "script": "04_extract_annexe_iv2.py", "desc": "Extract Annexe IV.2 data"},
            {"name": "05 - Extract Annexe V", "script": "05_extract_annexe_v.py", "desc": "Extract Annexe V data"},
            {"name": "06 - Merge All Annexes", "script": "06_merge_all_annexes.py", "desc": "Merge all annexe outputs into final CSV"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "PIPELINE_RUN_ID",
        "db_prefix": "cq",
    },
    "Malaysia": {
        "display_name": "Malaysia",
        "path": "scripts/Malaysia",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "steps/step_00_backup_clean.py", "desc": "Backup, clean, init DB and run_id"},
            {"name": "01 - Product Registration Number", "script": "steps/step_01_registration.py", "desc": "Get drug prices from MyPriMe (DB)"},
            {"name": "02 - Product Details", "script": "steps/step_02_product_details.py", "desc": "Get company/holder from QUEST3+ (DB)"},
            {"name": "03 - Consolidate Results", "script": "steps/step_03_consolidate.py", "desc": "Consolidate product details in DB"},
            {"name": "04 - Get Fully Reimbursable", "script": "steps/step_04_reimbursable.py", "desc": "Scrape fully reimbursable drugs (DB)"},
            {"name": "05 - Generate PCID Mapped", "script": "steps/step_05_pcid_export.py", "desc": "Generate PCID-mapped CSVs from DB"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "MALAYSIA_RUN_ID",
        "db_prefix": "my",
    },
    "Argentina": {
        "display_name": "Argentina",
        "path": "scripts/Argentina",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Get Product List", "script": "01_getProdList.py", "desc": "Extract product list for each company"},
            {"name": "02 - Prepare URLs", "script": "02_prepare_urls.py", "desc": "Prepare URLs and initialize scrape state"},
            {"name": "03 - Scrape Products (Selenium - Product Search)", "script": "03_alfabeta_selenium_scraper.py", "desc": "Scrape products using Selenium product name search"},
            {"name": "04 - Scrape Products (Selenium - Company Search)", "script": "03b_alfabeta_selenium_company_search.py", "desc": "Scrape remaining products using Selenium company name search"},
            {"name": "05 - Scrape Products (API)", "script": "04_alfabeta_api_scraper.py", "desc": "Scrape remaining products using API"},
            {"name": "06 - Translate Using Dictionary", "script": "05_TranslateUsingDictionary.py", "desc": "Translate Spanish to English"},
            {"name": "07 - Generate Output", "script": "06_GenerateOutput.py", "desc": "Generate final output report"},
            {"name": "08 - Scrape No-Data (Selenium Retry)", "script": "07_scrape_no_data_pipeline.py", "desc": "Retry scraping products with no data using Selenium worker (optional, skip by default)", "skip_by_default": True},
            {"name": "09 - Refresh Export", "script": "08_refresh_export.py", "desc": "Re-run translation and final export to include newly scraped data"},
            {"name": "10 - Statistics & Data Validation", "script": "08_stats_and_validation.py", "desc": "Detailed stats: URL coverage, OOS, PCID coverage, no-data gaps, scrape/translation counts"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "ARGENTINA_RUN_ID",
        "db_prefix": "ar",
    },
    "CanadaOntario": {
        "display_name": "Canada Ontario",
        "path": "scripts/canada_ontario",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Extract Product Details", "script": "01_extract_product_details.py", "desc": "Extract product details from Ontario Formulary"},
            {"name": "02 - Extract EAP Prices", "script": "02_ontario_eap_prices.py", "desc": "Extract Exceptional Access Program product prices"},
            {"name": "03 - Generate Final Output", "script": "03_GenerateOutput.py", "desc": "Generate final output report with standardized columns"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "CANADA_ONTARIO_RUN_ID",
        "db_prefix": "co",
    },
    "Netherlands": {
        "display_name": "Netherlands",
        "path": "scripts/Netherlands",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Hybrid Scraper", "script": "scraper.py", "desc": "Collect URLs, scrape product packs, and consolidate in one step"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "NETHERLANDS_RUN_ID",
        "db_prefix": "nl",
    },
    "Belarus": {
        "display_name": "Belarus",
        "path": "scripts/Belarus",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Extract RCETH Data", "script": "01_belarus_rceth_extract.py", "desc": "Extract drug registration and pricing data from rceth.by"},
            {"name": "02 - PCID Mapping", "script": "02_belarus_pcid_mapping.py", "desc": "Apply PCID mappings to extracted data"},
            {"name": "03 - Process and Translate", "script": "04_belarus_process_and_translate.py", "desc": "Translate Russian text using dictionary + AI fallback, cache to DB"},
            {"name": "04 - Format English Export Slate", "script": "03_belarus_format_for_export.py", "desc": "Format translated data into final export template (same as Russia)"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "BELARUS_RUN_ID",
        "db_prefix": "by",
    },
    "Russia": {
        "display_name": "Russia",
        "path": "scripts/Russia",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Extract VED Registry", "script": "01_russia_farmcom_scraper.py", "desc": "Extract VED drug pricing from farmcom.info/site/reestr (with page-level resume)"},
            {"name": "02 - Extract Excluded List", "script": "02_russia_farmcom_excluded_scraper.py", "desc": "Extract excluded drugs from farmcom.info/site/reestr?vw=excl (with page-level resume)"},
            {"name": "03 - Retry Failed Pages", "script": "03_retry_failed_pages.py", "desc": "Retry pages with missing EAN or extraction failures (MANDATORY before translation)"},
            {"name": "04 - Process and Translate", "script": "04_process_and_translate.py", "desc": "Process raw data, translate Russian text to English using dictionary and AI"},
            {"name": "05 - Format for Export", "script": "05_format_for_export.py", "desc": "Format translated data into final export template"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "RUSSIA_RUN_ID",
        "db_prefix": "ru",
    },
    "Taiwan": {
        "display_name": "Taiwan",
        "path": "scripts/Taiwan",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Collect Drug Code URLs", "script": "01_taiwan_collect_drug_code_urls.py.py", "desc": "Collect drug code URLs from NHI site"},
            {"name": "02 - Extract Drug Code Details", "script": "02_taiwan_extract_drug_code_details.py", "desc": "Extract license details for each drug code"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "TAIWAN_RUN_ID",
        "db_prefix": "tw",
    },
    "NorthMacedonia": {
        "display_name": "North Macedonia",
        "path": "scripts/north_macedonia",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Collect Detail URLs", "script": "01_collect_urls.py", "desc": "Collect detail URLs across overview pages"},
            {"name": "02 - Scrape Detail Pages", "script": "02_fast_scrape_details.py", "desc": "Scrape drug register detail data (httpx+lxml fast)"},
            {"name": "03 - Translate Using Dictionary", "script": "04_translate_using_dictionary.py", "desc": "Translate Macedonian terms to English using dictionary + Google Translate fallback"},
            {"name": "04 - Statistics & Validation", "script": "05_stats_and_validation.py", "desc": "Compute statistics, coverage metrics, and data validation warnings"},
            {"name": "05 - Generate PCID Export", "script": "06_generate_export.py", "desc": "Generate PCID-mapped CSV exports to exports/NorthMacedonia/"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "NORTH_MACEDONIA_RUN_ID",
        "db_prefix": "nm",
    },
    "Tender_Chile": {
        "display_name": "Tender Chile",
        "path": "scripts/tender_chile",
        "steps": [
            {"name": "00 - Backup and Clean", "script": "00_backup_and_clean.py", "desc": "Backup output folder and clean for fresh run"},
            {"name": "01 - Get Redirect URLs", "script": "01_fast_redirect_urls.py", "desc": "Get redirect URLs (httpx fast)"},
            {"name": "02 - Extract Tender Details", "script": "02_extract_tender_details.py", "desc": "Extract tender and lot details from MercadoPublico"},
            {"name": "03 - Extract Tender Awards", "script": "03_fast_extract_awards.py", "desc": "Extract bidder-level award data (httpx+BS4 fast)"},
            {"name": "04 - Merge Final CSV", "script": "04_merge_final_csv.py", "desc": "Merge all outputs into final EVERSANA-format CSV"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_resume.py",
        "run_id_env_var": "TENDER_CHILE_RUN_ID",
        "db_prefix": "tc",
    },
    "Tender_Brazil": {
        "display_name": "Tender Brazil",
        "path": "scripts/Tender - Brazil",
        "steps": [
            {"name": "01 - Get Data", "script": "GetData.py", "desc": "Process tenders using configured mode (Input.csv or Date filter)"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "scraper.py",
        "run_id_env_var": "TENDER_BRAZIL_RUN_ID",
        "db_prefix": "tb",
    },
    "Italy": {
        "display_name": "Italy",
        "path": "scripts/Italy",
        "steps": [
            {"name": "01 - Scrape Price Reductions", "script": "02_scrape_price_reductions_v2.py", "desc": "Scrape price reductions from AIFA"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "scraper.py",
        "run_id_env_var": "ITALY_RUN_ID",
        "db_prefix": "it",
    },
    "India": {
        "display_name": "India",
        "path": "scripts/India",
        "steps": [
            {"name": "01 - Initial Scrape", "script": "run_scrapy_india.py", "desc": "Step 1: Scrape all formulations from input (no retries)"},
            {"name": "02 - Retry Failed + Zero Records", "script": "run_scrapy_india.py", "desc": "Step 2: Retry failed formulations and zero_records together"},
            {"name": "03 - QC + CSV Export", "script": "05_qc_and_export.py", "desc": "Step 3: Quality gate checks and export latest run from PostgreSQL to CSV"},
        ],
        "pipeline_bat": "run_pipeline.bat",
        "pipeline_script": "run_pipeline_scrapy.py",
        "run_id_env_var": "INDIA_RUN_ID",
        "db_prefix": "in",
        "execution_mode": "distributed",
        "resume_options": {
            "supports_formulation_resume": True,
            "checkpoint_dir": ".checkpoints",
            "resume_script_args": ["--resume-details"],
        },
    },
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_scraper_names() -> List[str]:
    """Return list of all scraper keys."""
    return list(SCRAPER_CONFIGS.keys())


def get_scraper_config(name: str) -> Optional[Dict[str, Any]]:
    """Return config for a scraper, or None if not found."""
    return SCRAPER_CONFIGS.get(name)


def get_scraper_path(name: str) -> Optional[Path]:
    """Return absolute path to scraper directory."""
    cfg = SCRAPER_CONFIGS.get(name)
    if cfg:
        return REPO_ROOT / cfg["path"]
    return None


def get_pipeline_script(name: str) -> Optional[Path]:
    """Return absolute path to the pipeline runner script."""
    cfg = SCRAPER_CONFIGS.get(name)
    if cfg:
        return REPO_ROOT / cfg["path"] / cfg.get("pipeline_script", "run_pipeline_resume.py")
    return None


def get_execution_mode(name: str) -> str:
    """Return scraper execution mode: 'single' (default) or 'distributed'."""
    cfg = SCRAPER_CONFIGS.get(name) or {}
    mode = str(cfg.get("execution_mode", "single")).strip().lower()
    return "distributed" if mode == "distributed" else "single"


def get_run_id_env_var(name: str) -> str:
    """Return configured run-id env var name for scraper."""
    cfg = SCRAPER_CONFIGS.get(name) or {}
    env_name = str(cfg.get("run_id_env_var", "")).strip()
    if env_name:
        return env_name
    return f"{name.upper().replace(' ', '_').replace('-', '_')}_RUN_ID"


def resolve_country_name(name: str) -> Optional[str]:
    """Resolve a country name (case-insensitive, with aliases) to registry key."""
    # Direct match
    if name in SCRAPER_CONFIGS:
        return name
    # Case-insensitive match
    lower = name.lower().replace(" ", "").replace("_", "").replace("-", "")
    for key in SCRAPER_CONFIGS:
        if key.lower().replace("_", "") == lower:
            return key
    # Display name match
    for key, cfg in SCRAPER_CONFIGS.items():
        if cfg.get("display_name", "").lower().replace(" ", "") == lower:
            return key
    return None
