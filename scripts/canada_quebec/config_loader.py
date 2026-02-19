#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Loader for Canada Quebec Scraper (Facade for Core ConfigManager)

This module provides centralized config and path management for Canada Quebec scraper.
It acts as a facade, delegating all logic to core.config.config_manager.ConfigManager.
"""

import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_repo_root = _script_dir.parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.scraper_config_factory import create_config

SCRAPER_ID = "CanadaQuebec"
config = create_config(SCRAPER_ID)

# --- Path Accessors ---
def get_repo_root() -> Path: return config.get_repo_root()
def get_base_dir() -> Path: return config.get_base_dir()
def get_central_output_dir() -> Path: return config.get_central_output_dir()
def get_input_dir() -> Path: return config.get_input_dir()
def get_output_dir() -> Path: return config.get_output_dir()
def get_backup_dir() -> Path: return config.get_backup_dir()

# --- Environment Accessors ---
def load_env_file() -> None: pass  # no-op, already loaded on import
def get_env(key: str, default: str = "") -> str: return config.getenv(key, default)
def get_env_int(key: str, default: int = 0) -> int: return config.getenv_int(key, default)
def get_env_float(key: str, default: float = 0.0) -> float: return config.getenv_float(key, default)
def get_env_bool(key: str, default: bool = False) -> bool: return config.getenv_bool(key, default)
def get_env_list(key: str, default: list = None) -> list: return config.getenv_list(key, default or [])

# Standardized aliases
def getenv(key: str, default: str = "") -> str: return get_env(key, default)
def getenv_int(key: str, default: int = 0) -> int: return get_env_int(key, default)
def getenv_float(key: str, default: float = 0.0) -> float: return get_env_float(key, default)
def getenv_bool(key: str, default: bool = False) -> bool: return get_env_bool(key, default)
def getenv_list(key: str, default: list = None) -> list: return get_env_list(key, default or [])


def get_split_pdf_dir() -> Path:
    """Get split PDF directory."""
    base = get_output_dir()
    split_subdir = get_env("SPLIT_PDF_DIR", "split_pdf")
    if Path(split_subdir).is_absolute():
        return Path(split_subdir)
    result = base / split_subdir
    result.mkdir(parents=True, exist_ok=True)
    return result


def get_csv_output_dir() -> Path:
    """Get CSV output directory."""
    base = get_output_dir()
    csv_subdir = get_env("CSV_OUTPUT_DIR", "csv")
    if Path(csv_subdir).is_absolute():
        return Path(csv_subdir)
    result = base / csv_subdir
    result.mkdir(parents=True, exist_ok=True)
    return result


def get_qa_output_dir() -> Path:
    """Get QA output directory."""
    base = get_output_dir()
    qa_subdir = get_env("QA_OUTPUT_DIR", "qa")
    if Path(qa_subdir).is_absolute():
        return Path(qa_subdir)
    result = base / qa_subdir
    result.mkdir(parents=True, exist_ok=True)
    return result


# File names
DEFAULT_INPUT_PDF_NAME = get_env("DEFAULT_INPUT_PDF_NAME", "liste-med.pdf")
ANNEXE_IV1_PDF_NAME = get_env("ANNEXE_IV1_PDF_NAME", "annexe_iv1.pdf")
ANNEXE_IV2_PDF_NAME = get_env("ANNEXE_IV2_PDF_NAME", "annexe_iv2.pdf")
ANNEXE_V_PDF_NAME = get_env("ANNEXE_V_PDF_NAME", "annexe_v.pdf")
ANNEXE_IV1_CSV_NAME = "annexe_iv1_extracted.csv"
ANNEXE_IV2_CSV_NAME = "annexe_iv2_extracted.csv"
ANNEXE_V_CSV_NAME = "annexe_v_extracted.csv"
FINAL_REPORT_NAME_PREFIX = get_env("FINAL_REPORT_NAME_PREFIX", "canadaquebecreport_")
FINAL_REPORT_DATE_FORMAT = get_env("FINAL_REPORT_DATE_FORMAT", "%d%m%Y")
LOG_FILE_ANNEXE_V = get_env("LOG_FILE_ANNEXE_V", "annexe_v_extraction_log.txt")
LOG_FILE_MERGE = get_env("LOG_FILE_MERGE", "merge_log.txt")
PIPELINE_LOG_FILE_PREFIX = get_env("PIPELINE_LOG_FILE_PREFIX", "CanadaQuebec_run_")
INDEX_JSON_NAME = get_env("INDEX_JSON_NAME", "index.json")
PDF_STRUCTURE_REPORT_SUFFIX_JSON = get_env("PDF_STRUCTURE_REPORT_SUFFIX_JSON", "_pdf_structure_report.json")
PDF_STRUCTURE_REPORT_SUFFIX_TXT = get_env("PDF_STRUCTURE_REPORT_SUFFIX_TXT", "_pdf_structure_report.txt")
PDF_STRUCTURE_FLAGS_SUFFIX_CSV = get_env("PDF_STRUCTURE_FLAGS_SUFFIX_CSV", "_pdf_structure_flags.csv")
VALIDATION_INPUT_PDF_NAME = get_env("VALIDATION_INPUT_PDF_NAME", "legend_to_end.pdf")

# Final output columns (comma-separated string, will be converted to list)
FINAL_COLUMNS_STR = get_env("FINAL_COLUMNS", 
    "Generic Name,Currency,Ex Factory Wholesale Price,Unit Price,Region,Product Group,Marketing Authority,Local Pack Description,Formulation,Fill Size,Strength,Strength Unit,LOCAL_PACK_CODE")
FINAL_COLUMNS = [col.strip() for col in FINAL_COLUMNS_STR.split(",") if col.strip()]

# API Configuration (secrets)
# API Configuration (secrets)
def _load_secret(key, default=""):
    # 1. Try environment/standard config first
    val = get_env(key, "").strip()
    if val: return val
    
    # 2. Fallback: Try loading from secrets section of .env.json
    try:
        import json
        json_path = get_repo_root() / "config" / f"{SCRAPER_ID}.env.json"
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data.get("secrets", {}).get(key, default).strip()
    except Exception:
        pass
    return default

OPENAI_API_KEY = _load_secret("OPENAI_API_KEY")
OPENAI_MODEL = get_env("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TEMPERATURE = get_env_float("OPENAI_TEMPERATURE", 0.0)

# Static values
STATIC_CURRENCY = get_env("STATIC_CURRENCY", "CAD")
STATIC_REGION = get_env("STATIC_REGION", "NORTH AMERICA")

# Extraction tuning
X_TOL = get_env_float("X_TOL", 1.0)
Y_TOL = get_env_float("Y_TOL", 1.6)
ANNEXE_V_START_PAGE_1IDX = get_env_int("ANNEXE_V_START_PAGE_1IDX", 1)
ANNEXE_V_MAX_ROWS = get_env("ANNEXE_V_MAX_ROWS", "").strip()
if ANNEXE_V_MAX_ROWS and ANNEXE_V_MAX_ROWS.isdigit():
    ANNEXE_V_MAX_ROWS = int(ANNEXE_V_MAX_ROWS)
else:
    ANNEXE_V_MAX_ROWS = None

# Validation thresholds
MIN_PAGES_WITH_DIN = get_env_int("MIN_PAGES_WITH_DIN", 10)
MIN_HEADERS_RATIO = get_env_float("MIN_HEADERS_RATIO", 0.40)
MIN_ROW_SHAPE_RATIO = get_env_float("MIN_ROW_SHAPE_RATIO", 0.55)
MAX_FLAGGED_RATIO = get_env_float("MAX_FLAGGED_RATIO", 0.35)

# Default band configuration
DEFAULT_BAND_STR = get_env("DEFAULT_BAND", "0.42 0.42 0.60 0.58 0.73")
try:
    band_values = [float(x) for x in DEFAULT_BAND_STR.split()]
    if len(band_values) == 5:
        DEFAULT_BAND = {
            "brand_max": band_values[0],
            "manuf_min": band_values[1],
            "manuf_max": band_values[2],
            "pack_min": band_values[3],
            "unit_min": band_values[4],
        }
    else:
        raise ValueError("DEFAULT_BAND must have 5 values")
except (ValueError, AttributeError):
    DEFAULT_BAND = {
        "brand_max": 0.42,
        "manuf_min": 0.42,
        "manuf_max": 0.60,
        "pack_min": 0.58,
        "unit_min": 0.73,
    }

# Database configuration
DB_ENABLED = get_env_bool("DB_ENABLED", False)
DB_HOST = get_env("DB_HOST", "localhost")
DB_PORT = get_env_int("DB_PORT", 5432)
DB_NAME = get_env("DB_NAME", "scraper_db")
DB_USER = get_env("DB_USER", "postgres")
DB_PASSWORD = get_env("DB_PASSWORD", "")
# SCRAPER_ID (above) is for platform paths, loaded from scraper.id in JSON
# This is for database records (different format)
SCRAPER_ID_DB = get_env("SCRAPER_ID", "canada_quebec_ramq")

# AI & Transformation Mode
# Options: HEURISTIC (Regex only), AI_REFINEMENT (Regex + Gemini)
CLEANING_MODE = get_env("CLEANING_MODE", "AI_REFINEMENT").upper()


# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("CanadaQuebec Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Scraper ID: {SCRAPER_ID}")
    print()
    print("Paths:")
    print(f"  Base Dir: {get_base_dir()}")
    print(f"  Input Dir: {get_input_dir()}")
    print(f"  Output Dir: {get_output_dir()}")
    print(f"  CSV Output: {get_csv_output_dir()}")
    print(f"  Split PDF: {get_split_pdf_dir()}")
    print(f"  Backup Dir: {get_backup_dir()}")
    print()
    print("Config Values:")
    print(f"  OpenAI Model: {OPENAI_MODEL}")
    print(f"  OpenAI Key Set: {'Yes' if OPENAI_API_KEY else 'No'}")
    print(f"  DB Enabled: {DB_ENABLED}")
    print(f"  X Tolerance: {X_TOL}")
    print(f"  Y Tolerance: {Y_TOL}")
