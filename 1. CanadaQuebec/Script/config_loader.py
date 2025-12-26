#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Loader (Updated for Platform Config Integration)

This module now wraps platform_config.py for centralized path management.
Maintains backward compatibility with legacy .env files.

Precedence (highest to lowest):
1. Runtime overrides (function parameters)
2. Environment variables (OS-level)
3. Platform config (Documents/ScraperPlatform/config/)
4. Legacy .env files (backward compatibility)
5. Hardcoded defaults
"""

import os
import sys
from pathlib import Path

# Add repo root to path for platform_config import
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

# Try to import platform_config (preferred)
try:
    from platform_config import PathManager, ConfigResolver, get_path_manager, get_config_resolver
    _PLATFORM_CONFIG_AVAILABLE = True
except ImportError:
    _PLATFORM_CONFIG_AVAILABLE = False
    PathManager = None
    ConfigResolver = None

# Scraper ID for this scraper
SCRAPER_ID = "CanadaQuebec"

# Try to load legacy dotenv if available (fallback)
try:
    from dotenv import load_dotenv
    # Load .env from platform root or scraper root
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    else:
        fallback_path = Path(__file__).resolve().parents[1] / ".env"
        if fallback_path.exists():
            load_dotenv(dotenv_path=fallback_path, override=False)
except ImportError:
    pass


def get_env(key: str, default: str) -> str:
    """Get environment variable with default value."""
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        return cr.get(SCRAPER_ID, key, default)
    return os.getenv(key, default)


def get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer with default value."""
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        val = cr.get(SCRAPER_ID, key, str(default))
        try:
            return int(val)
        except (ValueError, TypeError):
            return default
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def get_env_float(key: str, default: float) -> float:
    """Get environment variable as float with default value."""
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        val = cr.get(SCRAPER_ID, key, str(default))
        try:
            return float(val)
        except (ValueError, TypeError):
            return default
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def get_env_bool(key: str, default: bool) -> bool:
    """Get environment variable as boolean with default value."""
    if _PLATFORM_CONFIG_AVAILABLE:
        cr = get_config_resolver()
        val = str(cr.get(SCRAPER_ID, key, "")).strip().lower()
    else:
        val = os.getenv(key, "").strip().lower()

    if val in ("1", "true", "yes", "on"):
        return True
    elif val in ("0", "false", "no", "off", ""):
        return False
    return default


def get_base_dir() -> Path:
    """
    Get base directory.

    With platform_config: Returns scraper-specific directory under platform root
    Legacy mode: Returns parent of Script folder
    """
    if _PLATFORM_CONFIG_AVAILABLE:
        # Use platform paths
        pm = get_path_manager()
        platform_root = pm.get_platform_root()

        # Check if BASE_DIR is explicitly set in config
        base_dir_str = get_env("BASE_DIR", "")
        if base_dir_str:
            return Path(base_dir_str).resolve()

        # Default: platform root
        return platform_root
    else:
        # Legacy: relative to script location
        base_dir_str = get_env("BASE_DIR", "")
        if base_dir_str:
            return Path(base_dir_str).resolve()
        return Path(__file__).resolve().parents[1]


def get_input_dir() -> Path:
    """Get input directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_input_dir(SCRAPER_ID)
    else:
        # Legacy mode
        base = get_base_dir()
        input_subdir = get_env("INPUT_DIR", "input")
        if Path(input_subdir).is_absolute():
            return Path(input_subdir)
        return base / input_subdir


def get_output_dir() -> Path:
    """Get output directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        # For now, return base output dir
        # Individual scripts can use get_csv_output_dir, get_split_pdf_dir, etc.
        return pm.get_output_dir()
    else:
        # Legacy mode
        base = get_base_dir()
        output_subdir = get_env("OUTPUT_DIR", "output")
        if Path(output_subdir).is_absolute():
            return Path(output_subdir)
        return base / output_subdir


def get_split_pdf_dir() -> Path:
    """Get split PDF directory."""
    base = get_output_dir()
    split_subdir = get_env("SPLIT_PDF_DIR", "split_pdf")
    if Path(split_subdir).is_absolute():
        return Path(split_subdir)
    return base / split_subdir


def get_csv_output_dir() -> Path:
    """Get CSV output directory."""
    base = get_output_dir()
    csv_subdir = get_env("CSV_OUTPUT_DIR", "csv")
    if Path(csv_subdir).is_absolute():
        return Path(csv_subdir)
    return base / csv_subdir


def get_qa_output_dir() -> Path:
    """Get QA output directory."""
    base = get_output_dir()
    qa_subdir = get_env("QA_OUTPUT_DIR", "qa")
    if Path(qa_subdir).is_absolute():
        return Path(qa_subdir)
    return base / qa_subdir


def get_backup_dir() -> Path:
    """Get backup directory."""
    if _PLATFORM_CONFIG_AVAILABLE:
        pm = get_path_manager()
        return pm.get_backups_dir()
    else:
        # Legacy mode
        base = get_base_dir()
        backup_subdir = get_env("BACKUP_DIR", "backups")
        if Path(backup_subdir).is_absolute():
            return Path(backup_subdir)
        return base / backup_subdir


# File names
DEFAULT_INPUT_PDF_NAME = get_env("DEFAULT_INPUT_PDF_NAME", "liste-med.pdf")
ANNEXE_IV1_PDF_NAME = get_env("ANNEXE_IV1_PDF_NAME", "annexe_iv1.pdf")
ANNEXE_IV2_PDF_NAME = get_env("ANNEXE_IV2_PDF_NAME", "annexe_iv2.pdf")
ANNEXE_V_PDF_NAME = get_env("ANNEXE_V_PDF_NAME", "annexe_v.pdf")
ANNEXE_IV1_CSV_NAME = get_env("ANNEXE_IV1_CSV_NAME", "annexe_iv1_extracted.csv")
ANNEXE_IV2_CSV_NAME = get_env("ANNEXE_IV2_CSV_NAME", "annexe_iv2_extracted.csv")
ANNEXE_V_CSV_NAME = get_env("ANNEXE_V_CSV_NAME", "annexe_v_extracted.csv")
FINAL_REPORT_NAME_PREFIX = get_env("FINAL_REPORT_NAME_PREFIX", "canadaquebecreport_")
FINAL_REPORT_DATE_FORMAT = get_env("FINAL_REPORT_DATE_FORMAT", "%d%m%Y")
LOG_FILE_ANNEXE_V = get_env("LOG_FILE_ANNEXE_V", "annexe_v_extraction_log.txt")
LOG_FILE_MERGE = get_env("LOG_FILE_MERGE", "merge_log.txt")
INDEX_JSON_NAME = get_env("INDEX_JSON_NAME", "index.json")

# API Configuration (secrets)
OPENAI_API_KEY = get_env("OPENAI_API_KEY", "")
OPENAI_MODEL = get_env("OPENAI_MODEL", "gpt-4o-mini")

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
SCRAPER_ID_CONFIG = get_env("SCRAPER_ID", "canada_quebec_ramq")


# Diagnostic function
if __name__ == "__main__":
    print("=" * 60)
    print("CanadaQuebec Config Loader - Diagnostic")
    print("=" * 60)
    print(f"Platform Config Available: {_PLATFORM_CONFIG_AVAILABLE}")
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
