#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Loader

Loads configuration from .env file using python-dotenv.
Provides default values matching original hardcoded values.
This ensures backward compatibility when .env is not present.
"""

import os
from pathlib import Path

# Try to load dotenv if available
try:
    from dotenv import load_dotenv
    # Load .env from project root (parent of doc/ folder)
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    else:
        # Also try loading from current directory
        load_dotenv(override=False)
except ImportError:
    # python-dotenv not installed, use os.environ directly
    pass


def get_env(key: str, default: str) -> str:
    """Get environment variable with default value."""
    return os.getenv(key, default)


def get_env_int(key: str, default: int) -> int:
    """Get environment variable as integer with default value."""
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def get_env_float(key: str, default: float) -> float:
    """Get environment variable as float with default value."""
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def get_env_bool(key: str, default: bool) -> bool:
    """Get environment variable as boolean with default value."""
    val = os.getenv(key, "").strip().lower()
    if val in ("1", "true", "yes", "on"):
        return True
    elif val in ("0", "false", "no", "off", ""):
        return False
    return default


def get_base_dir() -> Path:
    """Get base directory. Defaults to parent of Script/doc folder."""
    base_dir_str = get_env("BASE_DIR", "")
    if base_dir_str:
        return Path(base_dir_str).resolve()
    return Path(__file__).resolve().parents[1]


def get_input_dir() -> Path:
    """Get input directory."""
    base = get_base_dir()
    input_subdir = get_env("INPUT_DIR", "input")
    if Path(input_subdir).is_absolute():
        return Path(input_subdir)
    return base / input_subdir


def get_output_dir() -> Path:
    """Get output directory."""
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

# API Configuration
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
SCRAPER_ID = get_env("SCRAPER_ID", "canada_quebec_ramq")

