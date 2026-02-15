#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Validator Module

Post-processing validation for scraped data using pandera.
Runs AFTER scraping completes - does NOT touch scraping logic.

Usage:
    from core.data.data_validator import validate_output, get_validator
    
    # Validate a CSV file
    result = validate_output("output/Malaysia/products.csv", "Malaysia")
    
    # Or get a validator for custom use
    validator = get_validator("Malaysia")
    validated_df = validator.validate(df)
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from datetime import datetime

import pandas as pd

# Try to import pandera, gracefully degrade if not available
try:
    import pandera as pa
    from pandera import Column, DataFrameSchema, Check
    PANDERA_AVAILABLE = True
except ImportError:
    PANDERA_AVAILABLE = False
    pa = None
    Column = None
    DataFrameSchema = None
    Check = None

logger = logging.getLogger(__name__)


class DataValidator:
    """
    Post-processing data validator for scraped outputs.
    
    Does NOT modify scraping logic - only validates OUTPUT files.
    """
    
    # Common validation rules (can be extended per scraper)
    COMMON_CHECKS = {
        "non_empty_string": lambda: Check(lambda s: s.str.strip().str.len() > 0, 
                                          error="Value cannot be empty"),
        "positive_number": lambda: Check.ge(0, error="Value must be >= 0"),
        "valid_price": lambda: Check(lambda x: (x >= 0) | (x.isna()), 
                                     error="Price must be positive or null"),
        "valid_date": lambda: Check(lambda s: pd.to_datetime(s, errors='coerce').notna() | s.isna(),
                                    error="Invalid date format"),
    }
    
    # Scraper-specific schemas (column definitions)
    SCRAPER_SCHEMAS = {
        "Malaysia": {
            "columns": {
                "Registration No": {"dtype": str, "nullable": False},
                "Product Name": {"dtype": str, "nullable": False},
                "Active Ingredient": {"dtype": str, "nullable": True},
                "Dosage Form": {"dtype": str, "nullable": True},
                "Pack Size": {"dtype": str, "nullable": True},
                "Manufacturer": {"dtype": str, "nullable": True},
            },
            "strict": False,  # Allow extra columns
        },
        "Argentina": {
            "columns": {
                "PRODUCTO": {"dtype": str, "nullable": False},
                "LABORATORIO": {"dtype": str, "nullable": True},
                "PRECIO": {"dtype": float, "nullable": True, "checks": ["valid_price"]},
                "TROQUEL": {"dtype": str, "nullable": True},
            },
            "strict": False,
        },
        "India": {
            "columns": {
                "Medicine Name": {"dtype": str, "nullable": False},
                "Ceiling Price": {"dtype": float, "nullable": True, "checks": ["valid_price"]},
                "Unit": {"dtype": str, "nullable": True},
            },
            "strict": False,
        },
        "CanadaQuebec": {
            "columns": {
                "DIN": {"dtype": str, "nullable": True},
                "Product": {"dtype": str, "nullable": False},
                "Manufacturer": {"dtype": str, "nullable": True},
            },
            "strict": False,
        },
        "CanadaOntario": {
            "columns": {
                "local_pack_code": {"dtype": str, "nullable": False},
                "generic_name": {"dtype": str, "nullable": False},
                "brand_name_strength_dosage": {"dtype": str, "nullable": True},
            },
            "strict": False,
        },
        "Netherlands": {
            "columns": {
                "Product": {"dtype": str, "nullable": False},
            },
            "strict": False,
        },
        "Belarus": {
            "columns": {
                "Name": {"dtype": str, "nullable": False},
            },
            "strict": False,
        },
        "Russia": {
            "columns": {
                "Name": {"dtype": str, "nullable": False},
            },
            "strict": False,
        },
        "NorthMacedonia": {
            "columns": {
                "Product": {"dtype": str, "nullable": False},
            },
            "strict": False,
        },
        "Tender_Chile": {
            "columns": {
                "Product": {"dtype": str, "nullable": False},
            },
            "strict": False,
        },
    }
    
    def __init__(self, scraper_name: str):
        """
        Initialize validator for a specific scraper.
        
        Args:
            scraper_name: Name of the scraper (e.g., "Malaysia", "Argentina")
        """
        self.scraper_name = scraper_name
        self.schema = None
        self._build_schema()
    
    def _build_schema(self):
        """Build pandera schema from configuration."""
        if not PANDERA_AVAILABLE:
            logger.warning("pandera not installed. Validation will use basic checks only.")
            return
        
        schema_config = self.SCRAPER_SCHEMAS.get(self.scraper_name, {})
        if not schema_config:
            logger.info(f"No schema defined for {self.scraper_name}. Using generic validation.")
            return
        
        columns = {}
        for col_name, col_config in schema_config.get("columns", {}).items():
            dtype = col_config.get("dtype", str)
            nullable = col_config.get("nullable", True)
            checks = []
            
            # Add configured checks
            for check_name in col_config.get("checks", []):
                if check_name in self.COMMON_CHECKS:
                    checks.append(self.COMMON_CHECKS[check_name]())
            
            columns[col_name] = Column(
                dtype=dtype if dtype != float else float,
                nullable=nullable,
                checks=checks if checks else None,
                coerce=True,  # Try to coerce types
                required=not nullable,
            )
        
        if columns:
            self.schema = DataFrameSchema(
                columns=columns,
                strict=schema_config.get("strict", False),
                coerce=True,
            )
    
    def validate(self, df: pd.DataFrame, raise_on_error: bool = False) -> Dict[str, Any]:
        """
        Validate a DataFrame against the schema.
        
        Args:
            df: DataFrame to validate
            raise_on_error: If True, raise exception on validation failure
        
        Returns:
            Dict with validation results:
            {
                "valid": bool,
                "rows": int,
                "columns": list,
                "errors": list,
                "warnings": list,
                "stats": dict
            }
        """
        result = {
            "valid": True,
            "rows": len(df),
            "columns": list(df.columns),
            "errors": [],
            "warnings": [],
            "stats": {},
            "validated_at": datetime.now().isoformat(),
            "scraper": self.scraper_name,
        }
        
        # Basic checks (always run)
        result["stats"]["total_rows"] = len(df)
        result["stats"]["total_columns"] = len(df.columns)
        result["stats"]["null_counts"] = df.isnull().sum().to_dict()
        result["stats"]["duplicate_rows"] = df.duplicated().sum()
        
        # Check for empty DataFrame
        if len(df) == 0:
            result["warnings"].append("DataFrame is empty")
        
        # Check for duplicate rows
        if result["stats"]["duplicate_rows"] > 0:
            result["warnings"].append(
                f"Found {result['stats']['duplicate_rows']} duplicate rows"
            )
        
        # Pandera validation (if available and schema defined)
        if PANDERA_AVAILABLE and self.schema is not None:
            try:
                self.schema.validate(df, lazy=True)
            except pa.errors.SchemaErrors as e:
                result["valid"] = False
                for error in e.failure_cases.to_dict('records'):
                    result["errors"].append({
                        "column": error.get("column"),
                        "check": error.get("check"),
                        "failure_case": str(error.get("failure_case", ""))[:100],
                    })
                
                if raise_on_error:
                    raise
            except pa.errors.SchemaError as e:
                result["valid"] = False
                result["errors"].append({"message": str(e)[:500]})
                
                if raise_on_error:
                    raise
        else:
            # Basic validation without pandera
            result = self._basic_validation(df, result)
        
        return result
    
    def _basic_validation(self, df: pd.DataFrame, result: Dict) -> Dict:
        """Basic validation when pandera is not available."""
        schema_config = self.SCRAPER_SCHEMAS.get(self.scraper_name, {})
        
        for col_name, col_config in schema_config.get("columns", {}).items():
            if col_name not in df.columns:
                if not col_config.get("nullable", True):
                    result["valid"] = False
                    result["errors"].append({
                        "column": col_name,
                        "check": "column_exists",
                        "message": f"Required column '{col_name}' not found"
                    })
                continue
            
            # Check for nulls in non-nullable columns
            if not col_config.get("nullable", True):
                null_count = df[col_name].isnull().sum()
                if null_count > 0:
                    result["valid"] = False
                    result["errors"].append({
                        "column": col_name,
                        "check": "not_null",
                        "message": f"Column '{col_name}' has {null_count} null values"
                    })
            
            # Check for valid prices
            if "valid_price" in col_config.get("checks", []):
                if df[col_name].dtype in ['float64', 'int64']:
                    negative_count = (df[col_name] < 0).sum()
                    if negative_count > 0:
                        result["valid"] = False
                        result["errors"].append({
                            "column": col_name,
                            "check": "valid_price",
                            "message": f"Column '{col_name}' has {negative_count} negative values"
                        })
        
        return result
    
    def validate_file(self, file_path: Union[str, Path], **read_kwargs) -> Dict[str, Any]:
        """
        Validate a CSV/Excel file.
        
        Args:
            file_path: Path to the file
            **read_kwargs: Additional arguments for pd.read_csv/read_excel
        
        Returns:
            Validation result dict
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            return {
                "valid": False,
                "errors": [{"message": f"File not found: {file_path}"}],
                "file_path": str(file_path),
            }
        
        try:
            if file_path.suffix.lower() == '.xlsx':
                df = pd.read_excel(file_path, **read_kwargs)
            else:
                df = pd.read_csv(file_path, **read_kwargs)
            
            result = self.validate(df)
            result["file_path"] = str(file_path)
            result["file_size_bytes"] = file_path.stat().st_size
            return result
            
        except Exception as e:
            return {
                "valid": False,
                "errors": [{"message": f"Failed to read file: {str(e)}"}],
                "file_path": str(file_path),
            }


def get_validator(scraper_name: str) -> DataValidator:
    """Get a validator instance for a scraper."""
    return DataValidator(scraper_name)


def validate_output(file_path: Union[str, Path], scraper_name: str, **kwargs) -> Dict[str, Any]:
    """
    Convenience function to validate an output file.
    
    Args:
        file_path: Path to the output file
        scraper_name: Name of the scraper
        **kwargs: Additional arguments for file reading
    
    Returns:
        Validation result dict
    """
    validator = get_validator(scraper_name)
    return validator.validate_file(file_path, **kwargs)


def validate_dataframe(df: pd.DataFrame, scraper_name: str) -> Dict[str, Any]:
    """
    Convenience function to validate a DataFrame.
    
    Args:
        df: DataFrame to validate
        scraper_name: Name of the scraper
    
    Returns:
        Validation result dict
    """
    validator = get_validator(scraper_name)
    return validator.validate(df)


def validate_all_outputs(output_dir: Union[str, Path], scraper_name: str) -> List[Dict[str, Any]]:
    """
    Validate all CSV/Excel files in an output directory.
    
    Args:
        output_dir: Directory containing output files
        scraper_name: Name of the scraper
    
    Returns:
        List of validation results
    """
    output_dir = Path(output_dir)
    results = []
    
    validator = get_validator(scraper_name)
    
    for file_path in output_dir.glob("*.csv"):
        results.append(validator.validate_file(file_path))
    
    for file_path in output_dir.glob("*.xlsx"):
        results.append(validator.validate_file(file_path))
    
    return results


# CLI interface
if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 3:
        print("Usage: python data_validator.py <scraper_name> <file_path>")
        print("Example: python data_validator.py Malaysia output/Malaysia/products.csv")
        sys.exit(1)
    
    scraper_name = sys.argv[1]
    file_path = sys.argv[2]
    
    result = validate_output(file_path, scraper_name)
    
    print(json.dumps(result, indent=2, default=str))
    
    if result["valid"]:
        print(f"\n✓ Validation PASSED for {file_path}")
        sys.exit(0)
    else:
        print(f"\n✗ Validation FAILED for {file_path}")
        sys.exit(1)
