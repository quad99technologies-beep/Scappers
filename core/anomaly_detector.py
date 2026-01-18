#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Anomaly Detector Module

Post-processing anomaly detection for scraped data using scikit-learn.
Runs AFTER scraping completes - does NOT touch scraping logic.

Detects:
- Price outliers (unusually high/low prices)
- Data quality anomalies (missing patterns, format issues)
- Statistical outliers in numeric columns

Usage:
    from core.anomaly_detector import detect_price_anomalies, detect_anomalies
    
    # Detect price anomalies in a DataFrame
    result = detect_price_anomalies(df, price_column="Price")
    
    # Detect anomalies in a file
    result = detect_anomalies_in_file("output/Malaysia/products.csv", ["Price", "Quantity"])
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

import pandas as pd
import numpy as np

# Try to import scikit-learn, gracefully degrade if not available
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    from sklearn.neighbors import LocalOutlierFactor
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    IsolationForest = None
    StandardScaler = None
    LocalOutlierFactor = None

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Post-processing anomaly detector for scraped outputs.
    
    Does NOT modify scraping logic - only analyzes OUTPUT files.
    """
    
    # Default contamination rate (expected proportion of outliers)
    DEFAULT_CONTAMINATION = 0.05  # 5%
    
    # Scraper-specific numeric columns for anomaly detection
    SCRAPER_NUMERIC_COLUMNS = {
        "Malaysia": ["Price", "Retail Price", "Ceiling Price"],
        "Argentina": ["PRECIO", "PAMI_AF", "PAMI_OS"],
        "India": ["Ceiling Price", "MRP"],
        "CanadaQuebec": ["Price", "Unit Price"],
        "CanadaOntario": ["Price", "Unit Price", "EAP Price"],
        "Netherlands": ["Price"],
        "Belarus": ["Price"],
        "Russia": ["Price"],
        "NorthMacedonia": ["Price", "Max Price"],
        "Tender_Chile": ["Price", "Unit Price"],
    }
    
    def __init__(
        self, 
        method: str = "isolation_forest",
        contamination: float = DEFAULT_CONTAMINATION,
        random_state: int = 42
    ):
        """
        Initialize anomaly detector.
        
        Args:
            method: Detection method - "isolation_forest", "lof" (Local Outlier Factor),
                   "zscore", "iqr"
            contamination: Expected proportion of outliers (0.0 to 0.5)
            random_state: Random seed for reproducibility
        """
        self.method = method
        self.contamination = contamination
        self.random_state = random_state
    
    def detect_numeric_anomalies(
        self,
        values: Union[List[float], np.ndarray, pd.Series],
        method: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Detect anomalies in a numeric array.
        
        Args:
            values: Numeric values to analyze
            method: Override default method
        
        Returns:
            Dict with anomaly detection results:
            {
                "anomaly_indices": list of indices flagged as anomalies,
                "anomaly_values": list of anomaly values,
                "anomaly_scores": list of anomaly scores (if available),
                "stats": dict with statistical summary
            }
        """
        method = method or self.method
        
        # Convert to numpy array and handle NaN
        if isinstance(values, pd.Series):
            values = values.values
        values = np.array(values, dtype=float)
        
        # Track original indices for NaN handling
        valid_mask = ~np.isnan(values)
        valid_values = values[valid_mask]
        valid_indices = np.where(valid_mask)[0]
        
        if len(valid_values) < 3:
            return {
                "anomaly_indices": [],
                "anomaly_values": [],
                "anomaly_scores": [],
                "stats": {"error": "Not enough valid values for anomaly detection"},
            }
        
        # Calculate basic statistics
        stats = {
            "count": len(valid_values),
            "mean": float(np.mean(valid_values)),
            "std": float(np.std(valid_values)),
            "min": float(np.min(valid_values)),
            "max": float(np.max(valid_values)),
            "median": float(np.median(valid_values)),
            "q1": float(np.percentile(valid_values, 25)),
            "q3": float(np.percentile(valid_values, 75)),
        }
        stats["iqr"] = stats["q3"] - stats["q1"]
        
        # Detect anomalies based on method
        if method == "isolation_forest" and SKLEARN_AVAILABLE:
            anomaly_mask, scores = self._isolation_forest(valid_values)
        elif method == "lof" and SKLEARN_AVAILABLE:
            anomaly_mask, scores = self._local_outlier_factor(valid_values)
        elif method == "zscore":
            anomaly_mask, scores = self._zscore_method(valid_values, stats)
        elif method == "iqr":
            anomaly_mask, scores = self._iqr_method(valid_values, stats)
        else:
            # Fallback to IQR if sklearn not available
            anomaly_mask, scores = self._iqr_method(valid_values, stats)
        
        # Map back to original indices
        anomaly_indices = valid_indices[anomaly_mask].tolist()
        anomaly_values = valid_values[anomaly_mask].tolist()
        anomaly_scores = scores[anomaly_mask].tolist() if scores is not None else []
        
        return {
            "anomaly_indices": anomaly_indices,
            "anomaly_values": anomaly_values,
            "anomaly_scores": anomaly_scores,
            "anomaly_count": len(anomaly_indices),
            "total_count": len(values),
            "anomaly_rate": len(anomaly_indices) / len(values) if len(values) > 0 else 0,
            "method": method,
            "stats": stats,
        }
    
    def _isolation_forest(self, values: np.ndarray) -> tuple:
        """Detect anomalies using Isolation Forest."""
        clf = IsolationForest(
            contamination=self.contamination,
            random_state=self.random_state,
            n_estimators=100,
        )
        
        # Reshape for sklearn
        X = values.reshape(-1, 1)
        predictions = clf.fit_predict(X)
        scores = clf.decision_function(X)
        
        # -1 = anomaly, 1 = normal
        anomaly_mask = predictions == -1
        
        return anomaly_mask, scores
    
    def _local_outlier_factor(self, values: np.ndarray) -> tuple:
        """Detect anomalies using Local Outlier Factor."""
        # LOF needs at least n_neighbors + 1 samples
        n_neighbors = min(20, len(values) - 1)
        if n_neighbors < 2:
            return np.zeros(len(values), dtype=bool), np.zeros(len(values))
        
        clf = LocalOutlierFactor(
            n_neighbors=n_neighbors,
            contamination=self.contamination,
        )
        
        X = values.reshape(-1, 1)
        predictions = clf.fit_predict(X)
        scores = clf.negative_outlier_factor_
        
        anomaly_mask = predictions == -1
        
        return anomaly_mask, scores
    
    def _zscore_method(self, values: np.ndarray, stats: dict) -> tuple:
        """Detect anomalies using Z-score method."""
        if stats["std"] == 0:
            return np.zeros(len(values), dtype=bool), np.zeros(len(values))
        
        z_scores = np.abs((values - stats["mean"]) / stats["std"])
        
        # Values with |z| > 3 are anomalies
        threshold = 3.0
        anomaly_mask = z_scores > threshold
        
        return anomaly_mask, z_scores
    
    def _iqr_method(self, values: np.ndarray, stats: dict) -> tuple:
        """Detect anomalies using IQR method."""
        iqr = stats["iqr"]
        
        if iqr == 0:
            # All values are the same or very close
            return np.zeros(len(values), dtype=bool), np.zeros(len(values))
        
        lower_bound = stats["q1"] - 1.5 * iqr
        upper_bound = stats["q3"] + 1.5 * iqr
        
        anomaly_mask = (values < lower_bound) | (values > upper_bound)
        
        # Calculate distance from bounds as score
        scores = np.zeros(len(values))
        scores[values < lower_bound] = (lower_bound - values[values < lower_bound]) / iqr
        scores[values > upper_bound] = (values[values > upper_bound] - upper_bound) / iqr
        
        return anomaly_mask, scores
    
    def detect_in_dataframe(
        self,
        df: pd.DataFrame,
        numeric_columns: Optional[List[str]] = None,
        method: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Detect anomalies in multiple columns of a DataFrame.
        
        Args:
            df: DataFrame to analyze
            numeric_columns: List of columns to check (auto-detect if None)
            method: Detection method
        
        Returns:
            Dict with anomaly results per column
        """
        if numeric_columns is None:
            # Auto-detect numeric columns
            numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        results = {
            "columns_analyzed": [],
            "total_anomalies": 0,
            "anomalies_by_column": {},
            "analyzed_at": datetime.now().isoformat(),
        }
        
        for col in numeric_columns:
            if col not in df.columns:
                continue
            
            col_result = self.detect_numeric_anomalies(df[col], method)
            results["anomalies_by_column"][col] = col_result
            results["columns_analyzed"].append(col)
            results["total_anomalies"] += col_result["anomaly_count"]
        
        return results
    
    def get_anomaly_rows(
        self,
        df: pd.DataFrame,
        numeric_columns: Optional[List[str]] = None,
        method: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get rows containing anomalies.
        
        Args:
            df: DataFrame to analyze
            numeric_columns: Columns to check
            method: Detection method
        
        Returns:
            DataFrame with anomaly rows and a column indicating which column(s) had anomalies
        """
        results = self.detect_in_dataframe(df, numeric_columns, method)
        
        # Collect all anomaly indices
        anomaly_info = {}
        for col, col_result in results["anomalies_by_column"].items():
            for idx in col_result["anomaly_indices"]:
                if idx not in anomaly_info:
                    anomaly_info[idx] = []
                anomaly_info[idx].append(col)
        
        if not anomaly_info:
            return pd.DataFrame()
        
        # Get anomaly rows
        anomaly_indices = sorted(anomaly_info.keys())
        anomaly_df = df.iloc[anomaly_indices].copy()
        anomaly_df["_anomaly_columns"] = [
            ", ".join(anomaly_info[idx]) for idx in anomaly_indices
        ]
        
        return anomaly_df


def detect_price_anomalies(
    df: pd.DataFrame,
    price_column: str,
    method: str = "iqr",
    contamination: float = 0.05,
) -> Dict[str, Any]:
    """
    Convenience function to detect price anomalies.
    
    Args:
        df: DataFrame with price data
        price_column: Name of the price column
        method: Detection method
        contamination: Expected anomaly rate
    
    Returns:
        Anomaly detection results
    """
    detector = AnomalyDetector(method=method, contamination=contamination)
    
    if price_column not in df.columns:
        return {"error": f"Column '{price_column}' not found"}
    
    return detector.detect_numeric_anomalies(df[price_column])


def detect_anomalies_in_file(
    file_path: Union[str, Path],
    numeric_columns: Optional[List[str]] = None,
    method: str = "iqr",
    contamination: float = 0.05,
) -> Dict[str, Any]:
    """
    Detect anomalies in a CSV/Excel file.
    
    Args:
        file_path: Path to the file
        numeric_columns: Columns to analyze (auto-detect if None)
        method: Detection method
        contamination: Expected anomaly rate
    
    Returns:
        Anomaly detection results
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        return {"error": f"File not found: {file_path}"}
    
    try:
        if file_path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        detector = AnomalyDetector(method=method, contamination=contamination)
        result = detector.detect_in_dataframe(df, numeric_columns)
        result["file_path"] = str(file_path)
        
        return result
        
    except Exception as e:
        return {"error": str(e), "file_path": str(file_path)}


def detect_scraper_anomalies(
    file_path: Union[str, Path],
    scraper_name: str,
    method: str = "iqr",
) -> Dict[str, Any]:
    """
    Detect anomalies using scraper-specific column configuration.
    
    Args:
        file_path: Path to the file
        scraper_name: Name of the scraper
        method: Detection method
    
    Returns:
        Anomaly detection results
    """
    numeric_columns = AnomalyDetector.SCRAPER_NUMERIC_COLUMNS.get(scraper_name, [])
    return detect_anomalies_in_file(file_path, numeric_columns, method)


def flag_anomalies_in_file(
    file_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    numeric_columns: Optional[List[str]] = None,
    method: str = "iqr",
) -> Dict[str, Any]:
    """
    Add anomaly flags to a file and save.
    
    Args:
        file_path: Path to input file
        output_path: Path for output (default: adds "_flagged" suffix)
        numeric_columns: Columns to analyze
        method: Detection method
    
    Returns:
        Result dict with file paths and anomaly counts
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        return {"success": False, "error": f"File not found: {file_path}"}
    
    try:
        if file_path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        detector = AnomalyDetector(method=method)
        anomaly_df = detector.get_anomaly_rows(df, numeric_columns)
        
        # Add anomaly flag column to original df
        df["_is_anomaly"] = False
        if len(anomaly_df) > 0:
            df.loc[anomaly_df.index, "_is_anomaly"] = True
            df["_anomaly_columns"] = ""
            df.loc[anomaly_df.index, "_anomaly_columns"] = anomaly_df["_anomaly_columns"].values
        
        # Save
        if output_path is None:
            output_path = file_path.parent / f"{file_path.stem}_flagged{file_path.suffix}"
        else:
            output_path = Path(output_path)
        
        if output_path.suffix.lower() == '.xlsx':
            df.to_excel(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)
        
        return {
            "success": True,
            "input_path": str(file_path),
            "output_path": str(output_path),
            "total_rows": len(df),
            "anomaly_rows": len(anomaly_df),
            "anomaly_rate": len(anomaly_df) / len(df) if len(df) > 0 else 0,
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}


# CLI interface
if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 2:
        print("Usage: python anomaly_detector.py <file_path> [column1,column2,...] [method]")
        print("Example: python anomaly_detector.py output/Malaysia/products.csv Price,Quantity iqr")
        print("\nMethods: isolation_forest, lof, zscore, iqr (default)")
        sys.exit(1)
    
    file_path = sys.argv[1]
    columns = sys.argv[2].split(",") if len(sys.argv) > 2 else None
    method = sys.argv[3] if len(sys.argv) > 3 else "iqr"
    
    result = detect_anomalies_in_file(file_path, columns, method)
    
    # Summarize for display
    summary = {
        "file": result.get("file_path"),
        "columns_analyzed": result.get("columns_analyzed", []),
        "total_anomalies": result.get("total_anomalies", 0),
        "analyzed_at": result.get("analyzed_at"),
    }
    
    if "anomalies_by_column" in result:
        summary["per_column"] = {}
        for col, col_result in result["anomalies_by_column"].items():
            summary["per_column"][col] = {
                "anomaly_count": col_result.get("anomaly_count", 0),
                "anomaly_rate": f"{col_result.get('anomaly_rate', 0)*100:.1f}%",
                "stats": {
                    "mean": col_result.get("stats", {}).get("mean"),
                    "std": col_result.get("stats", {}).get("std"),
                    "min": col_result.get("stats", {}).get("min"),
                    "max": col_result.get("stats", {}).get("max"),
                }
            }
    
    print(json.dumps(summary, indent=2, default=str))
    
    if result.get("total_anomalies", 0) > 0:
        print(f"\n⚠ Found {result['total_anomalies']} anomalies")
    else:
        print(f"\n✓ No anomalies detected")
