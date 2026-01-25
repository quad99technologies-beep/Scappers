#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deduplicator Module

Post-processing fuzzy deduplication for scraped data using rapidfuzz.
Runs AFTER scraping completes - does NOT touch scraping logic.

Usage:
    from core.deduplicator import deduplicate_dataframe, deduplicate_file
    
    # Deduplicate a DataFrame
    result = deduplicate_dataframe(df, key_column="Product Name", threshold=90.0)
    
    # Deduplicate a CSV file
    result = deduplicate_file("output/Malaysia/products.csv", "Product Name")
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
from datetime import datetime

import pandas as pd

# Try to import rapidfuzz, gracefully degrade if not available
try:
    from rapidfuzz import fuzz, process
    from rapidfuzz.distance import Levenshtein
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    fuzz = None
    process = None

logger = logging.getLogger(__name__)


class Deduplicator:
    """
    Post-processing deduplicator for scraped outputs.
    
    Does NOT modify scraping logic - only processes OUTPUT files.
    Uses fuzzy matching to find near-duplicate records.
    """
    
    # Default thresholds for different match types
    THRESHOLDS = {
        "exact": 100.0,
        "high": 95.0,
        "medium": 85.0,
        "low": 75.0,
    }
    
    # Scraper-specific key columns for deduplication
    SCRAPER_KEY_COLUMNS = {
        "Malaysia": ["Product Name", "Registration No"],
        "Argentina": ["PRODUCTO", "TROQUEL"],
        "India": ["Medicine Name"],
        "CanadaQuebec": ["Product", "DIN"],
        "CanadaOntario": ["local_pack_code", "LOCAL_PACK_CODE"],
        "Netherlands": ["Product"],
        "Belarus": ["Name"],
        "Russia": ["Name"],
        "NorthMacedonia": ["Product"],
        "Tender_Chile": ["Product"],
    }
    
    def __init__(self, threshold: float = 90.0, scorer: str = "ratio"):
        """
        Initialize deduplicator.
        
        Args:
            threshold: Similarity threshold (0-100). Records with similarity >= threshold
                      are considered duplicates.
            scorer: Scoring method - "ratio", "partial_ratio", "token_sort_ratio", 
                   "token_set_ratio", "WRatio"
        """
        self.threshold = threshold
        self.scorer = scorer
        self._scorer_func = self._get_scorer_func(scorer)
    
    def _get_scorer_func(self, scorer: str):
        """Get the scoring function based on scorer name."""
        if not RAPIDFUZZ_AVAILABLE:
            return None
        
        scorers = {
            "ratio": fuzz.ratio,
            "partial_ratio": fuzz.partial_ratio,
            "token_sort_ratio": fuzz.token_sort_ratio,
            "token_set_ratio": fuzz.token_set_ratio,
            "WRatio": fuzz.WRatio,
        }
        return scorers.get(scorer, fuzz.ratio)
    
    def find_duplicates(
        self, 
        values: List[str], 
        threshold: Optional[float] = None
    ) -> List[Tuple[int, int, float]]:
        """
        Find duplicate pairs in a list of strings.
        
        Args:
            values: List of strings to check for duplicates
            threshold: Override default threshold
        
        Returns:
            List of tuples (index1, index2, similarity_score) for duplicate pairs
        """
        if not RAPIDFUZZ_AVAILABLE:
            logger.warning("rapidfuzz not installed. Using exact match only.")
            return self._exact_duplicates(values)
        
        threshold = threshold or self.threshold
        duplicates = []
        n = len(values)
        
        # Normalize values for comparison
        normalized = [str(v).lower().strip() if pd.notna(v) else "" for v in values]
        
        for i in range(n):
            if not normalized[i]:
                continue
            
            for j in range(i + 1, n):
                if not normalized[j]:
                    continue
                
                score = self._scorer_func(normalized[i], normalized[j])
                if score >= threshold:
                    duplicates.append((i, j, score))
        
        return duplicates
    
    def _exact_duplicates(self, values: List[str]) -> List[Tuple[int, int, float]]:
        """Fallback: find exact duplicates only."""
        duplicates = []
        seen = {}
        
        for i, v in enumerate(values):
            normalized = str(v).lower().strip() if pd.notna(v) else ""
            if normalized in seen:
                duplicates.append((seen[normalized], i, 100.0))
            else:
                seen[normalized] = i
        
        return duplicates
    
    def deduplicate(
        self,
        df: pd.DataFrame,
        key_column: str,
        threshold: Optional[float] = None,
        keep: str = "first",
        mark_only: bool = False,
    ) -> Dict[str, Any]:
        """
        Deduplicate a DataFrame based on fuzzy matching of a key column.
        
        Args:
            df: DataFrame to deduplicate
            key_column: Column to use for duplicate detection
            threshold: Similarity threshold (0-100)
            keep: Which duplicate to keep - "first", "last", or "best" (highest data completeness)
            mark_only: If True, don't remove duplicates, just mark them in a new column
        
        Returns:
            Dict with results:
            {
                "df": deduplicated DataFrame (or marked DataFrame if mark_only=True),
                "original_count": int,
                "deduplicated_count": int,
                "duplicates_found": int,
                "duplicate_pairs": list of (idx1, idx2, score),
                "removed_indices": list of removed row indices
            }
        """
        if key_column not in df.columns:
            raise ValueError(f"Column '{key_column}' not found in DataFrame")
        
        threshold = threshold or self.threshold
        original_count = len(df)
        
        # Get values from key column
        values = df[key_column].tolist()
        
        # Find duplicates
        duplicate_pairs = self.find_duplicates(values, threshold)
        
        result = {
            "original_count": original_count,
            "duplicate_pairs": duplicate_pairs,
            "duplicates_found": len(duplicate_pairs),
            "threshold": threshold,
            "key_column": key_column,
            "processed_at": datetime.now().isoformat(),
        }
        
        if not duplicate_pairs:
            result["df"] = df.copy()
            result["deduplicated_count"] = original_count
            result["removed_indices"] = []
            return result
        
        # Determine which indices to remove
        indices_to_remove = set()
        
        for idx1, idx2, score in duplicate_pairs:
            if keep == "first":
                indices_to_remove.add(idx2)
            elif keep == "last":
                indices_to_remove.add(idx1)
            elif keep == "best":
                # Keep the row with more non-null values
                row1_completeness = df.iloc[idx1].notna().sum()
                row2_completeness = df.iloc[idx2].notna().sum()
                if row1_completeness >= row2_completeness:
                    indices_to_remove.add(idx2)
                else:
                    indices_to_remove.add(idx1)
        
        result["removed_indices"] = sorted(list(indices_to_remove))
        
        if mark_only:
            # Mark duplicates instead of removing
            df_result = df.copy()
            df_result["_is_duplicate"] = False
            df_result.loc[list(indices_to_remove), "_is_duplicate"] = True
            result["df"] = df_result
            result["deduplicated_count"] = original_count
        else:
            # Remove duplicates
            indices_to_keep = [i for i in range(len(df)) if i not in indices_to_remove]
            result["df"] = df.iloc[indices_to_keep].reset_index(drop=True)
            result["deduplicated_count"] = len(result["df"])
        
        return result
    
    def deduplicate_multi_column(
        self,
        df: pd.DataFrame,
        key_columns: List[str],
        threshold: Optional[float] = None,
        keep: str = "first",
    ) -> Dict[str, Any]:
        """
        Deduplicate based on multiple columns combined.
        
        Args:
            df: DataFrame to deduplicate
            key_columns: List of columns to combine for duplicate detection
            threshold: Similarity threshold
            keep: Which duplicate to keep
        
        Returns:
            Deduplication result dict
        """
        # Combine columns into a single key
        combined_key = "_combined_key"
        df_work = df.copy()
        df_work[combined_key] = df_work[key_columns].fillna("").astype(str).agg(" | ".join, axis=1)
        
        result = self.deduplicate(df_work, combined_key, threshold, keep)
        
        # Remove the temporary combined key column
        if combined_key in result["df"].columns:
            result["df"] = result["df"].drop(columns=[combined_key])
        
        result["key_columns"] = key_columns
        return result


def deduplicate_dataframe(
    df: pd.DataFrame,
    key_column: str,
    threshold: float = 90.0,
    keep: str = "first",
    scorer: str = "ratio",
) -> Dict[str, Any]:
    """
    Convenience function to deduplicate a DataFrame.
    
    Args:
        df: DataFrame to deduplicate
        key_column: Column to use for duplicate detection
        threshold: Similarity threshold (0-100)
        keep: Which duplicate to keep - "first", "last", or "best"
        scorer: Scoring method
    
    Returns:
        Deduplication result dict
    """
    deduplicator = Deduplicator(threshold=threshold, scorer=scorer)
    return deduplicator.deduplicate(df, key_column, keep=keep)


def deduplicate_file(
    file_path: Union[str, Path],
    key_column: str,
    output_path: Optional[Union[str, Path]] = None,
    threshold: float = 90.0,
    keep: str = "first",
    scorer: str = "ratio",
) -> Dict[str, Any]:
    """
    Deduplicate a CSV/Excel file.
    
    Args:
        file_path: Path to input file
        key_column: Column to use for duplicate detection
        output_path: Path for deduplicated output (default: adds "_dedup" suffix)
        threshold: Similarity threshold (0-100)
        keep: Which duplicate to keep
        scorer: Scoring method
    
    Returns:
        Deduplication result dict with file paths
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        return {
            "success": False,
            "error": f"File not found: {file_path}",
        }
    
    try:
        # Read file
        if file_path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        # Deduplicate
        result = deduplicate_dataframe(df, key_column, threshold, keep, scorer)
        
        # Determine output path
        if output_path is None:
            output_path = file_path.parent / f"{file_path.stem}_dedup{file_path.suffix}"
        else:
            output_path = Path(output_path)
        
        # Save deduplicated file
        if output_path.suffix.lower() == '.xlsx':
            result["df"].to_excel(output_path, index=False)
        else:
            result["df"].to_csv(output_path, index=False)
        
        result["success"] = True
        result["input_path"] = str(file_path)
        result["output_path"] = str(output_path)
        
        # Remove df from result to keep it clean (file is saved)
        del result["df"]
        
        logger.info(
            f"Deduplicated {file_path.name}: {result['original_count']} -> "
            f"{result['deduplicated_count']} records ({result['duplicates_found']} duplicates removed)"
        )
        
        return result
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "input_path": str(file_path),
        }


def deduplicate_scraper_output(
    file_path: Union[str, Path],
    scraper_name: str,
    output_path: Optional[Union[str, Path]] = None,
    threshold: float = 90.0,
) -> Dict[str, Any]:
    """
    Deduplicate a scraper output using scraper-specific key columns.
    
    Args:
        file_path: Path to input file
        scraper_name: Name of the scraper (to get default key columns)
        output_path: Path for deduplicated output
        threshold: Similarity threshold
    
    Returns:
        Deduplication result dict
    """
    key_columns = Deduplicator.SCRAPER_KEY_COLUMNS.get(scraper_name, [])
    
    if not key_columns:
        return {
            "success": False,
            "error": f"No key columns defined for scraper: {scraper_name}",
        }
    
    file_path = Path(file_path)
    
    try:
        # Read file
        if file_path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        
        # Find first available key column
        key_column = None
        for col in key_columns:
            if col in df.columns:
                key_column = col
                break
        
        if key_column is None:
            return {
                "success": False,
                "error": f"None of the key columns {key_columns} found in file",
            }
        
        return deduplicate_file(file_path, key_column, output_path, threshold)
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
        }


def find_cross_file_duplicates(
    file_paths: List[Union[str, Path]],
    key_column: str,
    threshold: float = 90.0,
) -> Dict[str, Any]:
    """
    Find duplicates across multiple files.
    
    Args:
        file_paths: List of file paths to check
        key_column: Column to use for duplicate detection
        threshold: Similarity threshold
    
    Returns:
        Dict with cross-file duplicate information
    """
    all_records = []
    
    for file_path in file_paths:
        file_path = Path(file_path)
        if not file_path.exists():
            continue
        
        try:
            if file_path.suffix.lower() == '.xlsx':
                df = pd.read_excel(file_path)
            else:
                df = pd.read_csv(file_path)
            
            if key_column in df.columns:
                for idx, value in enumerate(df[key_column]):
                    all_records.append({
                        "file": str(file_path),
                        "index": idx,
                        "value": value,
                    })
        except Exception as e:
            logger.warning(f"Error reading {file_path}: {e}")
    
    if not all_records:
        return {"cross_file_duplicates": [], "total_records": 0}
    
    # Find duplicates
    deduplicator = Deduplicator(threshold=threshold)
    values = [r["value"] for r in all_records]
    duplicate_pairs = deduplicator.find_duplicates(values, threshold)
    
    # Filter to only cross-file duplicates
    cross_file_duplicates = []
    for idx1, idx2, score in duplicate_pairs:
        if all_records[idx1]["file"] != all_records[idx2]["file"]:
            cross_file_duplicates.append({
                "record1": all_records[idx1],
                "record2": all_records[idx2],
                "similarity": score,
            })
    
    return {
        "cross_file_duplicates": cross_file_duplicates,
        "total_records": len(all_records),
        "files_checked": len(file_paths),
    }


# CLI interface
if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 3:
        print("Usage: python deduplicator.py <file_path> <key_column> [threshold]")
        print("Example: python deduplicator.py output/Malaysia/products.csv 'Product Name' 90")
        sys.exit(1)
    
    file_path = sys.argv[1]
    key_column = sys.argv[2]
    threshold = float(sys.argv[3]) if len(sys.argv) > 3 else 90.0
    
    result = deduplicate_file(file_path, key_column, threshold=threshold)
    
    # Print result without large data
    print_result = {k: v for k, v in result.items() if k != "duplicate_pairs"}
    print_result["duplicate_pairs_count"] = len(result.get("duplicate_pairs", []))
    
    print(json.dumps(print_result, indent=2, default=str))
    
    if result.get("success"):
        print(f"\n✓ Deduplication complete: {result['output_path']}")
        print(f"  Original: {result['original_count']} | Deduplicated: {result['deduplicated_count']}")
    else:
        print(f"\n✗ Deduplication failed: {result.get('error')}")
        sys.exit(1)
