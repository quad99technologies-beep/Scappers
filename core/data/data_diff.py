#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Diff Module

Change detection between scraper runs.
Runs AFTER scraping completes - does NOT touch scraping logic.

Usage:
    from core.data.data_diff import compare_runs, detect_changes, DiffReport
    
    # Compare two CSV files
    diff = compare_runs("output/old.csv", "output/new.csv", key_column="product_id")
    
    # Get detailed change report
    report = diff.get_report()
    print(f"Added: {report['added_count']}, Removed: {report['removed_count']}")
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Set, Tuple
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Try to import datacompy for advanced comparison
try:
    import datacompy
    DATACOMPY_AVAILABLE = True
except ImportError:
    DATACOMPY_AVAILABLE = False
    datacompy = None


class DiffReport:
    """
    Report of differences between two datasets.
    """
    
    def __init__(
        self,
        old_df: pd.DataFrame,
        new_df: pd.DataFrame,
        key_column: str,
        compare_columns: List[str] = None,
    ):
        """
        Initialize diff report.
        
        Args:
            old_df: Previous/baseline DataFrame
            new_df: Current/new DataFrame
            key_column: Column to use as unique identifier
            compare_columns: Columns to compare (all if None)
        """
        self.old_df = old_df.copy()
        self.new_df = new_df.copy()
        self.key_column = key_column
        self.compare_columns = compare_columns
        
        self._added: Optional[pd.DataFrame] = None
        self._removed: Optional[pd.DataFrame] = None
        self._modified: Optional[pd.DataFrame] = None
        self._unchanged: Optional[pd.DataFrame] = None
        self._changes: List[Dict] = []
        
        self._compute_diff()
    
    def _compute_diff(self):
        """Compute differences between DataFrames."""
        # Ensure key column exists
        if self.key_column not in self.old_df.columns:
            raise ValueError(f"Key column '{self.key_column}' not found in old DataFrame")
        if self.key_column not in self.new_df.columns:
            raise ValueError(f"Key column '{self.key_column}' not found in new DataFrame")
        
        # Get key sets
        old_keys = set(self.old_df[self.key_column].dropna().astype(str))
        new_keys = set(self.new_df[self.key_column].dropna().astype(str))
        
        # Find added, removed, common
        added_keys = new_keys - old_keys
        removed_keys = old_keys - new_keys
        common_keys = old_keys & new_keys
        
        # Added records
        self._added = self.new_df[
            self.new_df[self.key_column].astype(str).isin(added_keys)
        ].copy()
        
        # Removed records
        self._removed = self.old_df[
            self.old_df[self.key_column].astype(str).isin(removed_keys)
        ].copy()
        
        # Find modified records
        if common_keys:
            self._find_modifications(common_keys)
        else:
            self._modified = pd.DataFrame()
            self._unchanged = pd.DataFrame()
    
    def _find_modifications(self, common_keys: Set[str]):
        """Find modified records among common keys."""
        # Determine columns to compare
        if self.compare_columns:
            cols_to_compare = [c for c in self.compare_columns if c in self.old_df.columns and c in self.new_df.columns]
        else:
            cols_to_compare = list(set(self.old_df.columns) & set(self.new_df.columns))
            cols_to_compare = [c for c in cols_to_compare if c != self.key_column]
        
        # Index by key for comparison
        old_indexed = self.old_df.set_index(self.old_df[self.key_column].astype(str))
        new_indexed = self.new_df.set_index(self.new_df[self.key_column].astype(str))
        
        modified_keys = []
        unchanged_keys = []
        
        for key in common_keys:
            try:
                old_row = old_indexed.loc[key]
                new_row = new_indexed.loc[key]
                
                # Handle duplicate keys (take first)
                if isinstance(old_row, pd.DataFrame):
                    old_row = old_row.iloc[0]
                if isinstance(new_row, pd.DataFrame):
                    new_row = new_row.iloc[0]
                
                # Compare values
                is_modified = False
                changes = []
                
                for col in cols_to_compare:
                    old_val = old_row.get(col)
                    new_val = new_row.get(col)
                    
                    # Handle NaN comparison
                    old_is_na = pd.isna(old_val)
                    new_is_na = pd.isna(new_val)
                    
                    if old_is_na and new_is_na:
                        continue
                    elif old_is_na != new_is_na:
                        is_modified = True
                        changes.append({
                            "column": col,
                            "old_value": None if old_is_na else old_val,
                            "new_value": None if new_is_na else new_val,
                        })
                    elif str(old_val) != str(new_val):
                        is_modified = True
                        changes.append({
                            "column": col,
                            "old_value": old_val,
                            "new_value": new_val,
                        })
                
                if is_modified:
                    modified_keys.append(key)
                    self._changes.append({
                        "key": key,
                        "changes": changes,
                    })
                else:
                    unchanged_keys.append(key)
                    
            except Exception as e:
                logger.warning(f"Error comparing key {key}: {e}")
        
        # Get modified and unchanged DataFrames
        self._modified = self.new_df[
            self.new_df[self.key_column].astype(str).isin(modified_keys)
        ].copy()
        
        self._unchanged = self.new_df[
            self.new_df[self.key_column].astype(str).isin(unchanged_keys)
        ].copy()
    
    @property
    def added(self) -> pd.DataFrame:
        """Records that are new (not in old)."""
        return self._added
    
    @property
    def removed(self) -> pd.DataFrame:
        """Records that were removed (not in new)."""
        return self._removed
    
    @property
    def modified(self) -> pd.DataFrame:
        """Records that were modified."""
        return self._modified
    
    @property
    def unchanged(self) -> pd.DataFrame:
        """Records that are unchanged."""
        return self._unchanged
    
    @property
    def changes(self) -> List[Dict]:
        """Detailed list of changes per record."""
        return self._changes
    
    def get_report(self) -> Dict[str, Any]:
        """Get summary report of differences."""
        return {
            "key_column": self.key_column,
            "old_count": len(self.old_df),
            "new_count": len(self.new_df),
            "added_count": len(self._added),
            "removed_count": len(self._removed),
            "modified_count": len(self._modified),
            "unchanged_count": len(self._unchanged),
            "total_changes": len(self._added) + len(self._removed) + len(self._modified),
            "change_rate": (
                (len(self._added) + len(self._removed) + len(self._modified)) / 
                max(len(self.old_df), 1) * 100
            ),
            "generated_at": datetime.now().isoformat(),
        }
    
    def get_change_details(self, limit: int = 100) -> Dict[str, Any]:
        """Get detailed change information."""
        return {
            "added": self._added.head(limit).to_dict('records'),
            "removed": self._removed.head(limit).to_dict('records'),
            "modified": self._changes[:limit],
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert full diff to dictionary."""
        report = self.get_report()
        report["details"] = self.get_change_details()
        return report
    
    def save_diff_report(
        self,
        output_dir: Union[str, Path],
        prefix: str = "diff",
    ) -> Dict[str, str]:
        """
        Save diff report to files.
        
        Args:
            output_dir: Directory to save files
            prefix: Filename prefix
        
        Returns:
            Dict with file paths
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        files = {}
        
        # Save added records
        if len(self._added) > 0:
            path = output_dir / f"{prefix}_added_{timestamp}.csv"
            self._added.to_csv(path, index=False)
            files["added"] = str(path)
        
        # Save removed records
        if len(self._removed) > 0:
            path = output_dir / f"{prefix}_removed_{timestamp}.csv"
            self._removed.to_csv(path, index=False)
            files["removed"] = str(path)
        
        # Save modified records
        if len(self._modified) > 0:
            path = output_dir / f"{prefix}_modified_{timestamp}.csv"
            self._modified.to_csv(path, index=False)
            files["modified"] = str(path)
        
        # Save summary report
        import json
        path = output_dir / f"{prefix}_summary_{timestamp}.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        files["summary"] = str(path)
        
        return files


class DataDiffTracker:
    """
    Tracks changes across multiple scraper runs.
    """
    
    def __init__(self, scraper_name: str, output_dir: Union[str, Path] = None):
        """
        Initialize diff tracker.
        
        Args:
            scraper_name: Name of the scraper
            output_dir: Directory for diff history
        """
        self.scraper_name = scraper_name
        
        if output_dir is None:
            try:
                from core.config.config_manager import ConfigManager
                output_dir = ConfigManager.get_output_dir(scraper_name)
            except:
                output_dir = Path(__file__).parent.parent / "output" / scraper_name
        
        self.output_dir = Path(output_dir)
        self.diff_dir = self.output_dir / ".diffs"
        self.diff_dir.mkdir(parents=True, exist_ok=True)
        
        self._history_file = self.diff_dir / "diff_history.json"
        self._history: List[Dict] = self._load_history()
    
    def _load_history(self) -> List[Dict]:
        """Load diff history."""
        if self._history_file.exists():
            try:
                import json
                with open(self._history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return []
        return []
    
    def _save_history(self):
        """Save diff history."""
        import json
        with open(self._history_file, 'w', encoding='utf-8') as f:
            json.dump(self._history[-100:], f, indent=2, default=str)  # Keep last 100
    
    def track_run(
        self,
        current_file: Union[str, Path],
        key_column: str,
        previous_file: Union[str, Path] = None,
    ) -> Dict[str, Any]:
        """
        Track changes from previous run.
        
        Args:
            current_file: Current output file
            key_column: Key column for comparison
            previous_file: Previous file (auto-detect if None)
        
        Returns:
            Diff report dict
        """
        current_file = Path(current_file)
        
        if not current_file.exists():
            return {"error": f"Current file not found: {current_file}"}
        
        # Find previous file
        if previous_file is None:
            previous_file = self._find_previous_file(current_file)
        
        if previous_file is None:
            # First run, no comparison possible
            self._history.append({
                "timestamp": datetime.now().isoformat(),
                "file": str(current_file),
                "is_first_run": True,
            })
            self._save_history()
            return {"is_first_run": True, "file": str(current_file)}
        
        previous_file = Path(previous_file)
        
        # Load DataFrames
        try:
            if current_file.suffix.lower() == '.xlsx':
                current_df = pd.read_excel(current_file)
            else:
                current_df = pd.read_csv(current_file)
            
            if previous_file.suffix.lower() == '.xlsx':
                previous_df = pd.read_excel(previous_file)
            else:
                previous_df = pd.read_csv(previous_file)
        except Exception as e:
            return {"error": f"Failed to read files: {e}"}
        
        # Compute diff
        diff = DiffReport(previous_df, current_df, key_column)
        report = diff.get_report()
        
        # Save diff files
        files = diff.save_diff_report(self.diff_dir, f"{self.scraper_name}")
        report["diff_files"] = files
        
        # Update history
        self._history.append({
            "timestamp": datetime.now().isoformat(),
            "current_file": str(current_file),
            "previous_file": str(previous_file),
            "report": report,
        })
        self._save_history()
        
        return report
    
    def _find_previous_file(self, current_file: Path) -> Optional[Path]:
        """Find the most recent previous version of a file."""
        # Look in backup directories
        backups_dir = self.output_dir.parent.parent / "backups" / self.scraper_name
        
        if not backups_dir.exists():
            return None
        
        # Find backup folders
        backup_folders = sorted(
            [d for d in backups_dir.iterdir() if d.is_dir()],
            key=lambda x: x.stat().st_mtime,
            reverse=True
        )
        
        # Look for matching file in backups
        for backup_folder in backup_folders[1:]:  # Skip most recent (current run)
            potential_file = backup_folder / current_file.name
            if potential_file.exists():
                return potential_file
            
            # Check in subdirectories
            for subdir in ["previous_outputs", "exports"]:
                potential_file = backup_folder / subdir / current_file.name
                if potential_file.exists():
                    return potential_file
        
        return None
    
    def get_history(self, limit: int = 10) -> List[Dict]:
        """Get recent diff history."""
        return self._history[-limit:]
    
    def get_trend(self) -> Dict[str, Any]:
        """Get trend analysis of changes over time."""
        if len(self._history) < 2:
            return {"message": "Not enough history for trend analysis"}
        
        # Extract metrics from history
        metrics = []
        for entry in self._history:
            if "report" in entry:
                metrics.append({
                    "timestamp": entry["timestamp"],
                    "added": entry["report"].get("added_count", 0),
                    "removed": entry["report"].get("removed_count", 0),
                    "modified": entry["report"].get("modified_count", 0),
                    "total": entry["report"].get("new_count", 0),
                })
        
        if not metrics:
            return {"message": "No metrics available"}
        
        # Calculate averages
        avg_added = sum(m["added"] for m in metrics) / len(metrics)
        avg_removed = sum(m["removed"] for m in metrics) / len(metrics)
        avg_modified = sum(m["modified"] for m in metrics) / len(metrics)
        
        return {
            "runs_analyzed": len(metrics),
            "average_added": round(avg_added, 1),
            "average_removed": round(avg_removed, 1),
            "average_modified": round(avg_modified, 1),
            "latest_total": metrics[-1]["total"] if metrics else 0,
            "history": metrics[-10:],
        }


def compare_runs(
    old_file: Union[str, Path],
    new_file: Union[str, Path],
    key_column: str,
    compare_columns: List[str] = None,
) -> DiffReport:
    """
    Compare two data files.
    
    Args:
        old_file: Path to old/baseline file
        new_file: Path to new/current file
        key_column: Column to use as unique identifier
        compare_columns: Columns to compare (all if None)
    
    Returns:
        DiffReport object
    """
    old_file = Path(old_file)
    new_file = Path(new_file)
    
    # Load DataFrames
    if old_file.suffix.lower() == '.xlsx':
        old_df = pd.read_excel(old_file)
    else:
        old_df = pd.read_csv(old_file)
    
    if new_file.suffix.lower() == '.xlsx':
        new_df = pd.read_excel(new_file)
    else:
        new_df = pd.read_csv(new_file)
    
    return DiffReport(old_df, new_df, key_column, compare_columns)


def detect_changes(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_column: str,
) -> Dict[str, Any]:
    """
    Detect changes between two DataFrames.
    
    Args:
        old_df: Previous DataFrame
        new_df: Current DataFrame
        key_column: Key column for comparison
    
    Returns:
        Dict with change summary
    """
    diff = DiffReport(old_df, new_df, key_column)
    return diff.get_report()


def compare_with_datacompy(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_columns: List[str],
) -> Dict[str, Any]:
    """
    Compare DataFrames using datacompy library (if available).
    
    Args:
        old_df: Previous DataFrame
        new_df: Current DataFrame
        key_columns: Key columns for joining
    
    Returns:
        Comparison result dict
    """
    if not DATACOMPY_AVAILABLE:
        return {"error": "datacompy library not available"}
    
    try:
        compare = datacompy.Compare(
            old_df,
            new_df,
            join_columns=key_columns,
            df1_name="Previous",
            df2_name="Current",
        )
        
        return {
            "match": compare.matches(),
            "rows_in_common": compare.count_matching_rows(),
            "rows_only_in_previous": len(compare.df1_unq_rows),
            "rows_only_in_current": len(compare.df2_unq_rows),
            "columns_in_common": list(compare.intersect_columns()),
            "columns_only_in_previous": list(compare.df1_unq_columns()),
            "columns_only_in_current": list(compare.df2_unq_columns()),
            "report": compare.report(),
        }
    except Exception as e:
        return {"error": str(e)}


# CLI interface
if __name__ == "__main__":
    import sys
    import json
    
    if len(sys.argv) < 4:
        print("Usage: python data_diff.py <old_file> <new_file> <key_column>")
        print("Example: python data_diff.py backup/products.csv output/products.csv 'Product ID'")
        sys.exit(1)
    
    old_file = sys.argv[1]
    new_file = sys.argv[2]
    key_column = sys.argv[3]
    
    print(f"Comparing files...")
    print(f"  Old: {old_file}")
    print(f"  New: {new_file}")
    print(f"  Key: {key_column}")
    print()
    
    try:
        diff = compare_runs(old_file, new_file, key_column)
        report = diff.get_report()
        
        print("=" * 50)
        print("DIFF REPORT")
        print("=" * 50)
        print(f"Old file records:  {report['old_count']:,}")
        print(f"New file records:  {report['new_count']:,}")
        print()
        print(f"Added:     {report['added_count']:,}")
        print(f"Removed:   {report['removed_count']:,}")
        print(f"Modified:  {report['modified_count']:,}")
        print(f"Unchanged: {report['unchanged_count']:,}")
        print()
        print(f"Change rate: {report['change_rate']:.1f}%")
        print("=" * 50)
        
        # Show sample changes
        if report['added_count'] > 0:
            print(f"\nSample added records (first 3):")
            for record in diff.added.head(3).to_dict('records'):
                print(f"  {record}")
        
        if report['removed_count'] > 0:
            print(f"\nSample removed records (first 3):")
            for record in diff.removed.head(3).to_dict('records'):
                print(f"  {record}")
        
        if diff.changes:
            print(f"\nSample modifications (first 3):")
            for change in diff.changes[:3]:
                print(f"  Key: {change['key']}")
                for c in change['changes'][:3]:
                    print(f"    {c['column']}: {c['old_value']} -> {c['new_value']}")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
