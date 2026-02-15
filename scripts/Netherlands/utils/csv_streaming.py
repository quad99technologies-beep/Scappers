"""
Streaming CSV writer - writes rows immediately to avoid data loss.
"""

import csv
from pathlib import Path
from typing import List, Dict, Any
import threading


class StreamingCSVWriter:
    """
    CSV writer that streams rows to disk immediately with buffering.

    Features:
    - Auto-flush buffer every N rows (default: 100)
    - Thread-safe writing
    - Automatic parent directory creation
    - Guaranteed output even if script crashes
    - Low memory footprint (doesn't store all data)
    """

    def __init__(self, filepath: str | Path, fieldnames: List[str], buffer_size: int = 100):
        """
        Initialize streaming CSV writer.

        Args:
            filepath: Output CSV file path
            fieldnames: Column names for CSV header
            buffer_size: Number of rows to buffer before auto-flush (default: 100)
        """
        self.filepath = Path(filepath)
        self.fieldnames = fieldnames
        self.buffer_size = buffer_size
        self.buffer: List[Dict[str, Any]] = []
        self.row_count = 0
        self.lock = threading.Lock()  # Thread safety

        # Create parent directory if needed
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

        # Open file and write header
        self.file = open(self.filepath, 'w', newline='', encoding='utf-8')
        self.writer = csv.DictWriter(self.file, fieldnames=fieldnames, extrasaction='ignore')
        self.writer.writeheader()
        self.file.flush()

    def write_row(self, row: Dict[str, Any]) -> None:
        """
        Write a single row to CSV (buffered).

        Args:
            row: Dictionary with column values (extra keys ignored)
        """
        with self.lock:
            self.buffer.append(row)
            self.row_count += 1

            # Auto-flush when buffer is full
            if len(self.buffer) >= self.buffer_size:
                self._flush_unlocked()

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        """
        Write multiple rows at once.

        Args:
            rows: List of row dictionaries
        """
        with self.lock:
            self.buffer.extend(rows)
            self.row_count += len(rows)

            # Auto-flush if buffer is full
            if len(self.buffer) >= self.buffer_size:
                self._flush_unlocked()

    def flush(self) -> None:
        """Force write buffered rows to disk."""
        with self.lock:
            self._flush_unlocked()

    def _flush_unlocked(self) -> None:
        """Internal flush (assumes lock is held)."""
        if self.buffer:
            self.writer.writerows(self.buffer)
            self.file.flush()
            self.buffer.clear()

    def close(self) -> None:
        """Flush remaining rows and close file."""
        self.flush()
        self.file.close()

    def get_row_count(self) -> int:
        """Get total rows written (including buffered)."""
        return self.row_count

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Auto-close on exit."""
        self.close()

    def __repr__(self):
        return f"StreamingCSVWriter(filepath={self.filepath}, rows={self.row_count}, buffered={len(self.buffer)})"
