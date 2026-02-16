from pathlib import Path
import os
import datetime
import json
import logging

log = logging.getLogger(__name__)

class DataWriter:
    """Standardized output writer."""
    
    def __init__(self, output_dir: Path, filename: str, encoding: str = 'utf-8'):
        self.output_dir = output_dir
        self.output_file = output_dir / filename
        self.encoding = encoding
        self._f = None
        
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def __enter__(self):
        self._f = open(self.output_file, 'w', encoding=self.encoding)
        return self
    
    def write_jsonl(self, item: dict):
        if not self._f:
            raise RuntimeError("File not open. Use context manager.")
        
        item.setdefault('scraped_at', datetime.datetime.now().isoformat())
        self._f.write(json.dumps(item, ensure_ascii=False) + "\n")
        self._f.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._f:
            self._f.close()
