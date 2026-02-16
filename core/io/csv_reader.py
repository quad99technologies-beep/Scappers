import csv
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from core.utils.text_utils import nk

log = logging.getLogger(__name__)

def read_state_rows(path: Path) -> Tuple[List[Dict], Dict]:
    """
    Read CSV file with fallback encoding detection.
    Returns: (list of rows, dict of {lowercase_normalized_key: original_key})
    """
    if not path.exists():
        return [], {}
        
    encoding_attempts = ["utf-8-sig", "utf-8", "latin-1", "windows-1252", "cp1252"]
    for encoding in encoding_attempts:
        try:
            with open(path, "r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                headers = {nk(h): h for h in fieldnames}
                return (list(reader), headers)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            log.warning(f"Error reading CSV {path} with {encoding}: {e}")
            break
            
    return ([], {})
