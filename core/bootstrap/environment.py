import sys
import os
from pathlib import Path

def setup_scraper_environment(file_path: str):
    """
    Standardize sys.path manipulation for all scrapers.
    Usage in scraper:
        from core.bootstrap import setup_scraper_environment
        setup_scraper_environment(__file__)
    """
    
    # 1. Determine paths
    script_path = Path(file_path).resolve()
    script_dir = script_path.parent
    repo_root = script_path.parents[2]  # Assuming standard depth: scripts/Country/script.py
    
    # If deeper depth, walk up until we find .git or specific marker
    if not (repo_root / ".git").exists():
        # Fallback search
        current = script_path
        while current.parent != current:
            if (current / ".git").exists():
                repo_root = current
                break
            current = current.parent

    # 2. Clean existing paths (remove duplicate core/repo references)
    # We want a clean slate to avoid shadowing
    cleaned_path = []
    for p in sys.path:
        # DISABLED: This filtration is too aggressive and removes standard library paths
        # if the python installation directory contains "core" (e.g. "pythoncore").
        # norm_p = os.path.normpath(p).lower()
        # if 'core' in norm_p and 'scrappers' not in norm_p: # Crude check for conflicting core modules?
        #      continue
        cleaned_path.append(p)
    sys.path = cleaned_path

    # 3. Insert paths in correct order
    # Priority:
    # 1. Script directory (for local modules)
    # 2. Region directory (e.g. scripts/Netherlands)
    # 3. Repo root (for 'core' package)
    
    paths_to_add = [str(script_dir), str(repo_root)]
    
    for p in paths_to_add:
        if p not in sys.path:
            sys.path.insert(0, p)

    # 4. Remove potentially conflicting 'db' module from sys.modules
    # This is a common issue in this repo where 'import db' grabs the wrong one
    if 'db' in sys.modules:
        del sys.modules['db']

    # 5. Setup basic logging? 
    # (Optional, maybe keep separate)

    return repo_root
