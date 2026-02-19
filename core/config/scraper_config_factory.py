"""
Factory for scraper configuration.
Replaces ~130 lines of boilerplate per config_loader.py with ~10 lines.
Does NOT change any environment variable names or config keys.
"""

import sys
from pathlib import Path
from typing import List, Optional

# Ensure core is in path if not already
_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from core.config.config_manager import ConfigManager


class ScraperConfig:

    def __init__(self, scraper_id: str):
        self.scraper_id = scraper_id
        ConfigManager.ensure_dirs()
        ConfigManager.load_env(scraper_id)
        self._load_legacy_json()

    def _load_legacy_json(self):
        """Load legacy .env.json for specific scrapers (e.g. Russia, Belarus)."""
        import os
        import json
        try:
            config_dir = ConfigManager.get_app_root() / "config"
            json_file = config_dir / f"{self.scraper_id}.env.json"
            if json_file.exists():
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Load "config" section into os.environ (if not already set)
                if "config" in data and isinstance(data["config"], dict):
                    for k, v in data["config"].items():
                        if k.startswith("_"): continue
                        if k not in os.environ:
                            if isinstance(v, bool):
                                os.environ[k] = "true" if v else "false"
                            elif isinstance(v, (int, float, str)):
                                os.environ[k] = str(v)
                            # Lists/Dicts stay in JSON for get_list/get_dict
        except Exception:
            pass

    # ── Path accessors ───────────────────────────────────

    def get_repo_root(self) -> Path:
        return ConfigManager.get_app_root()

    def get_base_dir(self) -> Path:
        return ConfigManager.get_app_root()

    def get_input_dir(self, subpath: str = None) -> Path:
        base = ConfigManager.get_input_dir(self.scraper_id)
        return base / subpath if subpath else base

    def get_output_dir(self, subpath: str = None) -> Path:
        base = ConfigManager.get_output_dir(self.scraper_id)
        return base / subpath if subpath else base

    def get_backup_dir(self) -> Path:
        return ConfigManager.get_backups_dir(self.scraper_id)

    def get_central_output_dir(self) -> Path:
        return ConfigManager.get_exports_dir(self.scraper_id)

    # ── Environment accessors ────────────────────────────

    def getenv(self, key: str, default: str = "") -> str:
        val = ConfigManager.get_env_value(self.scraper_id, key, default)
        return val if val is not None else ""

    def getenv_int(self, key: str, default: int = 0) -> int:
        try:
            return int(self.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    def getenv_float(self, key: str, default: float = 0.0) -> float:
        try:
            return float(self.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    def getenv_bool(self, key: str, default: bool = False) -> bool:
        val = self.getenv(key, str(default)).lower()
        return val in ("true", "1", "yes", "on")

    def getenv_list(self, key: str, default: Optional[List] = None) -> List:
        if default is None: default = []
        
        # Check for JSON format in env or legacy file
        raw = self.getenv(key, "")
        if not raw:
            import json
            try:
                json_file = ConfigManager.get_app_root() / "config" / f"{self.scraper_id}.env.json"
                if json_file.exists():
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    val = data.get("config", {}).get(key)
                    if isinstance(val, list): return val
            except Exception:
                pass
            return default
            
        if raw.startswith("[") and raw.endswith("]"):
            import json
            try:
                return json.loads(raw)
            except Exception:
                pass
        return [item.strip() for item in raw.split(",") if item.strip()]


def create_config(scraper_id: str) -> ScraperConfig:
    return ScraperConfig(scraper_id)
