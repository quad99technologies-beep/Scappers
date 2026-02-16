#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared Scraper Configuration

Provides a unified config interface for all scrapers. Each scraper's config_loader
can use ScraperConfig(scraper_id) and re-export the functions.
"""

import json
import os
from pathlib import Path
from typing import List, Optional


class ScraperConfig:
    """
    Unified configuration for a scraper. Uses ConfigManager for paths and env.
    """

    def __init__(self, scraper_id: str):
        self.scraper_id = scraper_id
        self._config_available = False
        try:
            from core.config.config_manager import ConfigManager
            self._ConfigManager = ConfigManager
            self._config_available = True
        except ImportError:
            self._ConfigManager = None

    def load_env(self) -> None:
        """Load environment for this scraper."""
        if self._config_available:
            try:
                self._ConfigManager.ensure_dirs()
                self._ConfigManager.load_env(self.scraper_id)
            except Exception:
                pass

    def getenv(self, key: str, default: Optional[str] = None) -> str:
        """Get config value as string."""
        if default is None:
            default = ""
        if self._config_available:
            try:
                val = self._ConfigManager.get_config_value(self.scraper_id, key, None)
                if val is not None:
                    return str(val)
                val = self._ConfigManager.get_env_value(self.scraper_id, key, None)
                if val is not None:
                    return str(val)
            except Exception:
                pass
        return os.getenv(key, default)

    def getenv_int(self, key: str, default: int = 0) -> int:
        """Get config value as int."""
        try:
            return int(self.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    def getenv_float(self, key: str, default: float = 0.0) -> float:
        """Get config value as float."""
        try:
            return float(self.getenv(key, str(default)))
        except (ValueError, TypeError):
            return default

    def getenv_bool(self, key: str, default: bool = False) -> bool:
        """Get config value as bool."""
        val = self.getenv(key, str(default))
        if isinstance(val, bool):
            return val
        return str(val).lower() in ("true", "1", "yes", "on")

    def getenv_list(self, key: str, default: Optional[List] = None) -> list:
        """Get config value as list."""
        if default is None:
            default = []
        if self._config_available:
            try:
                val = self._ConfigManager.get_config_value(self.scraper_id, key, default)
                if isinstance(val, list):
                    return val
            except Exception:
                pass
        val = os.getenv(key)
        if val is None:
            return default
        if isinstance(val, str):
            try:
                return json.loads(val)
            except (json.JSONDecodeError, ValueError):
                return [v.strip() for v in val.split(",") if v.strip()]
        return default

    def get_repo_root(self) -> Path:
        """Get repository root."""
        if self._config_available:
            return self._ConfigManager.get_app_root()
        return Path(__file__).resolve().parents[2]

    def get_output_dir(self, subpath: Optional[str] = None) -> Path:
        """Get output directory."""
        try:
            output_str = self.getenv("OUTPUT_DIR", "")
            if output_str and Path(output_str).is_absolute():
                base = Path(output_str)
            elif self._config_available:
                base = self._ConfigManager.get_output_dir(self.scraper_id)
            else:
                base = self.get_repo_root() / "output" / self.scraper_id
            base.mkdir(parents=True, exist_ok=True)
            return base / subpath if subpath else base
        except Exception:
            return self.get_repo_root() / "output" / self.scraper_id

    def get_input_dir(self, subpath: Optional[str] = None) -> Path:
        """Get input directory."""
        try:
            if self._config_available:
                base = self._ConfigManager.get_input_dir(self.scraper_id)
            else:
                base = self.get_repo_root() / "input" / self.scraper_id
            base.mkdir(parents=True, exist_ok=True)
            return base / subpath if subpath else base
        except Exception:
            return self.get_repo_root() / "input" / self.scraper_id

    def get_backup_dir(self) -> Path:
        """Get backup directory."""
        try:
            if self._config_available:
                return self._ConfigManager.get_backups_dir(self.scraper_id)
        except Exception:
            pass
        return self.get_repo_root() / "backups" / self.scraper_id

    def get_central_output_dir(self) -> Path:
        """Get central exports directory."""
        try:
            if self._config_available:
                d = self._ConfigManager.get_exports_dir(self.scraper_id)
                d.mkdir(parents=True, exist_ok=True)
                return d
        except Exception:
            pass
        return self.get_repo_root() / "output" / self.scraper_id


def create_config(scraper_id: str) -> ScraperConfig:
    """Factory to create a ScraperConfig instance."""
    return ScraperConfig(scraper_id)
