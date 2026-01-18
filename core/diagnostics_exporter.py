#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostics Exporter

Creates a zip bundle with system info and run metadata for support/debugging.
This module does not include config contents to avoid leaking secrets.
"""

from __future__ import annotations

import json
import platform
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from core.config_manager import ConfigManager
except Exception:
    ConfigManager = None


def _get_app_root() -> Path:
    if ConfigManager:
        ConfigManager.ensure_dirs()
        return ConfigManager.get_app_root()
    return Path.cwd()


def _get_exports_dir() -> Path:
    if ConfigManager:
        ConfigManager.ensure_dirs()
        return ConfigManager.get_exports_dir()
    return Path.cwd() / "exports"


def _get_runs_dir() -> Path:
    if ConfigManager:
        ConfigManager.ensure_dirs()
        return ConfigManager.get_runs_dir()
    return Path.cwd() / "runs"


def _get_cache_dir() -> Path:
    if ConfigManager:
        ConfigManager.ensure_dirs()
        return ConfigManager.get_cache_dir()
    return Path.cwd() / "cache"


def export_diagnostics(
    output_path: Optional[Path] = None,
    include_logs: bool = False,
    max_runs: int = 5,
) -> Path:
    """
    Export diagnostics bundle as a zip file.

    Args:
        output_path: Optional output path for the zip file.
        include_logs: Include run logs if True.
        max_runs: Max number of recent runs to include.

    Returns:
        Path to the created zip file.
    """
    app_root = _get_app_root()
    exports_dir = _get_exports_dir()
    exports_dir.mkdir(parents=True, exist_ok=True)

    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diagnostics_dir = exports_dir / "diagnostics"
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        output_path = diagnostics_dir / f"diagnostics_{timestamp}.zip"

    runs_dir = _get_runs_dir()
    cache_dir = _get_cache_dir()
    config_dir = app_root / "config"

    metadata = {
        "created_at": datetime.now().isoformat(),
        "app_root": str(app_root),
        "python_version": sys.version,
        "platform": platform.platform(),
        "config_files": sorted([p.name for p in config_dir.glob("*") if p.is_file()]) if config_dir.exists() else [],
        "runs_dir": str(runs_dir),
        "included_runs": [],
        "include_logs": include_logs,
    }

    run_dirs = []
    if runs_dir.exists():
        run_dirs = [p for p in runs_dir.iterdir() if p.is_dir()]
        run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        run_dirs = run_dirs[: max_runs if max_runs > 0 else 0]

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        index_path = cache_dir / "run_index.json"
        if index_path.exists():
            zf.write(index_path, arcname=str(index_path.relative_to(app_root)))

        for run_dir in run_dirs:
            run_entry = {"run_id": run_dir.name}
            manifest_path = run_dir / "manifest.json"
            metadata_path = run_dir / "metadata.json"
            log_path = run_dir / "logs" / "run.log"

            if manifest_path.exists():
                zf.write(manifest_path, arcname=str(manifest_path.relative_to(app_root)))
                run_entry["manifest"] = str(manifest_path)
            if metadata_path.exists():
                zf.write(metadata_path, arcname=str(metadata_path.relative_to(app_root)))
                run_entry["metadata"] = str(metadata_path)
            if include_logs and log_path.exists():
                zf.write(log_path, arcname=str(log_path.relative_to(app_root)))
                run_entry["log"] = str(log_path)

            metadata["included_runs"].append(run_entry)

        zf.writestr("diagnostics.json", json.dumps(metadata, indent=2))

    return output_path
