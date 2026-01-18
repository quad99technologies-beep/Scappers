#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run Ledger

Minimal file-based run ledger for Scappers.
Stores metadata per run in runs/<run_id>/metadata.json and maintains a cache index.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from core.config_manager import ConfigManager
except Exception:
    ConfigManager = None

log = logging.getLogger(__name__)


class RunStatus(str, Enum):
    """Run execution status."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RunMetadata:
    """Canonical run metadata structure."""

    run_id: str
    scraper_name: str
    created_at: str
    started_at: Optional[str]
    ended_at: Optional[str]
    status: RunStatus
    pipeline: Dict[str, Any]
    paths: Dict[str, Optional[str]]
    artifacts: Dict[str, List[str]]
    metrics: Dict[str, Any]
    error: Optional[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunMetadata":
        data = dict(data)
        data["status"] = RunStatus(data["status"])
        return cls(**data)


class FileRunLedger:
    """
    File-based run ledger using metadata.json per run.

    Structure:
      runs/<run_id>/metadata.json
      cache/run_index.json
    """

    def __init__(self, runs_dir: Optional[Path] = None, cache_dir: Optional[Path] = None) -> None:
        self.runs_dir = self._resolve_runs_dir(runs_dir)
        self.cache_dir = self._resolve_cache_dir(cache_dir)
        self.index_path = self.cache_dir / "run_index.json"
        self._index = self._load_index()

    def _resolve_runs_dir(self, runs_dir: Optional[Path]) -> Path:
        if runs_dir:
            return Path(runs_dir)
        if ConfigManager:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_runs_dir()
        return Path.cwd() / "runs"

    def _resolve_cache_dir(self, cache_dir: Optional[Path]) -> Path:
        if cache_dir:
            return Path(cache_dir)
        if ConfigManager:
            ConfigManager.ensure_dirs()
            return ConfigManager.get_cache_dir()
        return Path.cwd() / "cache"

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _load_index(self) -> Dict[str, Dict[str, str]]:
        if self.index_path.exists():
            try:
                with open(self.index_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as exc:
                log.warning("Failed to load run index: %s. Rebuilding...", exc)
        return self._rebuild_index()

    def _save_index(self, index: Dict[str, Dict[str, str]]) -> None:
        try:
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.index_path, "w", encoding="utf-8") as f:
                json.dump(index, f, indent=2)
        except Exception as exc:
            log.warning("Failed to save run index: %s", exc)

    def _rebuild_index(self) -> Dict[str, Dict[str, str]]:
        index: Dict[str, Dict[str, str]] = {}
        if not self.runs_dir.exists():
            return index

        for run_dir in self.runs_dir.iterdir():
            if not run_dir.is_dir():
                continue
            metadata_path = run_dir / "metadata.json"
            if not metadata_path.exists():
                continue
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                index[data["run_id"]] = {
                    "created_at": data.get("created_at", ""),
                    "status": data.get("status", ""),
                    "scraper_name": data.get("scraper_name", ""),
                    "run_dir": str(run_dir),
                }
            except Exception as exc:
                log.warning("Failed to index run %s: %s", run_dir.name, exc)

        self._save_index(index)
        return index

    def _update_index(self, metadata: RunMetadata, run_dir: Path) -> None:
        self._index[metadata.run_id] = {
            "created_at": metadata.created_at,
            "status": metadata.status.value,
            "scraper_name": metadata.scraper_name,
            "run_dir": str(run_dir),
        }
        self._save_index(self._index)

    def _metadata_path(self, run_dir: Path) -> Path:
        return run_dir / "metadata.json"

    def _write_metadata(self, metadata: RunMetadata, run_dir: Path) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        metadata_path = self._metadata_path(run_dir)
        temp_path = metadata_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(metadata.to_dict(), f, indent=2)
            temp_path.replace(metadata_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata.to_dict(), f, indent=2)

    def _load_metadata(self, run_dir: Path) -> Optional[RunMetadata]:
        metadata_path = self._metadata_path(run_dir)
        if not metadata_path.exists():
            return None
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return RunMetadata.from_dict(data)
        except Exception as exc:
            log.warning("Failed to read metadata for %s: %s", run_dir, exc)
            return None

    def record_run_start(
        self,
        run_id: str,
        scraper_name: str,
        run_dir: Optional[Path] = None,
        pipeline: Optional[Dict[str, Any]] = None,
        paths: Optional[Dict[str, Optional[str]]] = None,
    ) -> RunMetadata:
        run_dir = Path(run_dir) if run_dir else (self.runs_dir / run_id)
        created_at = self._now_iso()
        metadata = RunMetadata(
            run_id=run_id,
            scraper_name=scraper_name,
            created_at=created_at,
            started_at=created_at,
            ended_at=None,
            status=RunStatus.RUNNING,
            pipeline=pipeline or {},
            paths=paths or {"run_dir": str(run_dir)},
            artifacts={"logs": [], "outputs": [], "exports": [], "manifests": []},
            metrics={},
            error=None,
        )
        self._write_metadata(metadata, run_dir)
        self._update_index(metadata, run_dir)
        return metadata

    def record_run_end(
        self,
        run_id: str,
        status: RunStatus,
        run_dir: Optional[Path] = None,
        artifacts: Optional[Dict[str, List[str]]] = None,
        metrics: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
        paths: Optional[Dict[str, Optional[str]]] = None,
    ) -> RunMetadata:
        run_dir = Path(run_dir) if run_dir else (self.runs_dir / run_id)
        metadata = self._load_metadata(run_dir)
        if metadata is None:
            created_at = self._now_iso()
            metadata = RunMetadata(
                run_id=run_id,
                scraper_name="unknown",
                created_at=created_at,
                started_at=None,
                ended_at=None,
                status=status,
                pipeline={},
                paths={"run_dir": str(run_dir)},
                artifacts={"logs": [], "outputs": [], "exports": [], "manifests": []},
                metrics={},
                error=None,
            )

        metadata.status = status
        metadata.ended_at = self._now_iso()
        if artifacts:
            for key, value in artifacts.items():
                if isinstance(value, list):
                    metadata.artifacts.setdefault(key, [])
                    metadata.artifacts[key] = list(dict.fromkeys(metadata.artifacts[key] + value))
        if metrics:
            metadata.metrics.update(metrics)
        if error:
            metadata.error = error
        if paths:
            metadata.paths.update(paths)

        self._write_metadata(metadata, run_dir)
        self._update_index(metadata, run_dir)
        return metadata

    def get_run(self, run_id: str) -> Optional[RunMetadata]:
        run_dir = self.runs_dir / run_id
        return self._load_metadata(run_dir)

    def list_runs(
        self,
        limit: int = 100,
        status: Optional[RunStatus] = None,
        scraper_name: Optional[str] = None,
    ) -> List[RunMetadata]:
        if not self.index_path.exists():
            self._index = self._rebuild_index()

        items = []
        for run_id, entry in self._index.items():
            if status and entry.get("status") != status.value:
                continue
            if scraper_name and entry.get("scraper_name") != scraper_name:
                continue
            items.append((entry.get("created_at", ""), run_id))

        items.sort(reverse=True)
        results: List[RunMetadata] = []
        for _, run_id in items[:limit]:
            metadata = self.get_run(run_id)
            if metadata:
                results.append(metadata)
        return results
