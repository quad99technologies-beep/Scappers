#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standard progress tracking with ETA/throughput and persistent state.

Format:
  [PROGRESS] id=<id> current=<n> total=<n> pct=<n> rate=<n>/s eta=<hh:mm:ss> msg=<text>
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _format_eta(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0 or seconds == float("inf"):
        return "unknown"
    secs = int(seconds)
    hrs = secs // 3600
    mins = (secs % 3600) // 60
    rem = secs % 60
    return f"{hrs:02d}:{mins:02d}:{rem:02d}"


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    try:
        with open(temp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        temp_path.replace(path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)


@dataclass
class StandardProgress:
    task_id: str
    total: int
    unit: str = "items"
    logger: Optional[logging.Logger] = None
    state_path: Optional[Path] = None
    log_every: int = 1

    def __post_init__(self) -> None:
        self._start_time = time.monotonic()
        self._last_log_at = 0
        self._current = 0
        if self.logger is None:
            self.logger = log

    @property
    def current(self) -> int:
        return self._current

    def update(self, current: int, message: str = "", force: bool = False) -> None:
        self._current = max(0, int(current))
        elapsed = max(time.monotonic() - self._start_time, 0.0001)
        rate = self._current / elapsed if self._current else 0.0
        remaining = self.total - self._current if self.total else 0
        eta_seconds = (remaining / rate) if rate > 0 else None
        pct = (self._current / self.total * 100.0) if self.total else 0.0

        state = {
            "task_id": self.task_id,
            "current": self._current,
            "total": self.total,
            "unit": self.unit,
            "pct": round(pct, 2),
            "rate_per_sec": round(rate, 4),
            "eta_seconds": None if eta_seconds is None else int(eta_seconds),
            "message": message,
            "updated_at": _now_iso(),
        }

        if self.state_path:
            _atomic_write_json(self.state_path, state)

        should_log = force or self._current == self.total
        if not should_log and self.log_every > 0:
            should_log = (self._current - self._last_log_at) >= self.log_every

        if should_log:
            self._last_log_at = self._current
            eta_text = _format_eta(eta_seconds)
            msg = (
                "[PROGRESS] id={task_id} current={current} total={total} "
                "pct={pct:.1f} rate={rate:.2f}/s eta={eta} msg={message}"
            ).format(
                task_id=self.task_id,
                current=self._current,
                total=self.total,
                pct=pct,
                rate=rate,
                eta=eta_text,
                message=message or "-",
            )
            self.logger.info(msg)
