#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Atomic startup lock helpers for pipeline entry points (GUI / API / Telegram).

Lock file format:
  line1: PID (0 means "startup in progress")
  line2: ISO timestamp
  line3: optional log path or startup marker
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


def _default_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_lock_paths(scraper_name: str, repo_root: Optional[Path] = None) -> Tuple[Path, Path]:
    base_root = Path(repo_root) if repo_root else _default_repo_root()
    try:
        from core.config.config_manager import ConfigManager

        # Migrated: get_path_manager() -> ConfigManager
        new_lock = ConfigManager.get_sessions_dir() / f"{scraper_name}.lock"
    except Exception:
        new_lock = base_root / ".locks" / f"{scraper_name}.lock"
    old_lock = base_root / f".{scraper_name}_run.lock"
    return new_lock, old_lock


def read_lock_info(lock_file: Path) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    try:
        content = lock_file.read_text(encoding="utf-8", errors="replace").strip().splitlines()
    except Exception:
        return None, None, None
    pid = int(content[0]) if content and content[0].isdigit() else None
    started = content[1] if len(content) > 1 else None
    log_path = content[2] if len(content) > 2 else None
    return pid, started, log_path


def is_pid_running(pid: Optional[int]) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        import psutil

        return psutil.pid_exists(pid) and psutil.Process(pid).is_running()
    except Exception:
        if sys.platform == "win32":
            try:
                result = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {pid}"],
                    capture_output=True,
                    text=True,
                    timeout=2,
                    check=False,
                )
                return str(pid) in result.stdout
            except Exception:
                return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _parse_started_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _is_lock_active(lock_file: Path, starting_ttl_seconds: int = 120) -> Tuple[bool, str]:
    pid, started, marker = read_lock_info(lock_file)

    if pid and pid > 0 and is_pid_running(pid):
        return True, f"pid {pid} is running"

    started_dt = _parse_started_ts(started)
    age_seconds = None
    if started_dt:
        try:
            age_seconds = (datetime.now() - started_dt).total_seconds()
        except Exception:
            age_seconds = None

    # pid=0 means another controller is currently in launch handshake.
    if pid == 0:
        if age_seconds is None or age_seconds < max(5, int(starting_ttl_seconds)):
            return True, "startup lock in progress"
        return False, "stale startup lock"

    if marker and str(marker).startswith("STARTING:"):
        if age_seconds is None or age_seconds < max(5, int(starting_ttl_seconds)):
            return True, "startup lock in progress"
        return False, "stale startup marker"

    # No live pid and no active startup marker => stale.
    return False, "stale lock"


def _atomic_create_start_lock(lock_file: Path, owner: str) -> bool:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(str(lock_file), flags)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", errors="replace") as handle:
            handle.write(f"0\n{datetime.now().isoformat()}\nSTARTING:{owner}\n")
            handle.flush()
        return True
    except Exception:
        try:
            os.close(fd)
        except Exception:
            pass
        raise


def claim_pipeline_start_lock(
    scraper_name: str,
    owner: str = "unknown",
    repo_root: Optional[Path] = None,
    starting_ttl_seconds: int = 120,
) -> Tuple[bool, Path, str]:
    """
    Claim a startup lock atomically. Returns (acquired, lock_file, reason).
    """
    new_lock, old_lock = get_lock_paths(scraper_name, repo_root=repo_root)

    # Respect active legacy lock if present, otherwise clear stale legacy lock.
    if old_lock.exists() and old_lock != new_lock:
        active, reason = _is_lock_active(old_lock, starting_ttl_seconds=starting_ttl_seconds)
        if active:
            return False, new_lock, f"legacy lock active: {reason}"
        try:
            old_lock.unlink()
        except Exception:
            pass

    reason = "lock exists"
    for _ in range(2):
        try:
            _atomic_create_start_lock(new_lock, owner=owner)
            return True, new_lock, "claimed"
        except FileExistsError:
            active, reason = _is_lock_active(new_lock, starting_ttl_seconds=starting_ttl_seconds)
            if active:
                return False, new_lock, reason
            try:
                new_lock.unlink()
            except Exception:
                return False, new_lock, reason
            continue
        except Exception as exc:
            return False, new_lock, str(exc)
    return False, new_lock, reason


def update_pipeline_lock(lock_file: Path, pid: int, log_path: Optional[Path] = None) -> None:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    payload = [str(pid), datetime.now().isoformat()]
    if log_path:
        payload.append(str(log_path))
    lock_file.write_text("\n".join(payload) + "\n", encoding="utf-8")


def release_pipeline_lock(lock_file: Optional[Path]) -> None:
    if not lock_file:
        return
    try:
        if lock_file.exists():
            lock_file.unlink()
    except Exception:
        pass

