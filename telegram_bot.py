#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram bot controller for the Scraper platform.

Commands:
  /help
  /whoami
  /ping
  /list
  /status <scraper|all>
  /run <scraper> [fresh]
  /runfresh <scraper>
  /clear <scraper>
"""

from __future__ import annotations

import os
import sys
import time
import threading
import subprocess
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple, List

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent

SCRAPERS: Dict[str, Dict[str, Path]] = {
    "CanadaQuebec": {"path": REPO_ROOT / "scripts" / "CanadaQuebec"},
    "Malaysia": {"path": REPO_ROOT / "scripts" / "Malaysia"},
    "Argentina": {"path": REPO_ROOT / "scripts" / "Argentina"},
    "CanadaOntario": {"path": REPO_ROOT / "scripts" / "Canada Ontario"},
    "Netherlands": {"path": REPO_ROOT / "scripts" / "Netherlands"},
    "Belarus": {"path": REPO_ROOT / "scripts" / "Belarus"},
    "Russia": {"path": REPO_ROOT / "scripts" / "Russia"},
    "Taiwan": {"path": REPO_ROOT / "scripts" / "Taiwan"},
    "NorthMacedonia": {"path": REPO_ROOT / "scripts" / "North Macedonia"},
    "Tender_Chile": {"path": REPO_ROOT / "scripts" / "Tender- Chile"},
    "India": {"path": REPO_ROOT / "scripts" / "India"},
}


def _normalize_name(value: str) -> str:
    return "".join(ch.lower() for ch in value if ch.isalnum())


SCRAPER_ALIASES = { _normalize_name(name): name for name in SCRAPERS.keys() }


def resolve_scraper_name(raw_name: Optional[str]) -> Optional[str]:
    if not raw_name:
        return None
    key = _normalize_name(raw_name)
    return SCRAPER_ALIASES.get(key)


def _get_path_manager():
    try:
        from platform_config import get_path_manager
        return get_path_manager()
    except Exception:
        return None


def get_lock_paths(scraper_name: str) -> Tuple[Path, Path]:
    pm = _get_path_manager()
    if pm:
        new_lock = pm.get_lock_file(scraper_name)
    else:
        new_lock = REPO_ROOT / ".locks" / f"{scraper_name}.lock"
    old_lock = REPO_ROOT / f".{scraper_name}_run.lock"
    return new_lock, old_lock


def read_lock_info(lock_file: Path) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    try:
        content = lock_file.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return None, None, None
    pid = int(content[0]) if content and content[0].isdigit() else None
    started = content[1] if len(content) > 1 else None
    log_path = content[2] if len(content) > 2 else None
    return pid, started, log_path


def is_pid_running(pid: Optional[int]) -> bool:
    if not pid:
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
                    timeout=2
                )
                return str(pid) in result.stdout
            except Exception:
                return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def ensure_lock_file(lock_file: Path, pid: int, log_path: Optional[Path] = None) -> None:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    payload = [str(pid), datetime.now().isoformat()]
    if log_path:
        payload.append(str(log_path))
    lock_file.write_text("\n".join(payload) + "\n", encoding="utf-8")


def remove_lock_file(lock_file: Path) -> None:
    try:
        if lock_file.exists():
            lock_file.unlink()
    except Exception:
        pass


def get_logs_dir() -> Path:
    pm = _get_path_manager()
    if pm:
        logs_dir = pm.get_logs_dir()
    else:
        logs_dir = REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    telegram_dir = logs_dir / "telegram"
    telegram_dir.mkdir(parents=True, exist_ok=True)
    return telegram_dir


def _safe_read_last_lines(path: Path, max_lines: int = 200) -> List[str]:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
        if len(lines) > max_lines:
            return lines[-max_lines:]
        return lines
    except Exception:
        return []


def find_latest_log(scraper_name: str, status: Dict[str, Optional[str]]) -> Optional[Path]:
    if status.get("log_file"):
        log_path = Path(status["log_file"])
        if log_path.exists():
            return log_path

    candidates: List[Path] = []

    # Telegram bot logs (if bot started the pipeline)
    telegram_dir = get_logs_dir()
    candidates.extend(telegram_dir.glob(f"{scraper_name}_pipeline_*.log"))

    # Output logs from pipeline batch scripts
    try:
        from platform_config import get_path_manager
        pm = get_path_manager()
        output_dir = pm.get_output_dir(scraper_name)
    except Exception:
        output_dir = REPO_ROOT / "output" / scraper_name
    if output_dir.exists():
        candidates.extend(output_dir.glob("*.log"))

    # Generic logs dir (fallback)
    generic_logs = REPO_ROOT / "logs"
    if generic_logs.exists():
        candidates.extend(generic_logs.glob("*.log"))

    # Scraper-local logs folder
    scraper_path = SCRAPERS.get(scraper_name, {}).get("path")
    if scraper_path:
        local_logs = scraper_path / "logs"
        if local_logs.exists():
            candidates.extend(local_logs.glob("*.log"))

    if not candidates:
        return None

    candidates = [p for p in candidates if p.exists()]
    if not candidates:
        return None

    return max(candidates, key=lambda p: p.stat().st_mtime)


def extract_latest_progress(log_path: Path) -> Optional[str]:
    lines = _safe_read_last_lines(log_path, max_lines=300)
    if not lines:
        return None

    progress_patterns = [
        r"\[PROGRESS\]\s+Pipeline\s+Step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)\s*-\s*(.+)",
        r"\[PROGRESS\]\s+Pipeline\s+Step\s*:\s*(\d+)\s*/\s*(\d+)\s*\(([\d.]+)%\)",
        r"\[Step\s+(\d+)\s*/\s*(\d+)\]\s*(.+)",
        r"Step\s+(\d+)\s*/\s*(\d+)\s*:\s*(.+)",
    ]

    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        if "Pipeline completed" in line or "Execution completed" in line:
            return "Pipeline completed"
        for pattern in progress_patterns:
            match = re.search(pattern, line)
            if not match:
                continue
            step = match.group(1)
            total = match.group(2)
            if len(match.groups()) >= 4:
                percent = match.group(3)
                desc = match.group(4).strip()
                return f"Step {step}/{total} ({percent}%) - {desc}"
            if len(match.groups()) == 3:
                percent = match.group(3)
                return f"Step {step}/{total} ({percent}%)"
            if len(match.groups()) == 2:
                return f"Step {step}/{total}"
        if "Pipeline stopped" in line or "[STOPPED]" in line:
            return "Pipeline stopped"
    return None


def get_pipeline_status(scraper_name: str) -> Dict[str, Optional[str]]:
    new_lock, old_lock = get_lock_paths(scraper_name)
    lock_file = new_lock if new_lock.exists() else old_lock if old_lock.exists() else None
    if not lock_file:
        return {"state": "idle", "pid": None, "started": None, "lock_file": None, "log_file": None}

    pid, started, log_path = read_lock_info(lock_file)
    running = is_pid_running(pid)
    if running:
        return {
            "state": "running",
            "pid": str(pid) if pid else None,
            "started": started,
            "lock_file": str(lock_file),
            "log_file": log_path
        }
    return {
        "state": "stale",
        "pid": str(pid) if pid else None,
        "started": started,
        "lock_file": str(lock_file),
        "log_file": log_path
    }


def build_pipeline_command(scraper_name: str, fresh: bool = False) -> Tuple[List[str], Path]:
    scraper_path = SCRAPERS[scraper_name]["path"]
    resume_script = scraper_path / "run_pipeline_resume.py"
    if resume_script.exists():
        cmd = [sys.executable, "-u", str(resume_script)]
        if fresh:
            cmd.append("--fresh")
        return cmd, scraper_path

    pipeline_bat = scraper_path / "run_pipeline.bat"
    if pipeline_bat.exists():
        if sys.platform == "win32":
            cmd = ["cmd", "/c", str(pipeline_bat)]
        else:
            cmd = ["sh", str(pipeline_bat)]
        return cmd, scraper_path

    raise FileNotFoundError(f"No pipeline runner found for {scraper_name}")


def launch_pipeline(scraper_name: str, fresh: bool = False) -> Tuple[int, Path]:
    cmd, cwd = build_pipeline_command(scraper_name, fresh=fresh)
    log_dir = get_logs_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{scraper_name}_pipeline_{timestamp}.log"

    log_handle = log_path.open("a", encoding="utf-8", errors="replace")
    log_handle.write(f"[BOT] Starting {scraper_name} at {datetime.now().isoformat()}\n")
    log_handle.write(f"[BOT] Command: {' '.join(cmd)}\n\n")
    log_handle.flush()

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        env=env
    )

    new_lock, _old_lock = get_lock_paths(scraper_name)
    ensure_lock_file(new_lock, process.pid, log_path=log_path)

    def _monitor() -> None:
        exit_code = process.wait()
        try:
            with log_path.open("a", encoding="utf-8", errors="replace") as handle:
                handle.write(f"\n[BOT] {scraper_name} exited with code {exit_code} at {datetime.now().isoformat()}\n")
        finally:
            remove_lock_file(new_lock)
            try:
                log_handle.close()
            except Exception:
                pass

    threading.Thread(target=_monitor, daemon=True).start()
    return process.pid, log_path


def format_status_line(scraper_name: str, status: Dict[str, Optional[str]]) -> str:
    state = status.get("state")
    if state == "running":
        line = f"{scraper_name}: RUNNING"
        if status.get("pid"):
            line += f" (pid {status['pid']})"
        return line
    if state == "stale":
        line = f"{scraper_name}: STALE LOCK"
        if status.get("pid"):
            line += f" (pid {status['pid']})"
        return line
    return f"{scraper_name}: IDLE"


def format_status_details(scraper_name: str, status: Dict[str, Optional[str]]) -> str:
    lines = [format_status_line(scraper_name, status)]
    if status.get("state") == "running":
        log_path = find_latest_log(scraper_name, status)
        if log_path:
            progress = extract_latest_progress(log_path)
            if progress:
                lines.append(f"Current Step: {progress}")
            lines.append(f"Log: {log_path}")
        else:
            lines.append("Current Step: (log not available)")
    if status.get("started"):
        lines.append(f"Started: {status['started']}")
    if status.get("lock_file"):
        lines.append(f"Lock: {status['lock_file']}")
    return "\n".join(lines)


def parse_allowed_chat_ids(raw_value: Optional[str]) -> Optional[set]:
    if not raw_value:
        return None
    ids = set()
    for part in raw_value.split(","):
        part = part.strip()
        if not part:
            continue
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids or None


class TelegramBot:
    def __init__(self, token: str, allowed_chat_ids: Optional[set], default_scraper: Optional[str] = None):
        self.token = token
        self.allowed_chat_ids = allowed_chat_ids
        self.default_scraper = default_scraper
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.last_update_id = None

    def send_message(self, chat_id: int, text: str) -> None:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True
        }
        try:
            requests.post(f"{self.base_url}/sendMessage", data=payload, timeout=10)
        except Exception:
            pass

    def is_authorized(self, chat_id: int) -> bool:
        if not self.allowed_chat_ids:
            return True
        return chat_id in self.allowed_chat_ids

    def handle_command(self, chat_id: int, text: str) -> None:
        parts = text.strip().split()
        if not parts:
            return
        cmd = parts[0].split("@")[0].lower()
        args = parts[1:]

        if cmd in ("/start", "/help"):
            self.send_message(chat_id, self.help_text())
            return
        if cmd == "/whoami":
            self.send_message(chat_id, f"Chat ID: {chat_id}")
            return
        if cmd == "/ping":
            self.send_message(chat_id, "pong")
            return

        if cmd == "/list":
            names = ", ".join(sorted(SCRAPERS.keys()))
            self.send_message(chat_id, f"Available scrapers:\n{names}")
            return

        if cmd == "/status":
            if args and args[0].lower() in ("all", "*"):
                lines = []
                for name in SCRAPERS.keys():
                    status = get_pipeline_status(name)
                    line = format_status_line(name, status)
                    if status.get("state") == "running":
                        log_path = find_latest_log(name, status)
                        if log_path:
                            progress = extract_latest_progress(log_path)
                            if progress:
                                line += f" | {progress}"
                    lines.append(line)
                self.send_message(chat_id, "\n".join(lines))
                return
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "Usage: /status <scraper|all>")
                return
            status = get_pipeline_status(scraper_name)
            self.send_message(chat_id, format_status_details(scraper_name, status))
            return

        if cmd in ("/run", "/runfresh"):
            fresh = cmd == "/runfresh" or any(arg.lower() in ("fresh", "--fresh", "-f") for arg in args)
            scraper_arg = None
            for arg in args:
                if arg.lower() in ("fresh", "--fresh", "-f"):
                    continue
                scraper_arg = arg
                break
            scraper_name = resolve_scraper_name(scraper_arg) if scraper_arg else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "Usage: /run <scraper> [fresh]")
                return
            status = get_pipeline_status(scraper_name)
            if status["state"] == "running":
                self.send_message(chat_id, format_status_details(scraper_name, status))
                return
            try:
                pid, log_path = launch_pipeline(scraper_name, fresh=fresh)
                mode = "fresh" if fresh else "resume"
                self.send_message(
                    chat_id,
                    f"Started {scraper_name} ({mode})\nPID: {pid}\nLog: {log_path}"
                )
            except Exception as exc:
                self.send_message(chat_id, f"Failed to start {scraper_name}: {exc}")
            return

        if cmd == "/clear":
            scraper_name = resolve_scraper_name(args[0]) if args else self.default_scraper
            if not scraper_name:
                self.send_message(chat_id, "Usage: /clear <scraper>")
                return
            status = get_pipeline_status(scraper_name)
            if status["state"] == "running":
                self.send_message(chat_id, "Pipeline is running; lock not cleared.")
                return
            lock_path = status.get("lock_file")
            if lock_path:
                remove_lock_file(Path(lock_path))
                self.send_message(chat_id, f"Cleared lock for {scraper_name}.")
            else:
                self.send_message(chat_id, f"No lock file found for {scraper_name}.")
            return

        self.send_message(chat_id, "Unknown command. Use /help for options.")

    def help_text(self) -> str:
        return (
            "Scraper Bot Commands:\n"
            "/list - List available scrapers\n"
            "/whoami - Show your chat ID\n"
            "/ping - Health check\n"
            "/status <scraper|all> - Check pipeline status\n"
            "/run <scraper> [fresh] - Start pipeline if idle\n"
            "/runfresh <scraper> - Start a fresh pipeline\n"
            "/clear <scraper> - Clear stale lock file\n"
        )

    def poll(self) -> None:
        while True:
            try:
                params = {"timeout": 60}
                if self.last_update_id is not None:
                    params["offset"] = self.last_update_id + 1
                response = requests.get(f"{self.base_url}/getUpdates", params=params, timeout=70)
                response.raise_for_status()
                payload = response.json()
                if not payload.get("ok"):
                    time.sleep(2)
                    continue

                for update in payload.get("result", []):
                    self.last_update_id = update.get("update_id")
                    message = update.get("message") or update.get("edited_message")
                    if not message:
                        continue
                    chat_id = message.get("chat", {}).get("id")
                    text = message.get("text", "")
                    if not chat_id or not text:
                        continue
                    if not self.is_authorized(chat_id):
                        continue
                    if text.strip().startswith("/"):
                        self.handle_command(chat_id, text)
            except Exception:
                time.sleep(3)


def main() -> None:
    load_dotenv(REPO_ROOT / ".env")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("Missing TELEGRAM_BOT_TOKEN in environment.")
        sys.exit(1)
    allowed_chat_ids = parse_allowed_chat_ids(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS"))
    default_scraper = resolve_scraper_name(os.getenv("TELEGRAM_DEFAULT_SCRAPER"))

    bot = TelegramBot(token, allowed_chat_ids, default_scraper=default_scraper)
    print("Telegram bot started. Waiting for commands...")
    bot.poll()


if __name__ == "__main__":
    main()
