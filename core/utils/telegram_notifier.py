#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Notifier Module

Sends status updates and notifications to Telegram during scraper execution.

Usage:
    from core.utils.telegram_notifier import send_telegram_status, TelegramNotifier

    # Simple one-time notification
    send_telegram_status("NorthMacedonia", "Step 1/4 - Collecting URLs (25%)")

    # Persistent notifier with automatic rate limiting
    notifier = TelegramNotifier("NorthMacedonia")
    notifier.send_status("Step 1/4", "Collecting URLs", progress=25.0)
    notifier.send_error("Error occurred", details="Connection timeout")
    notifier.send_success("Pipeline completed", "All steps finished successfully")
"""

import os
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None


class TelegramNotifier:
    """
    Telegram notification handler with rate limiting and error handling.
    """

    # Default rate limit: minimum seconds between messages
    DEFAULT_RATE_LIMIT = 5.0

    # Maximum message length (Telegram limit is 4096)
    MAX_MESSAGE_LENGTH = 4000

    def __init__(
        self,
        scraper_name: str,
        rate_limit: float = DEFAULT_RATE_LIMIT,
        enabled: Optional[bool] = None,
    ):
        """
        Initialize Telegram notifier.

        Args:
            scraper_name: Name of the scraper (e.g., "NorthMacedonia")
            rate_limit: Minimum seconds between messages (default: 5.0)
            enabled: Override to force enable/disable (default: auto-detect from env)
        """
        self.scraper_name = scraper_name
        self.rate_limit = rate_limit
        self._last_send_time = 0

        # Load configuration from environment
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_ids = self._parse_chat_ids(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", ""))

        # Auto-detect enabled state if not explicitly set
        if enabled is None:
            self.enabled = bool(self.token and self.chat_ids and REQUESTS_AVAILABLE)
        else:
            self.enabled = enabled

        if not REQUESTS_AVAILABLE and self.enabled:
            logger.warning("Telegram notifications disabled: requests library not available")
            self.enabled = False

        if self.enabled and not self.token:
            logger.warning("Telegram notifications disabled: TELEGRAM_BOT_TOKEN not set")
            self.enabled = False

        if self.enabled and not self.chat_ids:
            logger.warning("Telegram notifications disabled: TELEGRAM_ALLOWED_CHAT_IDS not set")
            self.enabled = False

        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else ""

    @staticmethod
    def _parse_chat_ids(raw_value: str) -> list:
        """Parse comma-separated chat IDs from environment variable."""
        if not raw_value:
            return []
        ids = []
        for part in raw_value.split(","):
            part = part.strip()
            if part.lstrip("-").isdigit():
                ids.append(int(part))
        return ids

    def _should_send(self, force: bool = False) -> bool:
        """Check if enough time has passed since last message (rate limiting)."""
        if not self.enabled:
            return False
        if force:
            return True

        elapsed = time.time() - self._last_send_time
        return elapsed >= self.rate_limit

    def _send_message(self, text: str, force: bool = False) -> bool:
        """
        Send message to all configured chat IDs.

        Args:
            text: Message text
            force: Skip rate limiting

        Returns:
            True if message was sent successfully to at least one chat
        """
        if not self._should_send(force):
            return False

        # Truncate message if too long
        if len(text) > self.MAX_MESSAGE_LENGTH:
            text = text[:self.MAX_MESSAGE_LENGTH - 20] + "\n...[truncated]"

        success = False
        for chat_id in self.chat_ids:
            try:
                payload = {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                    "parse_mode": "HTML"
                }
                response = requests.post(
                    f"{self.base_url}/sendMessage",
                    data=payload,
                    timeout=10
                )
                if response.status_code == 200:
                    success = True
            except Exception as e:
                logger.debug(f"Failed to send Telegram message to {chat_id}: {e}")
                continue

        if success:
            self._last_send_time = time.time()

        return success

    def send_status(
        self,
        step: str,
        description: str,
        progress: Optional[float] = None,
        details: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        """
        Send status update.

        Args:
            step: Current step (e.g., "Step 1/4" or "Collect URLs")
            description: Brief description of current action
            progress: Optional progress percentage (0-100)
            details: Optional additional details
            force: Skip rate limiting

        Returns:
            True if message was sent

        Example:
            notifier.send_status("Step 1/4", "Collecting URLs", progress=25.0)
        """
        lines = [f"<b>{self.scraper_name}</b>"]

        if progress is not None:
            lines.append(f"{step} ({progress:.1f}%) - {description}")
        else:
            lines.append(f"{step} - {description}")

        if details:
            lines.append(f"\n{details}")

        text = "\n".join(lines)
        return self._send_message(text, force=force)

    def send_progress(
        self,
        current: int,
        total: int,
        description: str,
        details: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        """
        Send progress update with current/total format.

        Args:
            current: Current item number
            total: Total items
            description: Brief description
            details: Optional additional details
            force: Skip rate limiting

        Returns:
            True if message was sent

        Example:
            notifier.send_progress(15, 100, "Processing pages", "URLs collected: 3000")
        """
        if total > 0:
            progress = (current / total) * 100
        else:
            progress = 0

        step_text = f"{current}/{total}"
        return self.send_status(step_text, description, progress, details, force)

    def send_error(
        self,
        error_msg: str,
        details: Optional[str] = None,
        force: bool = True,
    ) -> bool:
        """
        Send error notification.

        Args:
            error_msg: Error message
            details: Optional error details
            force: Skip rate limiting (default: True for errors)

        Returns:
            True if message was sent

        Example:
            notifier.send_error("Navigation failed", "Timeout after 3 retries")
        """
        lines = [
            f"<b>❌ {self.scraper_name} - ERROR</b>",
            f"{error_msg}"
        ]

        if details:
            lines.append(f"\n{details}")

        text = "\n".join(lines)
        return self._send_message(text, force=force)

    def send_warning(
        self,
        warning_msg: str,
        details: Optional[str] = None,
        force: bool = False,
    ) -> bool:
        """
        Send warning notification.

        Args:
            warning_msg: Warning message
            details: Optional warning details
            force: Skip rate limiting

        Returns:
            True if message was sent

        Example:
            notifier.send_warning("Chrome session restarted", "Session became unresponsive")
        """
        lines = [
            f"<b>⚠️ {self.scraper_name} - WARNING</b>",
            f"{warning_msg}"
        ]

        if details:
            lines.append(f"\n{details}")

        text = "\n".join(lines)
        return self._send_message(text, force=force)

    def send_success(
        self,
        success_msg: str,
        details: Optional[str] = None,
        force: bool = True,
    ) -> bool:
        """
        Send success notification.

        Args:
            success_msg: Success message
            details: Optional success details
            force: Skip rate limiting (default: True for completion)

        Returns:
            True if message was sent

        Example:
            notifier.send_success("Pipeline completed", "Total URLs: 15000\\nTotal pages: 75")
        """
        lines = [
            f"<b>✅ {self.scraper_name} - SUCCESS</b>",
            f"{success_msg}"
        ]

        if details:
            lines.append(f"\n{details}")

        text = "\n".join(lines)
        return self._send_message(text, force=force)

    def send_started(
        self,
        description: str = "Pipeline started",
        force: bool = True,
    ) -> bool:
        """
        Send pipeline start notification.

        Args:
            description: Start description
            force: Skip rate limiting (default: True)

        Returns:
            True if message was sent
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = f"<b>🚀 {self.scraper_name} - STARTED</b>\n{description}\n\nTime: {timestamp}"
        return self._send_message(text, force=force)


# Convenience function for simple one-off notifications
def send_telegram_status(
    scraper_name: str,
    message: str,
    error: bool = False,
    force: bool = False,
) -> bool:
    """
    Send a simple status message to Telegram.

    Args:
        scraper_name: Name of the scraper
        message: Message to send
        error: Mark as error message
        force: Skip rate limiting

    Returns:
        True if message was sent

    Example:
        send_telegram_status("NorthMacedonia", "Step 1/4 - Collecting URLs (25%)")
    """
    notifier = TelegramNotifier(scraper_name)

    if error:
        return notifier.send_error(message, force=force)
    else:
        lines = [f"<b>{scraper_name}</b>", message]
        text = "\n".join(lines)
        return notifier._send_message(text, force=force)


# CLI interface for testing
if __name__ == "__main__":
    import sys

    print(f"Requests available: {REQUESTS_AVAILABLE}")
    print(f"Telegram bot token: {'set' if os.getenv('TELEGRAM_BOT_TOKEN') else 'not set'}")
    print(f"Chat IDs: {os.getenv('TELEGRAM_ALLOWED_CHAT_IDS', 'not set')}")
    print()

    if len(sys.argv) > 2:
        scraper_name = sys.argv[1]
        message = " ".join(sys.argv[2:])

        notifier = TelegramNotifier(scraper_name)

        if notifier.enabled:
            print(f"Sending test message to Telegram...")
            success = notifier.send_status("Test", message, force=True)
            if success:
                print("✓ Message sent successfully")
            else:
                print("✗ Failed to send message")
        else:
            print("✗ Telegram notifications not properly configured")
            print("  Make sure TELEGRAM_BOT_TOKEN and TELEGRAM_ALLOWED_CHAT_IDS are set in .env")
    else:
        print("Usage:")
        print("  python telegram_notifier.py <scraper_name> <message>")
        print()
        print("Example:")
        print('  python telegram_notifier.py NorthMacedonia "Test notification"')
