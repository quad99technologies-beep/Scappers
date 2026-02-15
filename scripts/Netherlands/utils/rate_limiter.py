"""
VPN-aware rate limiter - respects 5-minute IP rotation cycles.
"""

import time
import asyncio
from datetime import datetime


class RateLimiter:
    """
    Rate limiter that works with VPN rotation cycles.

    Features:
    - Tracks requests per VPN rotation cycle (default: 5 minutes)
    - Auto-pauses when cycle limit reached
    - Waits for VPN rotation before continuing
    - Handles 429 errors with exponential backoff
    """

    def __init__(
        self,
        vpn_rotation_minutes: int = 5,
        max_requests_per_cycle: int = 500,
        safety_margin_seconds: int = 10
    ):
        """
        Initialize rate limiter.

        Args:
            vpn_rotation_minutes: Minutes between VPN IP rotations (default: 5)
            max_requests_per_cycle: Max requests allowed per rotation (default: 500)
            safety_margin_seconds: Extra wait before cycle end (default: 10s)
        """
        self.vpn_rotation_interval = vpn_rotation_minutes * 60
        self.max_requests_per_cycle = max_requests_per_cycle
        self.safety_margin = safety_margin_seconds

        self.cycle_start_time = time.time()
        self.requests_this_cycle = 0
        self.total_requests = 0
        self.pauses = 0

    async def wait_if_needed(self) -> None:
        """
        Wait if we've hit the request limit for this cycle.
        Returns immediately if we have quota remaining.
        """
        # Check if we've hit the limit
        if self.requests_this_cycle >= self.max_requests_per_cycle:
            # Calculate time until next rotation
            elapsed = time.time() - self.cycle_start_time
            wait_time = self.vpn_rotation_interval - elapsed + self.safety_margin

            if wait_time > 0:
                self.pauses += 1
                print(f"\n[RATE LIMIT] Hit limit ({self.max_requests_per_cycle} requests)")
                print(f"[RATE LIMIT] Waiting {wait_time:.0f}s for VPN rotation...")
                print(f"[RATE LIMIT] Time: {datetime.now().strftime('%H:%M:%S')}")

                await asyncio.sleep(wait_time)

            # Reset counters for new cycle
            self.cycle_start_time = time.time()
            self.requests_this_cycle = 0
            print(f"[RATE LIMIT] New cycle started at {datetime.now().strftime('%H:%M:%S')}")

        # Increment request counter
        self.requests_this_cycle += 1
        self.total_requests += 1

    async def handle_429(self, retry_count: int = 0) -> None:
        """
        Handle 429 Too Many Requests error.

        Args:
            retry_count: Current retry attempt (for exponential backoff)
        """
        # Exponential backoff: 60s, 120s, 300s (5 min)
        wait_times = [60, 120, 300]
        wait_time = wait_times[min(retry_count, len(wait_times) - 1)]

        print(f"\n[429 ERROR] Too many requests detected")
        print(f"[429 ERROR] Waiting {wait_time}s (attempt {retry_count + 1})")
        print(f"[429 ERROR] Time: {datetime.now().strftime('%H:%M:%S')}")

        await asyncio.sleep(wait_time)

        # Reset cycle (we may have rotated IP during wait)
        self.cycle_start_time = time.time()
        self.requests_this_cycle = 0

    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.

        Returns:
            Dict with stats (total_requests, cycle_requests, pauses, etc.)
        """
        elapsed = time.time() - self.cycle_start_time
        time_remaining = max(0, self.vpn_rotation_interval - elapsed)
        quota_remaining = max(0, self.max_requests_per_cycle - self.requests_this_cycle)

        return {
            "total_requests": self.total_requests,
            "cycle_requests": self.requests_this_cycle,
            "quota_remaining": quota_remaining,
            "time_remaining_sec": time_remaining,
            "pauses": self.pauses,
            "requests_per_min": (
                self.requests_this_cycle / (elapsed / 60) if elapsed > 0 else 0
            ),
        }

    def __repr__(self):
        stats = self.get_stats()
        return (
            f"RateLimiter(total={stats['total_requests']}, "
            f"cycle={stats['cycle_requests']}/{self.max_requests_per_cycle}, "
            f"quota={stats['quota_remaining']}, "
            f"pauses={stats['pauses']})"
        )
