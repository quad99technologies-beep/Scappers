import os
import time
import random
from typing import Any

ENABLED = os.getenv("BROWSER_OBSERVER_ENABLED", "false").lower() in ("1", "true", "yes", "on")


class BrowserState:
    def __init__(self, backend: str, ref: Any):
        self.backend = backend
        self.ref = ref
        self.started = time.time()


def observe_selenium(driver):
    if not ENABLED:
        return BrowserState("selenium", None)
    return BrowserState("selenium", driver)


def observe_playwright(page):
    if not ENABLED:
        return BrowserState("playwright", None)
    return BrowserState("playwright", page)


def wait_until_idle(state: BrowserState, timeout: float = 10.0):
    if not ENABLED:
        return

    end = time.time() + timeout
    while time.time() < end:
        try:
            if state.backend == "selenium":
                if state.ref.execute_script("return document.readyState") == "complete":
                    break
            elif state.backend == "playwright":
                state.ref.wait_for_load_state("domcontentloaded", timeout=1000)
                break
        except Exception:
            pass
        time.sleep(0.1)

    time.sleep(random.uniform(0.2, 0.6))
