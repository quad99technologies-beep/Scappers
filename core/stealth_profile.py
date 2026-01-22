import os

ENABLED = os.getenv("STEALTH_PROFILE_ENABLED", "false").lower() in ("1", "true", "yes", "on")


def apply_selenium(options):
    if not ENABLED:
        return
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--lang=en-US,en")
    options.add_argument("--start-maximized")


def apply_playwright(context_kwargs: dict):
    if not ENABLED:
        return
    context_kwargs.update({
        "locale": "en-US",
        "timezone_id": "Asia/Kuala_Lumpur",
        "viewport": {"width": 1366, "height": 768}
    })
