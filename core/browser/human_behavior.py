
import time
import random

def jitter_sleep(min_seconds: float = 0.5, max_seconds: float = 1.5):
    """Sleep for a random amount of time between min and max seconds."""
    time.sleep(random.uniform(min_seconds, max_seconds))

def human_typing(element, text: str, min_delay: float = 0.05, max_delay: float = 0.15):
    """Type text into an element with random delays between keystrokes."""
    element.clear()
    for char in text:
        element.send_keys(char)
        time.sleep(random.uniform(min_delay, max_delay))

def human_scroll(driver, min_scroll: int = 100, max_scroll: int = 400):
    """Scroll the page by a random amount."""
    scroll_amount = random.randint(min_scroll, max_scroll)
    direction = 1 if random.random() > 0.2 else -1  # Mostly scroll down
    driver.execute_script(f"window.scrollBy(0, {scroll_amount * direction});")
    jitter_sleep(0.5, 1.0)

def human_mouse_move(driver):
    """Simulate random mouse movements (requires stealth JS injection usually, this is a placeholder for CDP actions if needed)."""
    # This is often best done via CDP or specific ActionChains if strictly required.
    # For now, we leave it as a placeholder or simple no-op if not strictly implementing CDP curves here.
    pass
