import os
import time
import random

ENABLED = os.getenv("HUMAN_ACTIONS_ENABLED", "false").lower() in ("1", "true", "yes", "on")


def pause(min_s=0.2, max_s=0.6):
    if not ENABLED:
        return
    time.sleep(random.uniform(min_s, max_s))


def type_delay():
    if not ENABLED:
        return 0
    return random.uniform(0.05, 0.15)
