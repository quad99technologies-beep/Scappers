#!/usr/bin/env python3
"""
Legacy wrapper for Step 2 (Product Details).
"""

from pathlib import Path
import runpy


def main() -> None:
    step_path = Path(__file__).resolve().parent / "steps" / "step_02_product_details.py"
    runpy.run_path(str(step_path), run_name="__main__")


if __name__ == "__main__":
    main()
