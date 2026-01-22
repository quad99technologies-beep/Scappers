#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify AI translation fallback functionality.
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

# Add repo root and script dir to path
_repo_root = Path(__file__).resolve().parents[2]
_script_dir = Path(__file__).parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))

from translation_utils import (
    enable_ai_fallback,
    load_dictionary,
    translate_value,
)


def main():
    """Test AI translation fallback with sample Russian words."""

    # Enable AI fallback
    print("=" * 80)
    print("Testing AI Translation Fallback")
    print("=" * 80)
    print()

    enable_ai_fallback()
    print()

    # Load dictionary
    dict_path = _repo_root / "input" / "Russia" / "Dictionary.csv"
    if not dict_path.exists():
        print(f"[ERROR] Dictionary not found: {dict_path}")
        return 1

    print(f"Loading dictionary: {dict_path}")
    mapping, english_set = load_dictionary(dict_path)
    print(f"Dictionary entries: {len(mapping)}")
    print()

    # Test cases: mix of dictionary words and new words
    test_cases = [
        "Амоксициллин",  # Should be in dictionary
        "Парацетамол",   # Should be in dictionary
        "Таблетки",      # Might not be in dictionary
        "Капсулы",       # Might not be in dictionary
        "Раствор для инъекций",  # Likely not in dictionary
        "Москва",        # City name, not in dictionary
        "Российская Федерация",  # Country name, not in dictionary
    ]

    miss_counter = Counter()
    miss_cols = defaultdict(set)

    print("Testing translations:")
    print("-" * 80)

    for test_word in test_cases:
        result = translate_value(
            test_word,
            mapping,
            english_set,
            "test_column",
            miss_counter,
            miss_cols,
        )

        status = "✓ DICT" if result != test_word else "✗ MISS"
        print(f"{status} | {test_word:<30} → {result}")

    print("-" * 80)
    print()

    if miss_counter:
        print(f"Still missing after AI fallback: {len(miss_counter)} words")
        for word in miss_counter:
            print(f"  - {word}")
    else:
        print("All words translated successfully!")

    print()
    print("=" * 80)
    print("Test completed")
    print("=" * 80)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
