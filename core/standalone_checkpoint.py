#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Helper utilities for marking pipeline checkpoints when running individual scripts.
"""

import logging
import os
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Union

from core.pipeline_checkpoint import get_checkpoint_manager

log = logging.getLogger("standalone_checkpoint")


def _normalize_output_files(output_files: Optional[Iterable[Union[str, Path]]]) -> List[str]:
    """Convert output file paths to absolute string form."""
    normalized = []
    if not output_files:
        return normalized
    for f in output_files:
        try:
            path = Path(f)
            normalized.append(str(path))
        except Exception:
            normalized.append(str(f))
    return normalized


def mark_pipeline_step(
    scraper_name: str,
    step_number: int,
    step_name: str,
    output_files: Optional[Iterable[Union[str, Path]]] = None
) -> None:
    """Mark a pipeline step as complete unless the pipeline runner is invoking the script."""
    if os.environ.get("PIPELINE_RUNNER") == "1":
        return

    try:
        cp = get_checkpoint_manager(scraper_name)
        cp.mark_step_complete(
            step_number,
            step_name,
            output_files=_normalize_output_files(output_files)
        )
    except Exception as exc:
        log.warning(f"[CHECKPOINT] Failed to mark step {step_number} ({step_name}): {exc}")


def run_with_checkpoint(
    main_callable: Callable,
    scraper_name: str,
    step_number: int,
    step_name: str,
    output_files: Optional[Iterable[Union[str, Path]]] = None
) -> None:
    """
    Run the provided main() callable and mark pipeline step on success.

    Raises:
        Whatever exception main_callable raises.
    """
    main_callable()
    mark_pipeline_step(scraper_name, step_number, step_name, output_files=output_files)
