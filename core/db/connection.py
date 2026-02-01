#!/usr/bin/env python3
"""Legacy DB connector placeholder for Malaysia workflow."""

from pathlib import Path
from typing import Any, Optional, Union

from core.db.postgres_connection import PostgresDB


class CountryDB(PostgresDB):
    """Simple wrapper that keeps legacy sqlite kwargs for compatibility."""

    def __init__(
        self,
        country: str,
        db_path: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(country)
        self.db_path = Path(db_path) if db_path is not None else None
        self._legacy_kwargs = kwargs


__all__ = ["CountryDB"]
