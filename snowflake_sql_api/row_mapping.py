"""Optional typed-row mapping.

Maps result :data:`~snowflake_sql_api.types.Row` dicts onto dataclasses or
Pydantic models. Pydantic is an optional extra (``[pydantic]``); the import is
guarded so the base install never imports it - using the feature without the
extra raises a clear error rather than failing at import time.

Scaffold only: mapping lands in Phase 8 (the v0.2.0 toolkit).
"""

from __future__ import annotations

from typing import List, Sequence, Type, TypeVar

from .types import Row

__all__ = ["map_rows"]

T = TypeVar("T")


def map_rows(rows: Sequence[Row], model: Type[T]) -> List[T]:
    """Map each row onto an instance of ``model`` (dataclass or Pydantic). (Phase 8.)"""
    raise NotImplementedError
