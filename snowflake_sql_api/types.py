"""Result-set type coercion.

The SQL API returns every cell as a string (or JSON for structured types) plus
per-column metadata describing the Snowflake type. This module turns that into
native Python values: ``NUMBER`` -> ``int``/``Decimal``, timestamps ->
``datetime`` with the right tz handling, ``VARIANT``/``OBJECT``/``ARRAY`` ->
parsed JSON, and so on.

Scaffold only: the full coercion matrix lands in Phase 2.
"""

from __future__ import annotations

from typing import Any, Dict, List

__all__ = ["Row", "ColumnMeta", "coerce_value", "coerce_rows"]

#: A result row keyed by (uppercased) column name.
Row = Dict[str, Any]


class ColumnMeta:
    """Column metadata from the SQL API ``resultSetMetaData.rowType`` entry.

    Holds the fields coercion needs: ``name``, ``type``, ``scale``,
    ``precision``, and nullability. Populated in Phase 2.
    """

    def __init__(
        self,
        name: str,
        type: str,  # mirrors the SQL API ``rowType[].type`` field name
        *,
        scale: int | None = None,
        precision: int | None = None,
        nullable: bool = True,
    ) -> None:
        self.name = name
        self.type = type
        self.scale = scale
        self.precision = precision
        self.nullable = nullable


def coerce_value(raw: Any, column: ColumnMeta) -> Any:
    """Coerce one raw SQL API cell to its native Python value. (Phase 2.)"""
    raise NotImplementedError


def coerce_rows(raw_rows: List[List[Any]], columns: List[ColumnMeta]) -> List[Row]:
    """Coerce a partition of raw rows into a list of keyed :data:`Row`. (Phase 2.)"""
    raise NotImplementedError
