"""Parameter binding.

The SQL API binds values server-side via the ``bindings`` object on the
statement request: each parameter is sent as ``{"type": ..., "value": ...}``
keyed by its 1-based position. This module converts Python values into that
wire shape so helpers never interpolate user values into SQL text.

Scaffold only: the value-to-binding mapping lands in Phase 2.
"""

from __future__ import annotations

from typing import Any, Dict, Sequence

__all__ = ["BindingValue", "to_bindings"]

#: One entry of the SQL API ``bindings`` map.
BindingValue = Dict[str, Any]


def to_bindings(params: Sequence[Any]) -> Dict[str, BindingValue]:
    """Convert positional params into the SQL API ``bindings`` map.

    Keys are 1-based string positions (``"1"``, ``"2"``, ...). Each Python
    value maps to a ``{"type", "value"}`` pair with the correct SQL API binding
    type. Implemented in Phase 2.
    """
    raise NotImplementedError
