"""Content versions for immutable investment Source observations."""

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal


def _source_value(value: object) -> str:
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported observation value type: {type(value).__name__}")


def observation_version(source_type: str, values: dict[str, object]) -> str:
    """Digest source values only; callers exclude delivery metadata."""
    content = json.dumps(
        values, sort_keys=True, separators=(",", ":"), default=_source_value
    )
    return f"{source_type}_{hashlib.sha256(content.encode()).hexdigest()[:16]}"
