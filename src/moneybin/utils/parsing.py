"""Shared parsing utilities for MoneyBin."""

import re
from datetime import timedelta
from decimal import Decimal
from typing import Any


def parse_duration(duration: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h' into a timedelta.

    Args:
        duration: Duration string (e.g., "30d", "7d", "24h", "60m").

    Returns:
        timedelta for the specified duration.

    Raises:
        ValueError: If format is invalid.
    """
    match = re.match(r"^(\d+)([dhm])$", duration.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use <number><unit> "
            "where unit is d (days), h (hours), or m (minutes)."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    else:
        return timedelta(minutes=value)


def coerce_to_decimal(v: Any) -> Decimal | None:
    """Coerce mixed numeric input to Decimal; return None for empty/None.

    Strips currency formatting (``$``, ``,``) from strings before conversion.
    Used by Pydantic ``mode="before"`` validators across extractors that parse
    OFX library output, PDF text, and CSV cells — inputs may arrive as
    Decimal, int, float, str (with or without formatting), empty string, or
    None.

    Required-field validators should call this and reject ``None`` before
    returning, so the field signature stays ``Decimal``.
    """
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        return v
    if isinstance(v, (int, float, str)):
        if isinstance(v, str):
            v = v.replace(",", "").replace("$", "").strip()
            if not v:
                return None
        return Decimal(str(v))
    raise ValueError(f"Cannot convert {type(v)} to Decimal")
