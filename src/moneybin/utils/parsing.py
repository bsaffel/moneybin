"""Shared parsing utilities for MoneyBin."""

import re
from datetime import timedelta


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
