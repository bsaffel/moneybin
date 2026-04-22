"""Deterministic slug generation for identifiers."""

import re


def slugify(name: str) -> str:
    """Generate a deterministic slug from a human-readable name.

    Lowercases, replaces non-alphanumeric runs with hyphens, strips
    leading/trailing hyphens.

    Args:
        name: Human-readable name (e.g. "Chase Checking", "My Account").

    Returns:
        Lowercase, hyphen-separated slug (e.g. "chase-checking", "my-account").
    """
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
