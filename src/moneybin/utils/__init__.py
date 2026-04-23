"""Utility modules for MoneyBin application.

This package provides shared utilities including user configuration management
and common helper functions.
"""

from .slugify import slugify
from .user_config import (
    ensure_default_profile,
    get_default_profile,
    get_user_config_path,
    normalize_profile_name,
    set_default_profile,
)

__all__ = [
    "ensure_default_profile",
    "get_default_profile",
    "get_user_config_path",
    "normalize_profile_name",
    "set_default_profile",
    "slugify",
]
