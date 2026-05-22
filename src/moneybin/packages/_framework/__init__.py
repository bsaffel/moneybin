"""Package framework runtime — discovery, validation, registration.

Implements docs/specs/extension-contracts.md §"Analysis Package contract".
The public surface re-exported here is the API other modules consume;
internal helpers like _sql_walk stay private.
"""

from moneybin.packages._framework.capabilities import Capability
from moneybin.packages._framework.discovery import PackageInfo, discover_packages
from moneybin.packages._framework.errors import (
    CapabilityViolation,
    PrefixViolation,
    QualityScaleViolation,
    ValidationError,
)
from moneybin.packages._framework.manifest import PackageManifest
from moneybin.packages._framework.registry import (
    PackageRegistry,
    get_registry,
    register_package,
    validate_package,
    validate_quality_scale,
)

__all__ = [
    "Capability",
    "CapabilityViolation",
    "PackageInfo",
    "PackageManifest",
    "PackageRegistry",
    "PrefixViolation",
    "QualityScaleViolation",
    "ValidationError",
    "discover_packages",
    "get_registry",
    "register_package",
    "validate_package",
    "validate_quality_scale",
]
