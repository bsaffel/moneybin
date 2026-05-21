"""Package framework runtime — discovery, validation, registration.

Implements docs/specs/extension-contracts.md §"Analysis Package contract".
The public re-export surface (Capability, PackageManifest, PackageRegistry,
etc.) is wired in Task 7 once all backing modules exist. Internal helpers
like _sql_walk stay private.
"""
