"""Profile-scoped HMAC redaction key.

The key is a 32-byte secret held in the same ``SecretStore`` vault as
the DuckDB encryption key. It is never transmitted, never logged, and
never derivable from the input alone — every deterministic redaction
transform that needs cross-call stability (e.g. "same account_id
always hashes to the same prefix") must HMAC its input with this key.

Why a dedicated key rather than reusing the DB encryption key:
separation-of-concerns. Rotating the DB key must not invalidate the
historical privacy log's deterministic identifiers; rotating the
redaction key must not require re-keying the database. Per-profile
scoping prevents leak across profiles in multi-tenant deployments.

PR 2 introduces no transform that actually consumes the key — the
``****<last4>`` rule for ACCOUNT_IDENTIFIER is deterministic by
definition. PR 3's hash-placeholder for MERCHANT_NAME /
DESCRIPTION is the first real consumer. The wiring lands now so PR 3
doesn't have to add the SecretStore round-trip during the consent
schema migration.
"""

from __future__ import annotations

import secrets

from moneybin.secrets import SecretNotFoundError, SecretStore

REDACTION_KEY_NAME = "PRIVACY__REDACTION_KEY"
_KEY_BYTES = 32

_CACHE: dict[str, bytes] = {}
# Cache slot used when no profile has been resolved (single-user bootstrap path
# before the profile resolver fires). Distinct from any real profile name.
_NO_PROFILE_KEY = "__base__"


def get_redaction_key() -> bytes:
    """Return the 32-byte HMAC key for the current profile.

    Generates and stores a fresh key on first call if none exists.
    Subsequent calls return the cached value.

    Keyed by profile name: a process that resolves multiple profiles in
    its lifetime (test runs, future multi-profile CLI flows) keeps each
    profile's key distinct. A constant cache key would silently return
    the first-resolved profile's key for every other profile and cross-
    contaminate HMAC identifiers — a key-confusion defect once PR 3's
    hash-placeholder transforms land.
    """
    from moneybin.config import (  # noqa: PLC0415 — defer to avoid import cycle
        get_current_profile,
    )

    try:
        profile = get_current_profile()
    except RuntimeError:
        profile = _NO_PROFILE_KEY
    if profile in _CACHE:
        return _CACHE[profile]
    store = SecretStore()
    try:
        hex_value = store.get_key(REDACTION_KEY_NAME)
        key = bytes.fromhex(hex_value)
    except SecretNotFoundError:
        key = secrets.token_bytes(_KEY_BYTES)
        store.set_key(REDACTION_KEY_NAME, key.hex())
    _CACHE[profile] = key
    return key
