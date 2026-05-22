"""Typed payloads for the privacy/consent MCP + CLI surfaces.

All fields are LOW-tier: consent state is operational metadata, not
financial data. ``grant_prompt`` is deliberately omitted (audit-only;
it classifies DESCRIPTION/MEDIUM and would raise the tier).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated

from moneybin.privacy.taxonomy import DataClass


@dataclass(frozen=True, slots=True)
class ConsentGrantRow:
    """One active consent grant, for display."""

    feature_category: Annotated[str, DataClass.CATEGORY]
    backend: Annotated[str, DataClass.INSTITUTION]
    consent_mode: Annotated[str, DataClass.TXN_TYPE]
    granted_at: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]


@dataclass(frozen=True, slots=True)
class PrivacyStatusPayload:
    """Result of privacy_status / `privacy status`.

    ``consent_policy`` is the backend re-prompt policy (standard/strict) from
    ``AIConfig`` — distinct from each grant's per-grant ``consent_mode``
    (persistent/one-time) under ``active_grants``.
    """

    default_backend: Annotated[str, DataClass.INSTITUTION]
    consent_policy: Annotated[str, DataClass.TXN_TYPE]
    active_grants: list[ConsentGrantRow] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ConsentMutationPayload:
    """Result of privacy_grant_consent / privacy_revoke_consent.

    ``consent_mode`` is None on revoke (the mode lived on the grant that was
    just removed) — null is honest where an empty string would mislead.
    """

    feature_category: Annotated[str, DataClass.CATEGORY]
    backend: Annotated[str, DataClass.INSTITUTION]
    consent_mode: Annotated[str | None, DataClass.TXN_TYPE]
    action: Annotated[str, DataClass.TXN_TYPE]  # "granted" | "revoked" | "noop"


@dataclass(frozen=True, slots=True)
class ConsentRevokeAllPayload:
    """Result of privacy_revoke_all — count of grants revoked."""

    revoked_count: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class PrivacyLogRow:
    """One privacy log event."""

    ts: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    action: Annotated[str, DataClass.TXN_TYPE]
    actor: Annotated[str, DataClass.TXN_TYPE]
    feature_category: Annotated[str, DataClass.CATEGORY]
    backend: Annotated[str, DataClass.INSTITUTION]


@dataclass(frozen=True, slots=True)
class PrivacyLogPayload:
    """Result of privacy_log / `privacy log`."""

    events: list[PrivacyLogRow] = field(default_factory=list)
