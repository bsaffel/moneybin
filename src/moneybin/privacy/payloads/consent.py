"""Typed payloads for the privacy/consent MCP + CLI surfaces.

All fields are LOW-tier: consent state is operational metadata, not
financial data. ``grant_prompt`` is deliberately omitted (audit-only;
it classifies DESCRIPTION/MEDIUM and would raise the tier).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Annotated, Any, Literal

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

    default_backend: Annotated[str | None, DataClass.INSTITUTION]
    consent_policy: Annotated[str, DataClass.TXN_TYPE]
    active_grants: list[ConsentGrantRow] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ConsentMutationPayload:
    """Result of privacy_consent_grant / privacy_consent_revoke.

    ``consent_mode`` is None on revoke (the mode lived on the grant that was
    just removed) — null is honest where an empty string would mislead.
    """

    feature_category: Annotated[str, DataClass.CATEGORY]
    backend: Annotated[str, DataClass.INSTITUTION]
    consent_mode: Annotated[str | None, DataClass.TXN_TYPE]
    action: Annotated[Literal["granted", "revoked", "noop"], DataClass.TXN_TYPE]


@dataclass(frozen=True, slots=True)
class ConsentRevokeAllPayload:
    """Result of privacy_revoke_all — count of grants revoked."""

    revoked_count: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class PrivacyLogRow:
    """One privacy log event — a consent grant/revoke or a tool_call.

    Consent events carry ``feature_category``/``backend``; ``tool_call``
    events (written by the MCP decorator and CLI render path) instead carry
    ``sensitivity``/``classes_returned``/``row_count``. Every field has a
    default so one row type renders both event shapes — absent fields stay
    empty rather than being silently dropped.
    """

    ts: Annotated[str, DataClass.TIMESTAMP_OBSERVABILITY]
    action: Annotated[str, DataClass.TXN_TYPE]
    actor: Annotated[str, DataClass.TXN_TYPE]
    feature_category: Annotated[str, DataClass.CATEGORY] = ""
    backend: Annotated[str, DataClass.INSTITUTION] = ""
    sensitivity: Annotated[str, DataClass.AGGREGATE] = ""
    row_count: Annotated[int, DataClass.AGGREGATE] = 0
    classes_returned: Annotated[list[str], DataClass.AGGREGATE] = field(
        default_factory=list
    )

    @classmethod
    def from_event(cls, event: dict[str, Any]) -> PrivacyLogRow:
        """Build a row from a raw privacy-log event dict (either event shape)."""
        return cls(
            ts=str(event.get("ts", "")),
            action=str(event.get("action", "")),
            actor=str(event.get("actor", "")),
            feature_category=str(event.get("feature_category", "")),
            backend=str(event.get("backend", "")),
            sensitivity=str(event.get("sensitivity", "")),
            row_count=int(event.get("row_count", 0) or 0),
            classes_returned=list(event.get("classes_returned") or []),
        )


@dataclass(frozen=True, slots=True)
class PrivacyLogPayload:
    """Result of privacy_log / `privacy log`."""

    events: list[PrivacyLogRow] = field(default_factory=list)
