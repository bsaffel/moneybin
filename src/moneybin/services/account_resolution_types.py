"""Input/result types for AccountResolver (keeps the resolve() signature stable)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceAccount:
    """One source account presented to the resolver.

    ``source_account_key`` is the source's own native account key (OFX number,
    CSV slug, Plaid token) — the ``source_native`` ref_value staging joins on.
    PII fields (``account_number``) are used as scoped confirmers and never logged.
    """

    source_type: str
    source_origin: str
    source_account_key: str
    account_name: str
    account_number: str | None = None
    last_four: str | None = None
    institution: str | None = None
    persistent_token: str | None = None
    explicit_account_id: str | None = None


@dataclass(frozen=True)
class ResolvedAccount:
    """The resolver's verdict for one source account."""

    account_id: str
    """Canonical, opaque uuid4[:12] (or the pinned/adopted existing id)."""

    is_new: bool
    """True when a fresh canonical account was minted this call."""

    pending_decision_ids: tuple[str, ...] = ()
    """Decision rows written for weak (institution+last4 / name) candidates."""

    outcome: str = "minted_new"
    """One of the ACCOUNT_LINK_OUTCOMES_TOTAL result labels."""
