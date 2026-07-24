"""Consent ledger primitives: the grant mode enum and the grant record.

This module is the consent-ledger counterpart to ``redaction.py``'s
``ConsentSet`` placeholder. The enforcement gate that resolves a
``ConsentSet`` per call and withholds data is deferred (see
``docs/specs/privacy-and-ai-trust.md`` — ledger-first, gate deferred).
This module defines only the persisted record shape; ``ConsentRepo``
and ``ConsentService`` build on it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from moneybin.vocabulary import CONSENT_FEATURE_CATEGORIES

# The four starter feature categories from privacy-and-ai-trust.md §Tier 2.
# The DB column is a free string (per-tool granularity is a deferred
# enhancement); this set is the documented/validated vocabulary the
# surfaces accept.
FEATURE_CATEGORIES: frozenset[str] = CONSENT_FEATURE_CATEGORIES


class ConsentMode(StrEnum):
    """How long a consent grant lasts.

    ``PERSISTENT`` (the default) survives across sessions until revoked.
    ``ONE_TIME`` records a single authorized use. Session-bound and
    time-bound modes from the brainstorm were dropped — the spec defines
    only these two.
    """

    PERSISTENT = "persistent"
    ONE_TIME = "one-time"


@dataclass(frozen=True, slots=True)
class GrantInfo:
    """One row of ``app.ai_consent_grants`` as a typed record.

    ``revoked_at`` is None for active grants. ``grant_prompt`` (the exact
    text the user agreed to) is intentionally NOT carried here — it is
    audit-only and lives in the table + audit log; surfaces that display
    grants stay low-sensitivity by omitting it.
    """

    grant_id: str
    feature_category: str
    backend: str
    consent_mode: ConsentMode
    granted_at: datetime
    revoked_at: datetime | None
