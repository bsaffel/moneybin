"""Shared staleness vocabulary for observation-based valuations.

Two domains value something they do not continuously observe: investment
holdings priced from a market close (`investments-price-feeds.md`) and physical
assets priced from a periodic appraisal (`asset-tracking.md`). Both answer the
same question — "how old is the last real observation, and is that too old to
report without a warning" — so both resolve it here rather than each growing its
own copy of the rule.

The vocabulary is three names. ``days_since_observed`` is the calendar age of
the observation a value rests on, published on the valued row. The threshold it
is judged against resolves in two tiers: a per-entity-type default, then the
domain's global config default. A per-entity override tier is specified in both
documents but deliberately unbuilt — no user has asked to grant one security a
longer leash, and the column is an additive migration whenever one does.

Staleness is informational. It never removes a value from a total, because a
figure the user can see and judge beats a hole they cannot.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

# Absorbs ordinary market closure, nothing more: markets close ~114 days a year,
# so a threshold tighter than the weekend it spans would fire on most days for
# most users and train the reader to ignore every staleness warning. 4 days
# covers a Friday close read on Tuesday after a Monday holiday; crypto trades
# continuously, so yesterday's close is already the stalest thing worth having.
# `cash` and `other` are absent on purpose — neither spec assigns them a number,
# and they fall through to the caller's global default rather than a guess.
SECURITY_TYPE_STALENESS_DAYS: Mapping[str, int] = MappingProxyType({
    "equity": 4,
    "etf": 4,
    "mutual_fund": 4,
    "bond": 4,
    "crypto": 1,
})


def resolve_threshold_days(
    entity_type: str,
    *,
    type_defaults: Mapping[str, int],
    global_default: int,
) -> int:
    """Resolve the staleness threshold in days for one entity type.

    Args:
        entity_type: The security type or asset type being valued.
        type_defaults: Per-type thresholds for the calling domain.
        global_default: Fallback for a type the table does not name.

    Returns:
        Age in days beyond which an observation is reported stale.
    """
    return type_defaults.get(entity_type, global_default)


def is_stale(days_since_observed: int | None, threshold_days: int) -> bool:
    """Whether an observation's age exceeds its threshold.

    The comparison is strictly greater-than: an observation sitting exactly on
    its threshold is still within it, matching `asset-tracking.md`'s "exceeds
    its staleness threshold". At 4 days, a Monday reading 3 days stale on an
    equity is an ordinary weekend, not a fault.

    Args:
        days_since_observed: Calendar age of the observation, or None when
            nothing was ever observed.
        threshold_days: Resolved threshold from :func:`resolve_threshold_days`.

    Returns:
        True when the observation is older than its threshold. False when
        nothing was observed at all — that is `unpriced`, a distinct status
        whose remedy is a price source, not a refresh.
    """
    if days_since_observed is None:
        return False
    return days_since_observed > threshold_days
