"""The best-effort refresh outcomes an embedded caller owes its user.

Its own module, and deliberately stdlib-only: the Pydantic result carriers that
embed it (``PullResult``) sit on the CLI's cold-start path, and both
``RefreshResult`` and ``RateBackfillResult`` reach polars behind them. Holding
flattened primitives keeps the type free to travel anywhere a result does.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypedDict


@dataclass(frozen=True)
class RefreshStepOutcome:
    """What the four best-effort refresh steps did, for a caller that ran them.

    ``refresh(steps=None)`` runs the whole cascade, so any command that closes
    with a refresh executes a matcher, a categorizer, an identity pass, and a
    network-touching rate backfill on the user's behalf. Each fails
    independently and routes to a *different* remedy, so these stay separate
    fields rather than one ``refresh_failed`` flag: merging any two sends the
    user to a remedy that cannot fix what actually broke.

    The SQLMesh apply step is not here. It is the only step that can hard-fail,
    every carrier already reports it as ``transforms_error``, and it gates a
    non-zero exit that these do not.
    """

    matching_error: str | None = None
    categorization_error: str | None = None
    # Only the domains that failed; an empty tuple means the pass ran clean.
    identity_errors: tuple[str, ...] = field(default_factory=tuple)
    # None means the rates step did not run — not that it ran and wrote
    # nothing. It is the only field that separates those two, because the three
    # pair tuples below are empty either way.
    rates_written: int | None = None
    rate_pairs_failed: tuple[str, ...] = field(default_factory=tuple)
    rate_pairs_unsupported: tuple[str, ...] = field(default_factory=tuple)
    rate_pairs_discarded: tuple[str, ...] = field(default_factory=tuple)
    rate_backfill_error: str | None = None

    @property
    def has_failure(self) -> bool:
        """True when any best-effort step crashed or came back short.

        Includes the three pair tuples, not just the four error strings: a
        provider that answered "no" for one pair is a gap in the user's data
        that no error string reports.
        """
        return bool(
            self.matching_error
            or self.categorization_error
            or self.identity_errors
            or self.rate_backfill_error
            or self.rate_pairs_failed
            or self.rate_pairs_unsupported
            or self.rate_pairs_discarded
        )


class RefreshStepFields(TypedDict):
    """The flattened field names, typed so ``**`` into a payload is checked.

    A bare ``dict[str, object]`` would splat into every typed payload without
    complaint and lose the one guarantee this shape exists to give: that each
    surface spells these identically and carries the same types.
    """

    matching_error: str | None
    categorization_error: str | None
    identity_errors: list[str]
    rates_written: int | None
    rate_pairs_failed: list[str]
    rate_pairs_unsupported: list[str]
    rate_pairs_discarded: list[str]
    rate_backfill_error: str | None


def refresh_steps_fields(outcome: RefreshStepOutcome | None) -> RefreshStepFields:
    """Flatten the outcome into the field names every public surface uses.

    One flattener rather than one per surface, and it lives here rather than in
    either adapter layer because both reach it: the CLI spreads it into a JSON
    envelope, MCP spreads it into a payload dataclass. Names match
    ``RefreshRunPayload`` exactly so an agent reading two surfaces learns one
    vocabulary for one outcome.

    Emitted whole even when no refresh ran, so a missing key never has to be
    told apart from a clean step — ``rates_written`` is null in that case, which
    is the one field that separates "did not run" from "ran and wrote nothing".
    """
    steps = outcome if outcome is not None else RefreshStepOutcome()
    return {
        "matching_error": steps.matching_error,
        "categorization_error": steps.categorization_error,
        "identity_errors": list(steps.identity_errors),
        "rates_written": steps.rates_written,
        "rate_pairs_failed": list(steps.rate_pairs_failed),
        "rate_pairs_unsupported": list(steps.rate_pairs_unsupported),
        "rate_pairs_discarded": list(steps.rate_pairs_discarded),
        "rate_backfill_error": steps.rate_backfill_error,
    }
