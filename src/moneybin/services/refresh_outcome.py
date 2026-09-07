"""The best-effort refresh outcomes an embedded caller owes its user.

Its own module, and deliberately stdlib-only: the Pydantic result carriers that
embed it (``PullResult``) sit on the CLI's cold-start path, and both
``RefreshResult`` and ``RateBackfillResult`` reach polars behind them. Holding
plain dataclasses keeps the type free to travel anywhere a result does.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True)
class StageOutcome:
    """What one refresh step did, in the vocabulary that step measures in.

    ``counts`` is a per-step mapping rather than a fixed set of fields because
    the six steps measure different things — rows pulled, pairs written,
    transactions categorized. Flattening them into one namespace gives a key
    set where which step a key belongs to is implied only by its prefix.

    ``ran`` separates a step that executed and found nothing from one the
    caller never asked for. Both leave every count at zero, and requirement 18
    needs a note on the first and silence on the second.
    """

    step: str
    ran: bool
    # Declared ``Mapping`` so a reader cannot write through it, but a plain
    # dict at runtime rather than a ``MappingProxyType``: ``PullResult`` embeds
    # this carrier, and Pydantic serializes a mappingproxy straight through —
    # ``model_dump`` warns and hands back a value ``json.dumps`` then refuses
    # with ``Object of type mappingproxy is not JSON serializable``. The
    # read-only *view* bought a guard against in-place mutation that the
    # declared type already discourages; it cost a break on a carrier that
    # crosses the wire. ``default_factory`` keeps each instance its own dict,
    # which is the actual hazard a bare ``{}`` default would have.
    counts: Mapping[str, int] = field(default_factory=dict)
    error: str | None = None

    def count(self, key: str) -> int:
        """One of this step's counts, or 0 when it did not report that key.

        The one way to read a count, so no consumer indexes ``counts`` directly
        and takes a ``KeyError`` off a step that declined to run — a skipped
        step carries no counts at all. Check ``ran`` before reading this 0 as a
        finding: "examined nothing" and "examined rows and found none" are the
        same 0 here, and only ``ran`` separates them.
        """
        return self.counts.get(key, 0)


def find_stage(stages: Sequence[StageOutcome], step: str) -> StageOutcome | None:
    """The outcome for one step, or ``None`` when it was never asked for.

    A present entry with ``ran=False`` means the step was requested and declined
    to run (a missing-view precondition, say); ``None`` means the caller
    narrowed ``steps`` and never included it at all. Both carriers that hold
    stages read them through here so the two surfaces cannot answer differently.
    """
    return next((s for s in stages if s.step == step), None)


# The SQLMesh apply. Named here rather than beside `CANONICAL_STEPS` in
# `orchestration.refresh` because this module is the leaf both carriers share
# and cannot import back up.
APPLY_STEP = "transform"


def best_effort(stages: Sequence[StageOutcome]) -> tuple[StageOutcome, ...]:
    """The stages whose failure degrades a run without failing it.

    Every stage except the apply. The apply rides in ``stages`` so a renderer
    has an entry to name for the one step every full refresh runs, but it
    answers a different question than the rest: it is the only step that can
    hard-fail, every carrier reports it separately as ``transforms_error``, and
    it gates a non-zero exit the best-effort steps do not. A caller asking "did
    something degrade that I can offer a retry for?" and counting the apply
    answers yes for a blocker, and sends the user to a retry instead of a fix.

    One function rather than an ``if step != …`` at each reader, because the
    two readers had already drifted once: this is the sort of filter that gets
    dropped as redundant by whoever touches only one of them.
    """
    return tuple(s for s in stages if s.step != APPLY_STEP)


@dataclass(frozen=True)
class RefreshStepOutcome:
    """What the best-effort refresh steps did, for a caller that ran them.

    ``refresh(steps=None)`` runs the whole cascade, so any command that closes
    with a refresh executes a matcher, a categorizer, an identity pass, and a
    network-touching rate backfill on the user's behalf. Each fails
    independently and routes to a *different* remedy, which is what ``stages``
    preserves: one entry per step, carrying that step's own error and its own
    counts. A single ``refresh_failed`` flag would send the user to a remedy
    that cannot fix what actually broke.

    The three pair tuples stay out of ``stages`` because they are not counts.
    They name the currency pairs a retry will never fill, which is what routes
    the user to ``moneybin fx set`` — a different question from how many rates
    were written. ``identity_errors`` stays out for the matching reason: it
    names *which* domains failed, and one stage ``error`` string cannot carry
    two domains that failed independently.

    The SQLMesh apply step *is* in ``stages`` — a renderer needs an entry to
    name for it — but it is not a best-effort step, so every question here
    about degradation reads :func:`best_effort` rather than ``stages`` whole.
    """

    # What each step that ran actually did, in canonical order.
    stages: tuple[StageOutcome, ...] = field(default_factory=tuple)
    # Only the domains that failed; an empty tuple means the pass ran clean.
    identity_errors: tuple[str, ...] = field(default_factory=tuple)
    rate_pairs_failed: tuple[str, ...] = field(default_factory=tuple)
    rate_pairs_unsupported: tuple[str, ...] = field(default_factory=tuple)
    rate_pairs_discarded: tuple[str, ...] = field(default_factory=tuple)

    def stage(self, step: str) -> StageOutcome | None:
        """This outcome's entry for one step. See :func:`find_stage`."""
        return find_stage(self.stages, step)

    @property
    def has_failure(self) -> bool:
        """True when any best-effort step crashed or came back short.

        Includes the three pair tuples, not just the per-stage errors: a
        provider that answered "no" for one pair is a gap in the user's data
        that no error string reports.
        """
        return bool(
            any(s.error for s in best_effort(self.stages))
            or self.identity_errors
            or self.rate_pairs_failed
            or self.rate_pairs_unsupported
            or self.rate_pairs_discarded
        )
