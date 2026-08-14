"""Reconcile accepted transfers against the dedup components that invalidate them.

Its own module because it has three callers in two layers — the matcher's run
and both service-layer accept paths — and belongs to neither. Folding it back
into either one re-creates the gap it exists to close: a single owner means
every other path that accepts a duplicate skips it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from moneybin.database import Database
from moneybin.matching.assignment import connected_components
from moneybin.matching.persistence import get_active_dedup_edges
from moneybin.metrics.registry import TRANSFER_RETIREMENTS_TOTAL
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

logger = logging.getLogger(__name__)

# The clause naming what collapsed. Two constants rather than one per trigger,
# and none of them names its trigger: the reconciliation walks every accepted
# transfer, not only the ones this call invalidated, so a count can include a
# transfer that an unrelated earlier decision broke and this call merely found.
# Saying "this decision invalidated" asserted a cause the number does not carry.
# What legitimately differs between surfaces is what *can* collapse — only a
# merge folds two accounts into one, so only its wording may say so.
#
# Here rather than in either surface's helpers, for the reason this module is
# separate at all: the sentence belongs to the event, and a copy per surface
# is how the CLI and MCP wordings drifted apart in the first place.
RETIRED_SIDES_COLLAPSED = "their two sides turned out to be one transaction"
RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED = (
    "their two sides turned out to be one transaction, "
    "or their two accounts one account"
)


class ReconciliationError(Exception):
    """A reconciliation that raised after committing some of its reversals.

    Reversals commit one at a time when the caller holds no transaction, so the
    count is durable state by the time anything can fail — but it lives in a
    local until the function returns, which an exception never lets it do. The
    matcher maps this to its own carrier; the accept paths never see it, because
    their rollback means nothing was retired.
    """

    def __init__(self, cause: BaseException, *, transfers_retired: int) -> None:
        """Wrap ``cause``, carrying the reversals that had already committed."""
        super().__init__(str(cause))
        self.transfers_retired = transfers_retired


def retire_transfers_invalidated_by_dedup(
    db: Database,
    *,
    decisions: MatchDecisionsRepo,
    actor: str = "system",
    in_outer_txn: bool = False,
) -> int:
    """Retire accepted transfers whose legs a dedup component has collapsed.

    What this protects is ``core.bridge_transfers``, which resolves each leg
    through the dedup mapping (``MAX(transaction_id)`` per group). Two transfer
    decisions whose legs landed in one component then name the same physical
    transaction, and it is double-counted by everything joining
    ``fct_transactions`` to ``bridge_transfers``. Tier 4 already refuses to
    *propose* that shape — it excludes rows in active transfers and every
    non-primary dedup member — but nothing revisited decisions made before the
    collapse. This is that missing direction.

    Every path that accepts a duplicate owes it, and they do not share a
    chokepoint: the matcher's own run auto-merges above the confidence
    threshold, an account merge makes two accounts' rows dedup candidates for
    the first time, and the review queue folds an edge the matcher proposed
    earlier. Only the first re-derives anything afterwards — ``set_status`` and
    ``accept_pending`` write the decision and return. That is not a deferral:
    ``prep.int_transactions__matched``, ``core.fct_transactions`` and
    ``core.bridge_transfers`` are all ``kind VIEW``, so the corrupt bridge is
    live on the next read whether or not a refresh ever follows.

    The invariant enforced is Tier 4's own: **a dedup component is a leg of at
    most one accepted transfer.** Decisions are walked earliest-decided first,
    so the first claimant of a component keeps it and any later decision reusing
    it is reversed; a transfer whose *own* two legs share a component is
    impossible at any ordering and always goes. Reversal (not deletion) leaves
    the row and its audit trail intact, so ``system audit undo`` can restore it.

    Deliberately global rather than scoped to any one account or decision: the
    invariant is global, and a pre-existing violation is corrupt whichever
    trigger exposed it. ``in_outer_txn`` lets a caller that already holds a
    transaction fold the reversals into it, so an accept and the retirement it
    forces commit together or not at all. The count is returned so callers can
    report it — a silent retirement of a decision the user accepted is exactly
    the unreviewed action this reconciliation exists to prevent.
    """
    edges = [
        (
            (e["source_type_a"], e["source_transaction_id_a"], e["account_id"]),
            (e["source_type_b"], e["source_transaction_id_b"], e["account_id"]),
        )
        # Accepted only. A pending dedup row is an unreviewed proposal and the
        # prep fold ignores it, so both source rows are still distinct
        # transactions in core and neither transfer is invalid yet. Reversing
        # one on that signal would undo a decision the user made on the strength
        # of a merge that has not happened — and may never, if they reject the
        # proposal.
        for e in get_active_dedup_edges(db, statuses=("accepted",))
    ]
    component: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    for members in connected_components(edges):
        root = min(members)
        for node in members:
            component[node] = root

    rows = db.execute(
        f"""
        SELECT match_id, source_type_a, source_transaction_id_a, account_id,
               source_type_b, source_transaction_id_b, account_id_b
        FROM {MATCH_DECISIONS.full_name}
        WHERE match_type = 'transfer' AND match_status = 'accepted'
        ORDER BY decided_at, match_id
        """  # noqa: S608 — TableRef constant, no interpolated values
    ).fetchall()

    claimed: set[tuple[str, str, str]] = set()
    retired = 0
    for match_id, type_a, stid_a, acct_a, type_b, stid_b, acct_b in rows:
        node_a = (type_a, stid_a, acct_a)
        # account_id_b is NULL on a same-account transfer; the leg still belongs
        # to account_id, and reading NULL as a distinct account would put the two
        # legs in different components and hide a genuine self-collapse.
        node_b = (type_b, stid_b, acct_b if acct_b is not None else acct_a)
        comp_a = component.get(node_a, node_a)
        comp_b = component.get(node_b, node_b)
        if comp_a != comp_b and comp_a not in claimed and comp_b not in claimed:
            claimed.update((comp_a, comp_b))
            continue
        try:
            decisions.reverse(
                match_id,
                reversed_by="system",
                actor=actor,
                in_outer_txn=in_outer_txn,
            )
        except Exception as exc:
            # `retired` is a local, so without this the reversals already made
            # die with the exception and the caller reports zero. Only when the
            # caller holds no transaction: with `in_outer_txn` its rollback
            # restores every one of them, and a count there would name as undone
            # a decision that is still standing.
            if in_outer_txn:
                raise
            raise ReconciliationError(exc, transfers_retired=retired) from exc
        retired += 1
        # Per reversal, not once per call: the counter has to survive the
        # exception above, which returns through ReconciliationError rather than
        # reaching the summary below.
        TRANSFER_RETIREMENTS_TOTAL.labels(cause="dedup_component").inc()
    if retired:
        logger.info(f"Retired {retired} transfer decision(s) invalidated by dedup")
    return retired
