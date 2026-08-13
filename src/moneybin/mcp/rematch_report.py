"""Shared `actions[]` prefix for the two accept surfaces that re-run matching.

`accounts_links_set` and `identity_links_decide` trigger the same post-merge
match pass and owe the caller the same disclosure. Two copies of the wording
drifted within one PR — one grew an explanatory comment the other lacked — so
the strings live here once. Coherence rule: two patterns for the same job is
the single largest source of codebase rot (`design-principles.md`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from moneybin.services.refresh import RefreshResult


def rematch_actions(rematch: RefreshResult | None) -> list[str]:
    """Hints describing what the merge's re-match did, most urgent first.

    Empty when no pass ran — a reject repoints nothing, so there is nothing to
    disclose, which is distinct from a pass that ran and found nothing.

    The two failures are independent: `refresh()` attempts the transform step
    whether or not the match step raised, and they mean different things
    (nothing was proposed, versus the merge is not visible in `core` yet), so
    both can appear.
    """
    if rematch is None:
        return []
    actions: list[str] = []
    if rematch.matching_skipped:
        # Zero counts here mean nothing was examined, not that nothing was
        # found. Reporting "no duplicates" off them would invent a result.
        actions.append(
            "The merge's re-match could not run — its matching views were "
            "missing or stale, so the newly co-resident rows were never "
            "examined. Rerun refresh_run() and check reviews(kind='matches')"
        )
    if rematch.matching_error is not None:
        # Not "still unproposed": the matcher commits each edge as it goes and
        # opens no transaction around the run, so a crash mid-pass leaves
        # earlier tiers' decisions durable while the counts stay at zero. The
        # honest statement is that the pass stopped partway and its effects are
        # unknown — including auto-merges that already landed.
        actions.append(
            "The merge's re-match stopped partway, so its reported counts are "
            "incomplete and some duplicates may already have been merged — "
            "rerun refresh_run(steps=['match','transform']), then check "
            "reviews(kind='matches') and system_audit"
        )
    if rematch.error is not None:
        # core.dim_accounts is kind FULL, so a failed apply leaves both accounts
        # standing and any counts below describe a collapse the user cannot see.
        actions.append(
            "The merge's rebuild failed, so the collapse is not reflected in "
            "core yet — retry with refresh_run(steps=['transform'])"
        )
    if rematch.matches_pending_review:
        actions.append(
            f"The merge exposed {rematch.matches_pending_review} new duplicate "
            "proposal(s) — review with reviews(kind='matches')"
        )
    if rematch.matches_pending_transfers:
        actions.append(
            f"The merge's pass raised {rematch.matches_pending_transfers} "
            "possible transfer(s) — review with reviews(kind='matches')"
        )
    if rematch.transfers_retired:
        # The user accepted these. Deduplication made each one name a physical
        # transaction another accepted transfer already claims, which
        # bridge_transfers would double-count — but the caller has to be told a
        # decision of theirs was undone, and how to put it back.
        actions.append(
            f"The merge's pass retired {rematch.transfers_retired} previously "
            "accepted transfer(s) whose two sides turned out to be the same "
            "transaction — inspect with system_audit(), restore with "
            "system_audit_undo() if that was wrong"
        )
    return actions
