"""Shared `actions[]` prefix for the two accept surfaces that re-run matching.

`accounts_links_set` and `identity_links_decide` trigger the same post-merge
match pass and owe the caller the same disclosure. Two copies of the wording
drifted within one PR — one grew an explanatory comment the other lacked — so
the strings live here once. Coherence rule: two patterns for the same job is
the single largest source of codebase rot (`design-principles.md`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from moneybin.matching.reconciliation import RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED

if TYPE_CHECKING:
    from moneybin.orchestration.refresh import RefreshResult


def retired_transfers_action(count: int, *, operation: str) -> str | None:
    """Hint that ``operation``'s refresh reversed ``count`` accepted transfers.

    Every surface that runs the full refresh reaches the reconciliation, so
    every one of them can undo a decision the user made — an import, a sync
    pull, and the inbox drain as much as `refresh_run`. They owe the caller the
    same fact and the same route back, so the sentence lives here once rather
    than being retyped per tool; ``operation`` names the surface, which is the
    only part that differs. None on zero, so callers append nothing and the
    hint keeps its meaning.
    """
    if not count:
        return None
    return (
        f"This {operation} retired {count} previously accepted transfer(s) — "
        "inspect with system_audit(), restore with system_audit_undo() if "
        "that was wrong"
    )


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
        # earlier tiers' decisions durable. `MatchRunError` carries those counts
        # and `refresh` copies them onto the result, so this names them instead
        # of calling the effects unknown. Zero is trustworthy too: a run that
        # committed nothing raises unwrapped and never populates them. Kept in
        # step with the CLI twin in cli/commands/accounts/links.py.
        landed = ", ".join(
            f"{count} {noun}"
            for count, noun in (
                (rematch.matches_auto_merged, "auto-merged"),
                (rematch.matches_pending_review, "new duplicate proposal(s)"),
                (rematch.matches_pending_transfers, "possible transfer(s)"),
            )
            if count
        )
        committed = (
            f"after committing {landed}, which are durable"
            if landed
            else "before it had committed anything"
        )
        actions.append(
            f"The merge's re-match stopped partway {committed}; its remaining "
            "counts are incomplete — rerun "
            "refresh_run(steps=['match','transform']), then check "
            "reviews(kind='matches') and system_audit"
        )
    if rematch.error is not None:
        # core.dim_accounts is kind FULL, so a failed apply leaves both accounts
        # standing and any counts below describe a collapse the user cannot see.
        actions.append(
            "The merge's rebuild failed, so the collapse is not reflected in "
            "core yet — retry with refresh_run(steps=['transform'])"
        )
    # Suppressed on the crash branch, which already named these exact counts
    # and already sent the caller to `reviews(kind='matches')`. Repeating them
    # in a sentence that reads as a finished pass works against the one above
    # saying the run's remaining counts are incomplete. The CLI twin gets this
    # from its if/elif/else; here the branches stay independent because a
    # failed match and a failed transform can both be true at once.
    partial = rematch.matching_error is not None
    if rematch.matches_pending_review and not partial:
        actions.append(
            f"The merge exposed {rematch.matches_pending_review} new duplicate "
            "proposal(s) — review with reviews(kind='matches')"
        )
    if rematch.matches_pending_transfers and not partial:
        actions.append(
            f"The merge's pass raised {rematch.matches_pending_transfers} "
            "possible transfer(s) — review with reviews(kind='matches')"
        )
    if rematch.transfers_retired:
        # The user accepted these, and two things can invalidate them: dedup
        # made a leg name a physical transaction another accepted transfer
        # already claims (bridge_transfers would double-count), or the collapse
        # made both endpoints one account (a transfer to itself). One counter,
        # because the caller is owed one fact either way — a decision of theirs
        # was undone — and one route back. The sentence says the merge *retired*
        # them, not that it invalidated them: the pass walks every accepted
        # transfer, so a count can include one an earlier decision broke. The
        # clause itself is the constant the CLI warning uses, so the two
        # surfaces cannot describe the same event differently.
        actions.append(
            f"The merge retired {rematch.transfers_retired} previously "
            f"accepted transfer(s) — {RETIRED_SIDES_OR_ACCOUNTS_COLLAPSED} — "
            "inspect with system_audit(), restore with system_audit_undo() if "
            "that was wrong"
        )
    return actions
