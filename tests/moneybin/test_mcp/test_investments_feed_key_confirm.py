"""Tests for the MCP feed-key bind confirmation.

A pending security-link decision routes two ways: a feed key BINDS a
market-data symbol, an identity ref MERGES two securities and DELETEs one. The
merge branch asks `accept_impact` what it will touch; the bind branch has no
such call — `accept_impact` raises when no accepted binding exists to move, and
a feed key has none by construction — so the adapter states that blast radius
itself.

That makes the two ends of one confirmation independently computed: the
proposal states a blast radius, and the confirm step recomputes it before
verifying the grant. A number that differs between them rejects the user's own
ratified bind as a mismatch, and the queue cannot be drained.
"""

from __future__ import annotations

from pathlib import Path

from moneybin.database import get_database
from moneybin.mcp.tools.investments import (
    _confirm_message,  # pyright: ignore[reportPrivateUsage]  # the merge prompt under test
    _load_pending_proposal,  # pyright: ignore[reportPrivateUsage]  # the adapter branch under test
    _MergeProposal,  # pyright: ignore[reportPrivateUsage]  # the prompt's input shape
    _security_link_binding,  # pyright: ignore[reportPrivateUsage]  # the grant under test
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)

_REF_KIND = "tiingo_ticker"
_REF_VALUE = "BHP"


def _mint(db: object, *, name: str) -> str:
    event = SecuritiesRepo(db).upsert(  # pyright: ignore[reportArgumentType]
        security_id=None,
        name=name,
        security_type="etf",
        created_by="user",
        actor="cli",
    )
    assert event.target_id is not None
    return event.target_id


def _queue(db: object, *, security_id: str, source_type: str) -> str:
    event = SecurityLinkDecisionsRepo(db).insert(  # pyright: ignore[reportArgumentType]
        ref_kind=_REF_KIND,
        ref_value=_REF_VALUE,
        source_type=source_type,
        candidate_security_id=security_id,
        provider_ticker=_REF_VALUE,
        provider_name="BHP Group Ltd",
        match_reason="name_divergence",
        actor="system",
    )
    assert event.target_id is not None
    return event.target_id


def test_a_feed_key_proposal_counts_only_the_decisions_accept_will_touch(
    mcp_db: Path,
) -> None:
    """Two feeds, one symbol: binding one must not claim the other's row.

    `_reject_pending_siblings` scopes to the full `(source_type, ref_kind,
    ref_value)` key so a symbol shared across providers never cross-rejects, and
    accepting the EOD decision therefore resolves exactly one decision row — its
    own. `pending()` groups by `(ref_kind, ref_value)` alone, so a proposal that
    counts its own group counts both feeds. The confirm step recomputes the
    number per-source_type, so the overstated preview is also a digest the
    ratified bind can no longer match.
    """
    with get_database(read_only=False) as db:
        security = _mint(db, name="BHP Group Ltd")
        eod_decision = _queue(db, security_id=security, source_type="tiingo")
        _queue(db, security_id=security, source_type="tiingo_iex")

    proposal = _load_pending_proposal(eod_decision)

    assert proposal.blast_radius["security_link_decisions"] == 1


def test_a_feed_key_proposal_merges_nothing_away(mcp_db: Path) -> None:
    """The bind branch reports no provisional security and one new link.

    A merge preview names the row it will DELETE. A feed-key bind has none —
    that absence is what routes it away from the merge path — so the proposal
    must say so rather than leaving a stale id in the prompt a human ratifies.
    """
    with get_database(read_only=False) as db:
        security = _mint(db, name="BHP Group Ltd")
        decision = _queue(db, security_id=security, source_type="tiingo")

    proposal = _load_pending_proposal(decision)

    assert proposal.is_feed_key is True
    assert proposal.provisional_security_id is None
    assert proposal.blast_radius["security_links"] == 1


def test_a_bind_grant_names_its_siblings_rather_than_counting_them(
    mcp_db: Path,
) -> None:
    """An equal-sized group is not the same group.

    Accepting one decision auto-rejects every sibling competing for the same
    ref. Bound to the count alone, a grant still verifies after a sibling is
    rejected and a later pull queues a different one — the total never moved —
    and the accept then rejects a decision the user was never shown. The ids
    must reach `resolved_ids`, so the two states are distinguishable.
    """
    with get_database(read_only=False) as db:
        first = _mint(db, name="BHP Group Ltd")
        second = _mint(db, name="BHP Billiton Plc")
        winner = _queue(db, security_id=first, source_type="tiingo")
        _queue(db, security_id=second, source_type="tiingo")

    before = _load_pending_proposal(winner)

    with get_database(read_only=False) as db:
        third = _mint(db, name="BHP Group Holdings")
        _queue(db, security_id=third, source_type="tiingo")

    after = _load_pending_proposal(winner)

    assert len(before.sibling_decision_ids) == 2
    assert set(before.sibling_decision_ids) < set(after.sibling_decision_ids)
    assert _binding_ids(before) != _binding_ids(after), (
        "a changed sibling group must change the binding, not just its count"
    )


def _binding_ids(proposal: _MergeProposal) -> tuple[str, ...]:
    """The resolved ids a grant for this proposal is bound to."""
    return _security_link_binding(
        decision_id=proposal.decision_id,
        candidate_security_id=proposal.candidate_security_id,
        provisional_security_id=proposal.provisional_security_id,
        blast_radius=proposal.blast_radius,
        sibling_decision_ids=proposal.sibling_decision_ids,
    ).resolved_ids


def _merge_proposal(*, marks: int) -> _MergeProposal:
    """An identity-merge proposal carrying ``marks`` price overrides to re-point."""
    return _MergeProposal(
        decision_id="dec-merge",
        ref_kind="plaid_security_id",
        ref_value="plaid-ref-1",
        provider_ticker="VTI",
        provider_name="Vanguard Total Stock Mkt",
        candidate_security_id="sec-survivor",
        candidate_ticker="VTI",
        candidate_name="Vanguard Total Stock Market ETF",
        match_reason="fuzzy_name",
        provisional_security_id="sec-provisional",
        blast_radius={
            "security_links": 1,
            "lot_selections": 0,
            "manual_investment_transactions": 0,
            "security_price_overrides": marks,
        },
        is_feed_key=False,
    )


def test_a_merge_prompt_names_the_price_marks_it_moves() -> None:
    """`accept_merge` re-points every override; the prompt named four other things.

    `_repoint_price_marks` moves the user's own valuations onto the survivor and
    `accept_impact` already counts them, but that count reaches only the
    confirmation digest. The elicited text listed provider refs, lot selections,
    manual ledger rows and the catalog deletion — so a human could ratify a merge
    without ever being told their hand-set prices were part of it.
    """
    message = _confirm_message(_merge_proposal(marks=3))

    assert "3 price marks" in message


def test_a_merge_prompt_says_so_when_no_price_mark_moves() -> None:
    """Silence reads as "not applicable", not as "none".

    Paired with the test above: a prompt that only ever appends the clause when
    the count is non-zero passes that one, and leaves a user who does keep manual
    valuations unsure whether this merge is about to touch them.
    """
    message = _confirm_message(_merge_proposal(marks=0))

    assert "no price mark" in message
