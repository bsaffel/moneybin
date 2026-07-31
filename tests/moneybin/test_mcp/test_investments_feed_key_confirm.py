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
    _load_pending_proposal,  # pyright: ignore[reportPrivateUsage]  # the adapter branch under test
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
