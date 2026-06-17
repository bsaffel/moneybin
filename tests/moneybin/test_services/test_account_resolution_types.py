"""Tests for AccountCandidate + AccountProposal (M1S.4 surfaced proposal types)."""

from __future__ import annotations

from moneybin.services.account_resolution_types import AccountCandidate, AccountProposal


def test_account_proposal_round_trips_to_dict() -> None:
    """A proposal with a weak candidate serialises cleanly and requires confirmation."""
    candidate = AccountCandidate(
        account_id="abc123",
        display_name="WF Checking",
        confidence=0.5,
        signal="institution_last4",
    )
    proposal = AccountProposal(
        source_account_key="wf-checking",
        proposed_account_id="def456",
        is_new=True,
        candidates=(candidate,),
        adopted_via=None,
    )
    assert proposal.requires_confirm is True
    d = proposal.to_dict()
    assert d["proposed_account_id"] == "def456"
    assert isinstance(d["candidates"], list)
    assert d["candidates"][0]["signal"] == "institution_last4"
    # to_dict must not expose ref_value / raw PII
    assert "ref_value" not in d["candidates"][0]


def test_strong_adoption_does_not_require_confirm() -> None:
    """A source_native-adopted account never surfaces for confirmation."""
    proposal = AccountProposal(
        source_account_key="plaid-tok-1",
        proposed_account_id="abc123",
        is_new=False,
        candidates=(),
        adopted_via="source_native",
    )
    assert proposal.requires_confirm is False


def test_new_standalone_requires_confirm() -> None:
    """A brand-new account (no adoption, no candidates) surfaces for confirmation."""
    proposal = AccountProposal(
        source_account_key="new-account",
        proposed_account_id="xyz789",
        is_new=True,
        candidates=(),
        adopted_via=None,
    )
    assert proposal.requires_confirm is True
