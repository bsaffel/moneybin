"""Tests for AccountCandidate + AccountProposal (M1S.4 surfaced proposal types)."""

from __future__ import annotations

from moneybin.services.account_resolution_types import (
    UNNAMED_ACCOUNT_LABEL,
    AccountCandidate,
    AccountProposal,
    resolvable_account_name,
)


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
    d = proposal.to_dict(proposal_ref="@0")
    assert d["proposed_account_id"] == "def456"
    assert isinstance(d["candidates"], list)
    assert d["candidates"][0]["signal"] == "institution_last4"
    # to_dict must not expose ref_value / raw PII
    assert "ref_value" not in d["candidates"][0]
    # The caller's positional referent rides through unchanged — it is the only
    # key in the payload a masking surface leaves readable.
    assert d["proposal_ref"] == "@0"


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


def test_a_mint_with_nothing_to_merge_into_does_not_require_confirm() -> None:
    """A brand-new account with no candidates proceeds; the mint is reported, not gated.

    Candidates are what make a proposal a *decision*: they mean the import is
    about to adopt or merge onto an account that already exists, on a weak
    signal, where a wrong answer is both hard to notice and hard to undo. With
    no candidates there is nothing to merge into and no second answer available
    — on a fresh database "new" is the only thing the user could say. Gating it
    stopped a first import of N files on N confirms that each had one legal
    answer, which is confirmation volume scaling with items instead of with
    uncertainty.

    The mint stays visible: the import reports the accounts it created. What it
    no longer does is block on them ("magic stays visible" calibrates to the
    cost of a wrong silent action, and a surprise account is cheap and
    self-evident next to a silent merge).
    """
    proposal = AccountProposal(
        source_account_key="new-account",
        proposed_account_id="xyz789",
        is_new=True,
        candidates=(),
        adopted_via=None,
    )
    assert proposal.requires_confirm is False


def test_a_source_with_no_identity_signal_requires_confirm_with_no_candidates() -> None:
    """A bare CSV asks even on an empty database, where the pick-list is empty.

    The other mint cases carry an identity the file actually stated — an OFX
    ``<ACCTID>``, a statement's issuer and last four — so minting is a faithful
    record of what arrived. A bare Date/Description/Amount CSV states nothing,
    and the name it would mint under is its own filename. That is a guess, and
    an unanswered guess is uncertainty, which is what a confirm is for.

    Candidates alone can't express this: on a first import there are no existing
    accounts to offer, so the pick-list is empty and the proposal would look
    identical to a confident mint.
    """
    proposal = AccountProposal(
        source_account_key="standard",
        proposed_account_id="xyz789",
        is_new=True,
        candidates=(),
        adopted_via=None,
        identity_unknown=True,
    )
    assert proposal.requires_confirm is True


def test_a_mint_with_a_candidate_still_requires_confirm() -> None:
    """Existing accounts it could plausibly be is exactly what makes it a question."""
    proposal = AccountProposal(
        source_account_key="new-account",
        proposed_account_id="xyz789",
        is_new=True,
        candidates=(
            AccountCandidate(
                account_id="abc123",
                display_name="WF Checking",
                confidence=0.5,
                signal="institution_last4",
            ),
        ),
        adopted_via=None,
    )
    assert proposal.requires_confirm is True


def test_the_sentinel_is_not_a_resolvable_account_name() -> None:
    """An account nothing could name answers to its id, never to the placeholder.

    Every unnameable account carries the identical label, so leaving it in the
    candidate's name slot reports an exact match on a string that tells two
    accounts apart not at all.
    """
    assert resolvable_account_name(UNNAMED_ACCOUNT_LABEL, "acct_x") == "acct_x"


def test_an_absent_name_still_falls_back_to_the_id() -> None:
    """The pre-existing ``display_name or account_id`` fallback is preserved."""
    assert resolvable_account_name(None, "acct_x") == "acct_x"
    assert resolvable_account_name("", "acct_x") == "acct_x"


def test_a_real_name_is_left_alone() -> None:
    """A name the user or the dim actually produced stays the resolvable name."""
    assert resolvable_account_name("Chase Checking", "acct_x") == "Chase Checking"
