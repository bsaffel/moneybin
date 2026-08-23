"""Tests for the sentence a human ratifies before an identity batch commits."""

from __future__ import annotations

from datetime import date

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app as cli_app
from moneybin.services.identity_confirmation import (
    COMPARED_LEDGER_FACTS,
    FACT_PHRASES,
    IDENTITY_BLAST_RADIUS_CATEGORIES,
    IDENTITY_BLAST_RADIUS_LABELS,
    UNDO_COMMANDS,
    AccountLedgerFacts,
    AccountMergeFacts,
    identity_confirm_message,
)
from moneybin.services.ledger_overlap import LedgerOverlap


def test_every_blast_radius_category_has_a_prompt_label() -> None:
    """Set equality, because a missing label is silent in the direction that matters.

    ``identity_confirm_message`` reports only the categories it can name. A
    category added to ``IDENTITY_BLAST_RADIUS_CATEGORIES`` without a label here
    would be counted in the digest the approval binds to and then omitted from
    the sentence the human reads — which is the same failure as counting five
    categories while moving six, one layer further out.
    """
    assert set(IDENTITY_BLAST_RADIUS_LABELS) == set(IDENTITY_BLAST_RADIUS_CATEGORIES)


def test_the_identity_prompt_counts_every_category_it_moves() -> None:
    """The registered description promises the prompt counts what it moves.

    Until now it did not: the counts lived only in the confirmation digest, which
    the human never sees, while the elicited text was fixed prose naming
    "security lots" and nothing else. A merge could move a user's hand-set
    valuations onto another instrument with no sentence anywhere saying so.
    """
    message = identity_confirm_message(
        {
            "accounts": 0,
            "merchants": 0,
            "securities": 2,
            "transactions": 7,
            "lots": 3,
            "price_marks": 4,
        },
        surface="mcp",
    )

    assert "4 price marks you set by hand" in message
    assert "7 transactions" in message
    assert "3 tax lots" in message
    assert "2 securities" in message


def test_the_identity_prompt_omits_categories_it_does_not_touch() -> None:
    """A radius padded with zeros reads as a bigger blast than it is.

    Paired with the test above: listing every category unconditionally satisfies
    that one, and would tell a user binding a price feed that the batch touches
    accounts and merchants it never opens.
    """
    message = identity_confirm_message(
        {
            "accounts": 0,
            "merchants": 0,
            "securities": 1,
            "transactions": 0,
            "lots": 0,
            "price_marks": 0,
        },
        surface="mcp",
    )

    assert "1 security" in message
    assert "account" not in message.split("This batch touches:")[-1]
    assert "tax lot" not in message.split("This batch touches:")[-1]


# ---------------------------------------------------------------------------
# The account-merge sentence
# ---------------------------------------------------------------------------


def _facts(
    account_id: str,
    *,
    display_name: str = "",
    source_types: tuple[str, ...] = (),
    subtype: str | None = None,
    currency_code: str | None = None,
    transactions: int = 0,
    first_date: date | None = None,
    last_date: date | None = None,
    last_four: str | None = None,
) -> AccountLedgerFacts:
    return AccountLedgerFacts(
        account_id=account_id,
        display_name=display_name,
        source_types=source_types,
        subtype=subtype,
        currency_code=currency_code,
        transactions=transactions,
        first_date=first_date,
        last_date=last_date,
        last_four=last_four,
    )


def _colliding_pair() -> tuple[AccountLedgerFacts, AccountLedgerFacts]:
    """Two accounts whose display labels are identical — the case that motivated this.

    Built from the shape, never from real data: one statement-derived side and
    one feed-derived side that render the same masked label, differing in source
    origin, ledger size, period, subtype, and currency.
    """
    shared_label = "Example Bank credit …0000"
    absorbed = _facts(
        "aaaaaaaaaaaa",
        display_name=shared_label,
        source_types=("pdf",),
        transactions=346,
        first_date=date(2024, 5, 1),
        last_date=date(2026, 8, 2),
    )
    survivor = _facts(
        "ssssssssssss",
        display_name=shared_label,
        source_types=("ofx",),
        subtype="credit card",
        currency_code="USD",
        transactions=2342,
        first_date=date(2019, 1, 4),
        last_date=date(2026, 8, 2),
    )
    return absorbed, survivor


def _merge(
    absorbed: AccountLedgerFacts,
    survivor: AccountLedgerFacts,
    overlap: LedgerOverlap | None = None,
) -> AccountMergeFacts:
    return AccountMergeFacts(
        absorbed=absorbed,
        survivor=survivor,
        overlap=overlap
        or LedgerOverlap(
            comparable=346,
            matched=345,
            window_start=date(2024, 5, 1),
            window_end=date(2026, 8, 2),
        ),
    )


def test_a_colliding_pair_still_renders_two_distinguishable_descriptions() -> None:
    """The split-account case is the one naming-by-last-four cannot serve.

    Both sides of the live pair rendered the same label, so a sentence built on
    it read "link your card (••NNNN) to your existing card record (••NNNN)" —
    useless in exactly the case this work exists to fix. Disambiguation has to
    come from what differs.
    """
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    sentence = message.splitlines()[0]
    described = sentence.split(" into ")
    assert len(described) == 2, sentence
    assert described[0] != described[1], sentence
    assert "pdf" in described[0].lower(), sentence
    assert "ofx" in described[1].lower(), sentence


def test_an_account_fed_by_two_sources_names_both() -> None:
    """A side that already merged two sources is described by both, joined.

    ``_source_label`` joins with ``"+"`` for exactly this case, and every other
    fixture here gives each side a single source — so the join had no coverage
    and would have rendered whatever one element it happened to pick. An
    account that already absorbed a statement archive into a feed is a normal
    survivor by the second merge, and describing it as feed-only next to a
    statement-only candidate is the label collision this sentence exists to
    avoid.
    """
    shared_label = "Example Bank credit …0000"
    absorbed = _facts("aaaaaaaaaaaa", display_name=shared_label, source_types=("pdf",))
    survivor = _facts(
        "ssssssssssss", display_name=shared_label, source_types=("pdf", "ofx")
    )

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    sentence = message.splitlines()[0]
    assert "PDF-derived" in sentence.split(" into ")[0], sentence
    assert "PDF+OFX-derived" in sentence.split(" into ")[1], sentence


def test_a_synced_account_is_named_by_its_channel_not_its_provider() -> None:
    """The sync server is opaque, and this prompt is a user-facing surface.

    ``source_type`` carries the provider slug the sync server happens to use,
    and `AGENTS.md` holds that external providers are implementation details
    hidden behind that server. Rendering it raw put the vendor's name into a
    sentence a human reads in the CLI and an agent reads over MCP — a leak with
    no user value, since the channel is what distinguishes the two sides.
    """
    shared_label = "Example Bank credit …0000"
    absorbed = _facts("aaaaaaaaaaaa", display_name=shared_label, source_types=("pdf",))
    survivor = _facts(
        "ssssssssssss", display_name=shared_label, source_types=("plaid",)
    )

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "SYNC-derived" in message
    assert "PLAID" not in message.upper()


def test_two_synced_sources_collapse_to_one_channel_label() -> None:
    """Neutralising two providers must not render the channel twice.

    ``_source_label`` joins the distinct source types, so mapping two mediated
    providers onto one label without de-duplicating would produce
    "SYNC+SYNC" — a string that leaks the provider *count* and reads as a bug.
    """
    shared_label = "Example Bank credit …0000"
    absorbed = _facts("aaaaaaaaaaaa", display_name=shared_label, source_types=("ofx",))
    survivor = _facts(
        "ssssssssssss", display_name=shared_label, source_types=("plaid", "plaid")
    )

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "SYNC-derived" in message
    assert "SYNC+SYNC" not in message


def test_the_sentence_names_the_survivor_and_the_absorbed_account() -> None:
    """A "link A to B" phrasing never says which one dies; this has to."""
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "aaaaaaaaaaaa" in message
    assert "ssssssssssss" in message
    absorbed_clause = next(
        line for line in message.splitlines() if "is folded into" in line
    )
    assert absorbed_clause.index("aaaaaaaaaaaa") < absorbed_clause.index(
        "ssssssssssss"
    ), absorbed_clause


def test_a_reversed_proposal_is_legible_as_reversed_from_the_sentence_alone() -> None:
    """A placeholder as survivor is the most expensive, least visible failure here.

    The live queue produced one: accepting would have made a malformed
    placeholder canonical and absorbed the real ledger into it. The rendered
    string has to make that readable without consulting anything else.
    """
    _, survivor = _colliding_pair()
    reversed_merge = AccountMergeFacts(
        absorbed=survivor,
        survivor=_facts("pppppppppppp", source_types=("pdf",)),
        overlap=LedgerOverlap(
            comparable=0, matched=0, window_start=None, window_end=None
        ),
    )

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2342},
        surface="mcp",
        merges=[reversed_merge],
        kinds=["account_link"],
    )

    assert "2,342 transactions" in message
    assert "no transactions of its own" in message
    absorbed_clause = next(
        line for line in message.splitlines() if "is folded into" in line
    )
    assert absorbed_clause.index("ssssssssssss") < absorbed_clause.index(
        "pppppppppppp"
    ), absorbed_clause


def test_the_sentence_carries_the_overlap_evidence() -> None:
    """The decisive fact is how much of the ledger the survivor already holds."""
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "345 of 346" in message


def test_no_comparable_period_reads_as_absent_evidence_not_as_zero_overlap() -> None:
    """Absence of evidence must not render as evidence of absence."""
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[
            _merge(
                absorbed,
                survivor,
                LedgerOverlap(
                    comparable=0, matched=0, window_start=None, window_end=None
                ),
            )
        ],
        kinds=["account_link"],
    )

    assert "0 of 0" not in message
    assert "no comparable period" in message.lower()


def test_the_account_paragraph_names_the_undo_path() -> None:
    """The undo already rides in the result envelope; the prompt never said so.

    The prompt is the one place a human decides whether the risk is acceptable,
    so the reversal belongs there rather than in the receipt afterwards.
    """
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "system_audit_undo" in message


@pytest.mark.parametrize("surface", sorted(UNDO_COMMANDS))
def test_each_surface_names_the_undo_command_that_runs_there(surface: str) -> None:
    """Derived from the mapping, so a surface added without wiring fails here.

    The rest of this sentence is shared precisely so the two surfaces cannot
    describe one merge differently. The recovery command is the exception that
    proves it: identical text would leave one of the two audiences holding an
    instruction it cannot run.
    """
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface=surface,
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert UNDO_COMMANDS[surface] in message
    for other, command in UNDO_COMMANDS.items():
        if other != surface:
            assert command not in message


def test_the_cli_prompt_names_a_command_the_cli_actually_registers() -> None:
    """The literal a CLI user would copy, resolved against the real command tree.

    Asserting the rendered string against a hand-written expectation would only
    prove the renderer agrees with the test. What makes the sentence true is that
    `moneybin system audit undo` exists — so ask the CLI, which also keeps this
    honest if the command is ever renamed.
    """
    words = UNDO_COMMANDS["cli"].split()
    assert words[0] == "moneybin"
    path = [w for w in words[1:] if not w.startswith("<")]

    result = CliRunner().invoke(cli_app, [*path, "--help"])

    assert result.exit_code == 0, result.output


def test_a_two_merge_batch_describes_both_merges_in_the_batch_order() -> None:
    """One batch can accept two account links, and both have to be legible.

    ``identity_links_decide`` takes an ordered list of decisions; nothing bounds
    it to one account merge per call. Every other test here passes a
    single-element ``merges``, which exercises the one-block case and leaves the
    per-merge mapping unpinned — a renderer that described only the first pair,
    or reordered them, would ratify a second merge the sentence never mentions.

    Order is the caller's here, unlike ``kinds``: the blocks follow the batch's
    own decision sequence so a human can read them against the call they made.
    The trailing "This batch touches" line stays singular and combined, because
    the blast radius is the batch's, not each merge's.
    """
    first_absorbed, first_survivor = _colliding_pair()
    second_absorbed = _facts(
        "bbbbbbbbbbbb",
        display_name="Example Credit Union checking …0000",
        source_types=("csv",),
        transactions=57,
        first_date=date(2025, 2, 3),
        last_date=date(2026, 7, 30),
    )
    second_survivor = _facts(
        "tttttttttttt",
        display_name="Example Credit Union checking …0000",
        source_types=("ofx",),
        transactions=612,
        first_date=date(2021, 6, 9),
        last_date=date(2026, 7, 30),
    )

    message = identity_confirm_message(
        {"accounts": 4, "transactions": 403},
        surface="mcp",
        merges=[
            _merge(first_absorbed, first_survivor),
            _merge(second_absorbed, second_survivor),
        ],
        kinds=["account_link"],
    )

    folds = [line for line in message.splitlines() if "is folded into" in line]
    assert len(folds) == 2, message
    assert "aaaaaaaaaaaa" in folds[0] and "ssssssssssss" in folds[0]
    assert "bbbbbbbbbbbb" in folds[1] and "tttttttttttt" in folds[1]
    assert message.count("This batch touches:") == 1
    assert "4 accounts, 403 transactions" in message


def test_the_paragraph_names_only_the_kinds_the_batch_contains() -> None:
    """A card-to-card account link was told about security lots and price marks."""
    absorbed, survivor = _colliding_pair()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 346},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "tax lot" not in message
    assert "merchant" not in message


def test_a_mixed_batch_names_every_kind_it_contains_in_one_sentence() -> None:
    """The multi-kind join is a real path: one batch can accept two link kinds.

    ``identity_links_decide`` takes an ordered batch, so an account link and a
    merchant link commit together routinely. Every other test here passes one
    kind, which exercises the single-clause branch and never the join — so the
    clause order and the separator between them were unpinned. Order follows the
    declared kind sequence rather than the caller's, because a human reading two
    consequences in one sentence should get them the same way every time.
    """
    message = identity_confirm_message(
        {"accounts": 2, "merchants": 3, "transactions": 346},
        surface="mcp",
        kinds=["merchant_link", "account_link"],
    )

    assert (
        "Accepted links move one account's whole transaction history onto the "
        "surviving account; merge merchant attribution onto the surviving "
        "merchant." in message
    )
    assert "tax lot" not in message


def test_a_security_batch_still_names_what_a_security_link_moves() -> None:
    """The other half of the kind-awareness boundary."""
    message = identity_confirm_message(
        {"securities": 2, "lots": 3, "price_marks": 4},
        surface="mcp",
        kinds=["security_link"],
    )

    assert "price marks you set by hand" in message
    assert "merchant" not in message


# ---------------------------------------------------------------------------
# Legibility of the merge sentence: a reviewer must be able to tell the two
# accounts apart without consulting an id. See identity_confirmation._account_label.
# ---------------------------------------------------------------------------


def _twins(
    *,
    absorbed_last_four: str | None = "0000",
    survivor_last_four: str | None = "0000",
) -> tuple[AccountLedgerFacts, AccountLedgerFacts]:
    """Two accounts alike in name AND source — the case that renders as ids today.

    ``_colliding_pair`` differs in source origin, so its two sides stayed
    distinguishable by source alone. Matching the source too is what dropped
    the old label all the way to a bare "the account".
    """
    shared = "Example Bank credit …0000"
    absorbed = _facts(
        "aaaaaaaaaaaa",
        display_name=shared,
        source_types=("ofx",),
        subtype="credit card",
        currency_code="USD",
        transactions=346,
        first_date=date(2024, 5, 1),
        last_date=date(2026, 8, 2),
        last_four=absorbed_last_four,
    )
    survivor = _facts(
        "ssssssssssss",
        display_name=shared,
        source_types=("ofx",),
        subtype="credit card",
        currency_code="USD",
        transactions=2342,
        first_date=date(2019, 1, 4),
        last_date=date(2026, 8, 2),
        last_four=survivor_last_four,
    )
    return absorbed, survivor


def test_a_shared_display_name_survives_into_both_labels() -> None:
    """The name is dropped exactly when it is the most useful thing on the line.

    ``_side_label`` discarded ``display_name`` whenever both sides carried the
    same one — which is the case the institution+last-four signal fires on, so
    the name vanished from every prompt that most needed it. "Example Bank
    credit" tells a reviewer more than "the account" even when both sides say it.
    """
    absorbed, survivor = _twins()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert message.count("Example Bank credit …0000") == 2


def test_no_side_is_described_as_the_account_when_it_has_a_name() -> None:
    """The bare fallback leaves the id as the only distinguishing text.

    A reviewer reading "Merge the account (aaaa…) into the account (ssss…)?" is
    being asked to ratify an irreversible merge from two opaque strings.
    """
    absorbed, survivor = _twins()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "the account" not in message


def test_the_name_leads_the_side_phrase_and_the_id_trails_it() -> None:
    """Name first, id last: the id is a reference, not the identity."""
    absorbed, survivor = _twins()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    question = message.splitlines()[0]
    assert question.index("Example Bank credit") < question.index("aaaaaaaaaaaa")


def test_the_prompt_offers_matching_last_fours_as_evidence() -> None:
    """The signal that fired is never shown, so the reviewer cannot weigh it.

    ``institution_last4`` is named for the last four, the prompt renders ledger
    overlap as its only evidence, and the field the proposal turned on appears
    nowhere. Agreement is the positive half of that evidence.
    """
    absorbed, survivor = _twins()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "same last four" in message
    assert "…0000" in message


def test_the_prompt_flags_last_fours_that_disagree() -> None:
    """Disagreement is evidence against the merge and has to be loud.

    ``account_resolver._last_fours_disagree`` already treats a positive
    contradiction as a veto at resolution time; a proposal that reaches review
    anyway must not hide it behind a silent omission.
    """
    absorbed, survivor = _twins(survivor_last_four="4432")

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "different last fours" in message


def test_two_accounts_alike_in_every_held_fact_say_so() -> None:
    """When only the ids differ, say that rather than let the ids imply meaning.

    The reviewer still has to decide, so the merge stays available — but a
    prompt that silently falls back to hashes reads as a rendering failure
    where this reads as the finding it is.
    """
    absorbed, survivor = (
        _facts(
            account_id,
            display_name="Example Bank credit …0000",
            source_types=("ofx",),
            subtype="credit card",
            currency_code="USD",
            transactions=346,
            first_date=date(2024, 5, 1),
            last_date=date(2026, 8, 2),
            last_four="0000",
        )
        for account_id in ("aaaaaaaaaaaa", "ssssssssssss")
    )

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 692},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "identical on every fact" in message


def test_the_compared_facts_cover_every_ledger_fact_but_the_id() -> None:
    """Set equality, because a fact left out of the comparison fails silently.

    ``_indistinguishable`` decides whether the prompt admits that only the ids
    separate two accounts. A field added to ``AccountLedgerFacts`` and forgotten
    here would be excluded from that judgement, so a genuinely distinguishable
    pair could still be declared identical — the prompt asserting a tie that its
    own rendered traits contradict.
    """
    from dataclasses import fields

    ledger_fields = {f.name for f in fields(AccountLedgerFacts)} - {"account_id"}
    assert set(COMPARED_LEDGER_FACTS) == ledger_fields


# ---------------------------------------------------------------------------
# The indistinguishable claim — only when there are facts to be identical on
# ---------------------------------------------------------------------------


def test_two_accounts_with_no_facts_are_not_called_identical_on_every_fact() -> None:
    """Holding nothing about either side is not the same as their facts matching.

    ``ledger_facts`` swallows ``CatalogException`` on both of its queries, so a
    profile with decisions but no materialized ``core`` describes both sides as
    empty — and every compared field then trivially agrees. Telling a reviewer
    the two accounts "are identical on every fact MoneyBin holds" at the moment
    MoneyBin holds none inverts the evidence in an irreversible confirm.
    """
    blank = (_facts("aaaaaaaaaaaa"), _facts("ssssssssssss"))

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 0},
        surface="mcp",
        merges=[_merge(*blank)],
        kinds=["account_link"],
    )

    assert "identical on every fact" not in message
    assert "holds no facts about either account" in message


def test_every_compared_fact_has_a_phrase_in_the_indistinguishable_sentence() -> None:
    """The sentence's field list is derived from the compared set, not retyped.

    ``COMPARED_LEDGER_FACTS`` is set-compared elsewhere so a new field cannot
    drop out of the check. That guard says nothing about the prose beside it: a
    ninth field would satisfy it while the hand-written "name, source, subtype,
    currency, date range, transaction count, and last four" silently became a
    list that omits one.
    """
    assert set(FACT_PHRASES) == set(COMPARED_LEDGER_FACTS)


def test_the_prompt_does_not_claim_the_last_four_is_why_the_proposal_fired() -> None:
    """Agreement is evidence; which signal fired is not something merge facts know.

    ``AccountMergeFacts`` carries no record of the signal, and ``account_name``
    matching can produce a pair whose last fours agree by coincidence — the
    causal sentence would then assert something the code cannot check, in the
    one place this change argues an irreversible confirm must never overstate
    its evidence.
    """
    absorbed, survivor = _twins()

    message = identity_confirm_message(
        {"accounts": 2, "transactions": 2688},
        surface="mcp",
        merges=[_merge(absorbed, survivor)],
        kinds=["account_link"],
    )

    assert "same last four" in message
    assert "the signal this proposal fired on" not in message
