"""The Python mirror of ``core.dim_accounts``'s display-name derivation.

An import has to name a freshly-minted account before any transform has run —
``import_confirm`` never refreshes at all — so the label cannot be read back
from ``core``. It is derived instead, from the same seeds the SQL model joins.
These tests pin the mirror arm by arm; ``tests/integration/
test_mint_report_names.py`` pins that the mirror and the model agree.
"""

from __future__ import annotations

import pytest

from moneybin.services.account_display_name import (
    AccountNameFacts,
    account_category,
    derive_display_name,
    derived_last_four,
    usable_source_label,
)
from moneybin.services.account_resolution_types import UNNAMED_ACCOUNT_LABEL


@pytest.mark.parametrize(
    ("institution_name", "category", "last_four", "expected"),
    [
        ("Chase", "credit card", "4242", "Chase credit card …4242"),
        ("Chase", None, "4242", "Chase …4242"),
        ("Chase", "credit card", None, "Chase credit card"),
        ("Chase", None, None, "Chase"),
        (None, "checking", "7777", "checking …7777"),
        (None, "checking", None, "checking"),
        (None, None, "7777", "…7777"),
        (None, None, None, "Unnamed account"),
    ],
)
def test_every_arm_of_the_chain_matches_the_model(
    institution_name: str | None,
    category: str | None,
    last_four: str | None,
    expected: str,
) -> None:
    """One case per COALESCE arm in ``dim_accounts.sql``, in the model's order.

    SQL ``||`` yields NULL when any operand is NULL, which is what makes the
    chain a precedence ladder rather than a set of independent fragments — each
    arm fires only when every fact it names is present.
    """
    assert (
        derive_display_name(
            institution_name=institution_name,
            category=category,
            last_four=last_four,
        )
        == expected
    )


@pytest.mark.parametrize(
    ("source_label", "expected"),
    [
        ("Everyday Spending", "Everyday Spending …4242"),
        # Already carries four digits, so it takes no more: joining "…4242" to
        # a masked number's residue would publish eight digits of one number,
        # and a year in a name reads exactly like that residue.
        ("Retirement Plan 2024 Rewards", "Retirement Plan 2024 Rewards"),
        # Masked upstream and still a name, so the rung keeps it.
        ("Checking ****1098", "Checking ****1098"),
        # A letter is a letter in every script. Both sides of the mirror tested
        # [A-Za-z], so they agreed with each other and were wrong together: a
        # label a person chose in a non-Latin script counted as "no letter" and
        # was demoted to the assembled label, discarding the name outright.
        ("储蓄账户", "储蓄账户 …4242"),
        ("Сбережения", "Сбережения …4242"),
        ("Ταμιευτήριο", "Ταμιευτήριο …4242"),
        # Still not letters, in any script: the guard has to keep holding for
        # the digits and mask characters it was written for.
        ("Ⅳ", "Chase credit card …4242"),
        # No letter: an account number under a name's column heading.
        ("****1098", "Chase credit card …4242"),
        ("987654321098", "Chase credit card …4242"),
        ("  ", "Chase credit card …4242"),
        (None, "Chase credit card …4242"),
    ],
)
def test_an_authored_label_outranks_every_assembled_name(
    source_label: str | None, expected: str
) -> None:
    """The top derived rung, and the one condition that stands it down.

    Everything below is assembled from bank fields the user never chose, and
    ``moneybin accounts`` prints institution and type in their own columns
    anyway — so a name a person wrote wins. A label with no letter is the
    account number wearing the name column's hat: masking makes it safe to show
    without making it a name, and ``****1098`` identifies the account strictly
    worse than the assembled label does.

    The label keeps the last four the rungs below it would have added, for the
    reason the next test states.
    """
    assert (
        derive_display_name(
            source_label=source_label,
            institution_name="Chase",
            category="credit card",
            last_four="4242",
        )
        == expected
    )


def test_two_accounts_sharing_one_label_are_told_apart_by_their_last_four() -> None:
    """A label is chosen, not unique — so it cannot be the whole name.

    Plaid sends the institution's own account name, and a household's two
    checking accounts routinely carry one product name. Naming both of them
    that is the exact defect this module exists to fix — a label no per-account
    fact distinguishes — and ``AccountService.resolve_strict`` raises
    ``AmbiguousAccountError`` on the duplicate, refusing a name reference that
    resolved before the rung existed.
    """
    siblings = [
        derive_display_name(
            source_label="HOUSEHOLD CHECKING",
            institution_name="Chase",
            category="checking",
            last_four=four,
        )
        for four in ("0000", "1111")
    ]

    assert len(set(siblings)) == 2, siblings


def test_a_label_that_already_shows_four_digits_is_not_given_four_more() -> None:
    """The discriminator must not become a second slice of the account number.

    ``mask_embedded_account_number`` reduces a pasted number to its last four,
    which is the whole disclosure the label is allowed. Appending the ladder's
    own last four beside that residue puts ``****5678 …9012`` on screen —
    eight digits of a twelve-digit number, in a field declared safe to show.
    """
    assert (
        derive_display_name(
            source_label="Checking ****5678",
            institution_name=None,
            category="checking",
            last_four="9012",
        )
        == "Checking ****5678"
    )


def test_a_label_alone_names_an_account_with_no_last_four() -> None:
    """The discriminator is added when there is one, never invented.

    SQL ``||`` yields NULL when any operand is NULL, so the model's last-four
    arm simply does not fire here and falls through to the bare label. An
    account whose source stated no number is named by what it does have.
    """
    assert (
        derive_display_name(
            source_label="Vacation Fund",
            institution_name="Chase",
            category="savings",
            last_four=None,
        )
        == "Vacation Fund"
    )


def test_an_authored_label_is_not_a_substitute_for_the_unnamed_terminal() -> None:
    """A label with nothing nameable in it still leaves the account unnamed.

    Guards the letter test against the reading that it merely reorders the
    ladder: standing down has to fall through to the *whole* remaining chain,
    terminal included, not to the label in a weaker position.
    """
    assert (
        derive_display_name(
            source_label="1098",
            institution_name=None,
            category=None,
            last_four=None,
        )
        == "Unnamed account"
    )


def test_a_blank_fact_is_the_same_as_a_missing_one() -> None:
    """Whitespace answers the ladder with silence, as ``NULLIF(TRIM(...), '')`` does."""
    assert (
        derive_display_name(institution_name="  ", category="", last_four=" ")
        == "Unnamed account"
    )


@pytest.mark.parametrize(
    ("source_spelling", "expected"),
    [
        ("CREDITCARD", "credit card"),
        ("CHECKING", "checking"),
        ("SAVINGS", "savings"),
        # The registry has no finer distinction than the canonical type here, so
        # the model's COALESCE(subtype, type) falls through to the type itself.
        ("DEPOSITORY", "depository"),
        ("CREDIT", "credit"),
        # Unregistered spellings keep their own word, lowercased — the staging
        # model's ELSE branch. A guess at a canonical type would be worse.
        ("Christmas Club", "christmas club"),
        ("", None),
        (None, None),
    ],
)
def test_the_category_comes_from_the_shared_type_map(
    source_spelling: str | None, expected: str | None
) -> None:
    """``seeds/account_type_map.csv`` is read directly, so no second copy can drift."""
    assert account_category(source_spelling) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4738291056474242", "4242"),
        ("****1234", "1234"),
        ("1111", "1111"),
        ("12-34", "1234"),
        # Fewer than four digits survive: the model emits NULL rather than a
        # short mask, so an alphanumeric PDF identifier never becomes a "last
        # four" that looks like a bank's.
        ("ACCT-9Z", None),
        ("", None),
        (None, None),
    ],
)
def test_the_last_four_is_digits_only_and_needs_four_of_them(
    value: str | None, expected: str | None
) -> None:
    """Mirrors the ``REGEXP_REPLACE(..., '[^0-9]', '')`` guard in every source arm."""
    assert derived_last_four(value) == expected


def test_caller_supplied_settings_reach_the_reported_name() -> None:
    """``account_metadata`` renames the account, so the report has to see it.

    ``dim_accounts`` reads ``COALESCE(s.last_four, w.last_four_derived)`` and
    ``COALESCE(s.account_subtype, w.account_subtype, ...)`` off
    ``app.account_settings``, which this import is about to write. A caller who
    passes either in ``account_metadata`` therefore changes the name the dim
    will store — and reporting the pre-override label would reopen the same
    two-readers split on the far side of a refresh that this module exists to
    close, in a quieter form no last-four assertion elsewhere would catch.

    Both fields at once, because each alone leaves the other's arm untested.
    """
    facts = AccountNameFacts(
        source_label=None,
        institution_name="Chase",
        category="depository",
        last_four="4242",
    )

    assert facts.display_name() == "Chase depository …4242"
    assert (
        facts.with_settings({
            "account_subtype": "credit card",
            "last_four": "1098",
        }).display_name()
        == "Chase credit card …1098"
    )
    # Padding a caller supplies is trimmed here and, because AccountSettings
    # now trims before the write, in app.account_settings too — the two halves
    # of one normalization. This side alone was the divergence: the model
    # COALESCEs that stored column with no TRIM.
    assert (
        facts.with_settings({
            "account_subtype": "  credit card  ",
            "last_four": "1098",
        }).display_name()
        == "Chase credit card …1098"
    )


def test_settings_that_state_nothing_leave_the_derived_facts_alone() -> None:
    """A blank override is not an override.

    ``_stated`` is what stands between an empty ``account_metadata`` cell and a
    name assembled from it. Without it a caller passing ``last_four=""`` would
    strip the last four the raw row supplied, and the account would render one
    rung lower here than ``dim_accounts`` renders it.
    """
    facts = AccountNameFacts(
        source_label=None,
        institution_name="Chase",
        category="depository",
        last_four="4242",
    )

    assert facts.with_settings(None).display_name() == "Chase depository …4242"
    assert facts.with_settings({}).display_name() == "Chase depository …4242"
    assert (
        facts.with_settings({"last_four": "  ", "account_subtype": ""}).display_name()
        == "Chase depository …4242"
    )


def test_the_unnamed_sentinel_is_not_a_usable_source_label() -> None:
    """The mirror rejects the sentinel for the reason the model does.

    ``usable_source_label`` asked only whether a label holds a letter, and the
    sentinel holds several. It is the ladder's own terminal arm, so a label
    equal to it says the source could not name the account either -- and
    ``is_a_name`` will discard it downstream, leaving the account unresolvable
    by the name it displays. Falling through to the institution-derived name is
    the better answer, and it is what the SQL arm now does too.
    """
    assert usable_source_label(UNNAMED_ACCOUNT_LABEL) is None
    assert usable_source_label(f"  {UNNAMED_ACCOUNT_LABEL}  ") is None
    # A real name that merely resembles it is still the user's name.
    assert usable_source_label("Unnamed account 2") == "Unnamed account 2"
    assert (
        derive_display_name(
            source_label=UNNAMED_ACCOUNT_LABEL,
            institution_name="Test Bank",
            category="checking",
            last_four="1098",
        )
        == "Test Bank checking …1098"
    )
