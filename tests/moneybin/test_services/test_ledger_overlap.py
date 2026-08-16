"""Tests for the ledger-overlap probe.

The probe answers one question a reviewer cannot answer from a signal label:
do these two accounts hold the same transactions? Fixtures are built from the
*shape* — two ledgers over a shared period, one carrying the same amounts and
one carrying different ones — never from real institution or account data.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.services.ledger_overlap import (
    IncomingTransaction,
    LedgerOverlap,
    probe_incoming_ledger_overlap,
    probe_ledger_overlap,
)
from tests.moneybin.db_helpers import create_core_tables

_TWIN = "twin_acct0001"
_SURVIVOR = "surv_acct0001"


def _probes(result: str) -> float:
    """Public read of the probe counter — no private attribute access."""
    return (
        REGISTRY.get_sample_value(
            "moneybin_account_link_overlap_probes_total", {"result": result}
        )
        or 0.0
    )


def _insert_txn(
    db: Database,
    *,
    account_id: str,
    txn_date: date,
    amount: str,
    suffix: str = "",
    currency: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount, currency_code) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            f"{account_id}-{txn_date.isoformat()}-{amount}{suffix}",
            account_id,
            txn_date,
            amount,
            currency,
        ],
    )


@pytest.fixture()
def core_db(db: Database) -> Database:
    """Database with the core tables the probe reads."""
    create_core_tables(db)
    return db


def test_a_true_twin_matches_every_row(core_db: Database) -> None:
    """Same amounts over the same period read as full overlap."""
    for day, amount in ((3, "-12.00"), (7, "-40.50"), (11, "-8.25")):
        _insert_txn(
            core_db, account_id=_TWIN, txn_date=date(2026, 5, day), amount=amount
        )
        _insert_txn(
            core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, day), amount=amount
        )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert overlap == LedgerOverlap(
        comparable=3,
        matched=3,
        window_start=date(2026, 5, 3),
        window_end=date(2026, 5, 11),
    )


def test_incoming_statement_matches_existing_account_within_posting_window(
    core_db: Database,
) -> None:
    _insert_txn(
        core_db,
        account_id=_SURVIVOR,
        txn_date=date(2026, 5, 4),
        amount="-12.00",
        currency="USD",
    )

    overlap = probe_incoming_ledger_overlap(
        core_db,
        transactions=(
            IncomingTransaction(
                transaction_date=date(2026, 5, 1),
                amount=Decimal("-12.00"),
                currency_code="usd",
            ),
            IncomingTransaction(
                transaction_date=date(2026, 5, 20),
                amount=Decimal("-99.00"),
                currency_code="USD",
            ),
        ),
        against_account_id=_SURVIVOR,
    )

    assert overlap == LedgerOverlap(
        comparable=1,
        matched=1,
        window_start=date(2026, 5, 1),
        window_end=date(2026, 5, 1),
    )


def test_incoming_unstated_currency_matches_existing_stated_currency(
    core_db: Database,
) -> None:
    _insert_txn(
        core_db,
        account_id=_SURVIVOR,
        txn_date=date(2026, 5, 4),
        amount="-12.00",
        currency="USD",
    )

    overlap = probe_incoming_ledger_overlap(
        core_db,
        transactions=(
            IncomingTransaction(
                transaction_date=date(2026, 5, 4),
                amount=Decimal("-12.00"),
                currency_code=None,
            ),
        ),
        against_account_id=_SURVIVOR,
    )

    assert (overlap.comparable, overlap.matched) == (1, 1)


def test_incoming_stated_currency_does_not_match_different_stated_currency(
    core_db: Database,
) -> None:
    _insert_txn(
        core_db,
        account_id=_SURVIVOR,
        txn_date=date(2026, 5, 4),
        amount="-12.00",
        currency="EUR",
    )

    overlap = probe_incoming_ledger_overlap(
        core_db,
        transactions=(
            IncomingTransaction(
                transaction_date=date(2026, 5, 4),
                amount=Decimal("-12.00"),
                currency_code="USD",
            ),
        ),
        against_account_id=_SURVIVOR,
    )

    assert (overlap.comparable, overlap.matched) == (1, 0)


def test_a_control_over_the_same_period_matches_nothing(core_db: Database) -> None:
    """Different amounts over the same period read as no overlap, not as absent evidence."""
    for day, amount in ((3, "-12.00"), (7, "-40.50"), (11, "-8.25")):
        _insert_txn(
            core_db, account_id=_TWIN, txn_date=date(2026, 5, day), amount=amount
        )
    for day, amount in ((3, "-99.00"), (7, "-1.05"), (11, "-77.25")):
        _insert_txn(
            core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, day), amount=amount
        )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert overlap.comparable == 3
    assert overlap.matched == 0


def test_equal_amounts_in_different_currencies_are_not_the_same_transaction(
    core_db: Database,
) -> None:
    """A nominal amount is not a sum of money until a currency names it.

    ``core.fct_transactions.currency_code`` is populated precisely because two
    accounts can differ on it — the transaction's own captured currency, else
    inherited from ``core.dim_accounts``. A multi-currency institution's USD
    checking and EUR savings can be proposed together by the name signal, and
    without this predicate their nominally equal rows would read as a full-
    overlap twin: the strongest possible evidence for a merge of two accounts
    that provably hold different money.
    """
    for day, amount in ((3, "-12.00"), (7, "-40.50"), (11, "-8.25")):
        _insert_txn(
            core_db,
            account_id=_TWIN,
            txn_date=date(2026, 5, day),
            amount=amount,
            currency="USD",
        )
        _insert_txn(
            core_db,
            account_id=_SURVIVOR,
            txn_date=date(2026, 5, day),
            amount=amount,
            currency="EUR",
        )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert overlap.comparable == 3
    assert overlap.matched == 0


def test_two_ledgers_of_unknown_currency_still_match(core_db: Database) -> None:
    """Silence on both sides is not disagreement — the predicate is NULL-safe.

    ``currency_code`` is nullable, and an account whose source never stated one
    inherits nothing to inherit. A plain ``=`` would make every such pair read
    as zero overlap, which is the louder failure: it turns a genuine twin's
    evidence off silently, for every ledger that predates a currency being
    known. Only a *stated* disagreement may veto a match.
    """
    for day, amount in ((3, "-12.00"), (7, "-40.50")):
        _insert_txn(
            core_db, account_id=_TWIN, txn_date=date(2026, 5, day), amount=amount
        )
        _insert_txn(
            core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, day), amount=amount
        )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (2, 2)


def test_the_same_currency_spelled_in_two_cases_still_matches(
    core_db: Database,
) -> None:
    """``usd`` and ``USD`` name one currency, so they may not veto each other.

    Only the tabular path can produce the lowercase side: ``currency`` is one of
    the extractor's pass-through string fields, copied out of the source cell
    verbatim, while OFX and Plaid carry ISO codes. So the mixed-case pair is
    exactly the cross-source shape this probe is asked about — and comparing the
    raw strings would report ``0 of N`` for a genuine twin, which is the silent
    evidence-loss the NULL-safe half of this predicate already refuses.
    """
    for day, amount in ((3, "-12.00"), (7, "-40.50")):
        _insert_txn(
            core_db,
            account_id=_TWIN,
            txn_date=date(2026, 5, day),
            amount=amount,
            currency="usd",
        )
        _insert_txn(
            core_db,
            account_id=_SURVIVOR,
            txn_date=date(2026, 5, day),
            amount=amount,
            currency="USD",
        )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (2, 2)


def test_a_padded_currency_code_still_matches_its_unpadded_twin(
    core_db: Database,
) -> None:
    """Surrounding whitespace is a spelling of the code, not a different currency.

    A CSV reader hands the cell over as written; nothing between the source file
    and ``core.fct_transactions`` strips it.
    """
    _insert_txn(
        core_db,
        account_id=_TWIN,
        txn_date=date(2026, 5, 3),
        amount="-12.00",
        currency=" USD ",
    )
    _insert_txn(
        core_db,
        account_id=_SURVIVOR,
        txn_date=date(2026, 5, 3),
        amount="-12.00",
        currency="USD",
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (1, 1)


def test_a_blank_currency_cell_reads_as_unstated_not_as_a_currency(
    core_db: Database,
) -> None:
    """An empty cell is silence, and silence is not disagreement.

    A statement that fills its currency column only on foreign transactions
    leaves the domestic rows blank rather than NULL, because the tabular
    extractor writes the cell through as-is. Reading ``''`` as a stated currency
    would make those rows disagree with every counterpart — turning the evidence
    off for precisely the ordinary domestic ledger the probe is usually asked
    about. It matches an unstated counterpart and, per the test below, still
    fails to match a stated one.
    """
    _insert_txn(
        core_db,
        account_id=_TWIN,
        txn_date=date(2026, 5, 3),
        amount="-12.00",
        currency="   ",
    )
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (1, 1)


def test_a_stated_currency_does_not_match_an_unstated_one(core_db: Database) -> None:
    """One side knowing its currency and the other not is still not a match.

    The NULL-safe predicate treats ``NULL`` as a value rather than as a
    wildcard, so an unknown currency matches only another unknown one. Erring
    this way costs a true twin its evidence where one source states a currency
    and the other does not; erring the other way would let a EUR ledger match a
    ledger of unknown denomination, which is the failure this probe exists to
    prevent.
    """
    _insert_txn(
        core_db,
        account_id=_TWIN,
        txn_date=date(2026, 5, 3),
        amount="-12.00",
        currency="USD",
    )
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (1, 0)


def test_posting_lag_inside_the_window_still_matches(core_db: Database) -> None:
    """A statement's transaction date and a feed's posting date name one transaction.

    This is the whole reason the probe carries a window: exact date + amount
    scored 23% on the live pair because the two sources date the same row
    differently.
    """
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 6), amount="-12.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (1, 1)


def test_a_gap_wider_than_the_window_does_not_match(core_db: Database) -> None:
    """One day past the window is a different transaction, not a lagged one.

    The survivor's second row is what makes the probed row comparable at all —
    without it the period is three days wide and the row falls outside, which is
    a different verdict (no comparable period) reached for a different reason.
    """
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 11), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 7), amount="-12.00"
    )
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 30), amount="-99.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (overlap.comparable, overlap.matched) == (1, 0)


def test_rows_outside_the_other_ledgers_period_are_not_comparable(
    core_db: Database,
) -> None:
    """History the survivor never covered cannot disconfirm the merge.

    A statement archive that predates the feed's download window would otherwise
    render as "0 of 400 match" — reading as evidence against a correct merge when
    it is only evidence the survivor was not there yet.
    """
    for year in (2019, 2020):
        _insert_txn(
            core_db, account_id=_TWIN, txn_date=date(year, 5, 3), amount="-12.00"
        )
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert overlap == LedgerOverlap(
        comparable=1,
        matched=1,
        window_start=date(2026, 5, 3),
        window_end=date(2026, 5, 3),
    )


def test_ledgers_that_never_overlap_report_no_comparable_period(
    core_db: Database,
) -> None:
    """No shared period is absence of evidence, and must not read as 0 of N."""
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2019, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert overlap == LedgerOverlap(
        comparable=0, matched=0, window_start=None, window_end=None
    )
    assert not overlap.measurable


def test_a_wider_survivor_span_pulls_more_rows_into_comparable(
    core_db: Database,
) -> None:
    """The displayed ratio can worsen after the prompt, and nothing re-checks it.

    ``span`` is ``MIN``/``MAX`` over the *survivor's* dates, so one survivor-side
    row arriving outside the current span widens the comparison window and admits
    absorbed rows that match nothing. The evidence a human ratified as "1 of 1"
    is "1 of 4" by the time the merge commits, while ``_drift_check`` still
    verifies — it holds the absorbed account's blast radius, not this window.

    This pins the behavior rather than the fix: closing it needs an asymmetric
    re-verification on both surfaces, tracked with the empty-survivor gap in
    ``account-identity-resolution.md``. Until then this test is what keeps the
    mechanism legible in code instead of only in prose.
    """
    for year in (2019, 2020, 2021):
        _insert_txn(
            core_db, account_id=_TWIN, txn_date=date(year, 5, 3), amount="-12.00"
        )
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    displayed = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    # A concurrent sync lands one older row on the survivor. It matches nothing
    # in the absorbed ledger, so only the denominator can move.
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2018, 1, 1), amount="-999.99"
    )

    at_commit = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert (displayed.matched, displayed.comparable) == (1, 1)
    assert (at_commit.matched, at_commit.comparable) == (1, 4)


def test_a_measurable_probe_is_counted_as_measurable(core_db: Database) -> None:
    """Without this counter, evidence that died in a deployment looks like silence.

    A schema or source drift that makes every probe return "no comparable period"
    degrades the merge prompt to prose with no number in it, and every surface
    keeps rendering normally. The two label values are what tell a dead probe
    from a quiet one.
    """
    before = _probes("measurable")
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    probe_ledger_overlap(core_db, account_id=_TWIN, against_account_id=_SURVIVOR)

    assert _probes("measurable") == before + 1


def test_a_probe_with_no_comparable_period_is_counted_as_unmeasurable(
    core_db: Database,
) -> None:
    """The failure mode worth alarming on is this label climbing alone."""
    before = _probes("unmeasurable")
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2019, 5, 3), amount="-12.00")
    _insert_txn(
        core_db, account_id=_SURVIVOR, txn_date=date(2026, 5, 3), amount="-12.00"
    )

    probe_ledger_overlap(core_db, account_id=_TWIN, against_account_id=_SURVIVOR)

    assert _probes("unmeasurable") == before + 1


def test_an_empty_other_ledger_reports_no_comparable_period(core_db: Database) -> None:
    """A survivor with no transactions yet bounds no window to compare against."""
    _insert_txn(core_db, account_id=_TWIN, txn_date=date(2026, 5, 3), amount="-12.00")

    overlap = probe_ledger_overlap(
        core_db, account_id=_TWIN, against_account_id=_SURVIVOR
    )

    assert not overlap.measurable


def test_an_unmaterialized_core_reports_no_comparable_period(db: Database) -> None:
    """A first import before any transform has no ledger to probe, and must not raise."""
    overlap = probe_ledger_overlap(db, account_id=_TWIN, against_account_id=_SURVIVOR)

    assert not overlap.measurable
