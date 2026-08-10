"""Tests for the ledger-overlap probe.

The probe answers one question a reviewer cannot answer from a signal label:
do these two accounts hold the same transactions? Fixtures are built from the
*shape* — two ledgers over a shared period, one carrying the same amounts and
one carrying different ones — never from real institution or account data.
"""

from __future__ import annotations

from datetime import date

import pytest

from moneybin.database import Database
from moneybin.services.ledger_overlap import LedgerOverlap, probe_ledger_overlap
from tests.moneybin.db_helpers import create_core_tables

_TWIN = "twin_acct0001"
_SURVIVOR = "surv_acct0001"


def _insert_txn(
    db: Database,
    *,
    account_id: str,
    txn_date: date,
    amount: str,
    suffix: str = "",
) -> None:
    db.execute(
        "INSERT INTO core.fct_transactions "
        "(transaction_id, account_id, transaction_date, amount) VALUES (?, ?, ?, ?)",
        [
            f"{account_id}-{txn_date.isoformat()}-{amount}{suffix}",
            account_id,
            txn_date,
            amount,
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
