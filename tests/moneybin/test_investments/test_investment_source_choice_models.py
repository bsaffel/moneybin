"""Integration: the per-account investment source choice through the real models.

An account holds one manual buy and one Plaid buy; the choice decides which of
them reaches ``core.fct_investment_transactions`` and everything derived from it.
Expectations are hand-derived from the seeded rows, not read back from the models.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.source_overlap import (
    investment_source_overlap,
    stale_source_choice_accounts,
)
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments._overlap_seed import (
    ACCOUNT as _ACCOUNT,
)
from tests.moneybin.test_investments._overlap_seed import (
    seed_manual_buy as _seed_manual_buy,
)
from tests.moneybin.test_investments._overlap_seed import (
    seed_overlap as _seed_overlap,
)
from tests.moneybin.test_investments._overlap_seed import (
    seed_security as _seed_security,
)

pytestmark = pytest.mark.integration

_ORPHAN = "acc_orphan"


def _ledger(db: Database, account_id: str = _ACCOUNT) -> list[tuple[str, str | None]]:
    rows = db.execute(
        """
        SELECT source_type, subtype
        FROM core.fct_investment_transactions
        WHERE account_id = ?
        ORDER BY source_type, subtype NULLS FIRST
        """,
        [account_id],
    ).fetchall()
    return [(str(r[0]), r[1]) for r in rows]


def _status(db: Database) -> str:
    row = db.execute(
        "SELECT valuation_status FROM core.dim_holdings WHERE account_id = ?",
        [_ACCOUNT],
    ).fetchone()
    assert row is not None, "the position produced no dim_holdings row"
    return str(row[0])


def test_no_choice_keeps_both_sources(db: Database) -> None:
    _seed_overlap(db)

    assert _ledger(db) == [("manual", None), ("plaid", None)]
    assert _status(db) == "source_overlap"


def test_manual_choice_keeps_only_recorded_rows(db: Database) -> None:
    _seed_overlap(db, snapshot_quantity="15")
    # Precondition: the unchosen ledger really carries the bootstrap row that
    # the choice must drop (snapshot 15 less the one in-window Plaid buy of 10).
    assert ("plaid", "opening_bootstrap") in _ledger(db)

    AccountService(db).settings_update(
        _ACCOUNT, actor="test", investment_source_type="manual"
    )

    assert _ledger(db) == [("manual", None)]
    assert _status(db) != "source_overlap"
    assert _ACCOUNT not in InvestmentService(db)._source_overlap_accounts()  # pyright: ignore[reportPrivateUsage]  # the claim under test is the overlap predicate lifting


def test_plaid_choice_keeps_synced_rows_and_bootstrap(db: Database) -> None:
    _seed_overlap(db, snapshot_quantity="15")

    AccountService(db).settings_update(
        _ACCOUNT, actor="test", investment_source_type="plaid"
    )

    assert _ledger(db) == [("plaid", None), ("plaid", "opening_bootstrap")]
    assert _status(db) != "source_overlap"


def test_clear_returns_rows_with_a_newer_watermark(db: Database) -> None:
    _seed_overlap(db)
    service = AccountService(db)
    service.settings_update(_ACCOUNT, actor="test", investment_source_type="manual")
    service.settings_update(_ACCOUNT, actor="test", investment_source_type=CLEAR)

    row = db.execute(
        """
        SELECT investment_source_type, investment_source_type_changed_at
        FROM app.account_settings WHERE account_id = ?
        """,
        [_ACCOUNT],
    ).fetchone()
    assert row is not None
    assert row[0] is None
    changed_at = row[1]
    assert isinstance(changed_at, datetime)

    # The clear brought the Plaid row back; its created_at (2026-02-01 receipt)
    # predates the clear, so only the folded change time can place it after.
    plaid_rows = db.execute(
        """
        SELECT updated_at FROM core.fct_investment_transactions
        WHERE account_id = ? AND source_type = 'plaid'
        """,
        [_ACCOUNT],
    ).fetchall()
    assert len(plaid_rows) == 1, "the clear did not restore the synced row"
    assert plaid_rows[0][0] >= changed_at

    holding = db.execute(
        "SELECT updated_at FROM core.dim_holdings WHERE account_id = ?", [_ACCOUNT]
    ).fetchone()
    assert holding is not None
    assert holding[0] >= changed_at
    assert _status(db) == "source_overlap", "clearing should bring the overlap back"


def _fail_restatement(*_args: object, committed_change: str) -> None:
    """Fail the post-write restate the way ``restate_after_write`` does."""
    raise UserError(
        f"The {committed_change} was saved, but the reports that read it could "
        "not be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
    )


def test_failed_restate_leaves_a_stale_choice_until_refresh(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The choice commits before the restate, so a failed restate must stay visible."""
    _seed_overlap(db)

    monkeypatch.setattr(
        "moneybin.services.restate.restate_after_write", _fail_restatement
    )
    with pytest.raises(UserError) as failed:
        AccountService(db).settings_update(
            _ACCOUNT, actor="test", investment_source_type="manual"
        )
    assert failed.value.code == error_codes.REFRESH_MODEL_FAILED

    assert investment_source_overlap(db) == [], "a chosen account is not offered"
    assert stale_source_choice_accounts(db) == [_ACCOUNT]
    assert _ledger(db) == [("manual", None), ("plaid", None)]

    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"

    assert stale_source_choice_accounts(db) == []
    assert _ledger(db) == [("manual", None)]


def test_a_built_choice_is_not_stale(db: Database) -> None:
    """No false positive from the dim and ledger timestamps the models stamp."""
    _seed_overlap(db)
    service = AccountService(db)

    service.settings_update(_ACCOUNT, actor="test", investment_source_type="manual")
    assert stale_source_choice_accounts(db) == []

    service.settings_update(_ACCOUNT, actor="test", investment_source_type=CLEAR)
    assert stale_source_choice_accounts(db) == []

    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"
    assert stale_source_choice_accounts(db) == []


def test_failed_restate_after_a_clear_is_stale_until_refresh(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Clearing a choice only adds rows back, so no wrong-source row exists to see."""
    _seed_overlap(db)
    AccountService(db).settings_update(
        _ACCOUNT, actor="test", investment_source_type="manual"
    )
    assert _ledger(db) == [("manual", None)]

    monkeypatch.setattr(
        "moneybin.services.restate.restate_after_write", _fail_restatement
    )
    with pytest.raises(UserError) as failed:
        AccountService(db).settings_update(
            _ACCOUNT, actor="test", investment_source_type=CLEAR
        )
    assert failed.value.code == error_codes.REFRESH_MODEL_FAILED

    assert _ledger(db) == [("manual", None)], "the failed restate built nothing"
    assert stale_source_choice_accounts(db) == [_ACCOUNT]

    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"

    assert stale_source_choice_accounts(db) == []
    assert _ledger(db) == [("manual", None), ("plaid", None)]


def test_undo_of_a_choice_advances_the_account_watermark(db: Database) -> None:
    """Undo restores the settings row's own updated_at from the old image.

    Only the source change time moves forward, so dim_accounts' updated_at must
    fold it in, or the undone choice reads as an unchanged account.
    """
    _seed_overlap(db)
    # An earlier unrelated write, so the undo restores the row rather than
    # deleting it (deleting a row that held a choice is refused).
    AccountService(db).settings_update(
        _ACCOUNT, actor="test", display_name="Synthetic Brokerage"
    )
    with operation() as op:
        AccountService(db).settings_update(
            _ACCOUNT, actor="test", investment_source_type="manual"
        )
    UndoService(db).undo(op, actor="test")

    row = db.execute(
        """
        SELECT investment_source_type, investment_source_type_changed_at, updated_at
        FROM core.dim_accounts WHERE account_id = ?
        """,
        [_ACCOUNT],
    ).fetchone()
    assert row is not None
    assert row[0] is None, "the undo did not clear the choice"
    assert isinstance(row[1], datetime)
    assert row[2] >= row[1]


def test_row_without_a_dim_accounts_match_stays(db: Database) -> None:
    """A NULL choice means every source, including for an account dim has never seen."""
    _seed_security(db)
    _seed_manual_buy(db, account_id=_ORPHAN, txn_id="manual_orphan_buy")
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"

    assert _ledger(db, _ORPHAN) == [("manual", None)]
