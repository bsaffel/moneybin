"""Integration: the per-account investment source choice through the real models.

An account holds one manual buy and one Plaid buy; the choice decides which of
them reaches ``core.fct_investment_transactions`` and everything derived from it.
Expectations are hand-derived from the seeded rows, not read back from the models.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from moneybin.database import Database
from moneybin.investments.source_overlap import (
    investment_source_overlap,
    stale_source_choice_accounts,
)
from moneybin.services.account_service import CLEAR, AccountService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.mutation_context import operation
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService

pytestmark = pytest.mark.integration

_ACCOUNT = "acc_choice"
_ORPHAN = "acc_orphan"
_SECURITY = "sec_choice_0001"
_PROVIDER_KEY = f"prov_{_SECURITY}"
_ORIGIN = "item_choice"
_TRADE_DATE = date(2026, 1, 5)


def _seed_security(db: Database) -> None:
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, ticker)
        VALUES (?, 'Choice Fund', 'etf', 'CHC')
        """,  # test fixture, not executing user SQL
        [_SECURITY],
    )
    # The accepted binding that resolves the provider's security id (and the
    # price row below) onto the canonical security.
    db.execute(
        """
        INSERT INTO app.security_links
            (link_id, security_id, ref_kind, ref_value, source_type,
             status, decided_by, decided_at)
        VALUES (?, ?, 'plaid_security_id', ?, 'plaid',
                'accepted', 'auto', CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [f"link_{_PROVIDER_KEY}", _SECURITY, _PROVIDER_KEY],
    )
    db.execute(
        """
        INSERT INTO raw.security_prices
            (provider_security_key, price_date, quote_currency, source_type,
             source_origin, close, price_basis, extracted_at, loaded_at)
        VALUES (?, CURRENT_DATE, 'USD', 'plaid', ?, 120.00, 'raw',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_PROVIDER_KEY, _ORIGIN],
    )


def _seed_plaid_account(db: Database) -> None:
    """One source account so ``acc_choice`` resolves to a core.dim_accounts row."""
    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, account_subtype, institution_name,
             official_name, mask, source_file, source_type, source_origin,
             extracted_at, loaded_at)
        VALUES (?, 'investment', 'brokerage', 'TestBank', 'Choice Brokerage',
                '1234', 'sync_test', 'plaid', ?,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_ACCOUNT, _ORIGIN],
    )


def _seed_manual_buy(db: Database, *, account_id: str, txn_id: str) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees,
            created_by, investment_transaction_id, currency_code, created_at
        ) VALUES (?, ?, ?, ?, 'CHC', 'buy', ?::DATE, 10, 100.00, -1000.00, 0.00,
                  'test', ?, 'USD', TIMESTAMP '2026-01-06 09:00:00')
        """,  # test fixture, not executing user SQL
        [txn_id, f"imp_{txn_id}", account_id, _SECURITY, _TRADE_DATE, txn_id],
    )


def _seed_plaid_buy(db: Database, *, loaded_at: str) -> None:
    """A Plaid buy on the canonical id; ``amount`` is Plaid's positive cash-out."""
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transactions (
            investment_transaction_id, account_id, security_id,
            investment_transaction_type, investment_transaction_subtype,
            transaction_date, quantity, price, amount, fees, iso_currency_code,
            observation_version, source_type, source_origin
        ) VALUES ('itx_choice_buy', ?, ?, 'buy', 'buy', ?,
                  10, 100.00, 1000.00, 0.00, 'USD', 'plaid_fixture', 'plaid', ?)
        """,  # test fixture, not executing user SQL
        [_ACCOUNT, _PROVIDER_KEY, _TRADE_DATE, _ORIGIN],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_transaction_receipts (
            investment_transaction_id, source_origin, source_file,
            observation_version, extracted_at, loaded_at
        ) VALUES ('itx_choice_buy', ?, 'sync_test', 'plaid_fixture',
                  CURRENT_TIMESTAMP, ?::TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_ORIGIN, loaded_at],
    )


def _seed_snapshot(db: Database, *, quantity: str) -> None:
    """The broker's snapshot: a receipt plus the holding it accounts for."""
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings_snapshots (
            source_origin, source_file, holdings_date, holdings_count,
            transactions_window_start, source_type, extracted_at, loaded_at
        ) VALUES (?, 'sync_job_1', CURRENT_DATE, 1, DATE '2026-01-01', 'plaid',
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_ORIGIN],
    )
    db.execute(
        """
        INSERT INTO raw.plaid_investment_holdings (
            account_id, security_id, holdings_date, institution_price,
            institution_price_as_of, institution_value, cost_basis, quantity,
            iso_currency_code, transactions_window_start, source_file,
            source_type, source_origin, extracted_at, loaded_at
        ) VALUES (?, ?, CURRENT_DATE, 120.00, CURRENT_DATE, NULL, NULL, ?,
                  'USD', DATE '2026-01-01', 'sync_job_1', 'plaid', ?,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [_ACCOUNT, _PROVIDER_KEY, quantity, _ORIGIN],
    )


def _seed_overlap(db: Database, *, snapshot_quantity: str | None = None) -> None:
    """One manual and one Plaid buy on ``acc_choice``, built into the models."""
    _seed_security(db)
    _seed_plaid_account(db)
    _seed_manual_buy(db, account_id=_ACCOUNT, txn_id="manual_choice_buy")
    _seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    if snapshot_quantity is not None:
        _seed_snapshot(db, quantity=snapshot_quantity)
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"


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


def test_failed_restate_leaves_a_stale_choice_until_refresh(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The choice commits before the restate, so a failed restate must stay visible."""
    _seed_overlap(db)

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("restate failed")

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_investment_ledger", _fail
    )
    with pytest.raises(RuntimeError, match="restate failed"):
        AccountService(db).settings_update(
            _ACCOUNT, actor="test", investment_source_type="manual"
        )

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

    def _fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("restate failed")

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_investment_ledger", _fail
    )
    with pytest.raises(RuntimeError, match="restate failed"):
        AccountService(db).settings_update(
            _ACCOUNT, actor="test", investment_source_type=CLEAR
        )

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
