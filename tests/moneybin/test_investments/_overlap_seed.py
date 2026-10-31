"""Shared seeds: one account holding a manual and a Plaid buy of one security."""

from __future__ import annotations

from datetime import date

from moneybin.database import Database
from moneybin.services.transform_service import TransformService

ACCOUNT = "acc_choice"
_SECURITY = "sec_choice_0001"
_PROVIDER_KEY = f"prov_{_SECURITY}"
_ORIGIN = "item_choice"
_TRADE_DATE = date(2026, 1, 5)


def seed_security(db: Database) -> None:
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


def seed_plaid_account(db: Database) -> None:
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
        [ACCOUNT, _ORIGIN],
    )


def seed_manual_buy(db: Database, *, account_id: str, txn_id: str) -> None:
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


def seed_manual_sell(db: Database, *, account_id: str, txn_id: str) -> None:
    """A manual sell of 4 units after the buy; engine reads abs() of qty/amount."""
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions (
            source_transaction_id, import_id, account_id, security_id,
            security_ref, type, trade_date, quantity, price, amount, fees,
            created_by, investment_transaction_id, currency_code, created_at
        ) VALUES (?, ?, ?, ?, 'CHC', 'sell', DATE '2026-03-05', 4, 120.00, 480.00,
                  0.00, 'test', ?, 'USD', TIMESTAMP '2026-03-06 09:00:00')
        """,  # test fixture, not executing user SQL
        [txn_id, f"imp_{txn_id}", account_id, _SECURITY, txn_id],
    )


def seed_plaid_buy(db: Database, *, loaded_at: str) -> None:
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
        [ACCOUNT, _PROVIDER_KEY, _TRADE_DATE, _ORIGIN],
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


def seed_snapshot(db: Database, *, quantity: str) -> None:
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
        [ACCOUNT, _PROVIDER_KEY, quantity, _ORIGIN],
    )


def seed_overlap(db: Database, *, snapshot_quantity: str | None = None) -> None:
    """One manual and one Plaid buy on ``acc_choice``, built into the models."""
    seed_security(db)
    seed_plaid_account(db)
    seed_manual_buy(db, account_id=ACCOUNT, txn_id="manual_choice_buy")
    seed_plaid_buy(db, loaded_at="2026-02-01 09:00:00")
    if snapshot_quantity is not None:
        seed_snapshot(db, quantity=snapshot_quantity)
    result = TransformService(db).apply()
    assert result.applied, f"transform failed: {result.error}"
