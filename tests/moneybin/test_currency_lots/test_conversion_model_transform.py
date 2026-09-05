"""Integration tests for the Core Currency conversion Python model."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.services.transform_service import TransformService

pytestmark = pytest.mark.integration


@pytest.mark.slow
def test_transform_materializes_exact_single_row_currency_conversion(
    db: Database,
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts (
            account_id, account_type, source_file, extracted_at, loaded_at,
            source_type, source_origin
        ) VALUES (
            'acct-eur', 'CHECKING', 'currency-fixture.ofx',
            '2026-03-15 09:00:00'::TIMESTAMP,
            '2026-03-15 10:00:00'::TIMESTAMP, 'ofx', 'fixture-bank'
        )
        """
    )
    db.execute(
        """
        INSERT INTO app.account_settings (account_id, currency_code, updated_at)
        VALUES ('acct-eur', 'EUR', '2026-03-17 15:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO app.profile_settings (home_currency, updated_at)
        VALUES ('USD', '2026-03-01 09:00:00'::TIMESTAMP)
        """
    )
    db.execute(
        """
        INSERT INTO raw.manual_transactions (
            source_transaction_id, import_id, account_id, transaction_date,
            amount, to_amount, description, currency_code, to_currency,
            created_at, created_by
        ) VALUES (
            'manual_conversion_1', 'import_conversion_1', 'acct-eur',
            '2026-03-16'::DATE, -80.00, 100.00, 'Currency conversion',
            NULL, 'USD', '2026-03-16 14:00:00'::TIMESTAMP, 'cli'
        )
        """
    )
    db.execute(
        """
        INSERT INTO raw.manual_transactions (
            source_transaction_id, import_id, account_id, transaction_date,
            amount, description, currency_code, created_at, created_by
        ) VALUES (
            'manual_own_currency_1', 'import_conversion_1', 'acct-eur',
            '2026-03-16'::DATE, -10.00, 'Own currency transaction',
            'GBP', '2026-03-16 13:00:00'::TIMESTAMP, 'cli'
        )
        """
    )

    result = TransformService(db).apply()
    assert result.applied, f"transform apply failed: {result.error}"

    transaction_row = db.execute(
        """
        SELECT currency_code, updated_at
        FROM core.fct_transactions
        WHERE transaction_id = '649f6d8958fb4c49'
        """
    ).fetchone()

    assert transaction_row == (
        "EUR",
        datetime(2026, 3, 17, 15, 0, 0),
    )

    own_currency_row = db.execute(
        """
        SELECT currency_code, updated_at
        FROM core.fct_transactions
        WHERE amount = -10.00
        """
    ).fetchone()

    assert own_currency_row == (
        "GBP",
        datetime(2026, 3, 16, 13, 0, 0),
    )

    rows = db.execute(
        """
        SELECT conversion_id, source_shape, transfer_pair_id, from_transaction_id,
               to_transaction_id, from_account_id, to_account_id,
               from_amount, from_currency, to_amount, to_currency,
               executed_rate, home_currency, home_value, valuation_rate,
               valuation_rate_date, valuation_source_type,
               from_source_type, from_source_origin,
               from_source_transaction_id, to_source_type, to_source_origin,
               to_source_transaction_id, coverage_status, coverage_reason,
               updated_at
        FROM core.bridge_currency_conversions
        """
    ).fetchall()

    assert rows == [
        (
            "fxc_3c30f82ad673cc5f",
            "single_row",
            None,
            "649f6d8958fb4c49",
            None,
            "acct-eur",
            "acct-eur",
            Decimal("80.00"),
            "EUR",
            Decimal("100.00"),
            "USD",
            Decimal("1.25000000"),
            "USD",
            Decimal("100.00"),
            Decimal("1.25000000"),
            date(2026, 3, 16),
            "actual",
            "manual",
            "user",
            "manual_conversion_1",
            "manual",
            "user",
            "manual_conversion_1",
            "complete",
            None,
            datetime(2026, 3, 17, 15, 0, 0),
        )
    ]
