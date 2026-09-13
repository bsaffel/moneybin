"""Comparison normalizes representation without rewriting source evidence."""

import pytest

from moneybin.database import Database
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def test_currency_case_and_space_are_representation_only(
    comparison_db: Database,
) -> None:
    seed_manual_event(comparison_db, "manual")
    comparison_db.execute("""
        UPDATE raw.manual_investment_transactions SET currency_code = ' usd '
        WHERE source_transaction_id = 'manual'
    """)
    seed_plaid_event(comparison_db, "plaid")
    install_comparison_models(comparison_db)
    assert comparison_db.execute("""
        SELECT currency_code FROM prep.int_investment_events__legs ORDER BY source_type
    """).fetchall() == [("USD",), ("USD",)]
    assert comparison_db.execute("""
        SELECT is_candidate FROM prep.int_investment_events__evidence
    """).fetchall() == [(True,)]
    assert comparison_db.execute("""
        SELECT currency_code FROM raw.manual_investment_transactions
    """).fetchone() == (" usd ",)


@pytest.mark.parametrize("field", ["description", "original_acquisition_date"])
def test_projection_field_changes_version_but_not_native_event_key(
    comparison_db: Database, field: str
) -> None:
    seed_manual_event(comparison_db, "manual")
    install_comparison_models(comparison_db)
    before = comparison_db.execute("""
        SELECT source_event_key, observation_version
        FROM prep.int_investment_events__legs
    """).fetchone()
    if field == "description":
        comparison_db.execute("""
            UPDATE raw.manual_investment_transactions SET description = 'Updated context'
        """)
    else:
        comparison_db.execute("""
            UPDATE raw.manual_investment_transactions
            SET original_acquisition_date = '2020-01-01'
        """)
    after = comparison_db.execute("""
        SELECT source_event_key, observation_version
        FROM prep.int_investment_events__legs
    """).fetchone()
    assert before is not None and after is not None
    assert after[0] == before[0]
    assert after[1] != before[1]
