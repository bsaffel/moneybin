"""Live execution tests for SQL-backed report models."""

from __future__ import annotations

import math
import re
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import SQLMESH_ROOT, Database

_REPORT_MODELS = SQLMESH_ROOT / "models" / "reports"


def _model_body(name: str) -> str:
    """Return a report model's executable SQL without its MODEL header."""
    raw = (_REPORT_MODELS / f"{name}.sql").read_text()
    return re.sub(
        r"^.*?MODEL\s*\(.*?\);\s*",
        "",
        raw,
        count=1,
        flags=re.DOTALL,
    ).strip()


def _install_report(db: Database, name: str) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(  # noqa: S608  # test-selected shipped model name
        f"CREATE OR REPLACE VIEW reports.{name} AS {_model_body(name)}"
    )


@pytest.fixture()
def model_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    database = Database(
        tmp_path / "report-model.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


def _install_balance_drift_sources(model_db: Database) -> None:
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            display_name VARCHAR,
            archived BOOLEAN
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            reconciliation_delta DECIMAL(18, 2)
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts VALUES (?, ?, ?)
        """,
        ["checking", "Checking", False],
    )


def test_balance_drift_uses_independent_transaction_derived_position(
    model_db: Database,
) -> None:
    """Assertion drift compares against the pre-assertion computed position."""
    _install_balance_drift_sources(model_db)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES (?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00")],
    )
    # Independently derived: the transaction-based position is $1,000 and the
    # user assertion is $1,200, so fct_balances_daily resets to $1,200 and
    # records reconciliation_delta = $1,200 - $1,000 = $200.
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES (?, ?, ?, ?)
        """,
        [
            "checking",
            "2026-04-01",
            Decimal("1200.00"),
            Decimal("200.00"),
        ],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        """
        SELECT asserted_balance, computed_balance, drift, drift_abs, status
        FROM reports.balance_drift
        """
    ).fetchone()
    assert row == (
        Decimal("1200.00"),
        Decimal("1000.00"),
        Decimal("200.00"),
        Decimal("200.00"),
        "drift",
    )


def test_balance_drift_without_prior_anchor_is_no_data(model_db: Database) -> None:
    """An assertion without a prior anchor has no independent comparison."""
    _install_balance_drift_sources(model_db)
    model_db.execute(
        """
        INSERT INTO app.balance_assertions (account_id, assertion_date, balance)
        VALUES (?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00")],
    )
    model_db.execute(
        """
        INSERT INTO core.fct_balances_daily VALUES (?, ?, ?, ?)
        """,
        ["checking", "2026-04-01", Decimal("1200.00"), None],
    )

    _install_report(model_db, "balance_drift")

    row = model_db.execute(
        """
        SELECT computed_balance, drift, status
        FROM reports.balance_drift
        """
    ).fetchone()
    assert row == (None, None, "no-data")


def test_spending_trend_uses_zero_filled_calendar_months(
    model_db: Database,
) -> None:
    """Calendar comparisons include missing category-months as zero spend."""
    model_db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            archived BOOLEAN
        )
    """)
    model_db.execute("""
        CREATE TABLE core.fct_transactions (
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            category VARCHAR,
            is_transfer BOOLEAN
        )
    """)
    model_db.execute(
        """
        INSERT INTO core.dim_accounts VALUES (?, ?)
        """,
        ["checking", False],
    )
    # Independently derived calendar series for Food:
    #   2024-01 = 100, 2024-02 = 0, 2024-03 = 300
    #   2025-03 = 500
    # Therefore Mar-2024 trailing average = (100 + 0 + 300) / 3,
    # and Mar-2025 YoY delta = 500 - 300.
    model_db.execute(
        """
        INSERT INTO core.fct_transactions VALUES
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?),
            (?, ?, ?, ?, ?)
        """,
        [
            "checking",
            "2024-01-15",
            Decimal("-100.00"),
            "Food",
            False,
            "checking",
            "2024-03-15",
            Decimal("-300.00"),
            "Food",
            False,
            "checking",
            "2025-03-15",
            Decimal("-500.00"),
            "Food",
            False,
        ],
    )

    _install_report(model_db, "spending_trend")

    february = model_db.execute(
        """
        SELECT total_spend, txn_count, prev_month_spend, mom_delta, mom_pct
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2024-02'
        """
    ).fetchone()
    assert february == (
        Decimal("0.00"),
        0,
        Decimal("100.00"),
        Decimal("-100.00"),
        -1.0,
    )

    march_2024 = model_db.execute(
        """
        SELECT prev_month_spend, mom_delta, mom_pct, trailing_3mo_avg
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2024-03'
        """
    ).fetchone()
    assert march_2024 is not None
    assert march_2024[:3] == (Decimal("0.00"), Decimal("300.00"), None)
    assert math.isclose(float(march_2024[3]), (100 + 0 + 300) / 3)

    march_2025 = model_db.execute(
        """
        SELECT prev_year_spend, yoy_delta, yoy_pct, trailing_3mo_avg
        FROM reports.spending_trend
        WHERE category = 'Food' AND year_month = '2025-03'
        """
    ).fetchone()
    assert march_2025 is not None
    assert march_2025[:2] == (Decimal("300.00"), Decimal("200.00"))
    assert math.isclose(float(march_2025[2]), 2 / 3)
    assert math.isclose(float(march_2025[3]), (0 + 0 + 500) / 3)
