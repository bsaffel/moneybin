"""Tests for the transactions_get MCP tool."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp.tools.transactions import (
    register_transaction_coarse_reads,
    register_transactions_tools,
    transactions_coarse,
    transactions_get,
)

pytestmark = pytest.mark.usefixtures("mcp_db")


def _insert_transactions() -> None:
    with get_database(read_only=False) as db:
        db.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                merchant_name, merchant_id, transaction_type, is_pending,
                currency_code, source_type, source_extracted_at, loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter, category, categorized_by,
                notes, tags, splits
            ) VALUES
                (
                    'txn_1', 'ACC001', '2025-06-01', -25.00,
                    25.00, 'expense', 'Morning coffee', 'Coffee House',
                    'merchant_coffee', 'DEBIT', false, 'USD', 'ofx',
                    '2025-06-01', CURRENT_TIMESTAMP, 2025, 6, 1, 0,
                    '2025-06', '2025-Q2', 'Food & Drink', 'user',
                    NULL, NULL, NULL
                ),
                (
                    'txn_2', 'ACC001', '2025-06-01', -75.00,
                    75.00, 'expense', 'Dinner', 'Restaurant',
                    'merchant_restaurant', 'DEBIT', false, 'USD', 'ofx',
                    '2025-06-01', CURRENT_TIMESTAMP, 2025, 6, 1, 0,
                    '2025-06', '2025-Q2', 'Food & Drink', 'user',
                    NULL, NULL, NULL
                )
            """
        )
        db.execute(
            """
            CREATE OR REPLACE VIEW core.dim_merchants AS
            SELECT
                'merchant_coffee'::VARCHAR AS merchant_id,
                'Coffee House'::VARCHAR AS raw_pattern,
                'contains'::VARCHAR AS match_type,
                'Coffee House'::VARCHAR AS canonical_name,
                'Food & Drink'::VARCHAR AS category,
                NULL::VARCHAR AS subcategory,
                'test'::VARCHAR AS created_by,
                []::VARCHAR[] AS exemplars,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            UNION ALL
            SELECT
                'merchant_restaurant', 'Restaurant', 'contains',
                'Restaurant', 'Food & Drink', NULL, 'test', [],
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            """  # noqa: S608  # test fixture view, literal test data only
        )


@pytest.mark.unit
async def test_transactions_get_returns_envelope(mcp_db: object) -> None:
    """transactions_get returns a valid ResponseEnvelope."""
    result = await transactions_get()
    d = result.to_dict()
    assert "summary" in d
    assert "data" in d
    assert "actions" in d
    # TransactionGetPayload → TransactionRow amount is TXN_AMOUNT → HIGH
    # (account_id is RECORD_ID per spec D6, no longer the driver).
    assert d["summary"]["sensitivity"] == "high"


@pytest.mark.unit
async def test_transactions_get_data_has_transactions_list(mcp_db: object) -> None:
    """Data field is a TransactionGetPayload dict with a 'transactions' list."""
    result = await transactions_get()
    d = result.to_dict()
    assert isinstance(d["data"], dict)
    assert isinstance(d["data"]["transactions"], list)


@pytest.mark.unit
async def test_transactions_get_no_cursor_when_empty(mcp_db: object) -> None:
    """next_cursor absent when all results fit in one page."""
    result = await transactions_get(limit=50)
    d = result.to_dict()
    # Fresh MCP DB has no transactions — no cursor expected
    assert "next_cursor" not in d or d.get("next_cursor") is None


@pytest.mark.unit
async def test_register_includes_transactions_get() -> None:
    """register_transactions_tools registers transactions_get."""
    srv = FastMCP("test")
    register_transactions_tools(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert "transactions_get" in names
    assert "transactions_search" not in names
    assert "transactions_review" in names
    assert "transactions_recurring_list" not in names


@pytest.mark.unit
async def test_transaction_coarse_registrar_registers_only_replacement() -> None:
    srv = FastMCP("test")
    register_transaction_coarse_reads(srv)
    names = {t.name for t in await srv._list_tools()}  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
    assert names == {"transactions"}


@pytest.mark.unit
async def test_transactions_coarse_preserves_operational_query_semantics(
    mcp_db: object,
) -> None:
    _insert_transactions()

    legacy = await transactions_get(
        accounts=["ACC001"],
        date_from="2025-06-01",
        date_to="2025-06-30",
        categories=["Food & Drink"],
        amount_min="-50.00",
        description="coffee",
        limit=100,
    )
    coarse = await transactions_coarse(
        account="ACC001",
        start=date(2025, 6, 1),
        end=date(2025, 6, 30),
        category="Food & Drink",
        min_amount=Decimal("-50.00"),
        text="coffee",
        limit=100,
    )

    assert coarse.data.transactions == legacy.data.transactions
    assert coarse.summary.total_count == 1
    assert coarse.summary.returned_count == 1
    assert coarse.summary.display_currency == legacy.summary.display_currency
    assert coarse.summary.period == "2025-06-01 to 2025-06-30"


@pytest.mark.unit
async def test_transactions_coarse_resolves_merchant_filter(
    mcp_db: object,
) -> None:
    _insert_transactions()

    result = await transactions_coarse(merchant="Coffee House")

    assert result.summary.total_count == 1
    assert [row.transaction_id for row in result.data.transactions] == ["txn_1"]


@pytest.mark.unit
async def test_transactions_coarse_paginates_with_exact_counts(
    mcp_db: object,
) -> None:
    _insert_transactions()

    first = await transactions_coarse(
        account="ACC001",
        start=date(2025, 6, 1),
        end=date(2025, 6, 30),
        category="Food & Drink",
        min_amount=Decimal("-100.00"),
        max_amount=Decimal("0.00"),
        limit=1,
    )
    second = await transactions_coarse(
        account="ACC001",
        start=date(2025, 6, 1),
        end=date(2025, 6, 30),
        category="Food & Drink",
        min_amount=Decimal("-100.00"),
        max_amount=Decimal("0.00"),
        limit=1,
        cursor=first.next_cursor,
    )

    assert first.summary.total_count == 2
    assert first.summary.returned_count == 1
    assert first.summary.has_more is True
    assert first.next_cursor is not None
    assert second.summary.total_count == 2
    assert second.summary.returned_count == 1
    assert second.summary.has_more is False
    assert second.next_cursor is None
    assert {
        first.data.transactions[0].transaction_id,
        second.data.transactions[0].transaction_id,
    } == {"txn_1", "txn_2"}
    continuation = next(
        action for action in first.actions if action.startswith("Continue with ")
    )
    for argument in (
        "account='ACC001'",
        "start='2025-06-01'",
        "end='2025-06-30'",
        "category='Food & Drink'",
        "min_amount=-100.00",
        "max_amount=0.00",
        "limit=1",
        f"cursor={first.next_cursor!r}",
    ):
        assert argument in continuation


@pytest.mark.unit
async def test_transactions_coarse_cursor_is_bound_to_filters(
    mcp_db: object,
) -> None:
    _insert_transactions()
    first = await transactions_coarse(account="ACC001", limit=1)

    response = await transactions_coarse(
        account="ACC001",
        category="Food & Drink",
        limit=1,
        cursor=first.next_cursor,
    )

    assert response.error is not None
    assert response.error.code == "TRANSACTION_CURSOR_INVALID"
