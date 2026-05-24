"""End-to-end tests for sql_query lineage-based privacy enforcement.

Verifies that:
- CRITICAL columns (account_id) are masked (****<last4>)
- HIGH columns (amount) pass through in the clear
- Aggregate queries return LOW sensitivity
- Nonexistent tables return an error envelope
- Invalid SQL returns an error envelope
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import get_database
from moneybin.mcp.tools.sql import sql_query
from moneybin.protocol.envelope import ResponseEnvelope


def _run(sql: str) -> ResponseEnvelope[Any]:
    return asyncio.run(sql_query(sql))  # type: ignore[arg-type]


@pytest.fixture()
def _seeded_txn(mcp_db: object) -> None:  # type: ignore[type-arg]
    """Insert one transaction row so amount-based tests have data."""
    with get_database() as db:
        db.execute("""
            INSERT INTO core.fct_transactions
                (transaction_id, account_id, transaction_date, amount,
                 amount_absolute, transaction_direction, description,
                 category, source_type, loaded_at, updated_at)
            VALUES (
                'TXN001', 'ACC001', '2025-06-15', -42.50,
                42.50, 'expense', 'Test coffee shop',
                'Food', 'ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
        """)


@pytest.mark.integration
def test_critical_column_masked(mcp_db: object) -> None:  # type: ignore[type-arg]
    """CRITICAL column (account_id) must be masked in sql_query output."""
    env = _run("SELECT account_id FROM core.dim_accounts LIMIT 1")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.sensitivity == "critical"
    for row in env.data:  # type: ignore[union-attr]
        assert str(row["account_id"]).startswith("****"), (
            f"account_id was not masked: {row['account_id']!r}"
        )


@pytest.mark.integration
def test_high_tier_passes_through(
    mcp_db: object,  # type: ignore[type-arg]
    _seeded_txn: None,
) -> None:
    """HIGH-tier column (amount) passes through in the clear — no consent gate."""
    env = _run("SELECT amount FROM core.fct_transactions LIMIT 1")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.sensitivity == "high"
    assert len(env.data) == 1  # type: ignore[arg-type]
    # DuckDB returns DECIMAL(18,2) as Decimal; amount is not masked (HIGH passes through).
    assert isinstance(env.data[0]["amount"], (int, float, Decimal))  # type: ignore[index]


@pytest.mark.integration
def test_low_aggregate_is_low(
    mcp_db: object,  # type: ignore[type-arg]
    _seeded_txn: None,
) -> None:
    """COUNT(*) over a classified table yields LOW aggregate sensitivity."""
    env = _run("SELECT category, COUNT(*) AS n FROM core.fct_transactions GROUP BY 1")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.sensitivity == "low"


@pytest.mark.integration
def test_nonexistent_table_returns_error_envelope(mcp_db: object) -> None:  # type: ignore[type-arg]
    """A query against a nonexistent table returns an error envelope."""
    env = _run("SELECT * FROM core.does_not_exist")
    assert env.error is not None


@pytest.mark.integration
def test_invalid_sql_returns_error_envelope(mcp_db: object) -> None:  # type: ignore[type-arg]
    """Syntactically invalid SQL returns an error envelope."""
    env = _run("SELECT FROM WHERE )(")
    assert env.error is not None


@pytest.mark.integration
def test_write_query_returns_error_envelope(mcp_db: object) -> None:  # type: ignore[type-arg]
    """Write SQL is blocked by validate_read_only_query — returns error envelope."""
    env = _run("INSERT INTO core.fct_transactions VALUES (1)")
    assert env.error is not None


@pytest.mark.integration
def test_classes_returned_in_envelope(mcp_db: object) -> None:  # type: ignore[type-arg]
    """classes_returned is carried in the envelope (non-serialized observability)."""
    env = _run("SELECT account_id FROM core.dim_accounts LIMIT 1")
    assert env.error is None
    assert env.classes_returned is not None
    assert "account_identifier" in env.classes_returned
    # classes_returned is not in the wire dict
    assert "classes_returned" not in env.to_dict()


@pytest.mark.integration
def test_routing_number_masked(mcp_db: object) -> None:  # type: ignore[type-arg]
    """routing_number (CRITICAL) is masked when selected directly."""
    env = _run("SELECT routing_number FROM core.dim_accounts LIMIT 1")
    assert env.error is None
    assert env.summary.sensitivity == "critical"
    for row in env.data:  # type: ignore[union-attr]
        # _mask_routing_number returns "*****"
        assert row["routing_number"] == "*****", (
            f"routing_number not masked: {row['routing_number']!r}"
        )


@pytest.mark.integration
def test_disallowed_schema_refused(mcp_db: object) -> None:  # type: ignore[type-arg]
    """Queries against schemas outside core/app are refused (closes the leak).

    The gate fires on the schema name before execution, so the table need not
    exist — raw.* is rejected regardless.
    """
    env = _run("SELECT account_id FROM raw.ofx_transactions")
    assert env.error is not None
    assert env.error.code == "sql_schema_not_allowed"


@pytest.mark.integration
def test_describe_bypasses_lineage(mcp_db: object) -> None:  # type: ignore[type-arg]
    """DESCRIBE is metadata, not row data — executes at LOW, not a lineage error."""
    env = _run("DESCRIBE core.fct_transactions")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.sensitivity == "low"
    assert len(env.data) > 0  # type: ignore[arg-type]


@pytest.mark.integration
def test_truncation_sets_has_more(
    mcp_db: object,  # type: ignore[type-arg]
    _seeded_txn: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When results exceed the row cap, has_more is true and total_count > returned."""
    with get_database() as db:
        db.execute("""
            INSERT INTO core.fct_transactions
                (transaction_id, account_id, transaction_date, amount,
                 amount_absolute, transaction_direction, description,
                 category, source_type, loaded_at, updated_at)
            VALUES (
                'TXN002', 'ACC001', '2025-06-16', -10.00,
                10.00, 'expense', 'Second row', 'Food', 'ofx',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """)
    # Cap at 1 row so the 2 seeded rows trigger truncation.
    monkeypatch.setattr("moneybin.mcp.tools.sql.get_max_rows", lambda: 1)
    env = _run("SELECT amount FROM core.fct_transactions")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.returned_count == 1
    assert env.summary.has_more is True
    assert env.summary.total_count > env.summary.returned_count


@pytest.mark.integration
def test_unaliased_aggregate_critical_masked(mcp_db: object) -> None:  # type: ignore[type-arg]
    """Unaliased MIN(account_id) is masked despite the sqlglot/DuckDB name split.

    sqlglot names the projection `''` while DuckDB calls the result column
    `min(account_id)`; name-keyed redaction would miss it and leak the account
    number. Position-aligned redaction masks it by the real column name.
    """
    env = _run("SELECT MIN(account_id) FROM core.dim_accounts")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.sensitivity == "critical"
    row = env.data[0]  # type: ignore[index]
    (value,) = row.values()
    assert str(value).startswith("****"), f"unaliased account_id not masked: {value!r}"


@pytest.mark.integration
def test_metadata_query_truncation_sets_has_more(
    mcp_db: object,  # type: ignore[type-arg]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DESCRIBE on a wide table truncates with has_more, not silently."""
    monkeypatch.setattr("moneybin.mcp.tools.sql.get_max_rows", lambda: 1)
    env = _run("DESCRIBE core.fct_transactions")
    assert env.error is None, f"Unexpected error: {env.error}"
    assert env.summary.returned_count == 1
    assert env.summary.has_more is True
