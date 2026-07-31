"""CLI ↔ MCP parity for the shared response envelope.

Both surfaces build ``summary`` through ``moneybin.protocol.envelope``, so a
field that means different things depending on which surface answered is a
contract break, not a presentation difference. An agent reading
``summary.total_count`` to size a backlog must get the same number either way.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import get_database
from moneybin.mcp.tools.transactions import transactions_coarse

pytestmark = pytest.mark.usefixtures("mcp_db")

runner = CliRunner()

_ROW_COUNT = 3


def _insert_transactions() -> None:
    """Insert three matching transactions so a limit of 1 truncates the page."""
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
                    25.00, 'expense', 'Morning coffee', NULL,
                    NULL, 'DEBIT', false, 'USD', 'ofx',
                    '2025-06-01', CURRENT_TIMESTAMP, 2025, 6, 1, 0,
                    '2025-06', '2025-Q2', 'Food & Drink', 'user',
                    NULL, NULL, NULL
                ),
                (
                    'txn_2', 'ACC001', '2025-06-02', -75.00,
                    75.00, 'expense', 'Dinner', NULL,
                    NULL, 'DEBIT', false, 'USD', 'ofx',
                    '2025-06-02', CURRENT_TIMESTAMP, 2025, 6, 2, 1,
                    '2025-06', '2025-Q2', 'Food & Drink', 'user',
                    NULL, NULL, NULL
                ),
                (
                    'txn_3', 'ACC001', '2025-06-03', -12.50,
                    12.50, 'expense', 'Bus fare', NULL,
                    NULL, 'DEBIT', false, 'USD', 'ofx',
                    '2025-06-03', CURRENT_TIMESTAMP, 2025, 6, 3, 2,
                    '2025-06', '2025-Q2', 'Transport', 'user',
                    NULL, NULL, NULL
                )
            """
        )


def _cli_envelope(*args: str) -> dict[str, object]:
    result = runner.invoke(app, [*args, "--output", "json"])
    assert result.exit_code == 0, result.output
    return json.loads(result.stdout)


@pytest.mark.unit
async def test_total_count_means_total_matching_rows_on_both_surfaces(
    mcp_db: object,
) -> None:
    """A truncated page reports the full match count, not the page size.

    Breaks if the CLI stops threading the service's total through
    ``build_envelope`` — ``summary.total_count`` would fall back to the
    inferred page length (1) while MCP still reports 3.
    """
    _insert_transactions()

    mcp_summary = (await transactions_coarse(limit=1)).to_dict()["summary"]
    cli_summary = _cli_envelope("transactions", "list", "--limit", "1")["summary"]

    assert isinstance(cli_summary, dict)
    assert mcp_summary["total_count"] == _ROW_COUNT
    assert cli_summary["total_count"] == mcp_summary["total_count"]
    assert cli_summary["returned_count"] == mcp_summary["returned_count"] == 1
    assert cli_summary["has_more"] == mcp_summary["has_more"] is True
