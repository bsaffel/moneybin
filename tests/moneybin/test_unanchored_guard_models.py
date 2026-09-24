"""Live execution tests for M2B.3's two core views over stubbed inputs.

Stubs carry only the columns each view reads, the same way
test_net_worth_models.py stubs the report rungs' core.* inputs.
"""

from __future__ import annotations

import re
from collections.abc import Generator
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import SQLMESH_ROOT, Database

pytestmark = pytest.mark.unit

_CORE_MODELS = SQLMESH_ROOT / "models" / "core"


def _install_core_view(db: Database, name: str) -> None:
    raw = (_CORE_MODELS / f"{name}.sql").read_text()
    body = re.sub(r"^.*?MODEL\s*\(.*?\);\s*", "", raw, count=1, flags=re.DOTALL)
    db.execute("CREATE SCHEMA IF NOT EXISTS core")
    db.execute(f"CREATE OR REPLACE VIEW core.{name} AS {body}")  # shipped model body


@pytest.fixture()
def guard_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    database = Database(
        tmp_path / "guard.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


# ---------------------------------------------------------------------------
# core.dim_holdings_broker_reported
# ---------------------------------------------------------------------------


def _install_broker_sources(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        "CREATE TABLE prep.stg_plaid__accounts "
        "(account_id VARCHAR, account_type VARCHAR, source_origin VARCHAR)"
    )
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings_snapshots "
        "(source_origin VARCHAR, source_file VARCHAR, extracted_at TIMESTAMP, "
        "ingestion_sequence BIGINT)"
    )
    db.execute(
        "CREATE TABLE prep.stg_plaid__investment_holdings "
        "(account_id VARCHAR, quantity DECIMAL(28, 10), "
        "institution_value DECIMAL(18, 2), cost_basis DECIMAL(18, 2), "
        "source_origin VARCHAR, source_file VARCHAR)"
    )
    _install_core_view(db, "dim_holdings_broker_reported")


def _plaid_account(
    db: Database, account_id: str, account_type: str | None, origin: str
) -> None:
    db.execute(
        "INSERT INTO prep.stg_plaid__accounts VALUES (?, ?, ?)",
        [account_id, account_type, origin],
    )


def _receipt(
    db: Database, origin: str, source_file: str, extracted_at: str, seq: int = 1
) -> None:
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings_snapshots VALUES (?, ?, ?, ?)",
        [origin, source_file, extracted_at, seq],
    )


def _holding(
    db: Database,
    account_id: str,
    origin: str,
    source_file: str,
    quantity: str | None,
    value: str | None,
    cost_basis: str | None = None,
) -> None:
    db.execute(
        "INSERT INTO prep.stg_plaid__investment_holdings VALUES (?, ?, ?, ?, ?, ?)",
        [account_id, quantity, value, cost_basis, origin, source_file],
    )


def _broker(db: Database) -> dict[str, tuple[object, object]]:
    rows = db.execute(
        "SELECT account_id, has_position, as_of FROM core.dim_holdings_broker_reported"
    ).fetchall()
    return {str(r[0]): (r[1], r[2]) for r in rows}


def test_broker_reported_nonzero_quantity_is_true(guard_db: Database) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", "5", None)
    assert _broker(guard_db)["inv"][0] is True


def test_broker_reported_value_alone_is_true(guard_db: Database) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", None, "250.00")
    assert _broker(guard_db)["inv"][0] is True


def test_broker_reported_zero_row_liquidation_is_false(guard_db: Database) -> None:
    """The row exists and reports zero: row presence is not a position."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", "0", None, cost_basis="0")
    assert _broker(guard_db)["inv"] == (False, date(2026, 9, 1))


def test_broker_reported_empty_receipt_is_false(guard_db: Database) -> None:
    """Account-level no-row liquidation: a receipt, no rows for this account."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _plaid_account(guard_db, "inv_sibling", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv_sibling", "item_1", "sync_1", "3", None)
    result = _broker(guard_db)
    assert result["inv"][0] is False
    assert result["inv_sibling"][0] is True


def test_broker_reported_ignores_rows_from_an_older_snapshot(
    guard_db: Database,
) -> None:
    """Item-level liquidation: the newest pull wrote no rows, the old one did."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_old", "2026-08-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_old", "10", "1000.00")
    _receipt(guard_db, "item_1", "sync_new", "2026-09-01 10:00:00")
    assert _broker(guard_db)["inv"][0] is False


def test_broker_reported_all_null_rows_are_null(guard_db: Database) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", None, None)
    assert _broker(guard_db)["inv"][0] is None


def test_broker_reported_mixed_zero_and_null_rows_are_null(guard_db: Database) -> None:
    """Deviation 2: one decisive zero beside an all-NULL row is not a definitive zero."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", "0", None)
    _holding(guard_db, "inv", "item_1", "sync_1", None, None)
    assert _broker(guard_db)["inv"][0] is None


def test_broker_reported_never_pulled_investment_account_is_absent(
    guard_db: Database,
) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    assert "inv" not in _broker(guard_db)


def test_broker_reported_depository_sibling_never_inherits_the_receipt(
    guard_db: Database,
) -> None:
    """Receipt scope: a checking account on the item is not a confirmed zero."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _plaid_account(guard_db, "chk", "depository", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    assert "chk" not in _broker(guard_db)
    assert _broker(guard_db)["inv"][0] is False


def test_broker_reported_unresolved_type_with_a_position_is_true(
    guard_db: Database,
) -> None:
    """The positive branch is type-agnostic: NULL account_type still surfaces."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "odd", None, "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "odd", "item_1", "sync_1", "2", None)
    assert _broker(guard_db)["odd"][0] is True


def test_broker_reported_never_reads_cost_basis(guard_db: Database) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_1")
    _receipt(guard_db, "item_1", "sync_1", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_1", "sync_1", "0", "0", cost_basis="500.00")
    assert _broker(guard_db)["inv"][0] is False


def test_broker_reported_relink_true_outranks_a_sibling_zero_or_absence(
    guard_db: Database,
) -> None:
    """Rule 1 first: one origin's position wins over another's zero or silence."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_old")
    _plaid_account(guard_db, "inv", "investment", "item_new")
    _plaid_account(guard_db, "inv", "investment", "item_silent")
    _receipt(guard_db, "item_old", "sync_a", "2026-08-01 10:00:00")
    _receipt(guard_db, "item_new", "sync_b", "2026-09-01 10:00:00")
    _holding(guard_db, "inv", "item_new", "sync_b", "4", None)
    assert _broker(guard_db)["inv"] == (True, date(2026, 9, 1))


def test_broker_reported_relink_absent_origin_hides_a_sibling_zero(
    guard_db: Database,
) -> None:
    """Rule 2 before rule 3: one never-pulled origin blocks a definitive zero."""
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_old")
    _plaid_account(guard_db, "inv", "investment", "item_new")
    _receipt(guard_db, "item_old", "sync_a", "2026-08-01 10:00:00")
    assert "inv" not in _broker(guard_db)


def test_broker_reported_relink_zero_as_of_is_the_older_receipt(
    guard_db: Database,
) -> None:
    _install_broker_sources(guard_db)
    _plaid_account(guard_db, "inv", "investment", "item_old")
    _plaid_account(guard_db, "inv", "investment", "item_new")
    _receipt(guard_db, "item_old", "sync_a", "2026-08-01 10:00:00")
    _receipt(guard_db, "item_new", "sync_b", "2026-09-01 10:00:00")
    assert _broker(guard_db)["inv"] == (False, date(2026, 8, 1))
