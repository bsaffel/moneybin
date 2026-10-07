"""Fresh-process inspection and SQL-side candidate narrowing."""

import json
import subprocess  # noqa: S404  # fresh-interpreter persistence test
import sys
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.services.investment_matching_service import InvestmentMatchingService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


@pytest.mark.integration
def test_pending_evidence_survives_writer_process_lifetime(
    comparison_db: Database, tmp_path: Path
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    InvestmentMatchingService(comparison_db).run()
    original = InvestmentMatchingService(comparison_db).pending()[0]["proposal_id"]
    comparison_db.close()
    script = """
import json, sys
from pathlib import Path
from unittest.mock import MagicMock
from moneybin.database import Database
from moneybin.services.investment_matching_service import InvestmentMatchingService
store = MagicMock()
store.get_key.return_value = 'test-encryption-key-for-unit-tests'
with Database(Path(sys.argv[1]), secret_store=store, read_only=True, no_auto_upgrade=True) as db:
    rows = InvestmentMatchingService(db).pending()
    print(json.dumps(rows, default=str))
"""
    result = subprocess.run(  # noqa: S603  # fixed interpreter and code, test-owned path
        [sys.executable, "-c", script, str(tmp_path / "test.duckdb")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    assert len(rows) == 1
    assert rows[0]["proposal_id"] == original
    assert rows[0]["status"] == "pending"
    assert len(rows[0]["legs"]) == 2


def _choose_source(db: Database, account_id: str, choice: str | None) -> None:
    """Save an account's investment source choice in app.account_settings."""
    db.execute(
        "INSERT INTO app.account_settings (account_id, investment_source_type) "
        "VALUES (?, ?) ON CONFLICT (account_id) DO UPDATE SET "
        "investment_source_type = EXCLUDED.investment_source_type",
        [account_id, choice],
    )


def _show_built_choice(db: Database, account_id: str, choice: str | None) -> None:
    """Set the choice the built core.dim_accounts shows, independent of settings."""
    columns = {
        row[0]
        for row in db.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE schema_name = 'core' AND table_name = 'dim_accounts'"
        ).fetchall()
    }
    if "investment_source_type" not in columns:
        db.execute(
            "ALTER TABLE core.dim_accounts ADD COLUMN investment_source_type VARCHAR"
        )
    db.execute(
        "UPDATE core.dim_accounts SET investment_source_type = ? WHERE account_id = ?",
        [choice, account_id],
    )


def test_a_source_choice_takes_the_account_out_of_planning(
    comparison_db: Database,
) -> None:
    """A chosen account has one ledger source, so a proposal there is noise."""
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    assert service.run().proposed == 1
    assert len(service.pending()) == 1

    # Negative: a choice on some other account leaves this proposal alone.
    _choose_source(comparison_db, "unrelated_account", "manual")
    assert service.run().stale == 0
    assert len(service.pending()) == 1

    _choose_source(comparison_db, "account", "manual")
    result = service.run()
    assert result.stale == 1
    assert result.proposed == 0
    assert service.pending() == []

    # Clearing the choice returns the account to the planner.
    _choose_source(comparison_db, "account", None)
    assert service.run().proposed == 1
    assert len(service.pending()) == 1


def test_the_saved_choice_wins_over_a_dim_that_has_not_been_rebuilt(
    comparison_db: Database,
) -> None:
    """Matching runs before the transform, so the saved setting is authoritative."""
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMatchingService(comparison_db)

    # Saved but not yet restated: the dim still shows no choice.
    _choose_source(comparison_db, "account", "manual")
    _show_built_choice(comparison_db, "account", None)
    result = service.run()
    assert result.proposed == 0
    assert service.pending() == []

    # Reverse: the dim is stale with a choice, but the setting was cleared.
    _choose_source(comparison_db, "account", None)
    _show_built_choice(comparison_db, "account", "manual")
    assert service.run().proposed == 1
    assert len(service.pending()) == 1


def test_unrelated_event_rows_do_not_cross_into_python_planning(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    import moneybin.services.investment_matching_service as service_module

    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    seed_manual_event(comparison_db, "unrelated", day=25)
    install_comparison_models(comparison_db)
    observed: list[tuple[int, int]] = []
    original = service_module.evaluate_plan

    def observe(headers: Any, legs: Any, evidence: Any, **kwargs: Any) -> Any:
        observed.append((len(headers), len(legs)))
        return original(headers, legs, evidence, **kwargs)

    monkeypatch.setattr(service_module, "evaluate_plan", observe)
    assert InvestmentMatchingService(comparison_db).run().proposed == 1
    assert observed == [(2, 2)]
