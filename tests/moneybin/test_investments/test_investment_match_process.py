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
