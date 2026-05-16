# ruff: noqa: S101
"""Integration test: MCP boot detects drift and exercises the self-heal path.

Builds a SQLMesh-applied DB via ImportService and simulates a drifted live
view, then asserts ``check_schema_at_boot`` walks the full self-heal flow:
detect drift → invoke ``TransformService.apply()`` → re-verify. End-to-end
"heal restores drift" relies on a real SQLMesh model-fingerprint change
(the production trigger) which can't be reproduced from tmp_path without
copying the model tree; that case lives in manual verification. This test
guarantees the boot helper runs the heal pipeline under a live SQLMesh
context and surfaces a clear error when the heal cannot resolve the
divergence.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, SchemaDriftError

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "tabular"


@pytest.mark.integration
def test_boot_check_runs_self_heal_under_real_sqlmesh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Boot helper triggers SQLMesh restate when live core.dim_accounts drifts."""
    from moneybin.mcp.server import check_schema_at_boot
    from moneybin.services.import_service import ImportService
    from moneybin.services.transform_service import TransformService

    secret_store = MagicMock()
    secret_store.get_key.return_value = "integration-test-key-0123456789abcdef"

    db_path = tmp_path / "drift.duckdb"
    db = Database(db_path, secret_store=secret_store)

    # sqlmesh_context() reads get_settings().database.path to key the
    # adapter cache. Point it at this test DB so SQLMesh reuses the
    # encrypted connection. get_database() (called by the boot helper)
    # also reads get_settings() for the path and instantiates a fresh
    # SecretStore() — patch SecretStore so the boot helper finds the same
    # encryption key without hitting the real keyring.
    mock_settings = MagicMock()
    mock_settings.database.path = db_path
    monkeypatch.setattr("moneybin.database.get_settings", lambda: mock_settings)
    monkeypatch.setattr("moneybin.database.SecretStore", lambda: secret_store)

    fixture = FIXTURES_DIR / "standard.csv"
    assert fixture.exists(), f"missing fixture: {fixture}"

    result = ImportService(db).import_file(
        fixture, account_name="checking", auto_accept=True
    )
    assert result.core_tables_rebuilt, "transforms must run to materialize core.*"

    # Simulate live-view drift by replacing the SQLMesh-managed view with
    # one that omits display_name. The underlying physical snapshot is
    # untouched, but the view's frozen column list now diverges from
    # EXPECTED_CORE_COLUMNS.
    db.execute(
        "CREATE OR REPLACE TABLE core._dim_accounts_drift AS "
        "SELECT * EXCLUDE (display_name) FROM core.dim_accounts"
    )
    db.execute("DROP VIEW core.dim_accounts")
    db.execute(
        "CREATE VIEW core.dim_accounts AS SELECT * FROM core._dim_accounts_drift"
    )
    db.close()

    # Count apply() invocations to confirm the heal pipeline ran under a
    # real SQLMesh context (not a mock).
    apply_calls = 0
    original_apply = TransformService.apply

    def _counting_apply(self: TransformService) -> object:
        nonlocal apply_calls
        apply_calls += 1
        return original_apply(self)

    monkeypatch.setattr(TransformService, "apply", _counting_apply)

    # SQLMesh's regular plan sees no model-file changes (the simulation
    # only tampered with the live view), so the plain apply re-promotes
    # nothing. The post-heal re-verify is expected to still see drift,
    # and the boot helper surfaces a clear "persist after auto-heal"
    # error. This is the safety net: detect, attempt, escalate.
    with pytest.raises(SchemaDriftError, match="persist after auto-heal"):
        check_schema_at_boot()

    assert apply_calls == 1, (
        f"expected exactly one TransformService.apply() call, got {apply_calls}"
    )
