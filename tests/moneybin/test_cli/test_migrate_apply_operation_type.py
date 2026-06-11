"""Verify ``moneybin migrate apply`` classifies its write open as migration.

Canary test pinning the migrate-apply call site that passes
``operation_type="migration"`` to ``get_database``. Mirrors
``test_transform_apply_operation_type.py``: a future refactor that drops the
kwarg would silently collapse migration runs into the generic ``"interactive"``
bucket and make the ``moneybin_db_write_lock_timeout_total{operation_type}``
metric label unreliable. ``migrate.py`` imports ``get_database`` at module
scope, so the patch targets its local binding (not
``moneybin.database.get_database``, which ``transform.py``'s function-scoped
import re-resolves at call time).
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch


def test_migrate_apply_command_classifies_as_migration() -> None:
    """``moneybin migrate apply`` opens the write lock as ``migration``."""
    from typer.testing import CliRunner

    from moneybin.cli.commands.migrate import app as migrate_app

    captured: dict[str, Any] = {}

    @contextmanager
    def fake_get_database(
        *, read_only: bool, **kwargs: Any
    ) -> Generator[MagicMock, None, None]:
        captured["read_only"] = read_only
        captured["operation_type"] = kwargs.get("operation_type")
        yield MagicMock()

    runner = CliRunner()
    with (
        patch("moneybin.cli.commands.migrate.get_database", fake_get_database),
        patch("moneybin.cli.commands.migrate.MigrationRunner") as mock_runner,
    ):
        mock_runner.return_value.apply_all.return_value = MagicMock(
            failed=False, applied_count=0
        )
        mock_runner.return_value.check_drift.return_value = []
        result = runner.invoke(migrate_app, ["apply"])
    assert result.exit_code == 0, result.stdout
    assert captured["read_only"] is False
    assert captured["operation_type"] == "migration"
