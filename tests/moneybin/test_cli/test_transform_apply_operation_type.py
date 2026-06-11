"""Verify ``moneybin transform apply`` classifies its write open as transform_apply.

Canary test for the three transform-class entry points that pass
``operation_type="transform_apply"`` to ``get_database`` (the main
``transform apply`` CLI, the ``refresh`` CLI umbrella, and the MCP
``refresh_run`` umbrella). The kwarg flow itself — from get_database
through ``write_lock`` to the file-lock metadata and the
``moneybin_db_write_lock_timeout_total{operation_type}`` label — is
exercised by ``tests/moneybin/test_db_lock/test_lock.py``. This test pins
the call site so a future refactor can't drop the kwarg silently and
collapse all transform runs into the generic ``"interactive"`` bucket.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch


def test_transform_apply_command_classifies_as_transform_apply() -> None:
    """``moneybin transform apply`` opens the write lock as ``transform_apply``."""
    from typer.testing import CliRunner

    from moneybin.cli.commands.transform import app as transform_app

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
        patch("moneybin.database.get_database", fake_get_database),
        patch("moneybin.services.transform_service.TransformService") as mock_service,
    ):
        mock_service.return_value.apply.return_value = MagicMock(
            applied=True, duration_seconds=0.0, error=None
        )
        result = runner.invoke(transform_app, ["apply"])
    assert result.exit_code == 0, result.stdout
    assert captured["read_only"] is False
    assert captured["operation_type"] == "transform_apply"
