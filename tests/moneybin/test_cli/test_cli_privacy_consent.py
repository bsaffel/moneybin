"""CLI tests for `moneybin privacy` consent commands (grant/revoke/status/log)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = make_curation_db(tmp_path)
    patch_db(monkeypatch, database)
    yield database
    database.close()


def _set_backend(monkeypatch: pytest.MonkeyPatch, backend: str = "anthropic") -> None:
    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", backend)
    clear_settings_cache()
    set_current_profile("test")


def test_cli_grant_then_status_json(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    result = runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert status.exit_code == 0, status.output
    body = json.loads(status.stdout)["data"]
    assert body["default_backend"] == "anthropic"
    cats = {g["feature_category"] for g in body["active_grants"]}
    assert "mcp-data-sharing" in cats


def test_cli_revoke(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    result = runner.invoke(app, ["privacy", "revoke", "mcp-data-sharing", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert json.loads(status.stdout)["data"]["active_grants"] == []


def test_cli_status_json_empty(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["active_grants"] == []
    # No configured backend → null, not a "(none)" sentinel that could be fed
    # back into grant/revoke as a real backend.
    assert data["default_backend"] is None


def test_cli_revoke_all(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    runner.invoke(app, ["privacy", "grant", "ml-categorization", "--yes"])
    result = runner.invoke(app, ["privacy", "revoke-all", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert json.loads(status.stdout)["data"]["active_grants"] == []


def test_cli_log_after_grant(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    result = runner.invoke(app, ["privacy", "log", "--last", "20"])
    assert result.exit_code == 0, result.output
    assert "consent.grant" in result.output
