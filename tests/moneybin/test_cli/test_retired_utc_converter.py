"""Installed UTC enforcement has no supported one-time conversion surface."""

from importlib.resources import files
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.db import app as db_app
from moneybin.cli.commands.migrate import app as migrate_app
from moneybin.database import Database


@pytest.mark.parametrize(
    "command", ["utc-inspect", "utc-prepare", "utc-apply", "utc-recover"]
)
def test_conversion_commands_are_unavailable(command: str) -> None:
    result = CliRunner().invoke(migrate_app, [command, "--help"])
    assert result.exit_code == 2, result.output
    assert "No such command" in result.output


def test_restore_help_exposes_offline_recovery() -> None:
    result = CliRunner().invoke(db_app, ["restore", "--help"])
    assert result.exit_code == 0, result.output
    assert "--recover" in result.output
    assert "source or destination" in result.output.replace("\n", " ")


@pytest.mark.parametrize("selection", [["--from", "backup.duckdb"], ["--latest"]])
def test_restore_recovery_refuses_backup_selection(selection: list[str]) -> None:
    result = CliRunner().invoke(db_app, ["restore", "--recover", *selection])
    assert result.exit_code == 2, result.output
    assert "--recover cannot be combined" in result.output


def test_database_constructor_has_no_conversion_bypass(tmp_path: Path) -> None:
    path = tmp_path / "never-created.duckdb"
    with pytest.raises(TypeError, match="_utc_upgrade"):
        Database(path, read_only=True, _utc_upgrade=None)  # type: ignore[call-arg]  # removed private bypass must fail before opening
    assert not path.exists()


def test_installed_package_has_no_converter_or_conversion_migration() -> None:
    package = files("moneybin")
    retired = [
        "utc_upgrade.py",
        "services/utc_upgrade_service.py",
        "services/utc_upgrade_lock.py",
        "sql/migrations/V080__normalize_utc_instants.py",
    ]
    assert not [name for name in retired if package.joinpath(name).is_file()]
