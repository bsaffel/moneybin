"""Tests for `profile set`/`profile show` managed-setting dispatch.

`profile set` is one front door over two stores: dotted `section.field` keys
write `config.yaml`, undotted managed keys write `app.profile_settings`.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from moneybin import config
from moneybin.cli.commands.profile import app
from moneybin.cli.main import app as root_app
from moneybin.database import Database
from moneybin.services.profile_settings_service import ProfileSettingsService

runner = CliRunner()


@pytest.fixture()
def profile_home(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db: Database
) -> Generator[Path, None, None]:
    """An active profile named 'alice' whose database is the `db` fixture."""
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    profile_dir = tmp_path / "profiles" / "alice"
    profile_dir.mkdir(parents=True)
    (profile_dir / "config.yaml").write_text("logging:\n  level: INFO\n")
    (profile_dir / "moneybin.duckdb").touch()

    @contextmanager
    def _fake_get_database(**_kwargs: object) -> Generator[Database, None, None]:
        yield db

    with (
        patch(
            "moneybin.cli.commands.profile.get_current_profile", return_value="alice"
        ),
        patch(
            "moneybin.services.profile_service.get_default_profile",
            return_value="alice",
        ),
        patch("moneybin.cli.commands.profile.get_database", _fake_get_database),
    ):
        yield profile_dir


def test_managed_key_writes_the_database_not_config_yaml(
    profile_home: Path, db: Database
) -> None:
    """`profile set home_currency EUR` lands in app.profile_settings.

    The whole point of the dispatch: a managed key must not become a dead
    config.yaml entry that no report or guard ever reads.
    """
    result = runner.invoke(app, ["set", "home_currency", "EUR"])

    assert result.exit_code == 0
    assert ProfileSettingsService(db).get_settings().home_currency == "EUR"

    config = yaml.safe_load((profile_home / "config.yaml").read_text())
    assert "home_currency" not in config
    assert config["logging"]["level"] == "INFO"


def test_dotted_key_still_writes_config_yaml(profile_home: Path, db: Database) -> None:
    """The existing config path is untouched by the dispatch."""
    result = runner.invoke(app, ["set", "logging.level", "DEBUG"])

    assert result.exit_code == 0

    config = yaml.safe_load((profile_home / "config.yaml").read_text())
    assert config["logging"]["level"] == "DEBUG"
    assert ProfileSettingsService(db).get_settings().home_currency is None


def test_invalid_currency_is_refused_with_a_usable_message(
    profile_home: Path, db: Database
) -> None:
    """A malformed code exits non-zero and never reaches the table."""
    result = runner.invoke(app, ["set", "home_currency", "euros"])

    assert result.exit_code == 1
    assert ProfileSettingsService(db).get_settings().home_currency is None


def test_show_reports_the_home_currency_from_the_database(
    profile_home: Path, db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """`profile show` surfaces managed settings, labelled apart from config."""
    ProfileSettingsService(db).set_setting("home_currency", "GBP", actor="test")

    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
        result = runner.invoke(app, ["show"])

    assert result.exit_code == 0
    assert "Settings (database):" in caplog.text
    assert "home_currency: GBP" in caplog.text
    assert "Config (config.yaml):" in caplog.text


def test_show_survives_a_database_that_predates_the_settings_table(
    profile_home: Path, db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """`profile show` on a pre-V044 database prints settings, not a traceback.

    `profile show` reads through `get_database(read_only=True)`, and read-only
    opens skip both `init_schemas` and the migration runner — so an existing
    user who upgrades and runs this before any write command meets a database
    with no `app.profile_settings`. The unset home currency is the correct
    answer there; a raw catalog error is not.
    """
    db.execute("DROP TABLE app.profile_settings")

    with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
        result = runner.invoke(app, ["show"])

    assert result.exit_code == 0
    assert result.exception is None
    assert "home_currency: (not set)" in caplog.text


def test_managed_key_on_a_non_active_profile_is_refused(
    profile_home: Path, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A managed key targets the active profile's database, or errors clearly.

    `profile set --profile other` works for config.yaml because that is a plain
    file. A managed key lives in the other profile's encrypted database, which
    this process has not opened — silently writing the active profile's value
    instead would be the worst outcome.
    """
    other = tmp_path / "profiles" / "other"
    other.mkdir(parents=True)
    (other / "config.yaml").write_text("logging:\n  level: INFO\n")

    with caplog.at_level(logging.ERROR, logger="moneybin.cli.commands.profile"):
        result = runner.invoke(
            app, ["set", "home_currency", "EUR", "--profile", "other"]
        )

    assert result.exit_code == 1
    assert "moneybin profile switch other" in caplog.text


@pytest.fixture()
def _no_ambient_profile(  # pyright: ignore[reportUnusedFunction]  # pytest fixture referenced by usefixtures name
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None, None, None]:
    """No profile activated in module state, as on a fresh CLI process."""
    monkeypatch.setattr(config, "_current_profile", None, raising=False)
    monkeypatch.setattr(config, "_profile_resolver", None, raising=False)
    yield


@pytest.mark.usefixtures("_no_ambient_profile")
def test_profile_set_managed_key_works_without_an_explicit_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, db: Database
) -> None:
    """`moneybin profile set home_currency EUR` works in the single-profile case.

    Driven through the root app so `main_callback` runs, and deliberately
    without patching `get_current_profile`: the `profile` group skips lazy
    profile resolution, so module state stays unset while ProfileService
    resolves the persisted default from disk. Every existing test in this file
    patches over that gap, which is why the suite stayed green while the
    documented invocation raised an unclassified RuntimeError out of
    get_settings().
    """
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)
    profile_dir = tmp_path / "profiles" / "alice"
    profile_dir.mkdir(parents=True)
    (profile_dir / "config.yaml").write_text("logging:\n  level: INFO\n")
    (profile_dir / "moneybin.duckdb").touch()

    # The real get_database() calls get_settings(), which needs an activated
    # profile; substituting it here would hide exactly the failure under test.
    # So record what the ambient profile *is* at the moment the command opens
    # the database — unset is what raises RuntimeError in production.
    opened_under: list[str | None] = []

    @contextmanager
    def _fake_get_database(**_kwargs: object) -> Generator[Database, None, None]:
        try:
            opened_under.append(config.get_current_profile(auto_resolve=False))
        except RuntimeError:
            opened_under.append(None)
        yield db

    with (
        patch(
            "moneybin.services.profile_service.get_default_profile",
            return_value="alice",
        ),
        patch("moneybin.cli.commands.profile.get_database", _fake_get_database),
    ):
        result = runner.invoke(root_app, ["profile", "set", "home_currency", "EUR"])

    assert result.exit_code == 0, result.output
    assert opened_under == ["alice"], (
        "the command must activate the profile it resolved before opening its "
        "database; None here is the unclassified RuntimeError users hit"
    )
    assert ProfileSettingsService(db).get_settings().home_currency == "EUR"
