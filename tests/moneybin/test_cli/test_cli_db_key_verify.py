"""Effective credential verification against a real synthetic encrypted file."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from keyring.errors import KeyringError
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import (
    MoneyBinSettings,
    clear_current_profile,
    clear_settings_cache,
)
from moneybin.database import Database
from moneybin.secrets import SecretStore

_KEY = "synthetic-verification-key-aaaaaaaa"
_WRONG_KEY = "synthetic-verification-key-bbbbbbbb"


@pytest.fixture
def verification_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))

    def missing_password(*_: object) -> None:
        return None

    monkeypatch.setattr("moneybin.secrets.keyring.get_password", missing_password)
    monkeypatch.setenv("MONEYBIN_PROFILE__VERIFY__DATABASE__ENCRYPTION_KEY", _KEY)

    def forbid_secret_mutation(*_: object) -> None:
        pytest.fail("verification attempted to change a credential")

    monkeypatch.setattr("moneybin.secrets.keyring.set_password", forbid_secret_mutation)
    monkeypatch.setattr(
        "moneybin.secrets.keyring.delete_password", forbid_secret_mutation
    )
    profile = tmp_path / "profiles" / "verify"
    profile.mkdir(parents=True)
    settings = MoneyBinSettings(profile="verify")
    with Database(
        settings.database.path,
        read_only=False,
        secret_store=SecretStore(profile="verify"),
        no_auto_upgrade=True,
    ) as db:
        db.execute("CREATE TABLE verification_marker AS SELECT 17 AS value")
    clear_settings_cache()
    return tmp_path


def _state(home: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(home)): path.read_bytes() if path.is_file() else b""
        for path in home.rglob("*")
        if "logs" not in path.parts
        and not path.name.startswith(("privacy.", ".privacy."))
    }


@pytest.mark.parametrize("output", ["text", "json"])
@pytest.mark.integration
def test_verify_reads_existing_database_without_mutation(
    verification_home: Path, runner: CliRunner, output: str
) -> None:
    before = _state(verification_home)
    result = runner.invoke(
        app, ["--profile", "verify", "db", "key", "verify", "-o", output]
    )
    assert result.exit_code == 0, result.output
    if output == "json":
        payload = json.loads(result.stdout)
        assert payload["data"]["verified"] is True
        assert payload["data"]["profile"] == "verify"
    else:
        assert "verified" in result.stdout.lower()
    assert _KEY not in result.output
    assert _state(verification_home) == before


@pytest.mark.parametrize(
    "failure", ["wrong", "missing", "unavailable", "corrupt", "absent"]
)
@pytest.mark.parametrize("output", ["text", "json"])
@pytest.mark.integration
def test_verify_reports_failures_without_mutation(
    verification_home: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    output: str,
) -> None:
    path = MoneyBinSettings(profile="verify").database.path
    if failure == "wrong":
        monkeypatch.setenv(
            "MONEYBIN_PROFILE__VERIFY__DATABASE__ENCRYPTION_KEY", _WRONG_KEY
        )
    elif failure in ("missing", "unavailable"):
        monkeypatch.delenv("MONEYBIN_PROFILE__VERIFY__DATABASE__ENCRYPTION_KEY")
        if failure == "unavailable":

            def denied(*_: object) -> str:
                raise KeyringError("synthetic denial")

            monkeypatch.setattr("moneybin.secrets.keyring.get_password", denied)
    elif failure == "corrupt":
        path.write_bytes(b"synthetic corrupt file")
    else:
        path.unlink()
    before = _state(verification_home)
    result = runner.invoke(
        app, ["--profile", "verify", "db", "key", "verify", "-o", output]
    )
    assert result.exit_code == 1, result.output
    assert "not yet implemented" not in result.output.lower()
    if output == "json":
        assert json.loads(result.stdout)["error"] is not None
    else:
        assert result.output.strip()
    assert _KEY not in result.output
    assert _WRONG_KEY not in result.output
    assert _state(verification_home) == before


@pytest.mark.integration
def test_verify_does_not_accept_stale_cached_credential(
    verification_home: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from moneybin.config import set_current_profile

    set_current_profile("verify")
    monkeypatch.setattr("moneybin.database._cached_encryption_key", ("verify", _KEY))
    monkeypatch.setenv("MONEYBIN_PROFILE__VERIFY__DATABASE__ENCRYPTION_KEY", _WRONG_KEY)
    result = runner.invoke(
        app, ["--profile", "verify", "db", "key", "verify", "-o", "json"]
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] is not None


@pytest.mark.integration
def test_verify_profile_isolation(
    verification_home: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other = verification_home / "profiles" / "other"
    other.mkdir()
    monkeypatch.setenv("MONEYBIN_PROFILE__OTHER__DATABASE__ENCRYPTION_KEY", _WRONG_KEY)
    source = MoneyBinSettings(profile="verify").database.path
    target = MoneyBinSettings(profile="other").database.path
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(source.read_bytes())
    result = runner.invoke(
        app, ["--profile", "other", "db", "key", "verify", "-o", "json"]
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] is not None


def test_verify_unconfigured_home_never_initializes(
    tmp_path: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    clear_settings_cache()
    clear_current_profile()
    result = runner.invoke(app, ["db", "key", "verify", "-o", "json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] is not None
    assert not (tmp_path / "config.yaml").exists()
    assert not (tmp_path / "profiles").exists()


@pytest.mark.e2e
def test_installed_verify_uses_environment_credential_without_writes(
    verification_home: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import os
    import subprocess  # noqa: S404  # run installed console against synthetic temporary data
    import sys

    monkeypatch.setenv(
        "PYTHON_KEYRING_BACKEND", "tests.e2e.memory_keyring.MemoryKeyring"
    )
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[3]))
    before = _state(verification_home)
    result = subprocess.run(  # noqa: S603  # fixed installed console, synthetic data only
        [
            str(Path(sys.executable).parent / "moneybin"),
            "--profile",
            "verify",
            "db",
            "key",
            "verify",
            "-o",
            "json",
        ],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["data"]["verified"] is True
    assert _KEY not in result.stdout + result.stderr
    assert _state(verification_home) == before


@pytest.mark.integration
def test_verify_keeps_keychain_precedence_over_environment(
    verification_home: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MONEYBIN_PROFILE__VERIFY__DATABASE__ENCRYPTION_KEY", _WRONG_KEY)

    def stored_password(*_: object) -> str:
        return _KEY

    monkeypatch.setattr("moneybin.secrets.keyring.get_password", stored_password)
    result = runner.invoke(
        app, ["--profile", "verify", "db", "key", "verify", "-o", "json"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["verified"] is True


@pytest.mark.parametrize("selection", ["saved", "environment"])
@pytest.mark.integration
def test_verify_resolves_existing_profile_without_initialization(
    verification_home: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    selection: str,
) -> None:
    from moneybin.utils.user_config import set_default_profile

    if selection == "saved":
        set_default_profile("verify")
    else:
        monkeypatch.setenv("MONEYBIN_PROFILE", "verify")
    clear_settings_cache()
    before = _state(verification_home)
    result = runner.invoke(app, ["db", "key", "verify", "-o", "json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["data"]["profile"] == "verify"
    assert _state(verification_home) == before


def test_verify_missing_profile_does_not_create_it(
    tmp_path: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    clear_settings_cache()
    result = runner.invoke(
        app, ["--profile", "absent", "db", "key", "verify", "-o", "json"]
    )
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] is not None
    assert not (tmp_path / "profiles").exists()


@pytest.mark.integration
def test_verify_does_not_disable_later_cli_profile_validation(
    verification_home: Path,
    runner: CliRunner,
) -> None:
    verified = runner.invoke(
        app, ["--profile", "verify", "db", "key", "verify", "-o", "json"]
    )
    assert verified.exit_code == 0, verified.output
    later = runner.invoke(app, ["--profile", "absent", "logs", "--print-path"])
    assert later.exit_code == 1
    assert "does not exist" in later.output
    assert not (verification_home / "profiles" / "absent").exists()


def test_verify_invalid_profile_never_initializes(
    tmp_path: Path,
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
    clear_settings_cache()
    result = runner.invoke(
        app, ["--profile", "///", "db", "key", "verify", "-o", "json"]
    )
    assert result.exit_code == 2
    assert _state(tmp_path) == {}
