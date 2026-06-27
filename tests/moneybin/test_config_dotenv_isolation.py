"""The settings layer must not read a CWD-relative `.env`.

pydantic-settings' default `env_file` source stats/reads `.env` from the current
working directory while `MoneyBinSettings` is constructed. The sandbox denies
reads of the repo-root `.env` (it holds secrets), so that default source makes
every sandboxed test and CLI invocation die with
`PermissionError: Operation not permitted: '.env'`. The real, profile-aware
dotenv source is supplied by `settings_customise_sources` (keyed off
`get_base_dir()`), so the CWD-relative default is redundant — and must not fire.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.config import MoneyBinSettings


def test_settings_construction_does_not_read_cwd_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Constructing settings must not touch a `.env` in the current directory."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    # An unreadable .env mimics the sandbox deny-read on the repo-root .env:
    # the read attempt raises PermissionError, exactly as the sandbox does.
    bad_env = cwd / ".env"
    bad_env.write_text("MONEYBIN_DATABASE__PATH=/should/not/be/read\n")
    bad_env.chmod(0o000)
    monkeypatch.chdir(cwd)
    # MONEYBIN_HOME points at a clean dir with no .env, so the profile-aware
    # source from settings_customise_sources finds nothing to read.
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("MONEYBIN_HOME", str(home))

    # Must construct without reading the unreadable CWD .env.
    settings = MoneyBinSettings(profile="dev")

    assert settings is not None
