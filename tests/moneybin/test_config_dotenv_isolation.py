"""Nothing in the test process may read a CWD-relative ``.env``.

pydantic-settings' default ``env_file`` source stats/reads ``.env`` from the
current working directory while a settings object is constructed. The sandbox
denies reads of the repo-root ``.env`` (it holds secrets), so any such source
makes every sandboxed test and CLI invocation die with
``PermissionError: Operation not permitted: '.env'``.

Two independent settings layers hit this:

- ``MoneyBinSettings`` — its real, profile-aware dotenv source is supplied by
  ``settings_customise_sources`` (keyed off ``get_base_dir()``), so the
  CWD-relative default is redundant and is disabled via ``env_file=None``.
- FastMCP's own settings — ``fastmcp/settings.py`` freezes
  ``env_file = os.getenv("FASTMCP_ENV_FILE", ".env")`` at import, and
  constructing the server's module-level ``FastMCP`` instance reads that CWD
  ``.env``. The root ``conftest.py`` sets ``FASTMCP_ENV_FILE`` to a non-file
  before FastMCP is imported so that read never fires.

The third layer is pytest collection itself: the root ``conftest.py``'s
``pytest_ignore_collect`` skips any ``.env`` file before the builtin hook stats
it. That branch is guarded here too — CI never sees a ``.env`` (it's gitignored),
so only a direct call exercises it.

All three mitigations are guarded below.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest

from moneybin.config import MoneyBinSettings


def _load_root_conftest() -> ModuleType:
    """Load the repo-root ``conftest.py`` as a module, by path.

    It is not importable as ``conftest`` from here — ``tests/conftest.py``
    shadows that name under pytest's prepend import mode — so load it explicitly
    from the repo root (three levels up from this file).
    """
    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location(
        "_moneybin_root_conftest", root / "conftest.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_settings_construction_does_not_read_cwd_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Constructing settings must not touch a ``.env`` in the current directory."""
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


def test_fastmcp_settings_construction_does_not_read_cwd_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """FastMCP's own settings must not read a CWD-relative ``.env`` either.

    Guards the root-conftest ``FASTMCP_ENV_FILE`` override. Without it, FastMCP's
    frozen ``env_file='.env'`` reads ``<cwd>/.env`` the moment its Settings are
    built — which happens on import of ``moneybin.mcp.server``.
    """
    from fastmcp.settings import Settings

    cwd = tmp_path / "cwd"
    cwd.mkdir()
    # chmod 0o000 mimics the sandbox deny-read: stat succeeds, open() raises
    # PermissionError — exactly what the repo-root .env does under the sandbox.
    bad_env = cwd / ".env"
    bad_env.write_text("FASTMCP_LOG_LEVEL=DEBUG\n")
    bad_env.chmod(0o000)
    monkeypatch.chdir(cwd)

    # Must construct without reading the unreadable CWD .env.
    settings = Settings()

    assert settings is not None


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (".env", True),
        (".env.local", True),
        (".env.dev", True),
        ("conftest.py", None),
        ("test_config_dotenv_isolation.py", None),
    ],
)
def test_pytest_ignore_collect_skips_dotenv_files(
    name: str, expected: bool | None
) -> None:
    """``pytest_ignore_collect`` ignores ``.env`` / ``.env.*`` and nothing else.

    This is the branch CI can never reach: ``.env`` is gitignored, so a CI
    checkout never contains one and the full-suite pass only exercises the
    ``return None`` path. Only a sandboxed run with a real ``.env`` present hits
    the ``return True`` branch — the exact failure mode this file guards. Calling
    the hook directly locks the predicate, so a regression (e.g. tightening to
    ``== ".env"``, which would stop ignoring ``.env.dev``) fails here, in CI.
    """
    conftest = _load_root_conftest()

    assert conftest.pytest_ignore_collect(Path(name)) is expected
