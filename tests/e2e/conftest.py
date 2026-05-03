"""Shared fixtures for E2E subprocess tests.

These tests run `uv run moneybin ...` as a real subprocess to catch
boot, schema, and init wiring bugs that in-process tests miss.
"""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess  # noqa: S404 — subprocess is intentional; we invoke the moneybin entrypoint directly
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

# Resolve the moneybin entrypoint inside the active venv. Tests run under
# `uv run pytest`, which prepends `.venv/bin` to PATH and sets sys.executable
# to the venv's python — so its sibling `moneybin` is what we want. Calling
# the script directly skips uv's per-invocation project-resolve overhead
# (~100-200ms × 100+ calls in the E2E suite).
_VENV_BIN = Path(sys.executable).parent
_MONEYBIN_BIN = _VENV_BIN / "moneybin"
if not _MONEYBIN_BIN.exists():
    msg = (
        f"moneybin entrypoint not found at {_MONEYBIN_BIN}. "
        f"Run `uv sync` to populate the venv before running E2E tests."
    )
    raise RuntimeError(msg)

# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CLIResult:
    """Result from a CLI subprocess invocation."""

    exit_code: int
    stdout: str
    stderr: str

    @property
    def output(self) -> str:
        """Combined stdout + stderr for simple assertions."""
        return self.stdout + self.stderr

    def assert_success(self) -> None:
        """Assert the command exited 0 with no Python tracebacks."""
        assert "Traceback (most recent call last)" not in self.stderr, (
            f"Python traceback in stderr:\n{self.stderr}"
        )
        assert self.exit_code == 0, (
            f"Expected exit code 0, got {self.exit_code}\n"
            f"stdout: {self.stdout}\nstderr: {self.stderr}"
        )


# ---------------------------------------------------------------------------
# CLI runner
# ---------------------------------------------------------------------------

FAST_ARGON2_ENV = {
    "MONEYBIN_DATABASE__ARGON2_TIME_COST": "1",
    "MONEYBIN_DATABASE__ARGON2_MEMORY_COST": "8192",
    "MONEYBIN_DATABASE__ARGON2_PARALLELISM": "1",
    # Use in-memory keyring backend so E2E subprocess tests don't touch the
    # real system keychain but set_key/get_key round-trips still work.
    # PYTHONPATH ensures the subprocess can import tests.e2e.memory_keyring.
    "PYTHON_KEYRING_BACKEND": "tests.e2e.memory_keyring.MemoryKeyring",
    "PYTHONPATH": str(Path(__file__).resolve().parent.parent.parent)
    + os.pathsep
    + os.environ.get("PYTHONPATH", ""),
}

TEST_ENCRYPTION_KEY = (
    "e2e-test-key-0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab"  # noqa: S105 — test-only key, not a real secret
)

TEST_PASSPHRASE = "e2e-test-passphrase-1234"  # noqa: S105 — test-only passphrase, not a real secret

# Fallback MONEYBIN_HOME for tests that don't provide their own env.
# Prevents the first-run setup wizard from intercepting CLI commands.
_FALLBACK_HOME = tempfile.mkdtemp(prefix="moneybin-e2e-fallback-")
atexit.register(shutil.rmtree, _FALLBACK_HOME, ignore_errors=True)
_FALLBACK_PROFILE = "e2e-fallback"
_fallback_profile_created = False


def base_env(home: Path, profile: str) -> dict[str, str]:
    """Base environment dict for E2E tests with encryption key."""
    return {
        "MONEYBIN_HOME": str(home),
        "MONEYBIN_PROFILE": profile,
        "MONEYBIN_DATABASE__ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
    }


def run_cli(
    *args: str,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    timeout: int = 120,
) -> CLIResult:
    """Run a moneybin CLI command as a subprocess.

    Args:
        *args: CLI arguments (e.g., "profile", "list").
        env: Environment variables (merged with os.environ).
        input_text: Text to pipe to stdin.
        timeout: Seconds before killing the process.

    Returns:
        CLIResult with exit_code, stdout, stderr.
    """
    cmd = [str(_MONEYBIN_BIN), *args]
    # When no env is provided, override MONEYBIN_PROFILE and MONEYBIN_HOME
    # to prevent the first-run setup wizard and isolate from the user's
    # real profile. The fallback profile dir is created on first use.
    if env is None:
        global _fallback_profile_created  # noqa: PLW0603 — lazy init for module-level state
        if not _fallback_profile_created:
            Path(_FALLBACK_HOME, "profiles", _FALLBACK_PROFILE).mkdir(
                parents=True, exist_ok=True
            )
            _fallback_profile_created = True
        env = {
            "MONEYBIN_HOME": _FALLBACK_HOME,
            "MONEYBIN_PROFILE": _FALLBACK_PROFILE,
        }
    full_env = {**os.environ, **FAST_ARGON2_ENV, **env}

    result = subprocess.run(  # noqa: S603 — input is controlled test commands, not user input
        cmd,
        capture_output=True,
        text=True,
        input=input_text,
        stdin=subprocess.DEVNULL if input_text is None else None,
        timeout=timeout,
        env=full_env,
        # Detach from controlling terminal so getpass (used by
        # hide_input=True prompts) falls back to reading stdin
        # instead of /dev/tty. Without this, piped input_text is
        # ignored and the subprocess hangs waiting for terminal input.
        start_new_session=True,
    )
    return CLIResult(
        exit_code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def e2e_home(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Temporary MONEYBIN_HOME — auto-removed after the test session."""
    return tmp_path_factory.mktemp("e2e_home")


@pytest.fixture(scope="session")
def e2e_env(e2e_home: Path) -> dict[str, str]:
    """Temp MONEYBIN_HOME with a profile created (but no DB initialized).

    Sets MONEYBIN_PROFILE so commands don't trigger ensure_default_profile()
    or fall through to the user's real profile. Includes the test encryption
    key so commands that touch the DB can create/open it.
    """
    profile_name = "e2e-test"
    env = base_env(e2e_home, profile_name)

    # Create profile — accept "already exists" as success since
    # set_current_profile() may create the directory as a side effect
    result = run_cli("profile", "create", profile_name, env=env)
    if result.exit_code != 0 and "already exists" not in result.stderr:
        msg = f"Failed to create profile: {result.stderr}"
        raise AssertionError(msg)

    return env


@pytest.fixture(scope="session")
def e2e_profile(e2e_env: dict[str, str], e2e_home: Path) -> dict[str, str]:
    """Initialize the e2e-test profile's database with encryption.

    Returns the env dict with MONEYBIN_PROFILE set. The database is
    ready for commands that need get_database(). Delegates to
    make_workflow_env() which is idempotent.
    """
    return make_workflow_env(e2e_home, "e2e-test")


def make_workflow_env(
    e2e_home: Path,
    profile_name: str,
) -> dict[str, str]:
    """Create a fresh profile with an initialized database for a workflow test.

    ``profile create`` generates an encryption key, stores it in the
    in-memory keyring, and initializes the encrypted database — one command,
    fully ready.  The env dict includes the test encryption key as a
    fallback for subsequent subprocess invocations (each subprocess gets
    a fresh in-memory keyring).

    Returns the env dict.  Idempotent — accepts "already exists" for
    profile create.
    """
    env = base_env(e2e_home, profile_name)
    # import_ field has a trailing underscore, so env_nested_delimiter="__" produces
    # three underscores: MONEYBIN_ + IMPORT_ + __ + INBOX_ROOT.
    env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(e2e_home / "inbox-root")

    result = run_cli("profile", "create", profile_name, env=env)
    if result.exit_code != 0 and "already exists" not in result.stderr:
        msg = f"Failed to create profile '{profile_name}': {result.stderr}"
        raise AssertionError(msg)

    return env


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"

# ---------------------------------------------------------------------------
# Snapshot-based fast workflow fixture
# ---------------------------------------------------------------------------

# A fixed profile name used inside the template snapshot. Each mutating test
# copies the entire MONEYBIN_HOME tree into its own isolated tmp_path, so
# they never collide on this name despite sharing it.
_TEMPLATE_PROFILE_NAME = "e2e-template"


@pytest.fixture(scope="session")
def _mutating_profile_template(  # pyright: ignore[reportUnusedFunction]  # pytest fixture referenced by parameter name
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """One-shot MONEYBIN_HOME with `e2e-template` profile created and DB initialized.

    Built once per pytest session by running `moneybin profile create` against
    a temp home. `make_workflow_env_fast` then copies this tree into each
    mutating test's tmp_path — skipping the per-test `profile create` cost
    (Argon2 key derivation + encrypted DB init + profile config write).
    """
    template_home = tmp_path_factory.mktemp("e2e_profile_template")
    env = base_env(template_home, _TEMPLATE_PROFILE_NAME)
    env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(template_home / "inbox-root")

    result = run_cli("profile", "create", _TEMPLATE_PROFILE_NAME, env=env)
    if result.exit_code != 0:
        msg = f"Failed to build profile snapshot: {result.stderr}"
        raise AssertionError(msg)

    return template_home


def make_workflow_env_fast(
    e2e_home: Path,
    subdir: str,
    template: Path,
) -> dict[str, str]:
    """Faster equivalent of `make_workflow_env()`.

    Copies the session-built profile template into `e2e_home / <subdir>`
    instead of running `profile create`. ``subdir`` only names the
    isolation directory under ``e2e_home`` — the active ``MONEYBIN_PROFILE``
    is always ``_TEMPLATE_PROFILE_NAME`` (``"e2e-template"``) regardless of
    what's passed here. Tests that need a specific active profile name in
    CLI output or arguments must use ``make_workflow_env()``.

    Returns the env dict (same shape as `make_workflow_env`).
    """
    target_home = e2e_home / subdir
    if target_home.exists():
        shutil.rmtree(target_home)
    shutil.copytree(template, target_home)

    env = base_env(target_home, _TEMPLATE_PROFILE_NAME)
    env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(target_home / "inbox-root")
    return env
