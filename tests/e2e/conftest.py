"""Shared fixtures for E2E subprocess tests.

These tests run `uv run moneybin ...` as a real subprocess to catch
boot, schema, and init wiring bugs that in-process tests miss.
"""

from __future__ import annotations

import atexit
import os
import shutil
import subprocess  # noqa: S404 — subprocess is intentional; we invoke uv as a test harness
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest

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
    "PYTHONPATH": str(Path(__file__).resolve().parent.parent.parent),
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
    cmd = ["uv", "run", "moneybin", *args]  # noqa: S607 — uv is on PATH in dev environments
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

    result = run_cli("profile", "create", profile_name, env=env)
    if result.exit_code != 0 and "already exists" not in result.stderr:
        msg = f"Failed to create profile '{profile_name}': {result.stderr}"
        raise AssertionError(msg)

    return env


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
