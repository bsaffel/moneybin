"""Shared fixtures for E2E subprocess tests.

These tests run `uv run moneybin ...` as a real subprocess to catch
boot, schema, and init wiring bugs that in-process tests miss.
"""

from __future__ import annotations

import os
import subprocess  # noqa: S404 — subprocess is intentional; we invoke uv as a test harness
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
    "MONEYBIN_DATABASE__ARGON2_MEMORY_COST": "1024",
    "MONEYBIN_DATABASE__ARGON2_PARALLELISM": "1",
    # Use null keyring backend so E2E subprocess tests don't read/write the
    # real system keychain. The encryption key is provided via
    # MONEYBIN_DATABASE__ENCRYPTION_KEY env var instead.
    "PYTHON_KEYRING_BACKEND": "keyring.backends.null.Keyring",
}

TEST_ENCRYPTION_KEY = (
    "e2e-test-key-0123456789abcdef0123456789abcdef0123456789abcdef0123456789ab"  # noqa: S105 — test-only key, not a real secret
)

TEST_PASSPHRASE = "e2e-test-passphrase-1234"  # noqa: S105 — test-only passphrase, not a real secret


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
    full_env = {**os.environ, **FAST_ARGON2_ENV, **(env or {})}

    result = subprocess.run(  # noqa: S603 — input is controlled test commands, not user input
        cmd,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=timeout,
        env=full_env,
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
    env = {
        "MONEYBIN_HOME": str(e2e_home),
        "MONEYBIN_PROFILE": profile_name,
        "MONEYBIN_DATABASE__ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
    }

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

    Uses a fixed encryption key via env var (MONEYBIN_DATABASE__ENCRYPTION_KEY)
    instead of ``db init`` to avoid system keychain interference.  The Database
    class creates and encrypts the file on first access, so no explicit init
    step is needed — the first command that calls ``get_database()`` will
    create the DB.

    Returns the env dict.  Call this at the start of each workflow test for
    isolation.  Idempotent — accepts "already exists" for profile create.
    """
    env = {
        "MONEYBIN_HOME": str(e2e_home),
        "MONEYBIN_PROFILE": profile_name,
        "MONEYBIN_DATABASE__ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
    }

    # Create profile — accept "already exists" as success since
    # set_current_profile() may create the directory as a side effect
    result = run_cli("profile", "create", profile_name, env=env)
    if result.exit_code != 0 and "already exists" not in result.stderr:
        msg = f"Failed to create profile '{profile_name}': {result.stderr}"
        raise AssertionError(msg)

    # Create the encrypted DB if it doesn't exist yet.
    # ``db migrate status`` calls get_database() which creates and encrypts
    # the file on first access.
    db_path = e2e_home / "profiles" / profile_name / "moneybin.duckdb"
    if not db_path.exists():
        result = run_cli("db", "migrate", "status", env=env)
        if result.exit_code != 0:
            msg = f"Failed to create DB for '{profile_name}': {result.stderr}"
            raise AssertionError(msg)

    return env


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
