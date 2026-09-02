"""Top-level pytest configuration.

Disables SQLMesh's internal ``ProcessPoolExecutor`` so the integration suite
can run under ``pytest-xdist``. SQLMesh hardcodes ``mp.get_context("fork")``
when launching its model-loading pool; nesting fork inside an xdist worker
that has already imported threaded libraries (DuckDB, sqlglot) segfaults on
Linux during sqlglot GC.

Setting ``MAX_FORK_WORKERS=1`` before SQLMesh imports tells it to use a
synchronous in-process executor — model loading runs single-threaded within
each xdist worker, but tests still parallelize across workers. Net win on
the integration suite is ~5x vs. running it serially.

Assigned unconditionally (not via ``setdefault``) so an externally exported
``MAX_FORK_WORKERS`` can't silently re-enable the forking pool and reintroduce
the segfault.
"""

from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

os.environ["MAX_FORK_WORKERS"] = "1"

# Test categories partition the suite. Every collected test gets exactly
# one of these; `unit` is auto-applied below if none is present, so test
# authors only mark when departing from unit. CI selects per-category
# with a single `-m <category>` (no exclusion gymnastics).
_CATEGORY_MARKERS = ("unit", "integration", "e2e", "scenarios")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    categories = set(_CATEGORY_MARKERS)
    for item in items:
        present = {m.name for m in item.iter_markers()} & categories
        if not present:
            item.add_marker(pytest.mark.unit)
        elif len(present) > 1:
            raise pytest.UsageError(
                f"{item.nodeid}: multiple category markers {sorted(present)}; "
                f"each test must have exactly one of {sorted(categories)}"
            )


# Force every Typer app to use plain Click help rendering during tests.
# Rich-mode help wraps option names in bold/dim ANSI escapes
# (`--\x1b[1moutput\x1b[0m`) under CI environments that set CLICOLOR_FORCE
# or FORCE_COLOR — breaking substring checks like `"--output" in stdout`.
# `NO_COLOR` doesn't help because bold/dim aren't colors. Patching the
# constructor here (before any moneybin module imports typer) ensures the
# root app and every sub-typer instance render help in plain text.
import typer  # noqa: E402

_typer_init = typer.Typer.__init__


def _typer_init_no_rich(self: typer.Typer, *args: object, **kwargs: object) -> None:
    kwargs["rich_markup_mode"] = None
    _typer_init(self, *args, **kwargs)  # type: ignore[arg-type]


typer.Typer.__init__ = _typer_init_no_rich  # type: ignore[method-assign]

# Ambient MoneyBin configuration must not reach the suite. Every field on
# MoneyBinSettings resolves from a MONEYBIN_-prefixed variable, so one the
# developer exports — their own Google OAuth client id, a sync server URL —
# silently replaces the shipped default inside any test that constructs
# settings without an explicit override. Such a test asserts against the
# machine it runs on: green in CI where no var is set, red locally, or the
# reverse. Clear the whole prefix here; the two vars the harness owns are
# re-added immediately below. The match is case-insensitive because
# MoneyBinSettings sets `case_sensitive=False`, so a lowercase export reaches
# the same field. Guarded by tests/moneybin/test_env_isolation.py.
_CLEARED_AMBIENT_ENV = sorted(
    _k for _k in os.environ if _k.upper().startswith("MONEYBIN_")
)
for _ambient in _CLEARED_AMBIENT_ENV:
    del os.environ[_ambient]

# Per-xdist-worker MoneyBin home so parallel tests don't trample each other's
# `.moneybin/profiles/` directory. Each worker (`gw0`, `gw1`, …) gets its own
# tempdir; serial runs use a single shared dir under `gw-main`.
_worker = os.environ.get("PYTEST_XDIST_WORKER", "gw-main")
_worker_home = Path(tempfile.gettempdir()) / "moneybin-test-home" / _worker
_worker_home.mkdir(parents=True, exist_ok=True)
os.environ["MONEYBIN_HOME"] = str(_worker_home)

# Defensive isolation for the import-inbox root. Without this, any test that
# constructs ImportSettings() without an explicit inbox_root — or triggers
# code that does (e.g. ProfileService._init_inbox) — falls through to
# Path.home() / "Documents" / "MoneyBin", leaking test directories into the
# user's real ~/Documents/MoneyBin/. The triple-underscore is intentional:
# the field name is `import_` (trailing underscore) and pydantic-settings
# joins with "__".
_worker_inbox_root = _worker_home / "inbox-root"
_worker_inbox_root.mkdir(parents=True, exist_ok=True)
os.environ["MONEYBIN_IMPORT___INBOX_ROOT"] = str(_worker_inbox_root)

# Snapshot the isolated environment now, at import, because import is when the
# isolation actually happens. A test that instead read os.environ at call time
# would also see whatever a test sharing its xdist worker had left behind, and
# would report that as the developer's shell leaking in.
MONEYBIN_ENV_AT_STARTUP = frozenset(
    _k for _k in os.environ if _k.upper().startswith("MONEYBIN_")
)


def pytest_configure(config: pytest.Config) -> None:
    """Name the ambient ``MONEYBIN_*`` vars this run ignored.

    Dropping them silently would make a deliberate
    ``MONEYBIN_LOGGING__LEVEL=DEBUG uv run pytest ...`` look broken rather than
    overridden. Names only, never values — these fields hold credentials.

    Written to stderr rather than through ``pytest_report_header`` because
    ``addopts`` bakes in ``-q``, which suppresses header hooks and would hide
    this from ``make test`` and ``make test-integration`` — the two gates run
    most often, and so the two that most need it. xdist workers inherit an
    environment the controller already cleared, but they are excluded
    explicitly so the line cannot multiply by worker count.
    """
    if not _CLEARED_AMBIENT_ENV or hasattr(config, "workerinput"):
        return
    sys.stderr.write(
        f"moneybin: ignored ambient env {', '.join(_CLEARED_AMBIENT_ENV)}\n"
    )


@pytest.fixture(scope="session")
def moneybin_env_at_startup() -> frozenset[str]:
    """``MONEYBIN_*`` names surviving conftest's environment isolation."""
    return MONEYBIN_ENV_AT_STARTUP


@pytest.fixture(scope="session", autouse=True)
def _in_memory_keyring() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Swap the OS keychain for an in-memory backend for the whole session.

    No automated test should reach the real platform keyring: it prompts or
    denies under sandbox + headless CI (the ``PasswordSetError -60008`` this
    prevents) and is platform-specific, so a green run on one OS proves
    nothing about another. Tests that exercise ``SecretStore``'s own logic
    patch ``moneybin.secrets.keyring`` directly (see test_secrets.py) and so
    are unaffected by this backend swap. The ``keyring`` library itself is
    upstream-tested — we only need a writable, controlled backend so that
    encrypted-DB opens can round-trip a key without the OS.

    Session-scoped (not per-test) so the in-memory store persists across the
    worker's tests — matching the real keychain's persistence. The per-worker
    ``MONEYBIN_HOME`` profile DB is reused across tests, so the key that
    created it must stay retrievable; clearing per test would orphan it and
    surface as "Wrong encryption key used to open the database file".
    """
    import keyring

    from tests.e2e.memory_keyring import MemoryKeyring

    previous = keyring.get_keyring()
    keyring.set_keyring(MemoryKeyring())
    try:
        yield
    finally:
        keyring.set_keyring(previous)
