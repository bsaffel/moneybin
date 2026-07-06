"""Root pytest configuration.

Skip dotenv files during collection. The OS sandbox denies ``stat()`` on
``.env`` credential files; Python 3.12's ``Path.is_dir()`` propagates that
``PermissionError`` (older versions swallowed it), which crashes pytest's
default rootdir collection walk the moment a real ``.env`` exists at the repo
root. Returning ``True`` from our own ``pytest_ignore_collect`` runs before the
builtin hook stats the path (``pytest_ignore_collect`` is ``firstresult``), so
the suite stays runnable inside the sandbox regardless of local ``.env``
presence. Dotenv files are never test modules, so ignoring them is safe.
"""

from __future__ import annotations

from pathlib import Path


def pytest_ignore_collect(collection_path: Path) -> bool | None:
    """Ignore ``.env`` / ``.env.*`` files before the builtin hook stats them."""
    if collection_path.name.startswith(".env"):
        return True
    return None
