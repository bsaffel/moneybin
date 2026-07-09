"""Root pytest configuration — keep the suite runnable inside the sandbox.

The OS sandbox denies every access to the repo-root ``.env`` credential file
(``stat()`` included), and Python 3.12's ``Path.is_dir()`` / ``Path.is_file()``
propagate that ``PermissionError`` (older versions swallowed it). A real
``.env`` at the repo root therefore crashes pytest two independent ways, both
neutralised here:

1. **Collection** — pytest's default rootdir walk stats every entry, hitting
   ``.env`` the moment it exists. ``pytest_ignore_collect`` (``firstresult``, so
   it runs before the builtin hook stats the path) returns ``True`` for any
   dotenv file. Dotenv files are never test modules, so ignoring them is safe.

2. **FastMCP settings** — ``fastmcp/settings.py`` freezes
   ``env_file = os.getenv("FASTMCP_ENV_FILE", ".env")`` at import, so building
   the server's module-level ``FastMCP`` instance (on import of
   ``moneybin.mcp.server``) reads a CWD-relative ``.env``. Pointing
   ``FASTMCP_ENV_FILE`` at ``os.devnull`` before FastMCP is imported makes that
   read a no-op. This must run at conftest-import time — pytest loads the
   rootdir conftest before any test or ``src`` module, so the frozen
   ``ENV_FILE`` picks up this value. No test configures FastMCP via a dotenv
   file, so the harness owns this unconditionally.
"""

from __future__ import annotations

import os
from pathlib import Path

os.environ["FASTMCP_ENV_FILE"] = os.devnull


def pytest_ignore_collect(collection_path: Path) -> bool | None:
    """Ignore ``.env`` / ``.env.*`` files before the builtin hook stats them."""
    if collection_path.name.startswith(".env"):
        return True
    return None
