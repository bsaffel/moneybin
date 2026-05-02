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
import tempfile
from pathlib import Path

os.environ["MAX_FORK_WORKERS"] = "1"

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
