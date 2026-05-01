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

# Disable Rich/Click ANSI styling so help-text assertions match plain strings.
# CI sets FORCE_COLOR=1, which causes Typer/Rich to inject color escapes inside
# option names (`--\x1b[36moutput\x1b[0m`) — breaking substring checks like
# `"--output" in result.stdout`. NO_COLOR is the standard opt-out.
os.environ["NO_COLOR"] = "1"
os.environ.pop("FORCE_COLOR", None)

# Per-xdist-worker MoneyBin home so parallel tests don't trample each other's
# `.moneybin/profiles/` directory. Each worker (`gw0`, `gw1`, …) gets its own
# tempdir; serial runs use a single shared dir under `gw-main`.
_worker = os.environ.get("PYTEST_XDIST_WORKER", "gw-main")
_worker_home = Path(tempfile.gettempdir()) / "moneybin-test-home" / _worker
_worker_home.mkdir(parents=True, exist_ok=True)
os.environ["MONEYBIN_HOME"] = str(_worker_home)
