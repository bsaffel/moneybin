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

os.environ["MAX_FORK_WORKERS"] = "1"
