"""Top-level pytest configuration.

Forces the ``spawn`` multiprocessing start method before any test collects.
SQLMesh launches its own ``ProcessPoolExecutor`` while loading models; under
``pytest-xdist`` each worker is itself a forked process, and forking again
from a process that has already imported threaded libraries (DuckDB, sqlglot)
segfaults on Linux. ``spawn`` re-initializes child interpreters cleanly and
costs a few hundred ms per pool launch — negligible compared to the wall-clock
win from running integration tests in parallel.
"""

from __future__ import annotations

import multiprocessing

multiprocessing.set_start_method("spawn", force=True)
