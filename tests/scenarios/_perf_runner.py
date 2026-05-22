"""Shared helper for capturing per-flow latency percentiles.

Used by both:
- the one-off Phase 0 baseline capture (writes JSON fixture)
- the Phase 9 regression assertion (reads JSON fixture, re-runs flows,
  asserts deltas within budget)

The runner is intentionally framework-agnostic: each flow is a
zero-arg callable returning a ResponseEnvelope. The runner times the
call wall-clock (perf_counter_ns), runs N iterations, and returns
p50/p95/p99 in milliseconds.
"""

from __future__ import annotations

import json
import statistics
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class FlowResult:
    """Latency percentiles for a single flow in milliseconds."""

    name: str
    iterations: int
    p50_ms: float
    p95_ms: float
    p99_ms: float


def measure_flow(name: str, fn: Callable[[], Any], iterations: int = 30) -> FlowResult:
    """Run ``fn`` ``iterations`` times; return percentile latency in ms."""
    samples_ns: list[int] = []
    # One warmup call: first call may pay JIT / cache costs we don't want
    # in the steady-state percentiles.
    fn()
    for _ in range(iterations):
        start = time.perf_counter_ns()
        fn()
        samples_ns.append(time.perf_counter_ns() - start)
    samples_ms = sorted(s / 1_000_000 for s in samples_ns)
    return FlowResult(
        name=name,
        iterations=iterations,
        p50_ms=statistics.median(samples_ms),
        p95_ms=_percentile(samples_ms, 0.95),
        p99_ms=_percentile(samples_ms, 0.99),
    )


def _percentile(sorted_samples: list[float], pct: float) -> float:
    """Return ``pct``-th percentile from a pre-sorted sample list."""
    if not sorted_samples:
        raise ValueError("empty samples")
    idx = int(round(pct * (len(sorted_samples) - 1)))
    return sorted_samples[idx]


def write_baseline(path: Path, results: list[FlowResult]) -> None:
    """Write percentile results to ``path`` as JSON for later comparison."""
    payload = {r.name: asdict(r) for r in results}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def read_baseline(path: Path) -> dict[str, FlowResult]:
    """Read a baseline JSON back as ``{flow_name: FlowResult}``."""
    raw: dict[str, dict[str, Any]] = json.loads(path.read_text())
    return {
        name: FlowResult(
            name=name,
            iterations=int(d["iterations"]),
            p50_ms=float(d["p50_ms"]),
            p95_ms=float(d["p95_ms"]),
            p99_ms=float(d["p99_ms"]),
        )
        for name, d in raw.items()
    }
