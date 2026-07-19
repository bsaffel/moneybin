"""Transaction-aware Prometheus observation buffering."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True)
class _Observation:
    """One deferred metric mutation."""

    kind: Literal["counter", "observe"]
    metric: Any
    value: float
    labels: dict[str, str]


@dataclass
class MetricObservations:
    """Buffer metric mutations until their database transaction is durable."""

    _items: list[_Observation] = field(default_factory=list)
    _callbacks: list[Callable[[], None]] = field(default_factory=list)

    def counter(
        self,
        metric: Any,
        *,
        labels: dict[str, str],
        amount: float = 1,
    ) -> None:
        """Defer a counter increment."""
        self._items.append(_Observation("counter", metric, amount, labels))

    def observe(
        self,
        metric: Any,
        value: float,
        *,
        labels: dict[str, str],
    ) -> None:
        """Defer a histogram observation."""
        self._items.append(_Observation("observe", metric, value, labels))

    def callback(self, callback: Callable[[], None]) -> None:
        """Defer a metric refresh that must query committed database state."""
        self._callbacks.append(callback)

    def flush(self) -> None:
        """Apply each buffered mutation exactly once."""
        items, self._items = self._items, []
        callbacks, self._callbacks = self._callbacks, []
        for item in items:
            child = item.metric.labels(**item.labels) if item.labels else item.metric
            if item.kind == "counter":
                if item.value == 1:
                    child.inc()
                else:
                    child.inc(item.value)
            else:
                child.observe(item.value)
        for callback in callbacks:
            callback()


def record_counter(
    metric: Any,
    *,
    labels: dict[str, str],
    emit_metrics: bool,
    observations: MetricObservations | None,
    amount: float = 1,
) -> None:
    """Record now, defer, or suppress a counter mutation."""
    if observations is not None:
        observations.counter(metric, labels=labels, amount=amount)
    elif emit_metrics:
        child = metric.labels(**labels) if labels else metric
        if amount == 1:
            child.inc()
        else:
            child.inc(amount)


def record_observation(
    metric: Any,
    value: float,
    *,
    labels: dict[str, str],
    emit_metrics: bool,
    observations: MetricObservations | None,
) -> None:
    """Record now, defer, or suppress a histogram observation."""
    if observations is not None:
        observations.observe(metric, value, labels=labels)
    elif emit_metrics:
        child = metric.labels(**labels) if labels else metric
        child.observe(value)
