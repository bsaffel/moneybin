"""Metric registration tests for the database writer-coordination counters."""

from __future__ import annotations

from prometheus_client import Counter

from moneybin.metrics.registry import (
    DB_CHECKPOINT_TOTAL,
    DB_WRITE_LOCK_TIMEOUT_TOTAL,
)


def test_db_write_lock_timeout_total_is_counter_with_operation_type_label() -> None:
    assert isinstance(DB_WRITE_LOCK_TIMEOUT_TOTAL, Counter)
    # prometheus_client exposes a labelnames tuple on the underlying _metric
    assert DB_WRITE_LOCK_TIMEOUT_TOTAL._labelnames == (  # type: ignore[reportPrivateUsage,reportUnknownMemberType] — testing prometheus internals
        "operation_type",
    )


def test_db_checkpoint_total_is_counter_with_reason_label() -> None:
    assert isinstance(DB_CHECKPOINT_TOTAL, Counter)
    assert DB_CHECKPOINT_TOTAL._labelnames == (  # type: ignore[reportPrivateUsage,reportUnknownMemberType] — testing prometheus internals
        "reason",
    )


def test_metric_names_follow_moneybin_seconds_total_convention() -> None:
    # Confirms _seconds (for histograms) vs _total (for counters) convention.
    assert (
        DB_WRITE_LOCK_TIMEOUT_TOTAL._name  # type: ignore[reportPrivateUsage,reportUnknownMemberType] — testing prometheus internals
        == "moneybin_db_write_lock_timeout"
    )
    assert (
        DB_CHECKPOINT_TOTAL._name  # type: ignore[reportPrivateUsage,reportUnknownMemberType] — testing prometheus internals
        == "moneybin_db_checkpoint"
    )
