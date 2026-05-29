"""Smoke tests verifying the six confirmation-layer instruments are registered."""

import pytest


@pytest.mark.unit
def test_confirmations_total_has_expected_labels() -> None:
    from moneybin.metrics.registry import IMPORT_CONFIRMATIONS_TOTAL

    # All three labels are required; passing them should not raise.
    IMPORT_CONFIRMATIONS_TOTAL.labels(
        channel="tabular", tier="high", outcome="accepted"
    ).inc()


@pytest.mark.unit
def test_detection_score_observes() -> None:
    from moneybin.metrics.registry import IMPORT_DETECTION_SCORE

    IMPORT_DETECTION_SCORE.observe(0.85)


@pytest.mark.unit
def test_per_channel_counters_have_channel_label() -> None:
    from moneybin.metrics.registry import (
        IMPORT_KNOWN_FORMAT_REUSE_TOTAL,
        IMPORT_OVERRIDE_TOTAL,
        IMPORT_REVALIDATION_FAILURE_TOTAL,
        IMPORT_SELF_ACCEPT_TOTAL,
    )

    IMPORT_SELF_ACCEPT_TOTAL.labels(channel="tabular").inc()
    IMPORT_OVERRIDE_TOTAL.labels(channel="gsheet").inc()
    IMPORT_KNOWN_FORMAT_REUSE_TOTAL.labels(channel="tabular").inc()
    IMPORT_REVALIDATION_FAILURE_TOTAL.labels(channel="tabular").inc()
