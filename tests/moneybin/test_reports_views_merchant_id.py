"""Shape tests for reports.* models: merchant_id propagation.

Four reports views project ``merchant_id`` alongside ``merchant_normalized``
and aggregate on the FK; NULL merchant_id rows collapse into a single
``'(uncategorized)'`` bucket.

These tests read the model SQL files and assert structural properties
(column present, GROUP/PARTITION key uses merchant_id). They do NOT run
the SQLMesh pipeline — behavioral correctness is covered by scenarios.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_MODELS_DIR = Path(__file__).resolve().parents[2] / "sqlmesh" / "models" / "reports"


def _read(name: str) -> str:
    return (_MODELS_DIR / name).read_text()


class TestMerchantActivityMerchantId:
    """``merchant_activity`` aggregates per ``merchant_id`` with display."""

    def test_projects_merchant_id(self) -> None:
        content = _read("merchant_activity.sql")
        assert "merchant_id," in content, (
            "merchant_activity must project merchant_id alongside merchant_normalized"
        )

    def test_groups_by_merchant_id(self) -> None:
        content = _read("merchant_activity.sql")
        # Normalize whitespace so the assertion survives sqlmesh-format
        # rewrites that could put GROUP BY on a single line.
        normalized = " ".join(content.split())
        assert "GROUP BY merchant_id" in normalized, (
            "merchant_activity must GROUP BY merchant_id"
        )

    def test_uncategorized_bucket_label(self) -> None:
        content = _read("merchant_activity.sql")
        assert "'(uncategorized)'" in content, (
            "NULL merchant_id rows should label as '(uncategorized)'"
        )


class TestRecurringSubscriptionsMerchantId:
    """``recurring_subscriptions`` partitions by ``merchant_id``."""

    def test_projects_merchant_id(self) -> None:
        content = _read("recurring_subscriptions.sql")
        assert "merchant_id," in content

    def test_partitions_by_merchant_id(self) -> None:
        content = _read("recurring_subscriptions.sql")
        # The interval-detection PARTITION BY must include merchant_id.
        assert "PARTITION BY account_id, merchant_id" in content, (
            "recurring_subscriptions must PARTITION BY merchant_id"
        )

    def test_uncategorized_bucket_label(self) -> None:
        content = _read("recurring_subscriptions.sql")
        assert "'(uncategorized)'" in content


class TestLargeTransactionsMerchantId:
    """``large_transactions`` projects merchant_id per row."""

    def test_projects_merchant_id(self) -> None:
        content = _read("large_transactions.sql")
        assert "merchant_id," in content


class TestUncategorizedQueueMerchantId:
    """``uncategorized_queue`` projects merchant_id per row."""

    def test_projects_merchant_id(self) -> None:
        content = _read("uncategorized_queue.sql")
        assert "merchant_id," in content
