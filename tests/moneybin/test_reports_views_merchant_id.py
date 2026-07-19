"""Shape tests for reports.*/core.uncategorized_queue models: merchant_id propagation.

Four models project ``merchant_id`` alongside ``merchant_normalized`` and
aggregate on the FK; NULL merchant_id rows collapse into a single
``'(uncategorized)'`` bucket. Three (``merchant_activity``,
``recurring_subscriptions``, ``large_transactions``) live in ``reports.*``;
``uncategorized_queue`` moved to ``core.*`` (reports-foundation.md R5) but
keeps the same shape contract.

These tests read the model SQL files and assert structural properties
(column present, GROUP/PARTITION key uses merchant_id). They do NOT run
the SQLMesh pipeline — behavioral correctness is covered by scenarios.
"""

from __future__ import annotations

import pytest

from moneybin.database import SQLMESH_ROOT

pytestmark = pytest.mark.unit

_MODELS_DIR = SQLMESH_ROOT / "models" / "reports"
# uncategorized_queue moved to core.* (reports-foundation.md R5) — it is
# service-internal, not a user-facing report, but this shape test still
# applies to it regardless of schema.
_CORE_MODELS_DIR = SQLMESH_ROOT / "models" / "core"


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
    """``core.uncategorized_queue`` projects merchant_id per row."""

    def test_projects_merchant_id(self) -> None:
        content = (_CORE_MODELS_DIR / "uncategorized_queue.sql").read_text()
        assert "merchant_id," in content
