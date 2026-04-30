"""Unit tests for AutoRuleService context-aware helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin.services.auto_rule_service import (
    AutoRuleService,
    BulkRecordingContext,
    TxnRow,
)


@pytest.fixture
def db_mock() -> MagicMock:
    """A mock Database whose ``execute`` records every call."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


class TestRecordCategorizationWithContext:
    """Verify context-aware path issues no read queries for description/rules/merchants."""

    def test_no_db_queries_when_context_provided(self, db_mock: MagicMock) -> None:
        ctx = BulkRecordingContext(
            txn_rows={
                "csv_a": TxnRow(description="STARBUCKS", amount=-5.0, account_id=None)
            },
            active_rules=[],
            merchant_mappings=[],
        )
        svc = AutoRuleService(db_mock)
        svc._find_pending_proposal = MagicMock(return_value=None)  # pyright: ignore[reportPrivateUsage]  # stubbing internal to isolate read paths

        svc.record_categorization("csv_a", "Food", subcategory=None, context=ctx)

        for call in db_mock.execute.call_args_list:
            sql = str(call.args[0]).lower()
            assert "from core.fct_transactions" not in sql
            assert "from app.merchants" not in sql
            assert "from app.categorization_rules" not in sql

    def test_falls_back_when_context_none(self, db_mock: MagicMock) -> None:
        svc = AutoRuleService(db_mock)
        svc._find_pending_proposal = MagicMock(return_value=None)  # pyright: ignore[reportPrivateUsage]  # stubbing internal to isolate read paths
        svc.record_categorization("csv_a", "Food", subcategory=None, context=None)
        assert db_mock.execute.called
