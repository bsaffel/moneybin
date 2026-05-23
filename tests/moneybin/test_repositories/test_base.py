"""Tests for ``BaseRepo`` — the shared contract for audited app.* repositories.

Verifies the three pieces of shared mechanics every ``*Repo`` inherits:
``_emit_audit`` (single audit-emission point + metric increment),
``_transaction`` (begin/commit/rollback, no-op when joining a caller's txn),
and ``_serialize_for_audit`` (JSON-friendly before/after row capture).
"""

# This module exercises BaseRepo's internal contract (_emit_audit, _transaction,
# _serialize_for_audit) directly — protected-member access is the point here.
# pyright: reportPrivateUsage=false
from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent


class _FakeRepo(BaseRepo):
    """Minimal concrete repo for exercising BaseRepo in isolation."""

    repository = "fake"


def _metric_value(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "fake", "action": action},
        )
        or 0.0
    )


@pytest.mark.unit
def test_emit_audit_returns_event_and_increments_metric() -> None:
    audit = MagicMock()
    sentinel = AuditEvent(
        audit_id="aid1",
        occurred_at="",
        actor="user",
        action="fake.do",
        target_schema="app",
        target_table="fake",
        target_id="t1",
        before_value=None,
        after_value={"k": "v"},
        parent_audit_id=None,
    )
    audit.record_audit_event.return_value = sentinel
    repo = _FakeRepo(MagicMock(), audit=audit)

    before = _metric_value("fake.do")
    event = repo._emit_audit(
        action="fake.do",
        target=("app", "fake", "t1"),
        before=None,
        after={"k": "v"},
        actor="user",
    )
    after = _metric_value("fake.do")

    assert event is sentinel
    audit.record_audit_event.assert_called_once_with(
        action="fake.do",
        target=("app", "fake", "t1"),
        before=None,
        after={"k": "v"},
        actor="user",
        parent_audit_id=None,
        context=None,
    )
    assert after - before == 1.0


@pytest.mark.unit
def test_transaction_in_outer_txn_is_a_noop() -> None:
    db = MagicMock()
    repo = _FakeRepo(db, audit=MagicMock())
    with repo._transaction(in_outer_txn=True):
        pass
    db.begin.assert_not_called()
    db.commit.assert_not_called()
    db.rollback.assert_not_called()


@pytest.mark.unit
def test_transaction_commits_on_success_and_rolls_back_on_error() -> None:
    db = MagicMock()
    repo = _FakeRepo(db, audit=MagicMock())

    with repo._transaction():
        pass
    db.begin.assert_called_once()
    db.commit.assert_called_once()
    db.rollback.assert_not_called()

    db.reset_mock()
    with pytest.raises(RuntimeError, match="boom"), repo._transaction():
        raise RuntimeError("boom")
    db.begin.assert_called_once()
    db.rollback.assert_called_once()
    db.commit.assert_not_called()


@pytest.mark.unit
def test_serialize_for_audit_handles_datetime_decimal_and_none() -> None:
    assert BaseRepo._serialize_for_audit(None) is None
    row = {
        "id": "x",
        "amount": Decimal("52.30"),
        "created_at": datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC),
        "as_of": date(2026, 5, 22),
        "active": True,
        "note": None,
    }
    out = BaseRepo._serialize_for_audit(row)
    assert out == {
        "id": "x",
        "amount": "52.30",
        "created_at": "2026-05-22T12:00:00+00:00",
        "as_of": "2026-05-22",
        "active": True,
        "note": None,
    }
