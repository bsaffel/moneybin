# tests/moneybin/test_mcp/test_envelope.py
"""Tests for the MCP response envelope."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from moneybin.protocol.envelope import (
    DetailLevel,
    ResponseEnvelope,
    SummaryMeta,
    build_envelope,
)


class TestDetailLevel:
    """Tests for the DetailLevel enum."""

    @pytest.mark.unit
    def test_values(self) -> None:
        assert DetailLevel.SUMMARY == "summary"
        assert DetailLevel.STANDARD == "standard"
        assert DetailLevel.FULL == "full"

    @pytest.mark.unit
    def test_from_string(self) -> None:
        assert DetailLevel("summary") == DetailLevel.SUMMARY
        assert DetailLevel("standard") == DetailLevel.STANDARD
        assert DetailLevel("full") == DetailLevel.FULL

    @pytest.mark.unit
    def test_invalid_raises(self) -> None:
        with pytest.raises(ValueError):
            DetailLevel("verbose")


class TestSummaryMeta:
    """Tests for the SummaryMeta dataclass."""

    @pytest.mark.unit
    def test_defaults(self) -> None:
        meta = SummaryMeta(total_count=10, returned_count=10)
        assert meta.has_more is False
        assert meta.sensitivity == "low"
        assert meta.display_currency == "USD"
        assert meta.degraded is False
        assert meta.degraded_reason is None
        assert meta.period is None

    @pytest.mark.unit
    def test_has_more_when_truncated(self) -> None:
        meta = SummaryMeta(total_count=100, returned_count=50, has_more=True)
        assert meta.has_more is True


class TestResponseEnvelope:
    """Tests for the ResponseEnvelope dataclass."""

    @pytest.mark.unit
    def test_to_dict_structure(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=3, returned_count=3),
            data=[{"period": "2026-04", "income": 5200.00}],
            actions=["Use reports_spending_by_category for breakdown"],
        )
        d = envelope.to_dict()
        assert set(d.keys()) == {"status", "summary", "data", "actions"}
        assert d["status"] == "ok"
        assert list(d.keys())[0] == "status"
        assert d["summary"]["total_count"] == 3
        assert d["summary"]["returned_count"] == 3
        assert d["summary"]["has_more"] is False
        assert d["summary"]["sensitivity"] == "low"
        assert len(d["data"]) == 1
        assert len(d["actions"]) == 1

    @pytest.mark.unit
    def test_to_json_serializes(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=1, returned_count=1),
            data=[{"amount": Decimal("42.50")}],
        )
        text = envelope.to_json()
        parsed = json.loads(text)
        assert parsed["summary"]["total_count"] == 1
        # Money values are emitted as JSON numbers, not quoted strings —
        # see `_DecimalEncoder` docstring for the wire contract.
        assert parsed["data"][0]["amount"] == 42.5
        assert isinstance(parsed["data"][0]["amount"], float)

    @pytest.mark.unit
    def test_empty_actions_default(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0),
            data=[],
        )
        assert envelope.actions == []

    @pytest.mark.unit
    def test_to_dict_status_ok_when_no_error(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=1, returned_count=1),
            data=[{"id": "abc"}],
        )
        assert envelope.to_dict()["status"] == "ok"

    @pytest.mark.unit
    def test_to_dict_status_error_when_error_set(self) -> None:
        from moneybin.errors import UserError

        err = UserError("DB locked", code="database_locked")
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0),
            data=[],
            error=err,
        )
        d = envelope.to_dict()
        assert d["status"] == "error"
        assert "error" in d

    @pytest.mark.unit
    def test_to_json_includes_status(self) -> None:
        import json

        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=0, returned_count=0),
            data=[],
        )
        parsed = json.loads(envelope.to_json())
        assert parsed["status"] == "ok"

    @pytest.mark.unit
    def test_degraded_envelope(self) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(
                total_count=247,
                returned_count=5,
                sensitivity="low",
                degraded=True,
                degraded_reason="Transaction-level data requires data-sharing consent",
            ),
            data=[{"category": "Groceries", "total": 1245.67}],
            actions=[
                "Run 'moneybin privacy grant mcp-data-sharing' to enable full details"
            ],
        )
        d = envelope.to_dict()
        assert d["summary"]["degraded"] is True
        assert "consent" in d["summary"]["degraded_reason"]


class TestBuildEnvelope:
    """Tests for the build_envelope helper."""

    @pytest.mark.unit
    def test_build_from_list(self) -> None:
        rows = [{"a": 1}, {"a": 2}, {"a": 3}]
        envelope = build_envelope(
            data=rows,
            sensitivity="low",
        )
        assert envelope.summary.total_count == 3
        assert envelope.summary.returned_count == 3
        assert envelope.summary.has_more is False

    @pytest.mark.unit
    def test_build_with_truncation(self) -> None:
        rows = [{"a": i} for i in range(50)]
        envelope = build_envelope(
            data=rows,
            sensitivity="medium",
            total_count=200,
        )
        assert envelope.summary.total_count == 200
        assert envelope.summary.returned_count == 50
        assert envelope.summary.has_more is True

    @pytest.mark.unit
    def test_build_with_period(self) -> None:
        envelope = build_envelope(
            data=[],
            sensitivity="low",
            period="2026-01 to 2026-04",
        )
        assert envelope.summary.period == "2026-01 to 2026-04"

    @pytest.mark.unit
    def test_build_with_actions(self) -> None:
        envelope = build_envelope(
            data=[],
            sensitivity="low",
            actions=["Try reports_spending_by_category"],
        )
        assert envelope.actions == ["Try reports_spending_by_category"]

    @pytest.mark.unit
    def test_build_write_result(self) -> None:
        """Write tools return a dict, not a list."""
        result = {"applied": 48, "skipped": 0, "errors": 2}
        envelope = build_envelope(
            data=result,
            sensitivity="medium",
            total_count=50,
        )
        assert envelope.summary.total_count == 50
        assert envelope.data == result


@pytest.mark.unit
def test_user_error_carries_structured_details() -> None:
    from moneybin.errors import UserError

    err = UserError(
        "Tool exceeded 30.0s cap",
        code="timed_out",
        hint=None,
        details={"tool": "import_inbox_sync", "elapsed_s": 30.1, "timeout_s": 30.0},
    )
    d = err.to_dict()
    assert d["code"] == "timed_out"
    assert d["details"]["tool"] == "import_inbox_sync"
    assert d["details"]["timeout_s"] == 30.0


@pytest.mark.unit
def test_transaction_curation_fields_in_to_dict() -> None:
    """Transaction.to_dict() includes curation fields when set."""
    from moneybin.services.transaction_service import Transaction

    t = Transaction(
        transaction_id="T1",
        account_id="A1",
        transaction_date="2026-04-10",
        amount=Decimal("-50.00"),
        description="Coffee Shop",
        memo=None,
        source_type="ofx",
        category="Food & Drink",
        subcategory=None,
        notes=[
            {
                "note_id": "N1",
                "text": "my note",
                "author": "user",
                "created_at": "2026-04-10T00:00:00",
            }
        ],
        tags=["personal", "recurring"],
        splits=None,
    )
    d = t.to_dict()
    assert d["notes"] == [
        {
            "note_id": "N1",
            "text": "my note",
            "author": "user",
            "created_at": "2026-04-10T00:00:00",
        }
    ]
    assert d["tags"] == ["personal", "recurring"]
    assert "splits" not in d


@pytest.mark.unit
def test_transaction_curation_fields_absent_when_none() -> None:
    """Transaction.to_dict() omits curation fields when None."""
    from moneybin.services.transaction_service import Transaction

    t = Transaction(
        transaction_id="T1",
        account_id="A1",
        transaction_date="2026-04-10",
        amount=Decimal("-50.00"),
        description="Coffee Shop",
        memo=None,
        source_type="ofx",
        category=None,
        subcategory=None,
    )
    d = t.to_dict()
    assert "notes" not in d
    assert "tags" not in d
    assert "splits" not in d


@pytest.mark.unit
def test_response_envelope_next_cursor_in_to_dict() -> None:
    """build_envelope sets next_cursor and has_more when next_cursor is provided."""
    from moneybin.protocol.envelope import build_envelope

    envelope = build_envelope(data=[], sensitivity="low", next_cursor="dGVzdA==")
    assert envelope.next_cursor == "dGVzdA=="
    assert envelope.summary.has_more is True
    d = envelope.to_dict()
    assert d["next_cursor"] == "dGVzdA=="
    assert d["summary"]["has_more"] is True


@pytest.mark.unit
def test_response_envelope_next_cursor_absent_when_none() -> None:
    """ResponseEnvelope.to_dict() omits next_cursor when None."""
    from moneybin.protocol.envelope import build_envelope

    envelope = build_envelope(data=[], sensitivity="low")
    d = envelope.to_dict()
    assert "next_cursor" not in d
