"""Tests for shared CLI output helpers."""

from __future__ import annotations

import json
from typing import Any

import pytest

from moneybin.cli.output import OutputFormat, emit_json_error, render_or_json
from moneybin.errors import UserError
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta


def _make_envelope(rows: list[dict[str, Any]] | None = None) -> ResponseEnvelope:
    data = rows if rows is not None else [{"id": "a1", "amount": "10.00"}]
    return ResponseEnvelope(
        summary=SummaryMeta(total_count=len(data), returned_count=len(data)),
        data=data,
    )


class TestRenderOrJson:
    """Tests for render_or_json helper."""

    @pytest.mark.unit
    def test_json_mode_emits_full_envelope(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(_make_envelope(), OutputFormat.JSON)
        out = json.loads(capsys.readouterr().out)
        assert out["status"] == "ok"
        assert out["data"][0]["id"] == "a1"

    @pytest.mark.unit
    def test_json_fields_filters_data_keys(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"id": "a1", "amount": "10.00", "description": "Coffee"}]
        render_or_json(_make_envelope(rows), OutputFormat.JSON, json_fields="id,amount")
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == [{"id": "a1", "amount": "10.00"}]

    @pytest.mark.unit
    def test_json_fields_ignored_for_dict_data(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        envelope = ResponseEnvelope(
            summary=SummaryMeta(total_count=1, returned_count=1),
            data={"applied": 3, "errors": 0},
        )
        render_or_json(envelope, OutputFormat.JSON, json_fields="applied")
        out = json.loads(capsys.readouterr().out)
        # dict data is passed through unchanged
        assert out["data"] == {"applied": 3, "errors": 0}

    @pytest.mark.unit
    def test_json_fields_none_returns_all_fields(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"id": "a1", "amount": "10.00", "description": "Coffee"}]
        render_or_json(_make_envelope(rows), OutputFormat.JSON)
        out = json.loads(capsys.readouterr().out)
        assert set(out["data"][0].keys()) == {"id", "amount", "description"}

    @pytest.mark.unit
    def test_text_mode_calls_render_fn(self) -> None:
        called: list[Any] = []
        render_or_json(
            _make_envelope(), OutputFormat.TEXT, render_fn=lambda e: called.append(e)
        )
        assert len(called) == 1

    @pytest.mark.unit
    def test_text_mode_no_render_fn_emits_nothing(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(_make_envelope(), OutputFormat.TEXT)
        assert capsys.readouterr().out == ""

    @pytest.mark.unit
    def test_json_fields_missing_field_silently_skipped(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"id": "a1", "amount": "10.00"}]
        render_or_json(
            _make_envelope(rows), OutputFormat.JSON, json_fields="id,nonexistent"
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == [{"id": "a1"}]

    @pytest.mark.unit
    def test_json_fields_strips_whitespace(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"id": "a1", "amount": "10.00", "description": "Coffee"}]
        render_or_json(
            _make_envelope(rows), OutputFormat.JSON, json_fields="id, amount"
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == [{"id": "a1", "amount": "10.00"}]

    @pytest.mark.unit
    def test_json_fields_skips_empty_segments(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rows = [{"id": "a1", "amount": "10.00", "description": "Coffee"}]
        render_or_json(
            _make_envelope(rows), OutputFormat.JSON, json_fields="id,,amount"
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == [{"id": "a1", "amount": "10.00"}]


class TestEmitJsonError:
    """Tests for emit_json_error helper."""

    @pytest.mark.unit
    def test_emits_error_envelope_to_stdout(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        err = UserError("DB locked", code="database_locked")
        emit_json_error(err)
        out = json.loads(capsys.readouterr().out)
        assert out["status"] == "error"
        assert out["error"]["code"] == "database_locked"
        assert out["error"]["message"] == "DB locked"

    @pytest.mark.unit
    def test_emits_valid_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        emit_json_error(UserError("oops", code="unknown"))
        raw = capsys.readouterr().out.strip()
        assert json.loads(raw)  # no exception = valid JSON
