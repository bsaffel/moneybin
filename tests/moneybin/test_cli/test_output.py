"""Tests for shared CLI output helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pytest

from moneybin import error_codes
from moneybin.cli.output import OutputFormat, emit_json_error, render_or_json
from moneybin.errors import UserError
from moneybin.privacy.payloads.gsheet import GsheetPullPayload, GsheetPullRow
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta, build_envelope


def _make_envelope(
    rows: list[dict[str, Any]] | None = None,
) -> ResponseEnvelope[list[dict[str, Any]]]:
    data = rows if rows is not None else [{"id": "a1", "amount": "10.00"}]
    return ResponseEnvelope(
        summary=SummaryMeta(total_count=len(data), returned_count=len(data)),
        data=data,
    )


@dataclass(frozen=True, slots=True)
class _AccountRow:
    """A row whose account number carries an active masking transform."""

    id: Annotated[str, DataClass.RECORD_ID]
    account_number: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    label: Annotated[str, DataClass.USER_NOTE]


@dataclass(frozen=True, slots=True)
class _OneListPayload:
    """The shape every migrated collection command uses: one list, plus counts."""

    rows: list[_AccountRow]
    total: Annotated[int, DataClass.AGGREGATE]


@dataclass(frozen=True, slots=True)
class _TwoListPayload:
    """Two collections in one payload — no single list to project into."""

    rows: list[_AccountRow]
    others: list[_AccountRow]


@dataclass(frozen=True, slots=True)
class _NoListPayload:
    """A scalar-only payload."""

    total: Annotated[int, DataClass.AGGREGATE]


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


class TestJsonFieldsOnTypedPayloads:
    """`--json-fields` reaches the rows inside a typed collection payload.

    The filter used to require a bare `list` payload, so every command migrated
    to a typed payload got a documented flag that silently did nothing — and
    the one command that kept the projection working (`sync status`) did it by
    hand, outside this path.
    """

    @staticmethod
    def _rows() -> list[_AccountRow]:
        return [_AccountRow(id="a1", account_number="123456789", label="Checking")]

    @pytest.mark.unit
    def test_projects_into_a_typed_payloads_single_list_field(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(
            build_envelope(data=_OneListPayload(rows=self._rows(), total=1)),
            OutputFormat.JSON,
            json_fields="id,label",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"]["rows"] == [{"id": "a1", "label": "Checking"}]
        # The sibling scalar is untouched: the projection narrows the rows, not
        # the payload around them.
        assert out["data"]["total"] == 1

    @pytest.mark.unit
    def test_projection_cannot_surface_a_field_redaction_masked(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Asking for a CRITICAL field by name returns it masked, never raw.

        The projection runs after `redact_typed`, and this is the assertion
        that keeps it there: a filter applied to the pre-redaction payload
        would hand back the account number the transform exists to hide.
        """
        render_or_json(
            build_envelope(data=_OneListPayload(rows=self._rows(), total=1)),
            OutputFormat.JSON,
            json_fields="account_number",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"]["rows"] == [{"account_number": "****6789"}]

    @pytest.mark.unit
    def test_no_ops_when_the_payload_carries_two_lists(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Half a projection is worse than none — the caller cannot see which half."""
        render_or_json(
            build_envelope(
                data=_TwoListPayload(rows=self._rows(), others=self._rows())
            ),
            OutputFormat.JSON,
            json_fields="id",
        )
        out = json.loads(capsys.readouterr().out)
        assert set(out["data"]["rows"][0]) == {"id", "account_number", "label"}
        assert set(out["data"]["others"][0]) == {"id", "account_number", "label"}

    @pytest.mark.unit
    def test_no_ops_on_a_collection_of_scalars(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A list of bare values has no fields to name.

        The old inline filter called `.items()` on every element, so this
        raised an AttributeError out of the output path rather than ignoring
        an inapplicable flag.
        """
        envelope: ResponseEnvelope[list[str]] = ResponseEnvelope(
            summary=SummaryMeta(total_count=2, returned_count=2),
            data=["a1", "a2"],
        )
        render_or_json(envelope, OutputFormat.JSON, json_fields="id")
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == ["a1", "a2"]

    @pytest.mark.unit
    def test_no_ops_when_the_payload_carries_no_list(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(
            build_envelope(data=_NoListPayload(total=4)),
            OutputFormat.JSON,
            json_fields="total",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"] == {"total": 4}


class TestRefreshDiagnosticsAreNotRowCollections:
    """A refresh's diagnostic lists must not be mistaken for the payload's rows.

    `GsheetPullPayload` carries `pulls` — the per-connection outcomes this call
    returned — beside the four best-effort refresh diagnostics
    (`identity_errors`, `rate_pairs_failed`, `rate_pairs_unsupported`,
    `rate_pairs_discarded`). Counting those as row collections gave the payload
    five, and both helpers that ask for "the" collection answered "several, so
    neither": `summary.returned_count` reported 1 for an N-connection pull, and
    `--json-fields` silently no-opped — the exact "flag accepted, does nothing"
    defect this path exists to eliminate.

    Pinned on the real payload, not a stand-in: the bug was the auxiliary set
    going stale against a shipped payload's fields, which a synthetic class
    cannot reproduce.
    """

    @staticmethod
    def _payload(n: int) -> GsheetPullPayload:
        rows = [
            GsheetPullRow(
                connection_id=f"c{i}",
                status="ok",
                rows_inserted=i,
                rows_upserted=0,
                rows_soft_deleted=0,
                drift_reason=None,
                error_message=None,
            )
            for i in range(n)
        ]
        return GsheetPullPayload(
            pulls=rows,
            identity_errors=["identity_resolution_failed"],
            rate_pairs_failed=["USD/EUR"],
            rate_pairs_unsupported=["USD/XYZ"],
            rate_pairs_discarded=["USD/GBP"],
        )

    @pytest.mark.unit
    def test_counts_the_pulls_not_the_diagnostics(self) -> None:
        envelope = build_envelope(data=self._payload(3))
        assert envelope.summary.returned_count == 3
        assert envelope.summary.total_count == 3

    @pytest.mark.unit
    def test_json_fields_still_finds_the_pull_rows(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(
            build_envelope(data=self._payload(2)),
            OutputFormat.JSON,
            json_fields="connection_id,status",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"]["pulls"] == [
            {"connection_id": "c0", "status": "ok"},
            {"connection_id": "c1", "status": "ok"},
        ]
        # The diagnostics ride through untouched — they are not rows to narrow.
        assert out["data"]["rate_pairs_failed"] == ["USD/EUR"]


class TestEmitJsonError:
    """Tests for emit_json_error helper."""

    @pytest.mark.unit
    def test_emits_error_envelope_to_stdout(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        err = UserError("DB locked", code=error_codes.INFRA_DATABASE_LOCKED)
        emit_json_error(err)
        out = json.loads(capsys.readouterr().out)
        assert out["status"] == "error"
        assert out["error"]["code"] == error_codes.INFRA_DATABASE_LOCKED
        assert out["error"]["message"] == "DB locked"

    @pytest.mark.unit
    def test_emits_valid_json(self, capsys: pytest.CaptureFixture[str]) -> None:
        emit_json_error(UserError("oops", code="unknown"))
        raw = capsys.readouterr().out.strip()
        assert json.loads(raw)  # no exception = valid JSON
