"""Tests for shared CLI output helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pytest

from moneybin import error_codes
from moneybin.cli import pager
from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    emit_json_error,
    render_or_json,
)
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.errors import UserError
from moneybin.privacy.payloads.gsheet import GsheetPullPayload, GsheetPullRow
from moneybin.privacy.payloads.sync import SyncPullInstitutionRow, SyncPullPayload
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, SummaryMeta, build_envelope
from moneybin.protocol.row_set import NO_ROW_SET, row_set


def _tty_policy(*, height: int = 20, page: bool = True) -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=page,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=True,
        ascii=True,
        width=20,
        height=height,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )


def _unexpected_page(text: str, *, color: bool, wide: bool) -> bool:
    raise AssertionError("paged")


def test_human_result_pages_only_after_its_rendered_height_exceeds_terminal(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Wrapped text plus the reserved pager hint decides the threshold."""
    calls: list[str] = []

    def _record_page(text: str, *, color: bool, wide: bool) -> bool:
        calls.append(text)
        return True

    monkeypatch.setattr(pager, "page_text", _record_page)

    emit_human_result(
        "heading\n" + "long name wraps here\n" * 19,
        policy=_tty_policy(),
        finite_read=True,
    )

    assert len(calls) == 1
    assert "q return to shell" in calls[0]
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("policy", "finite_read", "live_follow", "receipt"),
    [
        (_tty_policy(page=False), True, False, False),
        (_tty_policy(), False, False, False),
        (_tty_policy(), True, True, False),
        (_tty_policy(), True, False, True),
    ],
)
def test_human_result_does_not_page_in_ineligible_modes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    policy: TerminalPolicy,
    finite_read: bool,
    live_follow: bool,
    receipt: bool,
) -> None:
    """Paging must remain exclusive to interactive finite reads."""
    monkeypatch.setattr(pager, "page_text", _unexpected_page)

    emit_human_result(
        "result\n" * 30,
        policy=policy,
        finite_read=finite_read,
        live_follow=live_follow,
        receipt=receipt,
    )

    assert "result" in capsys.readouterr().out


def test_human_result_never_pages_json_or_emits_ansi(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A contradictory policy cannot leak human paging into JSON output."""
    from rich.text import Text

    policy = _tty_policy()
    policy = TerminalPolicy(
        output="json",
        interactive=policy.interactive,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=policy.width,
        height=policy.height,
        symbols=policy.symbols,
        minus="-",
    )
    monkeypatch.setattr(pager, "page_text", _unexpected_page)

    emit_human_result(Text("answer", style="red"), policy=policy, finite_read=True)

    assert capsys.readouterr().out == "answer\n"


def test_human_result_honors_explicit_no_pager(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The leaf flag remains a visible final gate after policy resolution."""
    monkeypatch.setattr(pager, "page_text", _unexpected_page)

    emit_human_result(
        "result\n" * 30, policy=_tty_policy(), finite_read=True, no_pager=True
    )

    assert "result" in capsys.readouterr().out


def test_human_result_strips_controls_from_colored_external_text_before_paging(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A data field cannot add its own terminal style beside Rich's semantic one."""
    from rich.text import Text

    paged: list[str] = []

    def _record_page(text: str, *, color: bool, wide: bool) -> bool:
        paged.append(text)
        return True

    policy = _tty_policy(height=1)
    policy = TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=True,
        style=True,
        animate_progress=False,
        stage_chatter=True,
        ascii=True,
        width=20,
        height=1,
        symbols=policy.symbols,
        minus="-",
    )
    monkeypatch.setattr(pager, "page_text", _record_page)

    emit_human_result(
        Text("safe \x1b[35minjected\x1b[0m", style="red"),
        policy=policy,
        finite_read=True,
    )

    assert "\x1b[35m" not in paged[0]
    assert "\x1b[31m" in paged[0]
    assert "safe injected" in paged[0]


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


@row_set("rows")
@dataclass(frozen=True, slots=True)
class _OneListPayload:
    """The shape every migrated collection command uses: one list, plus counts."""

    rows: list[_AccountRow]
    total: Annotated[int, DataClass.AGGREGATE]


@row_set(NO_ROW_SET)
@dataclass(frozen=True, slots=True)
class _TwoListPayload:
    """Two peer collections — the payload declares that neither is its rows."""

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
    def test_no_ops_when_the_payload_declares_no_row_set(
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


class TestSyncPullCountsInstitutions:
    """The overlap warning must not be mistaken for a second row collection.

    `SyncPullPayload` carries `institutions` — the per-institution outcomes the
    pull returned — beside `investment_source_overlap_accounts`, which names the
    accounts holding both manual and Plaid investment history. That is a warning
    about the rows, not another set of them, so counting it left the payload with
    two collections and the "sole collection" rule answered "several, so
    neither": `sync pull --output json`, `sync_pull` over MCP, and the CLI
    privacy audit row all reported `returned_count=1` however many institutions
    the pull covered.

    Pinned on the shipped payload rather than a stand-in, for the same reason as
    the class above: the defect is the auxiliary set going stale against a real
    payload's fields.
    """

    @staticmethod
    def _payload(n: int) -> SyncPullPayload:
        return SyncPullPayload(
            job_id="job_1",
            transactions_loaded=0,
            accounts_loaded=0,
            balances_loaded=0,
            transactions_removed=0,
            institutions=[
                SyncPullInstitutionRow(
                    provider_item_id=f"item_{i}",
                    institution_name=None,
                    status="ok",
                    transaction_count=i,
                    error=None,
                    error_code=None,
                )
                for i in range(n)
            ],
            transforms_applied=True,
            transforms_duration_seconds=None,
            transforms_error=None,
            investment_source_overlap_accounts=["acct_a", "acct_b"],
        )

    @pytest.mark.unit
    @pytest.mark.parametrize("count", [0, 1, 3])
    def test_counts_the_institutions_not_the_overlap_warning(self, count: int) -> None:
        payload = self._payload(count)
        envelope = build_envelope(data=payload)
        assert envelope.summary.returned_count == len(payload.institutions)
        assert envelope.summary.returned_count == count
        assert envelope.summary.total_count == count

    @pytest.mark.unit
    def test_json_fields_still_finds_the_institution_rows(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        render_or_json(
            build_envelope(data=self._payload(2)),
            OutputFormat.JSON,
            json_fields="provider_item_id,status",
        )
        out = json.loads(capsys.readouterr().out)
        assert out["data"]["institutions"] == [
            {"provider_item_id": "item_0", "status": "ok"},
            {"provider_item_id": "item_1", "status": "ok"},
        ]
        # The warning rides through untouched — it is not a row set to narrow.
        assert out["data"]["investment_source_overlap_accounts"] == [
            "acct_a",
            "acct_b",
        ]


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
