"""MCP consent before a PDF's content reaches the AI host (pdf-disclosure-consent.md)."""

from __future__ import annotations

import asyncio
import json
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import (
    AcceptedElicitation,
    CancelledElicitation,
    DeclinedElicitation,
)
from pytest import MonkeyPatch

import moneybin.mcp.pdf_disclosure as disclosure_module
from moneybin import error_codes
from moneybin.database import get_database
from moneybin.mcp.pdf_disclosure import DisclosureAnswer
from moneybin.mcp.tools.import_tools import (
    import_confirm_coarse,
    import_files_coarse,
    import_preview_coarse,
)
from moneybin.privacy.classified_envelope import classify
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope
from tests.moneybin.pdf_statement_fixtures import (
    CANARIES,
    CANARY_ACCOUNT_MIDDLE,
    CANARY_DESCRIPTION,
    CANARY_SSN_PREFIX,
)

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


def _ctx(
    *, name: str | None = "claude-ai", elicit: AsyncMock | None = None
) -> MagicMock:
    ctx = MagicMock()
    ctx.session.client_params.clientInfo.name = name
    ctx.session.client_params.clientInfo.version = "1.0.0"
    ctx.transport = "stdio"
    ctx.elicit = elicit or AsyncMock()
    return ctx


def _use(
    monkeypatch: MonkeyPatch, ctx: MagicMock | None, *, elicits: bool = False
) -> None:
    def _supports(_ctx: object) -> bool:
        return elicits

    monkeypatch.setattr(disclosure_module, "_active_context", lambda: ctx)
    monkeypatch.setattr(disclosure_module, "supports_elicitation", _supports)


@pytest.fixture()
def pdf(canary_pdf: Path, tmp_path: Path, monkeypatch: MonkeyPatch) -> Path:
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return canary_pdf


def _wire(env: ResponseEnvelope[Any]) -> dict[str, Any]:
    return json.loads(env.to_json())


def _assert_no_canary(env: ResponseEnvelope[Any]) -> None:
    body = env.to_json()
    for canary in CANARIES:
        assert canary not in body


def _consent_rows() -> list[dict[str, Any]]:
    with get_database(read_only=True) as db:
        rows = db.execute(
            "SELECT after_value FROM app.audit_log "
            "WHERE action = 'smart_import_consent' ORDER BY occurred_at"
        ).fetchall()
    return [json.loads(r[0]) for r in rows]


def _host_grants() -> list[tuple[str, str | None, str | None]]:
    with get_database(read_only=True) as db:
        return db.execute(
            "SELECT backend, approved_via, grant_prompt FROM app.ai_consent_grants "
            "WHERE recipient_kind = 'ai_host' AND revoked_at IS NULL"
        ).fetchall()


async def _token(pdf: Path) -> str:
    first = _wire(await import_preview_coarse(file_path=str(pdf)))
    return first["data"]["consent_request"]["confirmation_token"]


def _released_text(env: ResponseEnvelope[Any]) -> str:
    data = _wire(env)["data"]
    assert data["kind"] == "pdf_bridge", data
    return data["bridge_payload"]["document_text"]


async def test_preview_first_call_content_free_with_consent_request(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    _use(monkeypatch, None)

    env = await import_preview_coarse(file_path=str(pdf))

    _assert_no_canary(env)
    wire = _wire(env)
    assert wire["status"] == "ok"
    data = wire["data"]
    assert data["status"] == "ai_assistance_required"
    req = data["consent_request"]
    assert req["recipient"]["identified"] is False
    assert req["remember_available"] is False
    assert "risk_statement" in req and req["confirmation_token"]
    assert req["files"][0]["file"] == str(pdf)
    assert env.actions
    assert "preview_id" not in data
    assert _consent_rows() == []


async def test_elicitation_accept_releases_and_audits_prompt(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    elicit = AsyncMock(
        return_value=AcceptedElicitation(data=DisclosureAnswer(accept=True))
    )
    _use(monkeypatch, _ctx(elicit=elicit), elicits=True)

    env = await import_preview_coarse(file_path=str(pdf))

    text = _released_text(env)
    assert CANARY_DESCRIPTION in text
    assert CANARY_ACCOUNT_MIDDLE not in text
    assert CANARY_SSN_PREFIX not in text
    message = elicit.call_args.args[0]
    assert "SSNs" in message and "Claude" in message
    [row] = _consent_rows()
    assert row["outcome"] == "approved"
    assert row["approved_via"] == "prompt"
    assert row["remembered"] is False
    assert _host_grants() == []


@pytest.mark.parametrize(
    ("answer", "outcome"),
    [
        (DeclinedElicitation(), "declined"),
        (CancelledElicitation(), "declined"),
        (None, "timeout"),
    ],
    ids=["decline", "cancel", "timeout"],
)
async def test_elicitation_decline_cancel_timeout_release_nothing(
    mcp_db: object,
    pdf: Path,
    monkeypatch: MonkeyPatch,
    answer: object,
    outcome: str,
) -> None:
    if answer is None:

        async def _late(*_a: object, **_k: object) -> object:
            await asyncio.sleep(1.0)
            return AcceptedElicitation(data=DisclosureAnswer(accept=True))

        elicit = AsyncMock(side_effect=_late)
        monkeypatch.setattr(
            "moneybin.mcp.elicitation.elicitation_wait_seconds", lambda: 0.05
        )
    else:
        elicit = AsyncMock(return_value=answer)
    _use(monkeypatch, _ctx(elicit=elicit), elicits=True)

    env = await import_preview_coarse(file_path=str(pdf))

    _assert_no_canary(env)
    data = _wire(env)["data"]
    assert data["status"] == "ai_assistance_required"
    assert data["consent_request"]["refusal_reason"] == outcome
    assert data["consent_request"]["confirmation_token"] is None
    [row] = _consent_rows()
    assert row["outcome"] == outcome
    assert row["approved_via"] == "prompt"


async def test_token_on_elicitation_client_refused_unconsumed(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    ctx = _ctx()
    _use(monkeypatch, ctx)
    token = await _token(pdf)

    _use(monkeypatch, ctx, elicits=True)
    refused = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert refused.error is not None
    assert refused.error.code == error_codes.MUTATION_INVALID_INPUT
    _assert_no_canary(refused)
    ctx.elicit.assert_not_called()

    _use(monkeypatch, ctx)
    released = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert CANARY_DESCRIPTION in _released_text(released)


async def test_relay_token_releases_once(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    _use(monkeypatch, None)
    token = await _token(pdf)

    released = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert CANARY_DESCRIPTION in _released_text(released)
    assert released.actions and any(
        "import_confirm(preview_id=" in a for a in released.actions
    )

    replay = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert replay.error is not None
    assert replay.error.code == error_codes.MUTATION_CONFIRMATION_REPLAYED
    _assert_no_canary(replay)
    rows = _consent_rows()
    assert [r["outcome"] for r in rows] == ["approved", "expired"]
    assert rows[0]["approved_via"] == "agent_relay"
    assert rows[0]["refusal_code"] is None
    assert rows[1]["refusal_code"] == error_codes.MUTATION_CONFIRMATION_REPLAYED


@pytest.mark.parametrize("change", ["expired", "file_bytes", "recipient"])
async def test_relay_token_expired_and_mismatched(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch, change: str
) -> None:
    _use(monkeypatch, None)
    token = await _token(pdf)

    expected = {
        "expired": error_codes.MUTATION_CONFIRMATION_EXPIRED,
        "file_bytes": error_codes.MUTATION_CONFIRMATION_MISMATCH,
        "recipient": error_codes.MUTATION_CONFIRMATION_MISMATCH,
    }[change]
    if change == "expired":
        later = disclosure_module._utcnow() + timedelta(days=1)  # pyright: ignore[reportPrivateUsage]
        monkeypatch.setattr(disclosure_module, "_utcnow", lambda: later)
    elif change == "file_bytes":
        pdf.write_bytes(pdf.read_bytes() + b"\n% trailing comment\n")
    else:
        _use(monkeypatch, _ctx(name="claude-ai"))

    env = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)

    assert env.error is not None
    assert env.error.code == expected
    _assert_no_canary(env)
    [row] = _consent_rows()
    assert row["outcome"] == "expired"
    assert row["refusal_code"] == expected


async def test_remember_creates_host_grant_and_next_call_is_silent_with_notice(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    _use(monkeypatch, _ctx())
    first = _wire(await import_preview_coarse(file_path=str(pdf)))
    req = first["data"]["consent_request"]
    assert req["remember_available"] is True

    await import_preview_coarse(
        file_path=str(pdf),
        confirmation_token=req["confirmation_token"],
        remember_disclosure=True,
    )
    [(backend, approved_via, prompt)] = _host_grants()
    assert backend == "local:claude-ai"
    assert approved_via == "agent_relay"
    assert prompt == req["risk_statement"]

    again = await import_preview_coarse(file_path=str(pdf))
    assert CANARY_DESCRIPTION in _released_text(again)
    grant_id = _consent_rows()[-1]["grant_id"]
    assert grant_id
    assert any(grant_id in a and "privacy revoke" in a for a in again.actions)
    assert _consent_rows()[-1]["approved_via"] == "remembered"


async def test_unidentified_host_remember_ignored(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    elicit = AsyncMock(
        return_value=AcceptedElicitation(
            data=DisclosureAnswer(accept=True, remember=True)
        )
    )
    _use(monkeypatch, _ctx(name="some-unknown-client", elicit=elicit), elicits=True)

    env = await import_preview_coarse(file_path=str(pdf))

    assert CANARY_DESCRIPTION in _released_text(env)
    assert _host_grants() == []
    [row] = _consent_rows()
    assert row["remembered"] is False and row["identified"] is False
    assert "cannot identify" in elicit.call_args.args[0]


async def test_revocation_invalidates_outstanding_token(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    from moneybin.services.consent_service import ConsentService

    ctx = _ctx()
    _use(monkeypatch, ctx)
    token = await _token(pdf)
    host = disclosure_module.current_ai_host(ctx)
    with get_database(read_only=False) as db:
        ConsentService(db).grant_host_consent(
            host=host, approved_via="cli_prompt", actor="cli"
        )
        ConsentService(db).revoke_host_consent(
            recipient_key="local:claude-ai", actor="cli"
        )

    env = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)

    assert env.error is not None
    assert env.error.code == error_codes.MUTATION_CONFIRMATION_EXPIRED
    _assert_no_canary(env)


async def test_batch_one_consent_only_bridge_files(
    mcp_db: object, pdf: Path, tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    _use(monkeypatch, None)
    twin = tmp_path / "canary_statement_copy.pdf"
    shutil.copy(pdf, twin)
    qfx = tmp_path / "statement.qfx"
    shutil.copy(_FIXTURES / "sample_statement.qfx", qfx)

    first = await import_files_coarse(
        paths=[str(qfx), str(pdf), str(twin)], refresh=False
    )

    _assert_no_canary(first)
    data = _wire(first)["data"]
    statuses = {Path(f["path"]).name: f["status"] for f in data["files"]}
    assert statuses["statement.qfx"] == "imported"
    assert statuses[pdf.name] == statuses[twin.name] == "confirmation_required"
    req = data["consent_request"]
    assert sorted(f["file"] for f in req["files"]) == sorted([str(pdf), str(twin)])

    second = await import_files_coarse(
        paths=[str(pdf), str(twin)],
        refresh=False,
        confirmation_token=req["confirmation_token"],
    )
    rows = _wire(second)["data"]["files"]
    assert len(rows) == 2
    for row in rows:
        bridge = row["confirmation_payload"]["bridge_payload"]
        assert CANARY_DESCRIPTION in bridge["document_text"]
        assert CANARY_ACCOUNT_MIDDLE not in bridge["document_text"]
        assert row["preview_id"]
        assert any(
            f"import_confirm(preview_id='{row['preview_id']}'" in a
            for a in second.actions
        )
    assert not any("import_preview(" in a for a in second.actions)
    assert _wire(second)["data"]["consent_request"] is None
    [row] = [r for r in _consent_rows() if r["outcome"] == "approved"]
    assert len(row["file_sha256s"]) == 2


def test_ai_assistance_payload_declares_no_financial_class() -> None:
    from moneybin.privacy.payloads.imports import ImportAIAssistanceRequiredPayload

    classes = classify(ImportAIAssistanceRequiredPayload).classes
    assert classes
    for forbidden in (
        DataClass.TXN_AMOUNT,
        DataClass.BALANCE,
        DataClass.DESCRIPTION,
        DataClass.ACCOUNT_IDENTIFIER,
        DataClass.MERCHANT_NAME,
        DataClass.TXN_DATE,
    ):
        assert forbidden not in classes


def _parse_rows() -> int:
    with get_database(read_only=True) as db:
        row = db.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE action = 'smart_import_parse'"
        ).fetchone()
    return int(row[0]) if row else 0


_CANARY_RECIPE: dict[str, Any] = {
    "metadata_anchors": [
        {
            "name": "account_id",
            "pattern": r"Account\s+Number:\s+([X\d]{4} [X\d]{4} \d{4})",
            "cast": "str",
        },
        {
            "name": "period_start",
            "pattern": r"Statement\s+Period:\s+(\d{2}/\d{2}/\d{4})",
            "cast": "date",
            "date_format": "%m/%d/%Y",
        },
        {
            "name": "period_end",
            "pattern": r"to\s+(\d{2}/\d{2}/\d{4})",
            "cast": "date",
            "date_format": "%m/%d/%Y",
        },
        {
            "name": "opening_balance",
            "pattern": r"Opening\s+Funds:\s+\$([\d,]+\.\d{2})",
            "cast": "decimal",
        },
        {
            "name": "closing_balance",
            "pattern": r"Closing\s+Funds:\s+\$([\d,]+\.\d{2})",
            "cast": "decimal",
        },
    ],
    "row_region": {"start_anchor": "Date", "end_anchor": "@@END@@"},
    "row_split": r"\s{2,}",
    "fields": [
        {
            "name": "date",
            "pattern": r"\d{2}/\d{2}/\d{4}",
            "cast": "date",
            "date_format": "%m/%d/%Y",
        },
        {"name": "description", "pattern": r".+", "cast": "str"},
        {"name": "amount", "pattern": r"-?[\d,]+\.\d{2}", "cast": "decimal"},
    ],
    "sign_convention": "negative_is_expense",
    "routing": "transactions",
}
_CANARY_ROWS = [
    {"date": "01/03/2024", "description": CANARY_DESCRIPTION, "amount": "-47.31"},
    {"date": "01/10/2024", "description": "PAYROLL DEPOSIT", "amount": "1500.00"},
    {"date": "01/20/2024", "description": "UTILITY PAYMENT", "amount": "-152.69"},
]


async def test_batch_consent_reaches_import_confirm_with_one_decision(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    """One consent in import_files is enough to finish the import."""
    _use(monkeypatch, None)
    first = _wire(await import_files_coarse(paths=[str(pdf)], refresh=False))
    token = first["data"]["consent_request"]["confirmation_token"]

    second = _wire(
        await import_files_coarse(
            paths=[str(pdf)], refresh=False, confirmation_token=token
        )
    )
    [row] = second["data"]["files"]
    preview_id = row["preview_id"]
    assert preview_id

    confirmed = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": _CANARY_RECIPE, "rows": _CANARY_ROWS},
        account_bindings={"@0": "new"},
    )

    assert confirmed.error is None, confirmed.error
    assert [r["outcome"] for r in _consent_rows()] == ["approved"]
    assert _parse_rows() == 1


async def test_batch_bytes_changed_after_consent_release_nothing(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    """Consent covers the bytes the person was asked about, not a later file."""

    async def _accept_then_change(*_a: object, **_k: object) -> object:
        pdf.write_bytes(pdf.read_bytes() + b"\n% edited after consent\n")
        return AcceptedElicitation(data=DisclosureAnswer(accept=True))

    _use(
        monkeypatch,
        _ctx(elicit=AsyncMock(side_effect=_accept_then_change)),
        elicits=True,
    )

    env = await import_files_coarse(paths=[str(pdf)], refresh=False)

    _assert_no_canary(env)
    [row] = _wire(env)["data"]["files"]
    assert row["preview_id"] is None
    assert row["confirmation_payload"]["bridge_payload"] is None
    assert _parse_rows() == 0
    assert any("changed" in a for a in env.actions)


async def test_batch_elicitation_releases_with_preview(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    elicit = AsyncMock(
        return_value=AcceptedElicitation(data=DisclosureAnswer(accept=True))
    )
    _use(monkeypatch, _ctx(elicit=elicit), elicits=True)

    env = await import_files_coarse(paths=[str(pdf)], refresh=False)

    [row] = _wire(env)["data"]["files"]
    assert (
        CANARY_DESCRIPTION
        in row["confirmation_payload"]["bridge_payload"]["document_text"]
    )
    assert row["preview_id"]
    elicit.assert_awaited_once()
    [decision] = _consent_rows()
    assert decision["approved_via"] == "prompt"


async def test_batch_remembered_grant_releases_with_notice(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    from moneybin.services.consent_service import ConsentService

    ctx = _ctx()
    _use(monkeypatch, ctx)
    with get_database(read_only=False) as db:
        grant, _ = ConsentService(db).grant_host_consent(
            host=disclosure_module.current_ai_host(ctx),
            approved_via="cli_prompt",
            actor="cli",
        )

    env = await import_files_coarse(paths=[str(pdf)], refresh=False)

    [row] = _wire(env)["data"]["files"]
    assert row["preview_id"]
    assert any(grant.grant_id in a and "privacy revoke" in a for a in env.actions)
    assert _consent_rows()[-1]["approved_via"] == "remembered"
