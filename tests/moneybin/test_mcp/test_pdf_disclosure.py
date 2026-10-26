"""MCP consent before a PDF's content reaches the AI host (pdf-disclosure-consent.md)."""

from __future__ import annotations

import asyncio
import json
import shutil
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

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
from tests.moneybin.pdf_disclosure_helpers import mcp_host_ctx, use_mcp_host
from tests.moneybin.pdf_statement_fixtures import (
    CANARIES,
    CANARY_ACCOUNT_MIDDLE,
    CANARY_DESCRIPTION,
    CANARY_RECIPE,
    CANARY_ROWS,
    CANARY_SSN_PREFIX,
)

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"


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
    use_mcp_host(monkeypatch, None)

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
    use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)

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
    use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)

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
    ctx = mcp_host_ctx()
    use_mcp_host(monkeypatch, ctx)
    token = await _token(pdf)

    use_mcp_host(monkeypatch, ctx, elicits=True)
    refused = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert refused.error is not None
    assert refused.error.code == error_codes.MUTATION_INVALID_INPUT
    _assert_no_canary(refused)
    ctx.elicit.assert_not_called()

    use_mcp_host(monkeypatch, ctx)
    released = await import_preview_coarse(file_path=str(pdf), confirmation_token=token)
    assert CANARY_DESCRIPTION in _released_text(released)


async def test_relay_token_releases_once(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    use_mcp_host(monkeypatch, None)
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
    assert rows[0]["released"] is True
    assert rows[0]["refusal_code"] is None
    assert rows[1]["refusal_code"] == error_codes.MUTATION_CONFIRMATION_REPLAYED


@pytest.mark.parametrize("change", ["expired", "file_bytes", "recipient"])
async def test_relay_token_expired_and_mismatched(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch, change: str
) -> None:
    use_mcp_host(monkeypatch, None)
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
        use_mcp_host(monkeypatch, mcp_host_ctx(name="claude-ai"))

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
    use_mcp_host(monkeypatch, mcp_host_ctx())
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
    assert prompt is not None and prompt.startswith(req["risk_statement"])
    assert "future PDFs" in prompt

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
    use_mcp_host(
        monkeypatch,
        mcp_host_ctx(name="some-unknown-client", elicit=elicit),
        elicits=True,
    )

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

    ctx = mcp_host_ctx()
    use_mcp_host(monkeypatch, ctx)
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
    use_mcp_host(monkeypatch, None)
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


async def test_batch_consent_reaches_import_confirm_with_one_decision(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    """One consent in import_files is enough to finish the import."""
    use_mcp_host(monkeypatch, None)
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
        bridge_response={"recipe": CANARY_RECIPE, "rows": CANARY_ROWS},
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

    use_mcp_host(
        monkeypatch,
        mcp_host_ctx(elicit=AsyncMock(side_effect=_accept_then_change)),
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
    use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)

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

    ctx = mcp_host_ctx()
    use_mcp_host(monkeypatch, ctx)
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


# --- Final-review fixes (R22) ---------------------------------------------


def _track_threads(
    monkeypatch: MonkeyPatch,
    module: object,
    names: list[str],
    seen: list[tuple[str, int]],
) -> None:
    """Record the thread each named module function runs on."""
    for name in names:
        original = getattr(module, name)

        def _wrapped(
            *args: Any, _orig: Any = original, _name: str = name, **kwargs: Any
        ) -> Any:
            seen.append((_name, threading.get_ident()))
            return _orig(*args, **kwargs)

        monkeypatch.setattr(module, name, _wrapped)


_DISCLOSURE_DB_CALLS = [
    "_remembered_grant_id",
    "_record",
    "_remember",
    "_revoked_since",
]


async def test_import_files_runs_sync_work_off_the_event_loop(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    import moneybin.mcp.tools.import_tools as tools_module

    use_mcp_host(monkeypatch, mcp_host_ctx())
    first = _wire(await import_files_coarse(paths=[str(pdf)], refresh=False))
    token = first["data"]["consent_request"]["confirmation_token"]
    seen: list[tuple[str, int]] = []
    _track_threads(
        monkeypatch, tools_module, ["import_files", "_stage_pdf_bridge_preview"], seen
    )
    _track_threads(monkeypatch, disclosure_module, _DISCLOSURE_DB_CALLS, seen)

    env = await import_files_coarse(
        paths=[str(pdf)],
        refresh=False,
        confirmation_token=token,
        remember_disclosure=True,
    )

    assert env.error is None, env.error
    loop_thread = threading.get_ident()
    assert {name for name, _ in seen} == {
        "import_files",
        "_stage_pdf_bridge_preview",
        *_DISCLOSURE_DB_CALLS,
    }
    assert [name for name, ident in seen if ident == loop_thread] == []


async def test_import_preview_pdf_runs_sync_work_off_the_event_loop(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    import moneybin.mcp.tools.import_tools as tools_module

    use_mcp_host(monkeypatch, mcp_host_ctx())
    token = await _token(pdf)
    seen: list[tuple[str, int]] = []
    preview_calls = [
        "_read_pdf_preview_bytes",
        "_import_preview_pdf",
        "_persist_staged_preview",
    ]
    _track_threads(monkeypatch, tools_module, preview_calls, seen)
    _track_threads(monkeypatch, disclosure_module, _DISCLOSURE_DB_CALLS, seen)

    env = await import_preview_coarse(
        file_path=str(pdf), confirmation_token=token, remember_disclosure=True
    )

    assert CANARY_DESCRIPTION in _released_text(env)
    loop_thread = threading.get_ident()
    assert {name for name, _ in seen} == {*preview_calls, *_DISCLOSURE_DB_CALLS}
    assert [name for name, ident in seen if ident == loop_thread] == []


async def test_import_preview_tabular_runs_sync_work_off_the_event_loop(
    mcp_db: object, tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    import moneybin.mcp.tools.import_tools as tools_module

    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    csv = tmp_path / "statement.csv"
    csv.write_bytes(b"Date,Description,Amount\n2026-07-01,Coffee,-4.50\n")
    seen: list[tuple[str, int]] = []
    calls = [
        "_read_tabular_preview_bytes",
        "_import_preview_tabular",
        "_persist_staged_preview",
    ]
    _track_threads(monkeypatch, tools_module, calls, seen)

    env = await import_preview_coarse(file_path=str(csv))

    assert env.error is None, env.error
    loop_thread = threading.get_ident()
    assert {name for name, _ in seen} == set(calls)
    assert [name for name, ident in seen if ident == loop_thread] == []


async def test_import_files_tool_timeout_fires_during_blocking_import(
    mcp_db: object, monkeypatch: MonkeyPatch
) -> None:
    """A blocking import yields to the loop, so the per-tool cap can fire."""
    import time

    import moneybin.mcp.tools.import_tools as tools_module

    release = threading.Event()
    finished = threading.Event()

    def _blocking_import(**_kwargs: object) -> None:
        try:
            release.wait(timeout=5.0)
        finally:
            finished.set()

    monkeypatch.setattr(tools_module, "import_files", _blocking_import)
    monkeypatch.setattr("moneybin.mcp.decorator._get_timeout_seconds", lambda: 0.2)

    started = time.monotonic()
    try:
        env = await import_files_coarse(paths=["/tmp/x.csv"], refresh=False)  # noqa: S108  # never read: the stub blocks
        elapsed = time.monotonic() - started
    finally:
        release.set()
        assert finished.wait(timeout=5.0)

    assert env.error is not None
    assert env.error.code == error_codes.INFRA_TIMED_OUT
    assert elapsed < 5.0


@pytest.mark.parametrize("path", ["prompt", "agent_relay"])
async def test_failed_release_records_approval_but_no_grant(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch, path: str
) -> None:
    """The person's yes is audited; remembering waits for an actual release."""
    import moneybin.mcp.tools.import_tools as tools_module

    if path == "prompt":
        elicit = AsyncMock(
            return_value=AcceptedElicitation(
                data=DisclosureAnswer(accept=True, remember=True)
            )
        )
        use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)
        kwargs: dict[str, Any] = {}
    else:
        use_mcp_host(monkeypatch, mcp_host_ctx())
        kwargs = {"confirmation_token": await _token(pdf), "remember_disclosure": True}
    real = tools_module._import_preview_pdf  # pyright: ignore[reportPrivateUsage]

    def _fail_release(*args: Any, **kw: Any) -> Any:
        if kw.get("disclosure_consent") is not None:
            raise RuntimeError("release failed")
        return real(*args, **kw)

    monkeypatch.setattr(tools_module, "_import_preview_pdf", _fail_release)

    env = await import_preview_coarse(file_path=str(pdf), **kwargs)

    assert env.error is not None
    _assert_no_canary(env)
    assert _host_grants() == []
    [row] = _consent_rows()
    assert row["outcome"] == "approved" and row["approved_via"] == path
    assert row["released"] is False
    assert row["remembered"] is False and row["grant_id"] is None


async def test_batch_bytes_changed_records_approval_but_no_grant(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    async def _accept_then_change(*_a: object, **_k: object) -> object:
        pdf.write_bytes(pdf.read_bytes() + b"\n% edited after consent\n")
        return AcceptedElicitation(data=DisclosureAnswer(accept=True, remember=True))

    use_mcp_host(
        monkeypatch,
        mcp_host_ctx(elicit=AsyncMock(side_effect=_accept_then_change)),
        elicits=True,
    )

    env = await import_files_coarse(paths=[str(pdf)], refresh=False)

    _assert_no_canary(env)
    assert _host_grants() == []
    [row] = _consent_rows()
    assert row["outcome"] == "approved" and row["approved_via"] == "prompt"
    assert row["released"] is False
    assert row["remembered"] is False and row["grant_id"] is None


async def test_elicitation_remember_states_revoke_and_is_the_stored_prompt(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    elicit = AsyncMock(
        return_value=AcceptedElicitation(
            data=DisclosureAnswer(accept=True, remember=True)
        )
    )
    use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)

    env = await import_preview_coarse(file_path=str(pdf))

    assert CANARY_DESCRIPTION in _released_text(env)
    message = elicit.call_args.args[0]
    assert "future PDFs to Claude" in message
    assert "privacy revoke smart-import-parsing --recipient claude-ai" in message
    [(_backend, _via, prompt)] = _host_grants()
    assert prompt == message
    [row] = _consent_rows()
    assert row["released"] is True and row["remembered"] is True


async def test_relay_remember_grant_prompt_is_the_relayed_text(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    """R11/R20: the stored prompt is what the agent was told to show, remembering included."""
    use_mcp_host(monkeypatch, mcp_host_ctx())
    first = await import_preview_coarse(file_path=str(pdf))
    req = _wire(first)["data"]["consent_request"]

    await import_preview_coarse(
        file_path=str(pdf),
        confirmation_token=req["confirmation_token"],
        remember_disclosure=True,
    )

    [(_backend, _via, prompt)] = _host_grants()
    assert prompt is not None
    statement = req["risk_statement"]
    assert prompt.startswith(statement)
    remember = prompt[len(statement) :].strip()
    assert "future PDFs" in remember and "Claude" in remember
    assert any(remember in action for action in first.actions)


async def test_consent_row_carries_binding_id_of_parse_row(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    use_mcp_host(monkeypatch, None)
    first = _wire(await import_preview_coarse(file_path=str(pdf)))
    req = first["data"]["consent_request"]

    await import_preview_coarse(
        file_path=str(pdf), confirmation_token=req["confirmation_token"]
    )

    [consent] = _consent_rows()
    with get_database(read_only=True) as db:
        [(parse_context,)] = db.execute(
            "SELECT context_json FROM app.audit_log WHERE action = 'smart_import_parse'"
        ).fetchall()
    assert consent["binding_id"] == req["binding_id"]
    assert consent["binding_id"] == json.loads(parse_context)["binding_id"]


# --- Pre-push fixes --------------------------------------------------------


@pytest.mark.parametrize("transport", ["sse", "streamable-http"])
async def test_network_transport_known_name_is_one_time_only(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch, transport: str
) -> None:
    """No authenticated principal on network transports: never offer remember."""
    elicit = AsyncMock(
        return_value=AcceptedElicitation(
            data=DisclosureAnswer(accept=True, remember=True)
        )
    )
    ctx = mcp_host_ctx(elicit=elicit, transport=transport)
    use_mcp_host(monkeypatch, ctx, elicits=True)

    assert disclosure_module.current_ai_host(ctx).identified is False
    env = await import_preview_coarse(file_path=str(pdf))

    assert CANARY_DESCRIPTION in _released_text(env)
    message = elicit.call_args.args[0]
    assert "future PDFs" not in message and "--recipient" not in message
    assert elicit.call_args.kwargs["response_type"] is not DisclosureAnswer
    assert _host_grants() == []
    [row] = _consent_rows()
    assert row["remembered"] is False and row["grant_id"] is None


async def test_network_transport_relay_offers_no_remember(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    use_mcp_host(monkeypatch, mcp_host_ctx(transport="streamable-http"))

    first = await import_preview_coarse(file_path=str(pdf))
    req = _wire(first)["data"]["consent_request"]
    assert req["remember_available"] is False
    assert not any("future PDFs" in a for a in first.actions)

    await import_preview_coarse(
        file_path=str(pdf),
        confirmation_token=req["confirmation_token"],
        remember_disclosure=True,
    )
    assert _host_grants() == []


async def test_stdio_known_name_still_offers_remember(
    mcp_db: object, pdf: Path, monkeypatch: MonkeyPatch
) -> None:
    ctx = mcp_host_ctx()
    use_mcp_host(monkeypatch, ctx)
    host = disclosure_module.current_ai_host(ctx)
    assert host.identified is True and host.recipient_key == "local:claude-ai"
