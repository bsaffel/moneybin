"""No PDF content reaches any sink before consent, on every entry point.

Cross-surface sweep for pdf-disclosure-consent.md §1: each pre-consent entry
point runs against a canary statement, then every sink it can write to is
searched for every canary — the response, stdout/stderr, root-DEBUG logs (and the
``--verbose`` log file), the audit log, and the privacy log.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from fastmcp.server.elicitation import DeclinedElicitation
from pytest import MonkeyPatch
from typer.testing import CliRunner

from moneybin.cli.commands.import_cmd import app
from moneybin.database import get_database
from moneybin.mcp.tools.import_tools import (
    import_confirm_coarse,
    import_files_coarse,
    import_preview_coarse,
)
from moneybin.privacy.disclosure import host_for_recipient_name
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.consent_service import ConsentService
from moneybin.services.inbox_service import InboxService
from tests.moneybin.pdf_disclosure_helpers import (
    make_inbox_settings,
    mcp_host_ctx,
    use_mcp_host,
)
from tests.moneybin.pdf_statement_fixtures import (
    CANARIES,
    CANARY_ACCOUNT_MIDDLE,
    CANARY_AMOUNT,
    CANARY_DESCRIPTION,
    CANARY_RECIPE,
    CANARY_ROUTING,
    CANARY_ROWS,
    CANARY_SSN_PREFIX,
    write_canary_deterministic_pdf,
    write_canary_replay_failure_pdf,
)

runner = CliRunner()

_REDACTED: tuple[str, ...] = (CANARY_ACCOUNT_MIDDLE, CANARY_SSN_PREFIX, CANARY_ROUTING)


@pytest.fixture()
def sweep(
    consent_db: Path,
    canary_pdf: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> Path:
    """Profile DB, root-DEBUG capture and ``--verbose`` file logging; returns the PDF."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.delenv("CLAUDECODE", raising=False)
    caplog.set_level(logging.DEBUG)
    return canary_pdf


def _envelope_text(*envs: ResponseEnvelope[Any]) -> str:
    return "\n".join(
        [e.to_json() for e in envs] + [a for e in envs for a in e.actions or []]
    )


def _cli_text(*results: Any) -> str:
    return "\n".join(f"{r.stdout}\n{r.stderr}\n{r.exception!r}" for r in results)


def _audit_rows(*, skip: int = 0, action: str | None = None) -> list[str]:
    sql = "SELECT before_value, after_value, context_json FROM app.audit_log"
    params: list[str] = []
    if action:
        sql += " WHERE action = ?"
        params.append(action)
    with get_database(read_only=True) as db:
        rows = db.execute(sql + " ORDER BY occurred_at", params).fetchall()
    return ["\n".join(str(v) for v in row if v is not None) for row in rows[skip:]]


def _privacy_log_text(tmp_path: Path, *, required: bool) -> str:
    log = tmp_path / "privacy_log" / "privacy.log.jsonl"
    if required:
        assert log.exists(), "expected a privacy log row for the consented release"
        assert log.read_text().strip(), "privacy log is empty"
    return log.read_text() if log.exists() else ""


def _assert_sinks_absent(
    canaries: tuple[str, ...],
    *,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    log_dir: Path,
    response: str | None = None,
    audit_skip: int = 0,
    privacy_log_required: bool = False,
) -> None:
    sinks = {
        "caplog": caplog.text,
        "log file": "\n".join(p.read_text() for p in log_dir.glob("*.log")),
        "audit_log": "\n".join(_audit_rows(skip=audit_skip)),
        "privacy log": _privacy_log_text(tmp_path, required=privacy_log_required),
    }
    if response is not None:
        sinks["response"] = response
    for sink, text in sinks.items():
        for canary in canaries:
            assert canary not in text, f"canary leaked into {sink}"


def _assert_sinks_clean(
    response: str,
    *,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    log_dir: Path,
) -> None:
    """Pre-consent: no canary anywhere; a privacy-log row is not expected."""
    _assert_sinks_absent(
        CANARIES, tmp_path=tmp_path, caplog=caplog, log_dir=log_dir, response=response
    )


def _relay_token(env: ResponseEnvelope[Any]) -> str:
    return json.loads(env.to_json())["data"]["consent_request"]["confirmation_token"]


# --- MCP, no consent ------------------------------------------------------


async def test_mcp_import_preview_is_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    use_mcp_host(monkeypatch, None)

    env = await import_preview_coarse(file_path=str(sweep))

    assert json.loads(env.to_json())["data"]["status"] == "ai_assistance_required"
    _assert_sinks_clean(
        _envelope_text(env),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


async def test_mcp_import_preview_deterministic_fingerprint_is_redacted(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    """R14: header cells in the preview fingerprint carry no account number."""
    use_mcp_host(monkeypatch, None)
    deterministic = write_canary_deterministic_pdf(tmp_path)

    env = await import_preview_coarse(file_path=str(deterministic))

    data = json.loads(env.to_json())["data"]
    assert data.get("fingerprint"), data  # deterministic path reached a fingerprint
    assert "consent_request" not in data or data["consent_request"] is None
    assert CANARY_ACCOUNT_MIDDLE not in json.dumps(data["fingerprint"])
    _assert_sinks_absent(
        (CANARY_ACCOUNT_MIDDLE,),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
        response=_envelope_text(env),
    )


async def test_mcp_declined_elicitation_is_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    elicit = AsyncMock(return_value=DeclinedElicitation())
    use_mcp_host(monkeypatch, mcp_host_ctx(elicit=elicit), elicits=True)

    env = await import_preview_coarse(file_path=str(sweep))

    _assert_sinks_clean(
        _envelope_text(env),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


async def test_mcp_import_files_single_is_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    use_mcp_host(monkeypatch, None)

    env = await import_files_coarse(paths=[str(sweep)], refresh=False)

    _assert_sinks_clean(
        _envelope_text(env),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


async def test_mcp_import_files_batch_is_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    use_mcp_host(monkeypatch, None)
    twin = tmp_path / "canary_statement_copy.pdf"
    shutil.copy(sweep, twin)

    env = await import_files_coarse(paths=[str(sweep), str(twin)], refresh=False)

    _assert_sinks_clean(
        _envelope_text(env),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


async def test_mcp_recipe_replay_failure_is_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    """A saved recipe that no longer reconciles escalates without leaking content."""
    use_mcp_host(monkeypatch, None)
    first = await import_preview_coarse(file_path=str(sweep))
    released = await import_preview_coarse(
        file_path=str(sweep), confirmation_token=_relay_token(first)
    )
    preview_id = json.loads(released.to_json())["data"]["preview_id"]
    confirmed = await import_confirm_coarse(
        preview_id=preview_id,
        bridge_response={"recipe": CANARY_RECIPE, "rows": CANARY_ROWS},
        account_bindings={"@0": "new"},
    )
    assert confirmed.error is None, confirmed.error
    caplog.clear()
    audit_before = len(_audit_rows())
    changed = write_canary_replay_failure_pdf(tmp_path)

    env = await import_preview_coarse(file_path=str(changed))

    data = json.loads(env.to_json())["data"]
    assert data["status"] == "ai_assistance_required", data
    assert data["consent_request"]["files"][0]["decision_reason"] == (
        "replay_reconciliation_failed"
    )
    _assert_sinks_absent(
        CANARIES,
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
        response=_envelope_text(env),
        audit_skip=audit_before,
    )


# --- CLI, no consent ------------------------------------------------------


@pytest.mark.parametrize("agent_shell", [False, True], ids=["human", "agent"])
@pytest.mark.parametrize(
    "argv",
    [
        ["preview"],
        ["preview", "-o", "json"],
        ["files", "--no-refresh"],
        ["files", "--no-refresh", "--yes", "-o", "json"],
    ],
    ids=["preview-text", "preview-json", "files-text", "files-yes-json"],
)
def test_cli_entry_points_are_content_free(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
    argv: list[str],
    agent_shell: bool,
) -> None:
    if agent_shell:
        monkeypatch.setenv("CLAUDECODE", "1")
    else:
        monkeypatch.delenv("CLAUDECODE", raising=False)

    result = runner.invoke(app, [argv[0], str(sweep), *argv[1:]])

    # `files` is the human path: it seeds the PDF without disclosing, so the
    # sweep pins its logs/audit/errors, not the escalation branch.
    assert result.exit_code == 0, result.output
    if argv[0] == "files" and "json" in argv:
        [row] = json.loads(result.stdout)["data"]["files"]
        assert row["status"] == "imported"
    _assert_sinks_clean(
        _cli_text(result),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


# --- Inbox ----------------------------------------------------------------


@pytest.mark.parametrize("remembered", [False, True], ids=["no-grant", "grant"])
def test_inbox_drain_is_content_free(
    sweep: Path,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
    remembered: bool,
) -> None:
    with get_database(read_only=False) as db:
        if remembered:
            host = host_for_recipient_name("claude-code")
            assert host is not None
            ConsentService(db).grant_host_consent(
                host=host, approved_via="cli_prompt", actor="test"
            )
        svc = InboxService(db=db, settings=make_inbox_settings(tmp_path))
        svc.ensure_layout()
        shutil.copy(sweep, svc.inbox_dir / "canary_statement.pdf")
        result = svc.sync(year_month="2026-05", refresh=False)

    assert len(result.processed) == 1 and not result.failed
    _assert_sinks_clean(
        repr(result),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


# --- Post consent ---------------------------------------------------------


def _assert_released_and_redacted(
    response: str,
    *,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    log_dir: Path,
) -> None:
    assert CANARY_DESCRIPTION in response
    assert CANARY_AMOUNT in response
    assert _audit_rows(action="smart_import_parse"), "expected a parse audit row"
    _assert_sinks_absent(
        _REDACTED,
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=log_dir,
        response=response,
        privacy_log_required=True,
    )


async def test_mcp_post_consent_release_is_redacted(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    use_mcp_host(monkeypatch, None)
    first = await import_preview_coarse(file_path=str(sweep))

    released = await import_preview_coarse(
        file_path=str(sweep), confirmation_token=_relay_token(first)
    )

    _assert_released_and_redacted(
        _envelope_text(released),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )


def test_cli_json_post_consent_release_is_redacted(
    sweep: Path,
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    verbose_file_logging: Path,
) -> None:
    monkeypatch.setenv("CLAUDECODE", "1")
    receipt = runner.invoke(app, ["preview", str(sweep), "-o", "json"])
    rid = json.loads(receipt.stdout)["data"]["consent_request"]["request_id"]

    released = runner.invoke(
        app, ["preview", str(sweep), "--allow-ai-disclosure", rid, "-o", "json"]
    )

    assert released.exit_code == 0, released.output
    _assert_released_and_redacted(
        _cli_text(released),
        tmp_path=tmp_path,
        caplog=caplog,
        log_dir=verbose_file_logging,
    )
