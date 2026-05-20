"""CLI output redaction: render_or_json wires CRITICAL redaction + privacy.log.

Three tests covering:
1. CRITICAL fields are masked in --output json, non-CRITICAL pass through.
2. A privacy.log.jsonl event is written with cli.* actor when cli_actor= is set.
3. TEXT output bypasses redaction and writes no log event.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import pytest

from moneybin.cli.output import OutputFormat, render_or_json
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import build_envelope


@dataclass(frozen=True)
class _AccountPayload:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    amount: Annotated[float, DataClass.TXN_AMOUNT]


def test_render_or_json_redacts_account_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CRITICAL ACCOUNT_IDENTIFIER is masked; non-CRITICAL amount passes through."""
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: log_dir,
    )

    payload = _AccountPayload(account_id="acct_1234567890", amount=42.50)
    envelope = build_envelope(data=payload, sensitivity="critical")

    render_or_json(envelope, OutputFormat.JSON, cli_actor="transactions_list")

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["data"]["account_id"] == "****7890"
    assert data["data"]["amount"] == 42.50


def test_render_or_json_writes_privacy_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A privacy.log.jsonl event is written with actor=cli.<name> and action=tool_call."""
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: log_dir,
    )

    payload = _AccountPayload(account_id="acct_1234567890", amount=10.00)
    envelope = build_envelope(data=payload, sensitivity="critical")

    render_or_json(envelope, OutputFormat.JSON, cli_actor="transactions_list")

    log_path = log_dir / "privacy.log.jsonl"
    assert log_path.exists(), "privacy.log.jsonl was not created"
    event = json.loads(log_path.read_text().splitlines()[0])
    assert event["actor"] == "cli.transactions_list"
    assert event["action"] == "tool_call"


def test_text_output_does_not_redact_or_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """TEXT output path bypasses redaction and writes no privacy.log event."""
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: log_dir,
    )

    rendered: list[str] = []

    def _render_fn(env: object) -> None:
        rendered.append("rendered")

    payload = _AccountPayload(account_id="acct_1234567890", amount=10.00)
    envelope = build_envelope(data=payload, sensitivity="critical")

    render_or_json(
        envelope, OutputFormat.TEXT, render_fn=_render_fn, cli_actor="transactions_list"
    )

    # TEXT path calls render_fn, not typer.echo — verify render_fn was called
    assert rendered == ["rendered"]
    # No log event written
    log_path = log_dir / "privacy.log.jsonl"
    assert not log_path.exists(), (
        "privacy.log.jsonl should not be written for TEXT output"
    )
