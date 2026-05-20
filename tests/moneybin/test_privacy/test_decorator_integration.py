"""@mcp_tool integration: sensitivity derivation, redaction, privacy log."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import pytest

from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.privacy import Sensitivity
from moneybin.privacy.introspection import PrivacyContractError
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@dataclass(frozen=True)
class _Payload:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    notes: Annotated[str, DataClass.USER_NOTE]


@dataclass(frozen=True)
class _PayloadContainer:
    row: _Payload


def test_decorator_derives_sensitivity_from_return_type() -> None:
    @mcp_tool()
    def my_tool() -> ResponseEnvelope[_PayloadContainer]:
        return build_envelope(
            data=_PayloadContainer(
                row=_Payload(account_id="acct_1234567890", notes="x")
            ),
            sensitivity="critical",  # explicit redundant declaration
        )

    assert my_tool._mcp_sensitivity == Sensitivity.CRITICAL  # type: ignore[attr-defined]


def test_decorator_fails_on_unclassified_return_type() -> None:
    with pytest.raises(PrivacyContractError):

        @mcp_tool()
        def bad_tool() -> ResponseEnvelope[dict]:  # bare dict — no classes
            return build_envelope(data={"x": 1}, sensitivity="low")


def test_call_redacts_critical_fields_and_writes_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_dir = tmp_path / "profile"
    log_dir.mkdir(mode=0o700)
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: log_dir,
    )

    @mcp_tool()
    def my_tool() -> ResponseEnvelope[_PayloadContainer]:
        return build_envelope(
            data=_PayloadContainer(
                row=_Payload(account_id="acct_1234567890", notes="x")
            ),
            sensitivity="critical",
        )

    envelope = asyncio.run(my_tool())
    # CRITICAL masking applied to account_id
    assert envelope.data.row.account_id == "****7890"
    # USER_NOTE passed through
    assert envelope.data.row.notes == "x"
    # Privacy log written
    log = log_dir / "privacy.log.jsonl"
    assert log.exists()
    event = json.loads(log.read_text().splitlines()[0])
    assert event["actor"] == "mcp.my_tool"
    assert event["action"] == "tool_call"
    assert event["sensitivity"] == "critical"
