"""@mcp_tool integration: sensitivity derivation, redaction, privacy log."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import pytest

from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.introspection import PrivacyContractError
from moneybin.privacy.sensitivity import Sensitivity
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@dataclass(frozen=True)
class _Payload:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    notes: Annotated[str, DataClass.USER_NOTE]


@dataclass(frozen=True)
class _PayloadContainer:
    row: _Payload


@dataclass(frozen=True)
class _LowTierPayload:
    label: Annotated[str, DataClass.CATEGORY]


@dataclass(frozen=True)
class _HighTierPayload:
    balance: Annotated[str, DataClass.BALANCE]


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


def test_stamp_floors_a_higher_derived_sensitivity_instead_of_overriding_it() -> None:
    """MB-157: a call-computed sensitivity above the type's tier must survive.

    Direct inverse of the defect: ``_stamp_envelope_sensitivity`` used to
    unconditionally override ``summary.sensitivity`` with the decorator's
    static tier, in both directions. The payload type here derives LOW, but
    the call itself reports HIGH — the scenario the defect report names as
    "a future tool returning a more sensitive payload down a branch that
    doesn't rebuild". Overriding would silently downgrade the response (and
    the privacy audit row) back to LOW; flooring must not.
    """

    @mcp_tool()
    def low_tool() -> ResponseEnvelope[_LowTierPayload]:
        return build_envelope(data=_LowTierPayload(label="x"), sensitivity="high")

    assert low_tool._mcp_sensitivity == Sensitivity.LOW  # type: ignore[attr-defined]

    envelope = asyncio.run(low_tool())

    assert envelope.summary.sensitivity == "high"


def test_stamp_still_raises_a_lower_derived_sensitivity_to_the_declared_ceiling() -> (
    None
):
    """The legitimate direction of the floor must keep working.

    The payload type derives HIGH; the call itself never raises
    ``summary.sensitivity`` above ``build_envelope``'s "low" default. The
    decorator's static ceiling must still apply.
    """

    @mcp_tool()
    def high_tool() -> ResponseEnvelope[_HighTierPayload]:
        return build_envelope(data=_HighTierPayload(balance="1.00"))

    assert high_tool._mcp_sensitivity == Sensitivity.HIGH  # type: ignore[attr-defined]

    envelope = asyncio.run(high_tool())

    assert envelope.summary.sensitivity == "high"


def test_decorator_fails_on_unclassified_return_type() -> None:
    with pytest.raises(PrivacyContractError):

        @mcp_tool()
        def bad_tool() -> ResponseEnvelope[dict[str, Any]]:  # pyright: ignore[reportUnusedFunction]  # bare dict — no DataClass-annotated fields; the @mcp_tool decoration itself is the act under test
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
