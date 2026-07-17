"""Wire-only models for MCP response-envelope output schemas."""

from __future__ import annotations

from typing import Any, Literal, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from moneybin.errors import RecoveryAction
from moneybin.privacy.introspection import PrivacyContractError
from moneybin.protocol.envelope import ResponseEnvelope


class WireSummary(BaseModel):
    """Serialized fields emitted by ``SummaryMeta.to_dict()``."""

    model_config = ConfigDict(extra="forbid")

    total_count: int
    returned_count: int
    has_more: bool
    sensitivity: Literal["low", "medium", "high", "critical"]
    display_currency: str
    period: str | None = None
    degraded: bool = False
    degraded_reason: str | None = None


class WireError(BaseModel):
    """Serialized fields emitted by ``UserError.to_dict()`` in an envelope."""

    model_config = ConfigDict(extra="forbid")

    message: str
    code: str
    hint: str | None = None
    details: dict[str, Any] | None = None


class WireSuccessEnvelope[PayloadT](BaseModel):
    """Successful response-envelope wire shape."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok"]
    summary: WireSummary
    data: PayloadT
    actions: list[str]
    next_cursor: str | None = None
    recovery_actions: list[RecoveryAction] | None = None


class WireErrorEnvelope(BaseModel):
    """Error response-envelope wire shape."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["error"]
    summary: WireSummary
    data: list[Any] = Field(max_length=0)
    actions: list[str]
    error: WireError
    recovery_actions: list[RecoveryAction] | None = None


def output_schema_for(return_hint: object) -> dict[str, Any]:
    """Derive the public wire schema for ``ResponseEnvelope[T]``."""
    if get_origin(return_hint) is not ResponseEnvelope:
        raise PrivacyContractError("output schema requires ResponseEnvelope[T]")
    (payload_type,) = get_args(return_hint)
    schema = TypeAdapter(
        WireSuccessEnvelope[payload_type] | WireErrorEnvelope
    ).json_schema()
    return {"type": "object", **schema}
