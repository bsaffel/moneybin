"""Immutable identities and canonical bindings of a Golden investment member."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class MembershipBinding:
    """Canonical dependencies captured before a membership is published."""

    account_id: str
    security_id: str | None
    effective_currency: str
    currency_source: Literal["source", "account"]
    account_generation: str
    security_generation: str
    currency_generation: str


@dataclass(frozen=True)
class EventMember:
    """One exact source revision contributing to one Golden semantic leg."""

    event_group_id: str
    investment_transaction_id: str
    source_event_key: str
    source_type: str
    source_origin: str
    native_reference: str
    observation_version: str
    semantic_leg_role: str
    binding: MembershipBinding
    member_kind: Literal["source_event", "opening_lot_reconstruction"] = "source_event"
    decision_id: str | None = None
    reconstruction_evidence: str | None = None


@dataclass(frozen=True)
class FieldResolution:
    """An explicitly selected field and its exact immutable source provenance."""

    decision_id: str
    investment_transaction_id: str
    field_name: str
    source_type: str
    source_origin: str
    native_reference: str
    observation_version: str
