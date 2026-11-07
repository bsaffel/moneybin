"""Defer identity membership publication until a complete repository operation exists."""

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from moneybin.database import Database
    from moneybin.services.audit_service import AuditEvent


@dataclass
class _PendingDependencies:
    db: "Database"
    events: list["AuditEvent"] = field(default_factory=list)
    equivalences: dict[str, dict[str, str]] = field(
        default_factory=lambda: {"account": {}, "security": {}}
    )


_pending: ContextVar[_PendingDependencies | None] = ContextVar(
    "investment_dependencies", default=None
)


def record_dependency_change(db: "Database", event: "AuditEvent") -> None:
    """Capture only audited inputs that can affect investment bindings or visibility."""
    if event.target_schema != "app" or event.target_table not in {
        "account_links",
        "security_links",
        "account_settings",
    }:
        return
    before, after = event.before_value or {}, event.after_value or {}
    if event.target_table == "security_links" and (after or before).get("ref_kind") in {
        "tiingo_ticker",
        "coingecko_slug",
    }:
        return
    if event.target_table == "account_settings" and all(
        before.get(key) == after.get(key)
        for key in ("currency_code", "investment_source_type")
    ):
        return
    pending = _pending.get()
    if pending is None or pending.db is not db:
        raise RuntimeError(
            "Investment dependency mutation requires its transaction scope"
        )
    pending.events.append(event)


def record_equivalence(
    db: "Database",
    *,
    kind: Literal["account", "security"],
    previous: str,
    proposed: str,
) -> None:
    """Name an already ratified complete merge, never infer equivalence from repoint."""
    from moneybin import error_codes
    from moneybin.errors import UserError

    pending = _pending.get()
    if pending is None or pending.db is not db:
        raise RuntimeError(
            "Equivalent dependency requires its complete operation scope"
        )
    mapping = pending.equivalences[kind]
    if previous in mapping and mapping[previous] != proposed:
        raise UserError(
            "Conflicting canonical equivalences in one operation.",
            code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
        )
    mapping[previous] = proposed
    for identity in mapping:
        seen: set[str] = set()
        target = identity
        while target in mapping:
            if target in seen:
                raise UserError(
                    "Canonical equivalence contains a cycle.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            seen.add(target)
            target = mapping[target]


@contextmanager
def dependency_publication(db: "Database") -> Generator[None, None, None]:
    """Join nested repo changes; publish their complete transition before commit."""
    current = _pending.get()
    if current is not None and current.db is db:
        yield
        return
    pending = _PendingDependencies(db)
    token = _pending.set(pending)
    try:
        yield
        if pending.events:
            from moneybin.services.investment_membership_service import (
                InvestmentMembershipService,
            )

            InvestmentMembershipService(db).rebind_dependency(
                pending.events, equivalences=pending.equivalences
            )
    finally:
        _pending.reset(token)
