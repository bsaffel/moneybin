"""Which ``app.*`` inputs a materialized (``FULL``) SQLMesh model may read.

A model's kind is a freshness contract
(docs/specs/materialization-freshness-contract.md): a ``FULL`` model may read
only values whose every writer restates it. This registry classifies every
``app.*`` table SQLMesh declares as an external model, per column where one
table carries both kinds of value. Writers consult it through
``restate_after_write``; the materialization-freshness guards check it against
the model graph.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    BALANCE_ASSERTIONS,
    CATEGORY_OVERRIDES,
    CATEGORY_SOURCE_MAP,
    EXCHANGE_RATE_OVERRIDES,
    LOT_SELECTIONS,
    MATCH_DECISIONS,
    PROFILE_SETTINGS,
    SECURITIES,
    SECURITY_LINKS,
    SECURITY_PRICE_OVERRIDES,
    SEED_SOURCE_PRIORITY,
    TRANSACTION_CATEGORIES,
    TRANSACTION_NOTES,
    TRANSACTION_SPLITS,
    TRANSACTION_TAGS,
    USER_CATEGORIES,
    USER_MERCHANTS,
    TableRef,
)


class InputFreshness(StrEnum):
    """How an ``app.*`` value stays current in the models that read it."""

    RESTATES = "restates"
    """Every writer restates downstream models; a ``FULL`` model may read it."""

    READ_THROUGH = "read_through"
    """Written without a transform; only ``VIEW`` models may read it."""

    TARGET_SCOPED = "target_scoped"
    """``app.audit_log``: a row is as fresh as the table it describes."""


@dataclass(frozen=True, slots=True)
class InputClass:
    """One ``app.*`` table's class, with per-column overrides."""

    table: TableRef
    default: InputFreshness
    columns: Mapping[str, InputFreshness] = field(
        default_factory=lambda: dict[str, InputFreshness]()
    )

    def of(self, column: str) -> InputFreshness:
        """The class of one column of this table."""
        return self.columns.get(column, self.default)


_R = InputFreshness.RESTATES
_T = InputFreshness.READ_THROUGH

#: Display-only account settings: only core.dim_accounts (a view) reads them,
#: so editing them costs no restate. Every other column restates.
_ACCOUNT_SETTINGS_READ_THROUGH = (
    "display_name",
    "official_name",
    "last_four",
    "account_subtype",
    "holder_category",
    "credit_limit",
    "updated_at",
)

APP_INPUTS: tuple[InputClass, ...] = (
    InputClass(MATCH_DECISIONS, _R),
    InputClass(SEED_SOURCE_PRIORITY, _R),
    InputClass(ACCOUNT_LINKS, _R),
    InputClass(ACCOUNT_LINK_DECISIONS, _R),
    InputClass(
        ACCOUNT_SETTINGS,
        _R,
        dict.fromkeys(_ACCOUNT_SETTINGS_READ_THROUGH, _T),
    ),
    InputClass(BALANCE_ASSERTIONS, _R),
    InputClass(SECURITIES, _R),
    InputClass(SECURITY_LINKS, _R),
    InputClass(SECURITY_PRICE_OVERRIDES, _R),
    InputClass(LOT_SELECTIONS, _R),
    InputClass(EXCHANGE_RATE_OVERRIDES, _R),
    InputClass(PROFILE_SETTINGS, _R),
    InputClass(AUDIT_LOG, InputFreshness.TARGET_SCOPED),
    InputClass(TRANSACTION_CATEGORIES, _T),
    InputClass(TRANSACTION_NOTES, _T),
    InputClass(TRANSACTION_TAGS, _T),
    InputClass(TRANSACTION_SPLITS, _T),
    InputClass(USER_CATEGORIES, _T),
    InputClass(CATEGORY_OVERRIDES, _T),
    InputClass(USER_MERCHANTS, _T),
    InputClass(CATEGORY_SOURCE_MAP, _T),
)

_BY_NAME: dict[str, InputClass] = {c.table.full_name: c for c in APP_INPUTS}


def input_class(table: TableRef) -> InputClass | None:
    """The registry entry for *table*; ``None`` when no model reads it."""
    return _BY_NAME.get(table.full_name)


def restating_columns(table: TableRef, columns: Iterable[str]) -> frozenset[str]:
    """The subset of *columns* of *table* whose writes must restate."""
    if table.schema != "app":
        return frozenset(columns)
    entry = input_class(table)
    if entry is None:
        return frozenset()
    return frozenset(c for c in columns if entry.of(c) is _R)


def restates(table: TableRef) -> bool:
    """True when a write to *table* can change a materialized model.

    A table with any restating column counts; a writer of such a table that
    knows which columns it changed narrows with :func:`restating_columns`.
    Inputs outside ``app`` (``raw`` loads) always restate: refresh transforms
    them, and a writer outside refresh restates them itself. An ``app`` table
    with no entry is read by no model, which the freshness guard enforces.
    """
    if table.schema != "app":
        return True
    entry = input_class(table)
    if entry is None:
        return False
    return entry.default is _R or _R in entry.columns.values()


def fully_restates(table: TableRef) -> bool:
    """True when every column of *table* restates, so any write to it does."""
    entry = input_class(table)
    return entry is not None and entry.default is _R and not entry.columns
