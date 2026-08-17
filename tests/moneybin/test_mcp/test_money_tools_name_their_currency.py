"""Every money-bearing tool either states its currency or is a known gap.

``build_envelope`` now derives ``display_currency`` from the payload, so a tool
whose rows carry ``currency_code`` is correct automatically — there is nothing
left to forget. The residual risk is the other shape: a payload that returns
money with no currency field anywhere, which can only ever answer "unknown".

Those are enumerated below rather than counted. A count fires when someone adds
one and never when someone forgets; set equality fires both ways, so a new
money tool cannot join the silent set without this test naming it.

The silent set splits in two, because ``DataClass.TXN_AMOUNT`` marks a field as
*masking-sensitive*, not as *denominable*. A share count, a printed-versus-
recorded sign sample, and a polymorphic audit value all carry it and none has a
currency to state. Naming only the tool let those four sit in one undifferentiated
"gap" list, which read as four missing features when only one was real. Each
not-denominable entry therefore pins the money-classed leaves its reason rests
on, so a payload that grows a genuine amount has to argue the case again instead
of inheriting an exemption written for a different field.
"""

from __future__ import annotations

import inspect
import types
import typing
from typing import Any

import pytest

from moneybin.privacy.introspection import extract_data_classes
from moneybin.privacy.taxonomy import DataClass

_MONEY = {DataClass.BALANCE, DataClass.TXN_AMOUNT, DataClass.INCOME_AMOUNT}

# Money-bearing tools whose payload carries its own currency. build_envelope
# derives display_currency from these; no call site has to remember.
_STATES_ITS_CURRENCY = {
    "accounts",
    "accounts_balances",
    "accounts_set",
    "investments",
    # Moved out of the silent set in M1K.1: the categorization queue asks the
    # caller to act on an amount, so PendingTxnRow now carries currency_code.
    "reviews",
    # Moved out in M1K.1: the flagship transaction read returned bare amounts,
    # so a mixed-currency page had no denomination anywhere in the response.
    "transactions",
}

# Payloads carrying a CURRENCY-classed field that is NOT a denomination: a
# currency *pair* naming an exchange-rate series the run could not gather.
# `DataClass.CURRENCY` marks a field as *currency-shaped*, not as *the currency
# these amounts are in* — the split above rests on the second reading, and a
# pair satisfies only the first. Keeping these out of `_STATES_ITS_CURRENCY`
# rather than widening it: `build_envelope` derives `display_currency` from a
# reachable `currency_code` field (`envelope._CURRENCY_FIELD`), which a pair
# list is not and must never be mistaken for. A payload landing here still owes
# its own `currency_code` if it ever returns a denominable amount.
_NAMES_A_CURRENCY_PAIR = {
    # rate_pairs_failed/unsupported/discarded — the refresh this import ran
    # reached a rate provider on the user's behalf and reports which pairs it
    # could not fill. Its money leaves remain the sign samples pinned below.
    "import_files",
}

# Money-classed leaves that are not denominable amounts, so there is no currency
# to state and adding one would be noise. The value is (reason, leaves), where
# leaves is every money-classed (field name, declared type) reachable in the
# payload — the reason rests on those types, so the pin is what forces a re-argument.
_NOT_DENOMINABLE: dict[str, tuple[str, frozenset[tuple[str, str]]]] = {
    "import_files": (
        "sign samples show one magnitude printed and then negated so the caller "
        "can confirm a sign convention; both sides are strings and neither is an "
        "amount to denominate",
        frozenset({("as_printed", "str"), ("as_recorded", "str")}),
    ),
    "import_preview": (
        "same printed-versus-negated sign sample as import_files, and pre-ingest: "
        "the file's currency is not yet resolved when the preview is built",
        frozenset({("as_printed", "str"), ("as_recorded", "str")}),
    ),
    "investments_lots_select": (
        "quantity is a share count. The taxonomy classes it TXN_AMOUNT so it masks "
        "like money, but shares are denominated in the security, not a currency",
        frozenset({("quantity", "Decimal")}),
    ),
    "system_audit": (
        "before_value/after_value are polymorphic audit evidence — the audited "
        "column differs per event, so no one currency describes the pair",
        frozenset({("before_value", "Any"), ("after_value", "Any")}),
    ),
}

# Denominable money with no currency reachable in the payload: a real gap, not a
# classification artifact. Not leaf-pinned like the set above, because the claim
# is about the backing table rather than the field types.
_CURRENCY_GAP: dict[str, str] = {
    "transactions_categorize_rules": (
        "min_amount/max_amount are genuine bounds, but app.categorization_rules "
        "has no currency column, so a rule cannot say which currency it bounds "
        "and the matcher would compare across them. Needs schema work — M1K.2."
    ),
}


def _type_name(tp: Any) -> str:
    """Short, stable rendering of a leaf's declared type."""
    if tp is Any:
        return "Any"
    if tp is type(None):
        return "None"
    if typing.get_origin(tp) is typing.Union or isinstance(tp, types.UnionType):
        return " | ".join(_type_name(arg) for arg in typing.get_args(tp))
    return getattr(tp, "__name__", repr(tp))


def _money_leaves(tp: Any, seen: set[Any]) -> set[tuple[str, str]]:
    """Every money-classed (field name, declared type) reachable in ``tp``.

    ``extract_data_classes`` prunes each branch that holds no money at all, so
    this only descends where an answer can still be found. Keep that prune: it
    is also what makes a container form this walker cannot descend fail *closed*
    rather than open. The caller compares the result against a declared set, so
    an unreachable leaf shrinks the set and trips the assertion instead of
    passing silently. The one case that still slips through is a newly
    unsupported container holding a new money leaf while every previously
    reachable leaf is unchanged — narrow enough not to warrant a second walker
    to guard the first.
    """
    if not (extract_data_classes(tp) & _MONEY):
        return set()
    if typing.get_origin(tp) is not None:
        found: set[tuple[str, str]] = set()
        for arg in typing.get_args(tp):
            found |= _money_leaves(arg, seen)
        return found
    if not isinstance(tp, type) or tp in seen:
        return set()
    seen.add(tp)
    try:
        hints = typing.get_type_hints(tp, include_extras=True)
    except (NameError, TypeError):
        return set()
    leaves: set[tuple[str, str]] = set()
    for name, hint in hints.items():
        metadata = getattr(hint, "__metadata__", ())
        if any(isinstance(m, DataClass) and m in _MONEY for m in metadata):
            leaves.add((name, _type_name(typing.get_args(hint)[0])))
        leaves |= _money_leaves(hint, seen)
    return leaves


@pytest.fixture
async def money_payloads() -> dict[str, Any]:
    """Registered tool name -> its payload type, for every tool returning money.

    Enumerated from the live registry, not a checked-in list, so a tool that is
    registered but forgotten here still shows up. The payload type is kept
    rather than a has-currency bool: collapsing it to a bool is what let four
    not-denominable payloads read as missing features.
    """
    from moneybin.mcp.server import mcp, register_core_tools

    register_core_tools()
    result: dict[str, Any] = {}
    for tool in await mcp._list_tools():  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]
        fn = getattr(tool, "fn", None)
        if fn is None:
            continue
        try:
            hints = typing.get_type_hints(inspect.unwrap(fn), include_extras=True)
        except Exception as exc:  # noqa: BLE001
            # Every @mcp_tool is required to carry a resolvable
            # ResponseEnvelope[T] annotation, so this should be unreachable.
            # Fail loudly rather than skipping: a tool silently dropped from the
            # enumeration is exactly the blind spot this test exists to close.
            pytest.fail(f"cannot resolve return type for {tool.name}: {exc}")
        args = typing.get_args(hints.get("return"))
        if not args:
            continue
        if extract_data_classes(args[0]) & _MONEY:
            result[tool.name] = args[0]
    return result


def test_the_money_surface_is_fully_enumerated(
    money_payloads: dict[str, Any],
) -> None:
    """No money tool escapes classification into one of the three sets."""
    declared = _STATES_ITS_CURRENCY | set(_NOT_DENOMINABLE) | set(_CURRENCY_GAP)

    assert set(money_payloads) == declared


def test_tools_that_carry_a_currency_are_exactly_the_declared_set(
    money_payloads: dict[str, Any],
) -> None:
    """A payload gaining or losing its currency field must update the split."""
    carries = {
        name
        for name, payload in money_payloads.items()
        if DataClass.CURRENCY in extract_data_classes(payload)
    }

    assert carries == _STATES_ITS_CURRENCY | _NAMES_A_CURRENCY_PAIR


def test_the_silent_set_only_shrinks(money_payloads: dict[str, Any]) -> None:
    """Threading a currency into one of these is an improvement, not a break.

    Stated as its own assertion so the failure message says which tool changed
    and in which direction, rather than a bare set diff.
    """
    silent = {
        name
        for name, payload in money_payloads.items()
        if DataClass.CURRENCY not in extract_data_classes(payload)
    }
    newly_silent = silent - set(_NOT_DENOMINABLE) - set(_CURRENCY_GAP)

    assert not newly_silent, (
        f"new money tool(s) with no currency to report: {sorted(newly_silent)} — "
        "give the payload a currency_code field, or classify it deliberately as "
        "not-denominable or a tracked currency gap"
    )


def test_not_denominable_payloads_hold_only_the_leaves_they_named(
    money_payloads: dict[str, Any],
) -> None:
    """An exemption covers the fields it was written for and no others.

    Each reason above argues from a specific field's type — a string sample, a
    share count, an untyped audit value. A payload that grows a real
    ``Decimal`` amount is a different claim, and inheriting the old exemption
    would silently re-open the gap this module exists to enumerate.
    """
    for name, (reason, expected) in _NOT_DENOMINABLE.items():
        actual = _money_leaves(money_payloads[name], set())

        assert actual == expected, (
            f"{name} money-classed leaves changed to {sorted(actual)}; the "
            f"not-denominable reason on record covers {sorted(expected)} "
            f"({reason}). Re-argue it, or move the tool to _CURRENCY_GAP."
        )
