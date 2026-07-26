"""Every money-bearing tool either states its currency or is a known gap.

``build_envelope`` now derives ``display_currency`` from the payload, so a tool
whose rows carry ``currency_code`` is correct automatically — there is nothing
left to forget. The residual risk is the other shape: a payload that returns
money with no currency field anywhere, which can only ever answer "unknown".

Those are enumerated below rather than counted. A count fires when someone adds
one and never when someone forgets; set equality fires both ways, so a new
money tool cannot join the silent set without this test naming it.
"""

from __future__ import annotations

import inspect
import typing

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
}

# Money-bearing tools with no currency field anywhere in the payload. These
# answer display_currency=None — honest, but they cannot tell an agent what
# unit the amounts are in. Threading currency through each is tracked
# separately; adding a NEW entry here is a decision, not an oversight.
_NO_CURRENCY_TO_STATE = {
    "import_files",
    "import_preview",
    "investments_lots_select",
    "system_audit",
    "transactions",
    "transactions_categorize_rules",
}


@pytest.fixture
async def money_tools() -> dict[str, bool]:
    """Registered tool name -> whether its payload carries a currency field.

    Enumerated from the live registry, not a checked-in list, so a tool that is
    registered but forgotten here still shows up.
    """
    from moneybin.mcp.server import mcp, register_core_tools

    register_core_tools()
    result: dict[str, bool] = {}
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
        classes = extract_data_classes(args[0])
        if classes & _MONEY:
            result[tool.name] = DataClass.CURRENCY in classes
    return result


def test_the_money_surface_is_fully_enumerated(
    money_tools: dict[str, bool],
) -> None:
    """No money tool escapes classification into one of the two sets."""
    assert set(money_tools) == _STATES_ITS_CURRENCY | _NO_CURRENCY_TO_STATE


def test_tools_that_carry_a_currency_are_exactly_the_declared_set(
    money_tools: dict[str, bool],
) -> None:
    """A payload gaining or losing its currency field must update the split."""
    carries = {name for name, has_currency in money_tools.items() if has_currency}

    assert carries == _STATES_ITS_CURRENCY


def test_the_silent_set_only_shrinks(money_tools: dict[str, bool]) -> None:
    """Threading a currency into one of these is an improvement, not a break.

    Stated as its own assertion so the failure message says which tool changed
    and in which direction, rather than a bare set diff.
    """
    silent = {name for name, has_currency in money_tools.items() if not has_currency}
    newly_silent = silent - _NO_CURRENCY_TO_STATE

    assert not newly_silent, (
        f"new money tool(s) with no currency to report: {sorted(newly_silent)} — "
        "give the payload a currency_code field, or add it to "
        "_NO_CURRENCY_TO_STATE deliberately"
    )
