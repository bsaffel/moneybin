"""``build_envelope`` must never invent a currency it was not told.

The ``display_currency="USD"`` default made every one of 251 call sites claim
USD for free. Nine of the eleven money-bearing MCP tools inherited it, so a EUR
profile read its balances, holdings, and credit limits labelled USD — the exact
silent blend multi-currency.md Requirement 5 exists to prevent. Two review
rounds patched individual call sites; this pins the default itself, which is
the only place the claim can be re-introduced for every caller at once.
"""

from __future__ import annotations

import inspect
from decimal import Decimal

from moneybin.protocol.envelope import build_envelope, resolve_display_currency


def test_build_envelope_does_not_default_to_a_currency() -> None:
    """An unspecified display_currency is unknown, never USD."""
    envelope = build_envelope(data={"amount": Decimal("1.00")})

    assert envelope.summary.display_currency is None


def test_build_envelope_default_is_not_a_currency_literal() -> None:
    """Pin the signature: no future edit may restore a hardcoded default.

    The behavioural test above passes just as well if someone re-adds a
    ``"USD"`` default and updates that one assertion. This reads the parameter
    default itself, so restoring any currency literal fails here too. The
    default is a sentinel meaning "not stated" — anything a caller could mistake
    for a currency is a string, so that is what this rules out.
    """
    default = inspect.signature(build_envelope).parameters["display_currency"].default

    assert not isinstance(default, str)


def test_build_envelope_keeps_an_explicit_currency() -> None:
    """Naming a currency is still how a caller states one."""
    envelope = build_envelope(data={"amount": Decimal("1.00")}, display_currency="EUR")

    assert envelope.summary.display_currency == "EUR"


def test_to_dict_emits_display_currency_even_when_unknown() -> None:
    """A null on the wire is the answer; a missing key is not.

    ``None`` means "these rows are not one known currency" — a claim an agent
    has to be able to read. Dropping the key the way the neighbouring ``period``
    is dropped would turn that claim into silence, and silence reads as "the
    tool never considered it": exactly the distinction the ``Unset`` sentinel
    exists to preserve. ``SummaryMeta.to_dict``'s own docstring described this
    field as omitted-when-None, so the invitation to "fix" it was in the code.
    """
    summary = build_envelope(data={"amount": Decimal("1.00")}).to_dict()["summary"]

    assert "display_currency" in summary
    assert summary["display_currency"] is None


def test_resolve_display_currency_is_the_one_rule() -> None:
    """The helper callers use to answer the question agrees with the default.

    One known currency resolves; disagreement and unknown both decline.
    """
    assert resolve_display_currency(["EUR", "EUR"]) == "EUR"
    assert resolve_display_currency(["EUR", "USD"]) is None
    assert resolve_display_currency([None]) is None
    assert resolve_display_currency([]) is None
