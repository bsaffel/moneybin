"""One token for an unknown currency, across every CLI surface."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.cli.output import UNKNOWN_CURRENCY, currency_label

pytestmark = pytest.mark.unit

_CLI_ROOT = Path(__file__).resolve().parents[3] / "src" / "moneybin" / "cli"


def test_unknown_currency_token_reads_in_both_positions() -> None:
    """The token has to work as a heading and as a suffix.

    `reports networth` prints the currency as a label (`EUR: 1234.56`) while
    `accounts balance` prints it after the amount (`1234.56 EUR`). A bare `?`
    is cryptic in the first position and `unknown` reads as though the
    *amount* were unknown in the second, which is how the CLI ended up with
    one spelling in each.
    """
    assert UNKNOWN_CURRENCY == "n/a"
    assert currency_label(None) == UNKNOWN_CURRENCY
    assert currency_label("") == UNKNOWN_CURRENCY
    assert currency_label("EUR") == "EUR"


def test_no_cli_module_spells_the_unknown_currency_itself() -> None:
    """Every currency slot routes through `currency_label`.

    Scans source rather than behaviour because the failure this closes was a
    *second spelling* added at a new call site — a site with no test of its
    own would not otherwise be caught by any per-command assertion.
    """
    offenders = [
        f"{path.relative_to(_CLI_ROOT)}:{number}: {line.strip()}"
        for path in sorted(_CLI_ROOT.rglob("*.py"))
        for number, line in enumerate(path.read_text().splitlines(), 1)
        if "currency_code or " in line
    ]
    assert not offenders, (
        "inline currency fallback; use currency_label():\n" + "\n".join(offenders)
    )
