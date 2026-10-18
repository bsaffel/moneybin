"""Selective confirmation protects existing assertions without gating new data."""

from collections.abc import Generator, Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import date
from decimal import Decimal
from unittest.mock import patch

import pytest
from typer.testing import CliRunner, Result

from moneybin.cli.main import app
from moneybin.cli.utils import get_terminal_policy
from moneybin.database import Database
from moneybin.services.balance_service import BalanceService
from tests.moneybin.db_helpers import create_core_tables

BalanceCLI = tuple[BalanceService, list[dict[str, bool]]]


@pytest.fixture()
def balance_cli(db: Database) -> Iterator[BalanceCLI]:
    create_core_tables(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, institution_name, source_type)
        VALUES ('acct_a', 'CHECKING', 'Test Bank', 'ofx')""")
    active: list[dict[str, bool]] = []

    @contextmanager
    def database(**kwargs: bool) -> Generator[Database, None, None]:
        active.append(kwargs)
        try:
            yield db
        finally:
            active.pop()

    with patch("moneybin.cli.commands.accounts.balance.get_database", database):
        yield BalanceService(db), active


def invoke(runner: CliRunner, *args: str) -> Result:
    return runner.invoke(app, ["accounts", "balance", *args])


@pytest.mark.unit
@pytest.mark.parametrize(
    "amount,stored", [("100.005", "100.01"), ("-100.005", "-100.01")]
)
@pytest.mark.parametrize("identical", [False, True])
def test_preview_and_noop_use_the_stored_decimal_amount(
    runner: CliRunner,
    balance_cli: BalanceCLI,
    amount: str,
    stored: str,
    identical: bool,
) -> None:
    service, _ = balance_cli
    day = date(2026, 1, 31)
    service.assert_balance(
        "acct_a", day, Decimal(stored if identical else "0"), actor="cli"
    )
    with (
        patch(
            "moneybin.cli.commands.accounts.balance.get_terminal_policy",
            return_value=replace(get_terminal_policy(), interactive=True),
        ),
        patch(
            "typer.confirm",
            side_effect=AssertionError("unexpected prompt") if identical else None,
            return_value=True,
        ),
    ):
        args = ["assert", "acct_a", "2026-01-31"]
        if amount.startswith("-"):
            args.append("--")
        result = invoke(runner, *args, amount)
    assert result.exit_code == 0, result.output
    if not identical:
        after = next(
            line for line in result.output.splitlines() if "After balance" in line
        )
        assert stored in after and amount not in after
    row = service.get_assertion("acct_a", day)
    assert row is not None and row.balance == Decimal(stored)


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["assert", "assertion-delete"])
def test_existing_assertion_requires_terminal_or_yes(
    runner: CliRunner, balance_cli: BalanceCLI, operation: str
) -> None:
    service, _ = balance_cli
    service.assert_balance("acct_a", date(2026, 1, 31), Decimal("100"), actor="cli")
    args = [operation, "acct_a", "2026-01-31"]
    if operation == "assert":
        args.append("200")
    result = invoke(runner, *args)
    assert result.exit_code == 1, result.output
    assert "--yes" in result.output
    row = service.get_assertion("acct_a", date(2026, 1, 31))
    assert row is not None and row.balance == Decimal("100")


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["assert", "assertion-delete"])
@pytest.mark.parametrize("accept", [True, False])
def test_confirmation_previews_exact_state_outside_database(
    runner: CliRunner, balance_cli: BalanceCLI, operation: str, accept: bool
) -> None:
    service, active = balance_cli
    service.assert_balance(
        "acct_a", date(2026, 1, 31), Decimal("100"), notes="old note", actor="cli"
    )
    args = [operation, "acct_a", "2026-01-31"]
    if operation == "assert":
        args.extend(["200", "--notes", "new note"])

    def confirm(*args: object, **kwargs: object) -> bool:
        assert not active
        return accept

    with (
        patch(
            "moneybin.cli.commands.accounts.balance.get_terminal_policy",
            return_value=replace(get_terminal_policy(), interactive=True),
        ),
        patch("typer.confirm", confirm),
    ):
        result = invoke(runner, *args)
    assert result.exit_code == 0, result.output
    for value in ["acct_a", "2026-01-31", "100.00", "old note"]:
        assert value in result.output
    row = service.get_assertion("acct_a", date(2026, 1, 31))
    if not accept:
        assert row is not None and row.balance == Decimal("100")
    elif operation == "assert":
        assert "200" in result.output and "new note" in result.output
        assert (
            row is not None
            and row.balance == Decimal("200")
            and row.notes == "new note"
        )
    else:
        assert "absent" in result.output
        assert row is None


@pytest.mark.unit
def test_new_identical_and_missing_deletion_do_not_prompt(
    runner: CliRunner, balance_cli: BalanceCLI
) -> None:
    service, _ = balance_cli
    with patch("typer.confirm", side_effect=AssertionError("unexpected prompt")):
        assert invoke(runner, "assert", "acct_a", "2026-01-31", "100").exit_code == 0
        assert invoke(runner, "assert", "acct_a", "2026-01-31", "100").exit_code == 0
        assert (
            invoke(runner, "assertion-delete", "missing", "2026-01-31").exit_code == 0
        )
    row = service.get_assertion("acct_a", date(2026, 1, 31))
    assert row is not None and row.balance == Decimal("100")


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["assert", "assertion-delete"])
def test_changed_assertion_refuses_confirmed_mutation(
    runner: CliRunner, balance_cli: BalanceCLI, operation: str
) -> None:
    service, _ = balance_cli
    day = date(2026, 1, 31)
    service.assert_balance("acct_a", day, Decimal("100"), actor="cli")

    def confirm(*args: object, **kwargs: object) -> bool:
        service.assert_balance("acct_a", day, Decimal("300"), actor="cli")
        return True

    args = [operation, "acct_a", "2026-01-31"]
    if operation == "assert":
        args.append("200")
    with (
        patch(
            "moneybin.cli.commands.accounts.balance.get_terminal_policy",
            return_value=replace(get_terminal_policy(), interactive=True),
        ),
        patch("typer.confirm", confirm),
    ):
        result = invoke(runner, *args)
    assert result.exit_code == 1, result.output
    assert "changed" in result.output
    row = service.get_assertion("acct_a", day)
    assert row is not None and row.balance == Decimal("300")


@pytest.mark.unit
def test_notes_only_change_requires_confirmation(
    runner: CliRunner, balance_cli: BalanceCLI
) -> None:
    service, _ = balance_cli
    day = date(2026, 1, 31)
    service.assert_balance("acct_a", day, Decimal("100"), notes="keep", actor="cli")
    result = invoke(
        runner, "assert", "acct_a", "2026-01-31", "100", "--notes", "replace"
    )
    assert result.exit_code == 1, result.output
    row = service.get_assertion("acct_a", day)
    assert row is not None and row.notes == "keep"


@pytest.mark.unit
@pytest.mark.parametrize("operation", ["assert", "assertion-delete"])
def test_yes_skips_prompt_and_preserves_validation(
    runner: CliRunner, balance_cli: BalanceCLI, operation: str
) -> None:
    service, _ = balance_cli
    day = date(2026, 1, 31)
    service.assert_balance("acct_a", day, Decimal("100"), actor="cli")
    args = [operation, "acct_a", "2026-01-31", "--yes"]
    if operation == "assert":
        args.append("200")
    with patch("typer.confirm", side_effect=AssertionError("unexpected prompt")):
        result = invoke(runner, *args)
    assert result.exit_code == 0, result.output
    row = service.get_assertion("acct_a", day)
    if operation == "assert":
        assert row is not None and row.balance == Decimal("200")
        invalid = invoke(runner, "assert", "missing", "2026-01-31", "200", "--yes")
        assert invalid.exit_code == 1, invalid.output
        assert service.get_assertion("missing", day) is None
    else:
        assert row is None
