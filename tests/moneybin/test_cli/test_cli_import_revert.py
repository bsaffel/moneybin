"""CLI tests for ``moneybin import revert``.

The command reads a plan against a read-only connection, prompts on it, then
re-plans inside the write transaction and refuses if anything moved. Both
halves are CLI-owned wiring, so both are driven here through a real batch
rather than through the service alone.
"""

from __future__ import annotations

import dataclasses
import itertools
from collections.abc import Generator
from contextlib import contextmanager

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportRevertPlan, ImportService


@pytest.fixture()
def patched_db(db: Database, monkeypatch: pytest.MonkeyPatch) -> Database:
    """Redirect the command's deferred ``get_database`` to the test database."""

    @contextmanager
    def _shared(*_args: object, **_kwargs: object) -> Generator[Database, None, None]:
        yield db

    monkeypatch.setattr("moneybin.database.get_database", _shared)
    return db


def _seed_revertable_batch(database: Database, rows: int) -> str:
    """Import ``rows`` tabular transactions under one complete batch."""
    import_id = import_log.begin_import(
        database,
        source_file="/tmp/revert.csv",  # noqa: S108  # test fixture path
        source_type="csv",
        source_origin="tiller",
        account_names=["checking"],
    )
    for index in range(rows):
        database.execute(
            """
            INSERT INTO raw.tabular_transactions (
                transaction_id, account_id, transaction_date, amount, description,
                source_file, source_type, source_origin, import_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"csv_cli_{index}",
                "checking",
                "2026-01-01",
                "-10.00",
                "X",
                "/tmp/revert.csv",  # noqa: S108  # test fixture path
                "csv",
                "tiller",
                import_id,
            ],
        )
    import_log.finalize_import(
        database, import_id, status="complete", rows_total=rows, rows_imported=rows
    )
    return import_id


def _remaining(database: Database, import_id: str) -> int:
    row = database.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
        [import_id],
    ).fetchone()
    assert row is not None
    return int(row[0])


def _status(database: Database, import_id: str) -> str:
    row = database.execute(
        "SELECT status FROM raw.import_log WHERE import_id = ?", [import_id]
    ).fetchone()
    assert row is not None
    return str(row[0])


def _interactive_policy() -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )


def test_import_revert_deletes_a_seeded_batch_and_reports_the_count(
    runner: CliRunner, patched_db: Database
) -> None:
    """The success path deletes the batch's rows and says how many it removed."""
    import_id = _seed_revertable_batch(patched_db, rows=2)

    result = runner.invoke(app, ["import", "revert", import_id, "--yes"])

    assert result.exit_code == 0, result.output
    assert "Rows deleted: 2" in result.output
    assert _remaining(patched_db, import_id) == 0
    assert _status(patched_db, import_id) == "reverted"


def test_import_revert_receipt_keeps_the_full_actionable_import_id(
    runner: CliRunner, patched_db: Database
) -> None:
    """A receipt must not replace the identifier needed for follow-up inspection."""
    import_id = _seed_revertable_batch(patched_db, rows=1)

    result = runner.invoke(app, ["import", "revert", import_id, "--yes"])

    assert result.exit_code == 0, result.output
    assert import_id in result.stdout
    assert "Rows deleted:" in result.stdout


def test_import_revert_requires_explicit_confirmation_when_noninteractive(
    runner: CliRunner, patched_db: Database
) -> None:
    """A pipe cannot accidentally receive a destructive prompt and continue."""
    import_id = _seed_revertable_batch(patched_db, rows=2)

    result = runner.invoke(app, ["import", "revert", import_id], input="n\n")

    assert result.exit_code == 1, result.output
    assert "--yes" in result.output
    assert _remaining(patched_db, import_id) == 2
    assert _status(patched_db, import_id) == "complete"


def test_import_revert_decline_writes_an_unpaged_full_id_receipt(
    runner: CliRunner, patched_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An interactive decline records the no-write outcome without paging it."""
    import_id = _seed_revertable_batch(patched_db, rows=2)
    monkeypatch.setattr(
        "moneybin.cli.commands.import_cmd.get_terminal_policy", _interactive_policy
    )

    def decline(_message: str) -> bool:
        return False

    monkeypatch.setattr("typer.confirm", decline)

    def should_not_write(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("declined revert called the mutating service")

    monkeypatch.setattr(ImportService, "revert_confirmed", should_not_write)

    result = runner.invoke(app, ["import", "revert", import_id])

    assert result.exit_code == 0, result.output
    assert import_id in result.stdout
    assert "No rows were deleted" in result.stdout
    assert _remaining(patched_db, import_id) == 2
    assert _status(patched_db, import_id) == "complete"


def test_import_revert_refuses_when_the_batch_moved_after_the_plan(
    runner: CliRunner, patched_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A batch that changes between the prompt and the write deletes nothing.

    The command approves one exact plan. Here the planning read reports a row
    count the live batch no longer has, which is what a concurrent write would
    look like — the in-transaction re-plan must refuse rather than delete
    whatever it now finds.
    """
    import_id = _seed_revertable_batch(patched_db, rows=2)
    real_plan_revert = ImportService.plan_revert
    reads = itertools.count()

    def _stale_first_read(self: ImportService, target: str) -> ImportRevertPlan:
        """Skew only the command's planning read; the re-plan sees live state."""
        plan = real_plan_revert(self, target)
        if next(reads) > 0:
            return plan
        return dataclasses.replace(
            plan, table_counts=(("raw.tabular_transactions", 99),)
        )

    monkeypatch.setattr(ImportService, "plan_revert", _stale_first_read)

    result = runner.invoke(app, ["import", "revert", import_id, "--yes"])

    assert result.exit_code != 0, result.output
    assert "nothing was deleted" in result.output
    assert _remaining(patched_db, import_id) == 2
    assert _status(patched_db, import_id) == "complete"


def test_import_revert_of_an_unknown_batch_exits_nonzero(
    runner: CliRunner, patched_db: Database
) -> None:
    """A no-op plan reports the reason without prompting for confirmation."""
    result = runner.invoke(
        app, ["import", "revert", "00000000-0000-0000-0000-000000000000"]
    )

    assert result.exit_code == 1, result.output
