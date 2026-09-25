"""CLI tests for `moneybin privacy` consent commands (grant/revoke/status/log)."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.privacy.log import (
    build_tool_call_event,
    read_privacy_events,
    write_privacy_event,
)
from tests.moneybin.test_cli._curation_helpers import make_curation_db, patch_db


@pytest.fixture()
def db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Generator[Database, None, None]:
    database = make_curation_db(tmp_path)
    patch_db(monkeypatch, database)
    # Isolate the privacy log to tmp_path — grant/revoke/log all append events,
    # and the real profile dir is shared across tests + xdist workers.
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: tmp_path / "privacy_log",
    )
    yield database
    database.close()


def _set_backend(monkeypatch: pytest.MonkeyPatch, backend: str = "anthropic") -> None:
    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", backend)
    clear_settings_cache()
    set_current_profile("test")


def _interactive_policy(*, height: int = 4) -> TerminalPolicy:
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
        height=height,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )


def _interactive_policy_factory(**_: object) -> TerminalPolicy:
    return _interactive_policy()


def test_cli_grant_then_status_json(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    result = runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert status.exit_code == 0, status.output
    body = json.loads(status.stdout)["data"]
    assert body["default_backend"] == "anthropic"
    cats = {g["feature_category"] for g in body["active_grants"]}
    assert "mcp-data-sharing" in cats


def test_cli_revoke(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    result = runner.invoke(app, ["privacy", "revoke", "mcp-data-sharing", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert json.loads(status.stdout)["data"]["active_grants"] == []


def test_cli_status_json_empty(runner: CliRunner, db: Database) -> None:
    result = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)["data"]
    assert data["active_grants"] == []
    # No configured backend → null, not a "(none)" sentinel that could be fed
    # back into grant/revoke as a real backend.
    assert data["default_backend"] is None


def test_cli_revoke_all(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    runner.invoke(app, ["privacy", "grant", "ml-categorization", "--yes"])
    result = runner.invoke(app, ["privacy", "revoke-all", "--yes"])
    assert result.exit_code == 0, result.output
    status = runner.invoke(app, ["privacy", "status", "--output", "json"])
    assert json.loads(status.stdout)["data"]["active_grants"] == []


def test_cli_revoke_all_preview_names_the_same_grants_it_confirms(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A human sees the category/backend/mode selection before approving it."""
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    runner.invoke(
        app,
        ["privacy", "grant", "ml-categorization", "--backend", "openai", "--yes"],
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.privacy._presentation.get_terminal_policy",
        _interactive_policy_factory,
    )

    result = runner.invoke(app, ["privacy", "revoke-all"], input="n\n")

    assert result.exit_code == 0, result.output
    assert "mcp-data-sharing | anthropic | persistent" in result.output
    assert "ml-categorization | openai | persistent" in result.output


def test_cli_log_after_grant(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    result = runner.invoke(app, ["privacy", "log", "--last", "20"])
    assert result.exit_code == 0, result.output
    assert "consent.grant" in result.output


def test_cli_privacy_log_pages_the_same_complete_answer_as_no_pager(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The pager receives every selected event and its privacy framing."""
    from moneybin.cli import pager

    for index in range(8):
        write_privacy_event(
            build_tool_call_event(
                actor=f"tool-{index}",
                sensitivity="low",
                classes_returned=[f"CLASS_{index}"],
                row_count=index,
            )
        )
    monkeypatch.setattr(
        "moneybin.cli.commands.privacy.log.get_terminal_policy",
        _interactive_policy_factory,
    )
    paged: list[str] = []

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        paged.append(text)
        return True

    monkeypatch.setattr(
        pager,
        "page_text",
        capture_page,
    )

    page_result = runner.invoke(app, ["privacy", "log", "--last", "8"])
    direct_result = runner.invoke(app, ["privacy", "log", "--last", "8", "--no-pager"])

    assert page_result.exit_code == 0, page_result.output
    assert direct_result.exit_code == 0, direct_result.output
    assert paged and "CLASS_0" in paged[0] and "CLASS_7" in paged[0]
    assert (
        paged[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct_result.output.rstrip()
    )


def test_cli_privacy_log_pager_fallback_prints_the_complete_answer(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing pager never drops a selected privacy event."""
    from moneybin.cli import pager

    for index in range(8):
        write_privacy_event(
            build_tool_call_event(
                actor=f"tool-{index}",
                sensitivity="low",
                classes_returned=[f"CLASS_{index}"],
                row_count=index,
            )
        )
    monkeypatch.setattr(
        "moneybin.cli.commands.privacy.log.get_terminal_policy",
        _interactive_policy_factory,
    )

    def unavailable_pager(text: str, *, color: bool, wide: bool) -> bool:
        return False

    monkeypatch.setattr(pager, "page_text", unavailable_pager)

    result = runner.invoke(app, ["privacy", "log", "--last", "8"])

    assert result.exit_code == 0, result.output
    assert "CLASS_0" in result.output
    assert "CLASS_7" in result.output


def test_cli_privacy_log_caps_limit_keeps_tool_call_details_and_names_filtered_empty_scope(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The text log preserves complete tool-call facts without inventing totals.

    Width pinned to 100 columns — the terminal width `privacy log`'s wrapping
    defect was reported against. `When`, `Action`, and `Actor` are now
    declared `nowrap=`, so `Details` (the comma-joined class list included) is
    the only column that yields, and at this width it never has to: no
    `key=value` segment splits mid-token the way `TXN_A` / `MOUNT` used to.
    """
    monkeypatch.setenv("COLUMNS", "100")
    write_privacy_event(
        build_tool_call_event(
            actor="tool-actor",
            sensitivity="critical",
            classes_returned=["ACCOUNT_ID", "TXN_AMOUNT"],
            row_count=17,
        )
    )

    detailed = runner.invoke(app, ["privacy", "log", "--last", "1001", "--no-pager"])
    empty = runner.invoke(
        app, ["privacy", "log", "--actor", "no-such-actor", "--no-pager"]
    )

    assert detailed.exit_code == 0, detailed.output
    assert "Showing last 1000 events" in detailed.output
    assert "sensitivity=critical" in detailed.output
    assert "classes=ACCOUNT_ID,TXN_AMOUNT" in detailed.output
    assert "rows=17" in detailed.output
    assert "rows=17" in detailed.output
    assert "total matching events unknown" in detailed.output
    assert empty.exit_code == 0, empty.output
    assert "No events matched actor=no-such-actor." in empty.output


def test_cli_privacy_mutation_receipt_never_uses_the_pager(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A completed mutation is a receipt, even if terminal paging is enabled."""
    from moneybin.cli import pager

    _set_backend(monkeypatch)
    monkeypatch.setattr(
        "moneybin.cli.commands.privacy._presentation.get_terminal_policy",
        _interactive_policy_factory,
    )

    def unexpected_page(text: str, *, color: bool, wide: bool) -> bool:
        pytest.fail("receipt must not open pager")

    monkeypatch.setattr(
        pager,
        "page_text",
        unexpected_page,
    )

    result = runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])

    assert result.exit_code == 0, result.output
    assert "Consent granted" in result.output


def test_cli_privacy_grant_cancelled_text_has_no_mutation_or_privacy_event(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Declining the text confirmation leaves both durable ledgers untouched."""
    _set_backend(monkeypatch)
    monkeypatch.setattr(
        "moneybin.cli.commands.privacy._presentation.get_terminal_policy",
        _interactive_policy_factory,
    )

    result = runner.invoke(app, ["privacy", "grant", "mcp-data-sharing"], input="n\n")

    assert result.exit_code == 0, result.output
    assert "Consent grant cancelled" in result.output
    assert read_privacy_events({}, max_rows=20) == []
    assert (
        json.loads(
            runner.invoke(app, ["privacy", "status", "--output", "json"]).stdout
        )["data"]["active_grants"]
        == []
    )
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == (0,)


def test_cli_privacy_grant_noninteractive_text_refusal_has_no_mutation_or_event(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redirected text requires --yes rather than treating EOF as approval."""
    _set_backend(monkeypatch)

    result = runner.invoke(app, ["privacy", "grant", "mcp-data-sharing"])

    assert result.exit_code == 1, result.output
    assert "--yes" in result.output
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == (0,)
    assert read_privacy_events({}, max_rows=20) == []


def test_cli_one_time_grant_receipt_retains_enforcement_limitation(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    result = runner.invoke(
        app,
        [
            "privacy",
            "grant",
            "mcp-data-sharing",
            "--mode",
            "one-time",
            "--yes",
            "--quiet",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Consent granted" in result.output
    assert "persists until you" in result.output
    assert "revoke it" in result.output


@pytest.mark.parametrize(
    ("command", "actor"),
    [
        (["privacy", "grant", "mcp-data-sharing", "--output", "json"], "privacy_grant"),
        (
            ["privacy", "revoke", "mcp-data-sharing", "--output", "json"],
            "privacy_revoke",
        ),
        (["privacy", "revoke-all", "--output", "json"], "privacy_revoke_all"),
    ],
)
def test_cli_privacy_mutation_json_requires_yes_without_prompting(
    runner: CliRunner,
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
    actor: str,
) -> None:
    _set_backend(monkeypatch)
    result = runner.invoke(app, command)
    assert result.exit_code == 1, result.output
    body = json.loads(result.stdout)
    assert body["error"]["code"] == "mutation_confirmation_required"
    assert "--yes" in body["error"]["hint"]


def test_cli_privacy_status_and_log_accept_complete_read_presentation_controls(
    runner: CliRunner, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_backend(monkeypatch)
    runner.invoke(app, ["privacy", "grant", "mcp-data-sharing", "--yes"])
    status = runner.invoke(app, ["privacy", "status", "--quiet", "--no-pager"])
    log = runner.invoke(app, ["privacy", "log", "--last", "1", "--quiet", "--no-pager"])
    assert status.exit_code == 0, status.output
    assert "Category" in status.output
    assert log.exit_code == 0, log.output
    assert "Showing last 1 event" in log.output
