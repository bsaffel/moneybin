"""Human-output contracts for the six profile CLI leaves.

Each test names the presentation or confirmation branch it protects; services
are substituted only at the filesystem/database boundary.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli import pager
from moneybin.cli.commands.profile import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols
from moneybin.services.profile_service import ProfileService

runner = CliRunner()


def test_delete_refuses_redirected_confirmation_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_delete(*args: object, **kwargs: object) -> None:
        pytest.fail("redirected confirmation must not delete a profile")

    monkeypatch.setattr(ProfileService, "delete", unexpected_delete)

    result = runner.invoke(app, ["delete", "alice"], input="y\n")

    assert result.exit_code == 1, result.output
    assert "--yes" in result.stderr
    assert "Deletion cancelled" not in result.stdout


def _paging_policy() -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=True,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=40,
        height=2,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )


def _profile(name: str, *, active: bool = False) -> dict[str, object]:
    return {"name": name, "active": active, "path": f"/profiles/{name}"}


def _policy_factory(**_kwargs: object) -> TerminalPolicy:
    return _paging_policy()


def _capture_page(pages: list[str]):
    def capture(text: str, *, color: bool, wide: bool) -> bool:
        pages.append(text)
        return True

    return capture


def test_list_pages_one_complete_answer_and_no_pager_prints_the_same_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long profile list must page its whole answer, never each row separately."""
    service = MagicMock()
    service.list.return_value = [
        _profile(f"profile-{index}", active=index == 0) for index in range(8)
    ]
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )
    pages: list[str] = []
    monkeypatch.setattr(pager, "page_text", _capture_page(pages))

    paged = runner.invoke(app, ["list"])
    direct = runner.invoke(app, ["list", "--no-pager"])

    assert paged.exit_code == 0, paged.output
    assert direct.exit_code == 0, direct.output
    assert len(pages) == 1
    assert "Profiles" in pages[0]
    assert "profile-7" in pages[0]
    assert (
        pages[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct.stdout.rstrip()
    )


@pytest.mark.parametrize("quiet", [False, True])
def test_list_empty_keeps_scope_and_next_action_when_quiet(
    monkeypatch: pytest.MonkeyPatch, quiet: bool
) -> None:
    """Quiet suppresses chatter, not the requested empty profile answer."""
    service = MagicMock()
    service.list.return_value = []
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    result = runner.invoke(app, ["list", *(["--quiet"] if quiet else [])])

    assert result.exit_code == 0, result.output
    assert "Profiles" in result.stdout
    assert "No profiles found." in result.stdout
    assert "moneybin profile create <name>" in result.stdout


def test_list_json_remains_the_existing_profile_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Text paging must not alter the profile list's machine-readable contract."""
    service = MagicMock()
    service.list.return_value = [_profile("alice", active=True)]
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    result = runner.invoke(app, ["list", "--output", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["data"] == [
        {"name": "alice", "active": True, "path": "/profiles/alice"}
    ]


def test_show_pages_complete_config_and_settings_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long profile details retain configuration and managed settings in one pager."""
    service = MagicMock()
    service.show.return_value = {
        "name": "alice",
        "active": True,
        "path": "/profiles/alice",
        "database_path": "/profiles/alice/moneybin.duckdb",
        "database_exists": False,
        "config": {"logging": {f"field_{index}": "configured" for index in range(8)}},
    }
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )
    pages: list[str] = []
    monkeypatch.setattr(pager, "page_text", _capture_page(pages))

    result = runner.invoke(app, ["show"])
    direct = runner.invoke(app, ["show", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert direct.exit_code == 0, direct.output
    assert len(pages) == 1
    assert "Profile" in pages[0]
    assert "field_7" in pages[0]
    assert (
        pages[0].replace("\n\nq return to shell\n", "").rstrip()
        == direct.stdout.rstrip()
    )


def test_show_keeps_a_database_path_wider_than_the_terminal_whole(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requirement 10: a `Database:` path wider than the terminal folds, never truncates.

    A path no line can hold folds across lines; joining them gives the path
    back, so nothing the reader needs to open the file is lost. An `…` would
    lose the filename.
    """
    long_path = "/profiles/a-fairly-long-profile-name/moneybin.duckdb"
    service = MagicMock()
    service.show.return_value = {
        "name": "alice",
        "active": True,
        "path": "/profiles/alice",
        "database_path": long_path,
        "database_exists": True,
        "config": {},
    }
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    def _narrow_policy(**_kwargs: object) -> TerminalPolicy:
        return replace(_paging_policy(), page=False, width=40, height=24)

    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _narrow_policy
    )

    result = runner.invoke(app, ["show", "--no-pager"])

    assert result.exit_code == 0, result.output
    joined = "".join(line.strip() for line in result.stdout.splitlines())
    assert long_path in joined, "the database path lost characters"
    assert "…" not in result.stdout


def test_create_receipt_distinguishes_adoption_and_preserved_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An adopted directory with a database is not reported as a new profile."""
    service = MagicMock()
    service.exists.return_value = True
    service.has_database.return_value = True
    service.create.return_value = Path("/profiles/alice")
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    result = runner.invoke(app, ["create", "Alice"])

    assert result.exit_code == 0, result.output
    assert "Profile setup completed" in result.stdout
    assert "alice" in result.stdout
    assert "Existing database was preserved." in result.stdout
    assert "Created profile" not in result.stdout


def test_switch_and_set_receipts_name_actual_target_without_echoing_input_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Receipts name the selected profile but never reconstruct a setting command."""
    service = MagicMock()
    service.show.return_value = {
        "name": "alice",
        "active": True,
        "path": "/profiles/alice",
        "database_path": "/profiles/alice/moneybin.duckdb",
        "database_exists": True,
        "config": {"logging": {"level": "INFO"}},
    }
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    def current_profile(*, auto_resolve: bool = False) -> str:
        return "alice"

    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_current_profile", current_profile
    )

    switched = runner.invoke(app, ["switch", "alice"])
    saved = runner.invoke(app, ["set", "logging.level", "super-secret-value"])

    assert switched.exit_code == 0, switched.output
    assert "Active profile: alice" in switched.stdout
    assert saved.exit_code == 0, saved.output
    assert "Profile setting saved" in saved.stdout
    assert "Profile: alice" in saved.stdout
    assert "Setting: logging.level" in saved.stdout
    assert "super-secret-value" not in saved.stdout


def test_mutation_receipts_use_the_normalized_profile_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mixed-case input must not make a receipt disagree with the service target."""
    service = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )

    switched = runner.invoke(app, ["switch", "Alice_Work"])
    deleted = runner.invoke(app, ["delete", "Alice_Work", "--yes"])
    saved = runner.invoke(
        app, ["set", "logging.level", "INFO", "--profile", "Alice_Work"]
    )

    assert switched.exit_code == 0, switched.output
    assert "Active profile: alice-work" in switched.stdout
    assert deleted.exit_code == 0, deleted.output
    assert "Profile: alice-work" in deleted.stdout
    assert saved.exit_code == 0, saved.output
    assert "Profile: alice-work" in saved.stdout


def test_managed_setting_receipt_uses_the_normalized_profile_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Managed settings use the same canonical profile name as config settings."""
    service = MagicMock()
    service.show.return_value = {
        "name": "alice-work",
        "active": True,
        "path": "/profiles/alice-work",
        "database_path": "/profiles/alice-work/moneybin.duckdb",
        "database_exists": True,
        "config": {},
    }
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    def set_managed(
        _svc: ProfileService,
        _target: str,
        _key: str,
        _value: str,
        *,
        explicit_profile: str | None,
    ) -> None:
        del explicit_profile

    def read_managed(
        _svc: ProfileService, _info: Mapping[str, object]
    ) -> dict[str, object]:
        return {"home_currency": "EUR"}

    monkeypatch.setattr(
        "moneybin.cli.commands.profile._set_managed_setting",
        set_managed,
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.profile._read_managed_settings",
        read_managed,
    )

    result = runner.invoke(
        app, ["set", "home_currency", "eur", "--profile", "Alice_Work"]
    )

    assert result.exit_code == 0, result.output
    assert "Profile:" in result.stdout and "alice-work" in result.stdout
    assert "home_currency: EUR" in result.stdout


def test_create_skips_inbox_prompt_when_terminal_policy_is_not_interactive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TTY input alone cannot prompt when the result stream is unusable."""
    service = MagicMock()
    service.exists.return_value = False
    service.create.return_value = Path("/profiles/alice")
    policy = replace(_paging_policy(), interactive=False)
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    def noninteractive_policy(**_kwargs: object) -> TerminalPolicy:
        return policy

    def unexpected_prompt(*_args: object, **_kwargs: object) -> bool:
        raise AssertionError("create prompted without a usable terminal")

    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", noninteractive_policy
    )
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.typer.confirm", unexpected_prompt
    )

    result = runner.invoke(app, ["create", "Alice"])

    assert result.exit_code == 0, result.output
    service.create.assert_called_once_with("Alice", init_inbox=False)


def test_profile_mutation_receipts_never_open_the_pager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paging is a read-only affordance even on a tiny terminal."""
    service = MagicMock()
    service.exists.return_value = False
    service.create.return_value = Path("/profiles/alice")
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )

    def pager_called(*_args: object, **_kwargs: object) -> bool:
        raise AssertionError("mutation receipt opened a pager")

    monkeypatch.setattr(pager, "page_text", pager_called)

    results = [
        runner.invoke(app, ["create", "alice", "--no-init-inbox"]),
        runner.invoke(app, ["switch", "alice"]),
        runner.invoke(app, ["delete", "alice", "--yes"]),
        runner.invoke(app, ["set", "logging.level", "INFO", "--profile", "alice"]),
    ]

    assert all(result.exit_code == 0 for result in results)


@pytest.mark.parametrize(
    ("command", "service_method"),
    [
        (["create", "___", "--no-init-inbox"], "create"),
        (["switch", "___"], "switch"),
        (["delete", "___"], "delete"),
        (["delete", "___", "--yes"], "delete"),
    ],
)
def test_invalid_profile_name_refuses_before_switch_or_delete_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    command: list[str],
    service_method: str,
) -> None:
    """Invalid names fail before a confirmation prompt or profile mutation."""
    service = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )

    def unexpected_prompt(*_args: object, **_kwargs: object) -> bool:
        raise AssertionError("invalid delete reached confirmation")

    monkeypatch.setattr(
        "moneybin.cli.commands.profile.typer.confirm", unexpected_prompt
    )

    result = runner.invoke(app, command)

    assert result.exit_code == 1
    assert result.exception is not None
    assert "contains no valid characters" in result.stderr
    getattr(service, service_method).assert_not_called()


def test_invalid_profile_name_refuses_before_setting_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config settings do not create an invalid profile target on normalization failure."""
    service = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)

    result = runner.invoke(app, ["set", "logging.level", "INFO", "--profile", "___"])

    assert result.exit_code == 1
    assert result.exception is not None
    assert "contains no valid characters" in result.stderr
    service.set.assert_not_called()


def test_delete_noninteractive_without_yes_refuses_before_service_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A piped delete cannot turn EOF into an accidental profile removal."""
    service = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    policy = _paging_policy()

    def noninteractive_policy(**_kwargs: object) -> TerminalPolicy:
        return replace(policy, interactive=False)

    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", noninteractive_policy
    )

    result = runner.invoke(app, ["delete", "alice"])

    assert result.exit_code == 1
    assert "explicit confirmation" in result.stderr.lower()
    service.delete.assert_not_called()


def test_delete_decline_is_a_truthful_unpaged_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Declining an interactive delete remains a successful no-write outcome."""
    service = MagicMock()
    monkeypatch.setattr("moneybin.cli.commands.profile.ProfileService", lambda: service)
    monkeypatch.setattr(
        "moneybin.cli.commands.profile.get_terminal_policy", _policy_factory
    )

    result = runner.invoke(app, ["delete", "alice"], input="n\n")

    assert result.exit_code == 0, result.output
    assert "Deletion cancelled" in result.stdout
    assert "Profile alice was not deleted." in result.stdout
    service.delete.assert_not_called()
