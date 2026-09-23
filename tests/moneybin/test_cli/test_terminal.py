"""Terminal capability policy is pure, stream-based, and safe before startup."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pytest

from moneybin.cli.output import OutputFormat
from moneybin.cli.render import Money, format_money, render_rows
from moneybin.cli.terminal import TerminalPolicy, resolve_terminal_policy
from moneybin.cli.utils import (
    get_terminal_policy,
    set_output_flag,
    set_quiet_flag,
    stash_cli_flags,
)
from moneybin.config import CLISettings, MoneyBinSettings


@dataclass
class _Stream:
    """A terminal stream value, not a mock of the policy under test."""

    tty: bool = True
    encoding: str | None = "UTF-8"
    written: list[str] = field(default_factory=list)

    def isatty(self) -> bool:
        return self.tty

    def write(self, value: str) -> int:
        self.written.append(value)
        return len(value)

    def flush(self) -> None:
        pass

    def getvalue(self) -> str:
        return "".join(self.written)

    def read(self, *_: object, **__: object) -> str:
        raise AssertionError("terminal policy must never consume stdin")


@pytest.fixture
def streams() -> dict[str, _Stream]:
    return {"stdin": _Stream(), "stdout": _Stream(), "stderr": _Stream()}


@pytest.fixture
def settings() -> CLISettings:
    return CLISettings()


def _resolve(
    streams: dict[str, _Stream],
    settings: CLISettings,
    *,
    output: Literal["text", "json"] = "text",
    quiet: bool = False,
    no_pager: bool = False,
) -> TerminalPolicy:
    return resolve_terminal_policy(
        stdin=streams["stdin"],
        stdout=streams["stdout"],
        stderr=streams["stderr"],
        output=output,
        quiet=quiet,
        no_pager=no_pager,
        settings=settings,
    )


@pytest.mark.parametrize("stream_name", ["stdin", "stdout"])
def test_redirected_stream_disables_interaction_and_paging(
    stream_name: str, streams: dict[str, _Stream], settings: CLISettings
) -> None:
    streams[stream_name].tty = False

    policy = _resolve(streams, settings)

    assert not policy.interactive
    assert not policy.page


def test_color_depends_on_stdout_not_stderr(
    monkeypatch: pytest.MonkeyPatch,
    streams: dict[str, _Stream],
    settings: CLISettings,
) -> None:
    monkeypatch.delenv("NO_COLOR", raising=False)
    streams["stderr"].tty = False

    policy = _resolve(streams, settings)

    assert policy.color


@pytest.mark.parametrize(
    ("output", "quiet", "expected_animation", "expected_stage_chatter"),
    [
        ("text", False, True, True),
        ("text", True, False, False),
        ("json", False, False, False),
    ],
)
def test_output_mode_and_quiet_have_distinct_progress_policies(
    streams: dict[str, _Stream],
    settings: CLISettings,
    output: Literal["text", "json"],
    quiet: bool,
    expected_animation: bool,
    expected_stage_chatter: bool,
) -> None:
    policy = _resolve(streams, settings, output=output, quiet=quiet)

    assert policy.output == output
    assert policy.animate_progress is expected_animation
    assert policy.stage_chatter is expected_stage_chatter


def test_reduced_motion_keeps_static_stages_but_disables_animation(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, CLISettings(reduced_motion=True))

    assert not policy.animate_progress
    assert policy.stage_chatter


def test_redirected_text_uses_sparse_static_stages_without_animation(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    streams["stdout"].tty = False

    policy = _resolve(streams, settings)

    assert not policy.animate_progress
    assert policy.stage_chatter


def test_no_tty_stderr_disables_progress_animation(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    streams["stderr"].tty = False

    policy = _resolve(streams, settings)

    assert not policy.animate_progress
    assert policy.stage_chatter


def test_redirected_stderr_refuses_prompts_but_keeps_stdout_paging(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    """A hidden confirmation prompt must not block an otherwise visible terminal."""
    streams["stderr"].tty = False

    policy = _resolve(streams, settings)

    assert not policy.interactive
    assert policy.page


def test_no_color_disables_all_styling(
    monkeypatch: pytest.MonkeyPatch,
    streams: dict[str, _Stream],
    settings: CLISettings,
) -> None:
    monkeypatch.setenv("NO_COLOR", "1")

    policy = _resolve(streams, settings)

    assert not policy.color
    assert not policy.style


def test_ascii_setting_selects_portable_symbols_and_minus(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, CLISettings(ascii=True))

    assert policy.ascii
    assert policy.symbols.success == "OK"
    assert policy.symbols.attention == "!"
    assert policy.symbols.failure == "X"
    assert policy.symbols.action == ">"
    assert policy.minus == "-"


def test_default_symbols_preserve_the_human_terminal_vocabulary(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, settings)

    assert policy.symbols.success == "✓"
    assert policy.symbols.attention == "!"
    assert policy.symbols.failure == "×"
    assert policy.symbols.action == "›"
    assert policy.minus == "−"


@pytest.mark.parametrize("encoding", ["ascii", "latin-1", "cp1252"])
def test_non_unicode_stdout_automatically_uses_ascii_symbols(
    streams: dict[str, _Stream], settings: CLISettings, encoding: str
) -> None:
    streams["stdout"].encoding = encoding

    policy = _resolve(streams, settings)

    assert policy.ascii
    assert policy.symbols.success == "OK"
    assert policy.symbols.failure == "X"
    assert policy.symbols.action == ">"
    assert policy.minus == "-"


def test_money_formatter_keeps_the_value_when_using_the_ascii_minus(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, CLISettings(ascii=True))

    assert format_money(-84.27, "flow", minus=policy.minus) == "-84.27"


def test_rows_apply_the_policy_ascii_minus(
    monkeypatch: pytest.MonkeyPatch,
    streams: dict[str, _Stream],
    settings: CLISettings,
) -> None:
    stream = _Stream()
    monkeypatch.setattr("sys.stdout", stream)
    policy = _resolve(streams, CLISettings(ascii=True))

    render_rows(
        ["amount"],
        [(-84.27,)],
        money={"amount": Money("flow")},
        terminal=policy,
    )

    assert "-84.27" in stream.getvalue()
    assert "−84.27" not in stream.getvalue()


def test_rows_resolve_the_ascii_preference_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _active_profile(*, auto_resolve: bool) -> str:
        return "test"

    stream = _Stream()
    monkeypatch.setattr("sys.stdout", stream)
    monkeypatch.setenv("MONEYBIN_CLI__ASCII", "true")
    monkeypatch.setattr("moneybin.config.get_current_profile", _active_profile)
    monkeypatch.setattr(
        "moneybin.config.get_settings", lambda: MoneyBinSettings(profile="test")
    )

    render_rows(["amount"], [(-84.27,)], money={"amount": Money("flow")})

    assert "-84.27" in stream.getvalue()
    assert "−84.27" not in stream.getvalue()


def test_policy_reads_saved_default_profile_preferences_without_resolving_it(
    monkeypatch: pytest.MonkeyPatch, streams: dict[str, _Stream]
) -> None:
    """Presentation honors saved profile settings without startup side effects."""
    requested_profiles: list[str] = []

    def _no_active_profile(*, auto_resolve: bool) -> str:
        raise RuntimeError("no active profile")

    def _settings_for(profile: str) -> MoneyBinSettings:
        requested_profiles.append(profile)
        return MoneyBinSettings.model_construct(
            cli=CLISettings(auto_pager=False, reduced_motion=True, ascii=True)
        )

    monkeypatch.setattr("sys.stdin", streams["stdin"])
    monkeypatch.setattr("sys.stdout", streams["stdout"])
    monkeypatch.setattr("sys.stderr", streams["stderr"])
    monkeypatch.setattr("moneybin.config.get_current_profile", _no_active_profile)
    monkeypatch.setattr(
        "moneybin.config.get_settings",
        lambda: pytest.fail("DB-free policy must not resolve startup settings"),
    )
    monkeypatch.setattr("moneybin.cli.utils.get_default_profile", lambda: "alice")
    monkeypatch.setattr("moneybin.config.MoneyBinSettings", _settings_for)
    stash_cli_flags(profile=None, verbose=False)

    policy = get_terminal_policy()

    assert requested_profiles == ["alice"]
    assert policy.ascii
    assert not policy.animate_progress
    assert not policy.page


@pytest.mark.parametrize(
    ("profile_flag", "environment_profile", "expected_profile"),
    [
        ("from-flag", "from-environment", "from-flag"),
        (None, "from-environment", "from-environment"),
    ],
)
def test_policy_prefers_profile_hints_before_the_saved_default(
    monkeypatch: pytest.MonkeyPatch,
    streams: dict[str, _Stream],
    profile_flag: str | None,
    environment_profile: str,
    expected_profile: str,
) -> None:
    """A terminal policy matches command profile precedence before startup resolves it."""
    requested_profiles: list[str] = []

    def _no_active_profile(*, auto_resolve: bool) -> str:
        raise RuntimeError("no active profile")

    def _settings_for(profile: str) -> MoneyBinSettings:
        requested_profiles.append(profile)
        return MoneyBinSettings.model_construct(cli=CLISettings())

    monkeypatch.setattr("sys.stdin", streams["stdin"])
    monkeypatch.setattr("sys.stdout", streams["stdout"])
    monkeypatch.setattr("sys.stderr", streams["stderr"])
    monkeypatch.setattr("moneybin.config.get_current_profile", _no_active_profile)
    monkeypatch.setattr(
        "moneybin.cli.utils.get_default_profile", lambda: "saved-default"
    )
    monkeypatch.setattr("moneybin.config.MoneyBinSettings", _settings_for)
    monkeypatch.setenv("MONEYBIN_PROFILE", environment_profile)
    stash_cli_flags(profile=profile_flag, verbose=False)

    get_terminal_policy()

    assert requested_profiles == [expected_profile]


def test_no_color_policy_removes_all_ansi_styles_from_rendered_rows(
    monkeypatch: pytest.MonkeyPatch,
    streams: dict[str, _Stream],
    settings: CLISettings,
) -> None:
    stream = _Stream()
    monkeypatch.setattr("sys.stdout", stream)
    monkeypatch.setenv("NO_COLOR", "1")
    from rich.console import Console

    def _force_terminal(*args: object, **kwargs: object) -> Console:
        kwargs.setdefault("force_terminal", True)
        kwargs.setdefault("color_system", "standard")
        return Console(*args, **kwargs)

    monkeypatch.setattr("rich.console.Console", _force_terminal)
    policy = _resolve(streams, settings)

    render_rows(
        ["amount"],
        [(-84.27,)],
        money={"amount": Money("flow")},
        terminal=policy,
    )

    assert "\x1b[" not in stream.getvalue()


def test_root_flag_stashing_resets_output_and_quiet_between_invocations(
    monkeypatch: pytest.MonkeyPatch, streams: dict[str, _Stream]
) -> None:
    def _no_profile(*, auto_resolve: bool) -> str:
        raise RuntimeError("no active profile")

    monkeypatch.setattr("sys.stdin", streams["stdin"])
    monkeypatch.setattr("sys.stdout", streams["stdout"])
    monkeypatch.setattr("sys.stderr", streams["stderr"])
    monkeypatch.setattr("moneybin.config.get_current_profile", _no_profile)
    set_output_flag(OutputFormat.JSON)
    set_quiet_flag(True)

    stash_cli_flags(profile=None, verbose=False)
    policy = get_terminal_policy()

    assert policy.output == "text"
    assert policy.stage_chatter


def test_policy_exposes_terminal_dimensions_for_pager_measurement(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, settings)

    assert policy.width > 0
    assert policy.height > 0


def test_cli_settings_use_the_standard_nested_environment_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MONEYBIN_CLI__AUTO_PAGER", "false")
    monkeypatch.setenv("MONEYBIN_CLI__REDUCED_MOTION", "true")
    monkeypatch.setenv("MONEYBIN_CLI__ASCII", "true")

    settings = MoneyBinSettings(profile="test")

    assert settings.cli == CLISettings(
        auto_pager=False, reduced_motion=True, ascii=True
    )


def test_json_never_interacts_pages_or_styles(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, settings, output="json")

    assert not policy.interactive
    assert not policy.page
    assert not policy.style


def test_no_pager_flag_overrides_the_persisted_preference(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, settings, no_pager=True)

    assert not policy.page


def test_auto_pager_setting_can_disable_paging(
    streams: dict[str, _Stream], settings: CLISettings
) -> None:
    policy = _resolve(streams, CLISettings(auto_pager=False))

    assert not policy.page
