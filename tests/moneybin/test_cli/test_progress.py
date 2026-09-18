"""Progress presentation stays separate from sync and refresh work."""

from __future__ import annotations

from dataclasses import replace
from io import StringIO
from typing import Never

import pytest

from moneybin.cli.progress import operation_progress
from moneybin.cli.terminal import (
    TerminalPolicy,
    TerminalSymbols,
    resolve_terminal_policy,
)
from moneybin.progress import ProgressEvent


class _ProgressDriver:
    def __init__(self, *columns: object, **_kwargs: object) -> None:
        self.columns = columns
        self.stopped = False
        self.active_tasks = 0
        self.calls: list[tuple[str, dict[str, object]]] = []

    def __enter__(self) -> _ProgressDriver:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()

    def add_task(self, description: str, **kwargs: object) -> int:
        self.active_tasks += 1
        self.calls.append((description, kwargs))
        return 1

    def remove_task(self, _task_id: int) -> None:
        self.active_tasks -= 1
        self.calls.append(("remove", {}))

    def start(self) -> None:
        return None

    def update(self, _task_id: int, **kwargs: object) -> None:
        self.calls.append(("update", kwargs))

    def stop(self) -> None:
        self.stopped = True
        self.active_tasks = 0


class _Stream:
    def __init__(self, *, tty: bool) -> None:
        self._buffer = StringIO()
        self.tty = tty
        self.encoding = "utf-8"

    def isatty(self) -> bool:
        return self.tty

    def write(self, text: str) -> int:
        return self._buffer.write(text)

    def getvalue(self) -> str:
        return self._buffer.getvalue()


def _rich_must_not_initialize(*_args: object, **_kwargs: object) -> Never:
    pytest.fail("Rich progress must not be created")


class _Settings:
    auto_pager = True
    reduced_motion = False
    ascii = False


def _resolved_policy(
    *,
    output: str = "text",
    quiet: bool = False,
    stdout_tty: bool = True,
    reduced_motion: bool = False,
) -> tuple[TerminalPolicy, _Stream]:
    stderr = _Stream(tty=True)
    settings = _Settings()
    settings.reduced_motion = reduced_motion
    policy = resolve_terminal_policy(
        stdin=_Stream(tty=True),
        stdout=_Stream(tty=stdout_tty),
        stderr=stderr,
        output=output,  # type: ignore[arg-type]  # parametrized literal values
        quiet=quiet,
        no_pager=False,
        settings=settings,
    )
    return policy, stderr


@pytest.fixture
def animated_policy() -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=False,
        color=False,
        style=False,
        animate_progress=True,
        stage_chatter=True,
        ascii=False,
        width=80,
        height=24,
        symbols=TerminalSymbols(success="✓", attention="!", failure="×", action="›"),
        minus="−",
    )


@pytest.fixture
def progress_driver(monkeypatch: pytest.MonkeyPatch) -> dict[str, _ProgressDriver]:
    holder: dict[str, _ProgressDriver] = {}

    def _factory(*args: object, **kwargs: object) -> _ProgressDriver:
        driver = _ProgressDriver(*args, **kwargs)
        holder["driver"] = driver
        return driver

    monkeypatch.setattr("moneybin.cli.progress.Progress", _factory)
    return holder


def test_progress_uses_one_active_task_and_never_writes_stdout(
    animated_policy: TerminalPolicy,
    progress_driver: dict[str, _ProgressDriver],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stdout = StringIO()
    monkeypatch.setattr("sys.stdout", stdout)

    with operation_progress(animated_policy) as report:
        report(ProgressEvent("Syncing institutions"))
        report(ProgressEvent("Loading transactions", completed=3, total=10))
        report(ProgressEvent("Refreshing reports"))

    assert stdout.getvalue() == ""
    driver = progress_driver["driver"]
    assert driver.stopped
    assert driver.active_tasks == 0
    assert [call[0] for call in driver.calls].count("Syncing institutions") == 1
    assert any(
        column.__class__.__name__ == "MofNCompleteColumn" for column in driver.columns
    )
    assert driver.calls[1] == (
        "update",
        {"description": "Loading transactions", "completed": 3, "total": 10},
    )


def test_progress_cleans_up_on_interrupt(
    animated_policy: TerminalPolicy, progress_driver: dict[str, _ProgressDriver]
) -> None:
    with pytest.raises(KeyboardInterrupt):
        with operation_progress(animated_policy) as report:
            report(ProgressEvent("Refreshing reports"))
            raise KeyboardInterrupt

    assert progress_driver["driver"].stopped
    assert progress_driver["driver"].active_tasks == 0


@pytest.mark.parametrize(("output", "quiet"), [("json", False), ("text", True)])
def test_json_and_quiet_resolved_policies_emit_no_progress(
    monkeypatch: pytest.MonkeyPatch, output: str, quiet: bool
) -> None:
    policy, stderr = _resolved_policy(output=output, quiet=quiet)
    monkeypatch.setattr(
        "moneybin.cli.progress.Progress",
        _rich_must_not_initialize,
    )

    with operation_progress(policy, quiet=quiet) as report:
        report(ProgressEvent("Refreshing reports"))

    assert stderr.getvalue() == ""


@pytest.mark.parametrize(
    ("stdout_tty", "reduced_motion"), [(False, False), (True, True)]
)
def test_nonanimated_resolved_policies_use_plain_static_stages(
    monkeypatch: pytest.MonkeyPatch, stdout_tty: bool, reduced_motion: bool
) -> None:
    policy, stderr = _resolved_policy(
        stdout_tty=stdout_tty, reduced_motion=reduced_motion
    )
    monkeypatch.setattr(
        "moneybin.cli.progress.Progress",
        _rich_must_not_initialize,
    )
    monkeypatch.setattr("sys.stderr", stderr)

    with operation_progress(policy) as report:
        report(ProgressEvent("Refreshing reports"))

    assert stderr.getvalue() == "Refreshing reports\n"
    assert "\x1b[" not in stderr.getvalue()


def test_animated_progress_updates_counts_and_replaces_known_work_with_spinner(
    animated_policy: TerminalPolicy, progress_driver: dict[str, _ProgressDriver]
) -> None:
    with operation_progress(animated_policy) as report:
        report(ProgressEvent("Loading transactions", completed=3, total=10))
        report(ProgressEvent("Loading transactions", completed=4, total=10))
        report(ProgressEvent("Refreshing reports"))

    assert progress_driver["driver"].calls == [
        ("Loading transactions", {"completed": 3, "total": 10}),
        (
            "update",
            {"description": "Loading transactions", "completed": 4, "total": 10},
        ),
        ("remove", {}),
        ("Refreshing reports", {"total": None}),
    ]


@pytest.mark.parametrize("failure", ["start", "stop"])
def test_progress_lifecycle_failure_does_not_mask_successful_operation(
    animated_policy: TerminalPolicy,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    class _FailingDriver(_ProgressDriver):
        def start(self) -> None:
            if failure == "start":
                raise RuntimeError("terminal unavailable")

        def stop(self) -> None:
            if failure == "stop":
                raise RuntimeError("terminal unavailable")
            super().stop()

    monkeypatch.setattr("moneybin.cli.progress.Progress", _FailingDriver)

    with operation_progress(animated_policy) as report:
        report(ProgressEvent("Refreshing reports"))


def test_static_progress_deduplicates_stages_and_preserves_warnings(
    animated_policy: TerminalPolicy, monkeypatch: pytest.MonkeyPatch
) -> None:
    stderr = StringIO()
    monkeypatch.setattr("sys.stderr", stderr)
    policy = replace(animated_policy, animate_progress=False)

    with operation_progress(policy) as report:
        report(ProgressEvent("Syncing institutions"))
        report(ProgressEvent("Syncing institutions"))
        print("warning: source delayed", file=stderr)
        report(ProgressEvent("Refreshing reports"))
        report(ProgressEvent("Syncing institutions"))

    assert stderr.getvalue().splitlines() == [
        "Syncing institutions",
        "warning: source delayed",
        "Refreshing reports",
        "Syncing institutions",
    ]
