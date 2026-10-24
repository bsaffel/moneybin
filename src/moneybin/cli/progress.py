"""Transient terminal presentation for operation progress."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager

from rich.console import Console, RenderableType
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
)
from rich.text import Text

from moneybin.cli.terminal import TerminalPolicy
from moneybin.progress import ProgressEvent

logger = logging.getLogger(__name__)


class _WhenCounted(ProgressColumn):
    """Render the wrapped column only for a task with a known total.

    Without a total, Rich draws a pulsing bar and ``0/?`` — a count that
    reads like progress toward an unknown number of things.
    """

    def __init__(self, column: ProgressColumn) -> None:
        super().__init__(table_column=column.get_table_column())
        self._column = column

    def render(self, task: Task) -> RenderableType:
        """Delegate to the wrapped column, or render nothing when uncounted."""
        return self._column.render(task) if task.total is not None else Text("")


@contextmanager
def operation_progress(
    policy: TerminalPolicy, *, quiet: bool = False
) -> Generator[Callable[[ProgressEvent], None], None, None]:
    """Present a single transient task or sparse static stage labels."""
    last_static_stage: str | None = None
    task_id: TaskID | None = None
    task_total: int | None = None
    progress: Progress | None = None
    if policy.animate_progress and not quiet:
        try:
            progress = Progress(
                SpinnerColumn(),
                TextColumn("{task.description}"),
                _WhenCounted(BarColumn()),
                _WhenCounted(MofNCompleteColumn()),
                console=Console(file=sys.stderr, no_color=not policy.color),
                transient=True,
            )
            progress.start()
        except Exception as exc:
            logger.debug(f"Progress presentation failed: {exc}")
            progress = None

    def report(event: ProgressEvent) -> None:
        nonlocal last_static_stage, task_id, task_total
        try:
            if quiet or not policy.stage_chatter:
                return
            if progress is None:
                if event.stage == last_static_stage:
                    return
                sys.stderr.write(f"{event.stage}\n")
                last_static_stage = event.stage
                return
            if task_id is None:
                if event.total is None:
                    task_id = progress.add_task(event.stage, total=None)
                else:
                    task_id = progress.add_task(
                        event.stage,
                        completed=event.completed or 0,
                        total=event.total,
                    )
                task_total = event.total
                return
            if event.total is None and task_total is not None:
                progress.remove_task(task_id)
                task_id = progress.add_task(event.stage, total=None)
                task_total = None
            elif event.total is None:
                progress.update(task_id, description=event.stage)
            else:
                progress.update(
                    task_id,
                    description=event.stage,
                    completed=event.completed or 0,
                    total=event.total,
                )
                task_total = event.total
        except Exception as exc:
            logger.debug(f"Progress presentation failed: {exc}")

    try:
        yield report
    finally:
        if progress is not None:
            try:
                progress.stop()
            except Exception as exc:
                logger.debug(f"Progress presentation failed: {exc}")
