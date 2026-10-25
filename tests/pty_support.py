"""Small real-terminal support for tests that exercise hidden prompts."""

from __future__ import annotations

import gc
import io
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pexpect
import pytest


@dataclass(frozen=True)
class PTYProcess:
    """A Python child connected to a real pseudo-terminal and its transcript."""

    child: Any
    transcript: io.StringIO


def spawn_python_pty(
    program: str,
    *,
    request: pytest.FixtureRequest,
    env: Mapping[str, str] | None = None,
) -> PTYProcess:
    """Start a Python program with terminal input and a captured transcript."""
    child_env = {**os.environ, "TERM": "dumb", **(env or {})}
    transcript = io.StringIO()
    # forkpty() in a threaded pytest worker can run a GC pass in the child before
    # exec; on CI that pass segfaulted in logging's after-fork hook, so the child
    # exited before printing its prompt. GC state is inherited, so pausing it here
    # keeps the pre-exec window allocation-collection free.
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        child = pexpect.spawn(
            sys.executable,
            ["-c", program],
            env=child_env,
            encoding="utf-8",
            timeout=30,
            echo=False,
        )
    finally:
        if gc_was_enabled:
            gc.enable()
    request.addfinalizer(lambda: child.close(force=True))
    child.logfile_read = transcript
    return PTYProcess(child=child, transcript=transcript)
