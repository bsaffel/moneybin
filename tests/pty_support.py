"""Small real-terminal support for tests that exercise hidden prompts."""

from __future__ import annotations

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
    child = pexpect.spawn(
        sys.executable,
        ["-c", program],
        env=child_env,
        encoding="utf-8",
        timeout=30,
        echo=False,
    )
    request.addfinalizer(lambda: child.close(force=True))
    child.logfile_read = transcript
    return PTYProcess(child=child, transcript=transcript)
