"""Shared fixtures for the CLI command tests."""

from __future__ import annotations

import pytest


@pytest.fixture
def wide_terminal(monkeypatch: pytest.MonkeyPatch) -> None:
    """Render tables unwrapped, so a content assertion is only about content.

    `render_rows` builds a Rich table, and Rich assumes 80 columns when stdout
    is not a terminal — which it never is under `CliRunner`. A row wider than
    that wraps, splitting values across lines, so a test checking that a value
    *reaches* the output would silently also be asserting that it fits in 80
    columns. Those are two different guarantees: the second is requirement 9 of
    `docs/specs/cli-output-coherence.md`, which selects each report's default
    columns and is tested on its own terms. Widening the console here keeps a
    content test from failing for a width reason, and keeps it from passing as
    though it had checked the width.

    Opt in by name rather than autouse: a test that means to exercise the
    default width must not get this silently.
    """
    monkeypatch.setenv("COLUMNS", "200")
