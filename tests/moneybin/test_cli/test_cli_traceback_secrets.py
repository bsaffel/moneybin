"""The CLI's rich tracebacks must never render frame locals.

Frames on the database-open path hold the plaintext encryption key
(``build_attach_sql`` interpolates it — DuckDB cannot parameterize
ENCRYPTION_KEY) and profile passphrases. A rich traceback is not a log
record, so ``SanitizedLogFormatter`` never sees it: rendered locals would go
straight to terminal scrollback and CI logs.

Typer 0.25.1 already defaults ``pretty_exceptions_show_locals`` to False, so
these tests pin an existing guarantee rather than a change in behavior. That
is the point — the project's dependency floor is a range (``typer>=0.24.1``),
and the guarantee must not rest on a dependency's default staying put.
"""

from __future__ import annotations

import sys

import pytest
import typer.main

from moneybin.cli.main import app


def test_root_app_does_not_render_locals_in_tracebacks() -> None:
    assert app.pretty_exceptions_show_locals is False


def test_show_locals_setting_reaches_typers_renderer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assert the wiring, not just the attribute.

    Typer carries the setting to its excepthook by stamping a
    ``DeveloperExceptionConfig`` onto the escaping exception in the *root*
    app's ``__call__``. That is the only ``__call__`` the CLI invokes —
    ``main()`` calls ``app()``, and every command group is mounted with
    ``add_typer`` — so what this asserts for one command holds for all of
    them, at any nesting depth.
    """
    monkeypatch.setattr(sys, "excepthook", sys.excepthook)

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("secret-bearing frame")

    monkeypatch.setattr("moneybin.cli.commands.stats.get_database", _boom)

    with pytest.raises(RuntimeError) as excinfo:
        app(["stats"], standalone_mode=False)

    config = getattr(
        excinfo.value,
        typer.main._typer_developer_exception_attr_name,  # pyright: ignore[reportPrivateUsage]
        None,
    )
    assert config is not None, "Typer did not stamp its traceback config"
    assert config.pretty_exceptions_show_locals is False
