"""Test-synthetic CLI registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import typer

_REGISTERED: list[str] = []


def register(app: typer.Typer) -> None:  # noqa: ARG001  # signature matches the register() contract
    _REGISTERED.append("cli.register")


def calls() -> list[str]:
    return list(_REGISTERED)


def reset() -> None:
    _REGISTERED.clear()
