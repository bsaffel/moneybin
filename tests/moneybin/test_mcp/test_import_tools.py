"""Tests for import-tool helpers, including the file-path security boundary."""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest import MonkeyPatch

from moneybin.errors import UserError
from moneybin.mcp.tools.import_tools import (
    _validate_file_path,  # pyright: ignore[reportPrivateUsage]
)


def test_valid_path_within_home_returns_resolved_path(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Paths inside the user's home directory resolve and are returned."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    target = tmp_path / "statements" / "bank.csv"
    target.parent.mkdir(parents=True)
    target.touch()

    assert _validate_file_path(str(target)) == target


def test_path_outside_home_raises_user_error(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Absolute paths outside the home directory are rejected."""
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")

    with pytest.raises(UserError) as excinfo:
        _validate_file_path("/etc/passwd")

    assert excinfo.value.code == "invalid_file_path"


def test_symlink_escaping_home_raises_user_error(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    """Symlinks inside home that resolve outside home are rejected."""
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "secret.csv"
    target.touch()
    link = home / "link.csv"
    link.symlink_to(target)

    monkeypatch.setattr(Path, "home", lambda: home)

    with pytest.raises(UserError) as excinfo:
        _validate_file_path(str(link))

    assert excinfo.value.code == "invalid_file_path"
