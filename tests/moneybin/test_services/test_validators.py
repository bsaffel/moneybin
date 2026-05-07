"""Tests for service-layer slug + note-text validators."""

from __future__ import annotations

import pytest

from moneybin.services._validators import (
    NOTE_MAX_LEN,
    InvalidSlugError,
    validate_note_text,
    validate_slug,
)


@pytest.mark.parametrize(
    "good",
    [
        "tax",
        "tax:business",
        "tax:business-expense",
        "vacation:hawaii-2026",
        "recurring",
        "review-later",
        "a:b",
        "a_b",
    ],
)
def test_validate_slug_accepts(good: str) -> None:
    validate_slug(good)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "Tax",  # uppercase
        "tax space",
        "tax::business",  # double colon
        "a:b:c",  # only one optional namespace allowed
        ":foo",
        "foo:",
        "foo!",
    ],
)
def test_validate_slug_rejects(bad: str) -> None:
    with pytest.raises(InvalidSlugError):
        validate_slug(bad)


def test_validate_note_text_max_length() -> None:
    validate_note_text("x" * NOTE_MAX_LEN)
    with pytest.raises(ValueError):
        validate_note_text("x" * (NOTE_MAX_LEN + 1))


def test_validate_note_text_rejects_empty() -> None:
    with pytest.raises(ValueError):
        validate_note_text("")
