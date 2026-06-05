"""Tests for the PDF extraction confidence scorer."""

from __future__ import annotations

import pytest

from moneybin.extractors.pdf.confidence import is_high_confidence, score


def test_full_required_full_important_is_one() -> None:
    assert (
        score(
            required_filled=4, required_total=4, important_filled=3, important_total=3
        )
        == 1.0
    )


def test_partial_required_zeroes_important_does_not_compensate() -> None:
    # 50% required, 0% important: 0.7 * 0.5 + 0.3 * 0.0 = 0.35
    assert score(
        required_filled=2, required_total=4, important_filled=0, important_total=3
    ) == pytest.approx(0.35)  # type: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete


def test_threshold_at_boundary_is_high_confidence() -> None:
    assert is_high_confidence(0.7) is True


def test_just_below_threshold_is_not_high_confidence() -> None:
    assert is_high_confidence(0.69) is False


def test_zero_is_not_high_confidence() -> None:
    assert is_high_confidence(0.0) is False


def test_no_required_fields_treats_required_as_satisfied() -> None:
    # required_total=0 → req defaults to 1.0; 0.7 * 1.0 + 0.3 * 1.0 = 1.0
    assert score(
        required_filled=0, required_total=0, important_filled=0, important_total=0
    ) == pytest.approx(1.0)  # type: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete


def test_mid_range_score() -> None:
    # 100% required, 0% important: 0.7 * 1.0 + 0.3 * 0.0 = 0.7
    assert score(
        required_filled=3, required_total=3, important_filled=0, important_total=2
    ) == pytest.approx(0.7)  # type: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete


def test_partial_both_fields() -> None:
    # 75% required, 50% important: 0.7 * 0.75 + 0.3 * 0.5 = 0.525 + 0.15 = 0.675
    assert score(
        required_filled=3, required_total=4, important_filled=1, important_total=2
    ) == pytest.approx(0.675)  # type: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete
