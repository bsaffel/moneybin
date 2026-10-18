"""Strict UTC instants and explicit legacy local-time interpretation."""

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo

import pytest

from moneybin.temporal import (
    as_utc,
    calendar_bounds,
    format_utc,
    interpret_legacy_time,
    parse_utc,
)


def test_utc_conversion_preserves_instant() -> None:
    value = datetime(2026, 10, 15, 10, tzinfo=timezone(timedelta(hours=-4)))

    result = as_utc(value)

    assert result == datetime(2026, 10, 15, 14, tzinfo=UTC)
    assert result.tzinfo is UTC
    assert format_utc(value) == "2026-10-15T14:00:00.000000Z"


@pytest.mark.parametrize("convert", [as_utc, format_utc])
def test_naive_instants_are_rejected(
    convert: Callable[[datetime], datetime | str],
) -> None:
    with pytest.raises(ValueError):
        convert(datetime(2026, 10, 15, 10))


def test_timezone_without_an_offset_is_rejected() -> None:
    class UnknownOffset(tzinfo):
        def utcoffset(self, dt: datetime | None) -> None:
            return None

    with pytest.raises(ValueError):
        as_utc(datetime(2026, 10, 15, 10, tzinfo=UnknownOffset()))


def test_format_utc_preserves_microseconds() -> None:
    assert format_utc(datetime(2026, 10, 15, 14, 1, 2, 123456, tzinfo=UTC)) == (
        "2026-10-15T14:01:02.123456Z"
    )


@pytest.mark.parametrize(
    "value",
    [
        "2026-10-15T14:00:00.123456Z",
        "2026-10-15T10:00:00.123456-04:00",
        "2026-10-15T19:30:00.123456+05:30",
    ],
)
def test_parse_utc_normalizes_explicit_offsets(value: str) -> None:
    result = parse_utc(value)

    assert result == datetime(2026, 10, 15, 14, 0, 0, 123456, tzinfo=UTC)
    assert result.tzinfo is UTC


@pytest.mark.parametrize(
    "value", ["2026-10-15", "2026-10-15T10:00:00", "not-an-instant"]
)
def test_parse_utc_rejects_missing_or_invalid_offset(value: str) -> None:
    with pytest.raises(ValueError):
        parse_utc(value)


def test_legacy_fold_requires_evidence() -> None:
    zone = ZoneInfo("America/New_York")
    value = datetime(2026, 11, 1, 1, 30)

    with pytest.raises(ValueError):
        interpret_legacy_time(value, zone)
    with pytest.raises(ValueError):
        interpret_legacy_time(value.replace(fold=1), zone)

    first = interpret_legacy_time(value, zone, fold=0)
    second = interpret_legacy_time(value, zone, fold=1)

    assert first == datetime(2026, 11, 1, 5, 30, tzinfo=UTC)
    assert second == datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    assert first.tzinfo is UTC
    assert second.tzinfo is UTC
    assert second - first == timedelta(hours=1)


@pytest.mark.parametrize("fold", [None, 0, 1])
def test_legacy_gap_is_rejected(fold: int | None) -> None:
    with pytest.raises(ValueError):
        interpret_legacy_time(
            datetime(2026, 3, 8, 2, 30), ZoneInfo("America/New_York"), fold=fold
        )


@pytest.mark.parametrize("fold", [None, 0, 1])
def test_unambiguous_legacy_time_has_one_instant(fold: int | None) -> None:
    result = interpret_legacy_time(
        datetime(2026, 10, 15, 10), ZoneInfo("America/New_York"), fold=fold
    )

    assert result == datetime(2026, 10, 15, 14, tzinfo=UTC)
    assert result.tzinfo is UTC


def test_legacy_interpretation_rejects_aware_input() -> None:
    with pytest.raises(ValueError):
        interpret_legacy_time(
            datetime(2026, 10, 15, 10, tzinfo=UTC), ZoneInfo("America/New_York")
        )


@pytest.mark.parametrize("fold", [-1, 2])
def test_legacy_interpretation_rejects_invalid_fold(fold: int) -> None:
    with pytest.raises(ValueError):
        interpret_legacy_time(datetime(2026, 10, 15, 10), ZoneInfo("UTC"), fold=fold)


@pytest.mark.parametrize(
    ("start", "end", "expected_start", "expected_end", "hours"),
    [
        (
            date(2026, 3, 8),
            date(2026, 3, 9),
            datetime(2026, 3, 8, 5, tzinfo=UTC),
            datetime(2026, 3, 9, 4, tzinfo=UTC),
            23,
        ),
        (
            date(2026, 11, 1),
            date(2026, 11, 2),
            datetime(2026, 11, 1, 4, tzinfo=UTC),
            datetime(2026, 11, 2, 5, tzinfo=UTC),
            25,
        ),
    ],
)
def test_calendar_bounds_follow_dst(
    start: date,
    end: date,
    expected_start: datetime,
    expected_end: datetime,
    hours: int,
) -> None:
    zone = ZoneInfo("America/New_York")

    lower, upper = calendar_bounds(start, end, zone)

    assert (lower, upper) == (expected_start, expected_end)
    assert lower.tzinfo is UTC
    assert upper.tzinfo is UTC
    assert upper - lower == timedelta(hours=hours)
    assert lower.astimezone(zone).date() == start
    assert upper.astimezone(zone).date() == end


@pytest.mark.parametrize(
    ("start", "end", "zone"),
    [
        (date(2011, 12, 30), date(2011, 12, 31), "Pacific/Apia"),
        (date(2011, 12, 29), date(2011, 12, 30), "Pacific/Apia"),
        (date(2026, 11, 1), date(2026, 11, 2), "America/Havana"),
        (date(2026, 10, 31), date(2026, 11, 1), "America/Havana"),
    ],
)
def test_calendar_bounds_reject_missing_or_ambiguous_midnight(
    start: date, end: date, zone: str
) -> None:
    with pytest.raises(ValueError):
        calendar_bounds(start, end, ZoneInfo(zone))


def test_calendar_bounds_allow_an_empty_interval() -> None:
    expected = datetime(2026, 10, 15, 4, tzinfo=UTC)

    assert calendar_bounds(
        date(2026, 10, 15), date(2026, 10, 15), ZoneInfo("America/New_York")
    ) == (expected, expected)


def test_calendar_bounds_reject_reversed_dates() -> None:
    with pytest.raises(ValueError):
        calendar_bounds(date(2026, 10, 16), date(2026, 10, 15), ZoneInfo("UTC"))
