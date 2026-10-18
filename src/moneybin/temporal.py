"""Strict UTC instants and explicit migration of legacy local times."""

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo


def as_utc(value: datetime) -> datetime:
    """Normalize an aware instant to UTC."""
    if value.utcoffset() is None:
        raise ValueError("An instant requires an explicit UTC offset")
    return value.astimezone(UTC)


def format_utc(value: datetime) -> str:
    """Emit an aware instant with fixed microseconds and a UTC suffix."""
    return as_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str) -> datetime:
    """Parse an ISO instant with an explicit offset and normalize it to UTC."""
    return as_utc(datetime.fromisoformat(value))


def interpret_legacy_time(
    value: datetime, zone: ZoneInfo, *, fold: int | None = None
) -> datetime:
    """Interpret a naive legacy time only with unambiguous timezone evidence."""
    if value.utcoffset() is not None:
        raise ValueError("Legacy interpretation requires a naive local time")
    if fold not in (None, 0, 1):
        raise ValueError("Fold must be 0, 1 or None")

    candidates: dict[int, datetime] = {}
    for candidate_fold in (0, 1):
        candidate = value.replace(tzinfo=zone, fold=candidate_fold).astimezone(UTC)
        round_trip = candidate.astimezone(zone).replace(tzinfo=None)
        if round_trip == value.replace(tzinfo=None):
            candidates[candidate_fold] = candidate

    if not candidates:
        raise ValueError("Legacy local time does not exist in the supplied timezone")
    if len(set(candidates.values())) > 1 and fold is None:
        raise ValueError("Ambiguous legacy local time requires explicit fold evidence")
    if fold is not None:
        return candidates[fold]
    return next(iter(candidates.values()))


def calendar_bounds(
    start: date, end: date, zone: ZoneInfo
) -> tuple[datetime, datetime]:
    """Convert the local calendar interval [start, end) to strict UTC bounds."""
    if end < start:
        raise ValueError("Calendar interval end must not precede start")
    return (
        interpret_legacy_time(datetime.combine(start, time.min), zone),
        interpret_legacy_time(datetime.combine(end, time.min), zone),
    )
