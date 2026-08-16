"""Stand-in databases for tests that mock the connection rather than open one.

A bare ``MagicMock`` answers a ``SELECT`` with another ``MagicMock``, which most
call sites ignore harmlessly — but a repository zips that row against its column
list and raises ``ValueError: zip() argument 2 is shorter than argument 1``. The
failure surfaces inside the repository, nowhere near the code under test, and
reads as a bug in the feature rather than in the fixture.

Every report surface reads ``app.profile_settings`` for the display currency to
default to (``multi-currency.md`` Requirement 9), so any test that mocks the
database for a report command needs an answer for that read.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def without_a_profile(db: MagicMock) -> MagicMock:
    """Answer ``db``'s ``app.profile_settings`` read with "no row", and return it.

    Configures a caller's own mock rather than building one, for the tests that
    need a ``MagicMock(spec=Database)`` — a spec'd mock still answers every read
    with a child mock, so it needs this exactly as much as a bare one does.
    """
    db.execute.return_value.fetchone.return_value = None
    return db


def no_profile_database() -> MagicMock:
    """A database context whose ``app.profile_settings`` row is absent.

    The honest stand-in for a user who has chosen no home currency: reports run
    unconverted, in whatever currencies their rows already carry.

    Returns the context manager, not the connection — patch it as
    ``patch("...get_database", return_value=no_profile_database())`` and reach
    the connection through ``.__enter__.return_value`` when a test needs it.
    """
    context = MagicMock()
    context.__enter__.return_value = without_a_profile(MagicMock())
    return context
