"""`_attach_encrypted` must never surface an exception carrying key material.

``build_attach_sql`` interpolates the plaintext encryption key into the ATTACH
statement (DuckDB cannot parameterize it). DuckDB echoes the statement text
back in ``ParserException`` messages — a ~72-character window centred on the
error position — so a syntax error positioned after the key puts a long
contiguous run of it into ``str(exc)``. A traceback is not a log record, so
``SanitizedLogFormatter`` never sees it: the message reaches terminal
scrollback and CI logs verbatim.

Runs, not just whole keys: DuckDB's excerpt truncates, so the realistic leak
is 60 of 64 hex characters rather than the full key.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import (
    DatabaseLockError,
    _attach_encrypted,  # pyright: ignore[reportPrivateUsage]
    build_attach_sql,
)

# Random-looking hex so no run of it can collide with the surrounding path.
_KEY = "3f9c1a7d4e2b86055c0d9f31ae74b2c8d61f0a9573e4bc28d05a6e1f7b3c94d2"
_DB_PATH = Path("/tmp/probe.duckdb")  # noqa: S108 — SQL string fixture, never opened
_MIN_RUN = 8


def _leaked_runs(text: str, key: str = _KEY, run: int = _MIN_RUN) -> list[str]:
    """Every ``run``-length slice of ``key`` that survives in ``text``."""
    return [
        key[i : i + run] for i in range(len(key) - run + 1) if key[i : i + run] in text
    ]


def _assert_no_key_material(exc: BaseException) -> None:
    assert _leaked_runs(str(exc)) == [], f"key material in str(): {str(exc)!r}"
    for arg in exc.args:
        assert _leaked_runs(str(arg)) == [], f"key material in args: {arg!r}"


def _make_mock_conn(exc: Exception) -> MagicMock:
    conn = MagicMock()
    conn.execute.side_effect = exc
    return conn


def _attach_sql(read_only: bool = False) -> str:
    return build_attach_sql(_DB_PATH, _KEY, read_only=read_only)


def test_attach_sql_actually_contains_the_key() -> None:
    """Pins the premise — if ATTACH stops carrying the key, these tests are moot."""
    assert _KEY in _attach_sql()


def test_parser_error_echoing_key_excerpt_is_scrubbed() -> None:
    """DuckDB's ``LINE 1: ...`` excerpt can centre on the key. Mask the run.

    Mirrors real DuckDB 1.5.4 output for a syntax error positioned after the
    ENCRYPTION_KEY literal: the echo is truncated, so 60 of the 64 key
    characters survive into the message.
    """
    leaked_run = _KEY[4:]
    msg = (
        'Parser Error: syntax error at or near "GARBAGE"\n\n'
        f"LINE 1: ...{leaked_run}') GARBAGE ;;\n"
        "                                    ^"
    )
    exc = duckdb.ParserException(msg)
    conn = _make_mock_conn(exc)

    with pytest.raises(duckdb.ParserException) as excinfo:
        _attach_encrypted(conn, _attach_sql(), _KEY)

    _assert_no_key_material(excinfo.value)


def test_full_key_echo_is_scrubbed_without_reclassifying() -> None:
    """A whole-key echo is masked, and the error keeps its original type."""
    exc = duckdb.IOException(f"IO Error: failed while running {_attach_sql()}")
    conn = _make_mock_conn(exc)

    with pytest.raises(duckdb.IOException) as excinfo:
        _attach_encrypted(conn, _attach_sql(), _KEY)

    _assert_no_key_material(excinfo.value)
    assert not isinstance(excinfo.value, DatabaseLockError)


def test_lock_error_scrubs_both_the_wrapper_and_its_cause() -> None:
    """The chained ``__cause__`` prints too — scrubbing only the wrapper leaks.

    ``raise DatabaseLockError(...) from e`` makes Python print "The above
    exception was the direct cause", followed by the original message. Both
    have to be clean.
    """
    msg = (
        'IO Error: Could not set lock on file "/tmp/probe.duckdb": Resource '
        f"temporarily unavailable, while running: {_attach_sql()}"
    )
    conn = _make_mock_conn(duckdb.IOException(msg))

    with pytest.raises(DatabaseLockError) as excinfo:
        _attach_encrypted(conn, _attach_sql(), _KEY)

    _assert_no_key_material(excinfo.value)
    cause = excinfo.value.__cause__
    assert cause is not None
    _assert_no_key_material(cause)


def test_real_duckdb_parser_error_does_not_surface_the_key() -> None:
    """End-to-end against real DuckDB, not a hand-written mock message.

    The mocked cases above encode what DuckDB 1.5.4 emits today; this one
    asks DuckDB itself. A grammar change that moves the echo window still
    has to clear the same bar.
    """
    conn = duckdb.connect()
    sql = f"{_attach_sql()} GARBAGE ;;"

    with pytest.raises(duckdb.Error) as excinfo:
        _attach_encrypted(conn, sql, _KEY)

    assert "Parser Error" in str(excinfo.value)
    _assert_no_key_material(excinfo.value)


def test_a_decoy_in_the_database_path_does_not_shadow_the_real_key() -> None:
    """The scrubbed secret must be the key itself, not one recovered from text.

    The path is interpolated ahead of the options, so anything recovered by
    scanning the statement can be shadowed by path text — and
    ``MONEYBIN_DATABASE__PATH`` is user-settable.
    """
    decoy = Path("/tmp/ENCRYPTION_KEY 'decoy'/probe.duckdb")  # noqa: S108 — SQL fixture
    conn = duckdb.connect()

    with pytest.raises(duckdb.Error) as excinfo:
        _attach_encrypted(conn, f"{build_attach_sql(decoy, _KEY)} GARBAGE ;;", _KEY)

    _assert_no_key_material(excinfo.value)


def test_error_without_key_material_is_left_untouched() -> None:
    """No key in the message means no rewriting — identity and args preserved.

    Guards against the scrubber churning ordinary diagnostics, which would
    also break the classification tests' ``is exc`` identity contract.
    """
    exc = duckdb.IOException("IO Error: No space left on device")
    conn = _make_mock_conn(exc)

    with pytest.raises(duckdb.IOException) as excinfo:
        _attach_encrypted(conn, _attach_sql(), _KEY)

    assert excinfo.value is exc
    assert excinfo.value.args == ("IO Error: No space left on device",)
