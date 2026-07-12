"""The extension seal: crypto in, remote filesystems out, and no way back.

Every MoneyBin connection loads (or inherits) the ``httpfs`` extension, because
since DuckDB 1.4.1 the OpenSSL crypto that ships inside it is the only supported
way to *write* an encrypted database. httpfs also carries the http/s3
filesystems — on the same handle an MCP agent runs SQL against. These tests pin
the boundary that keeps the crypto and revokes the filesystems: the remote
schemes are refused by DuckDB itself, and the configuration is locked so the
refusal cannot be lifted from inside a session.

Asserting on DuckDB's *refusal* keeps these deterministic and offline — nothing
here touches the network.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import (
    _DISABLED_FILESYSTEMS,  # pyright: ignore[reportPrivateUsage]  # security constant under test
    Database,
    DatabaseCryptoError,
    _load_crypto_extension,  # pyright: ignore[reportPrivateUsage]  # failure branch under test
)

# Every URL DuckDB would route through a filesystem httpfs registers.
# gcs://, gs:// and r2:// are all served by S3FileSystem; hf:// by
# HuggingFaceFileSystem — the third remote filesystem httpfs registers.
REMOTE_URLS = [
    "https://example.com/statements.csv",
    "http://example.com/statements.csv",
    "s3://bucket/statements.parquet",
    "gcs://bucket/statements.parquet",
    "gs://bucket/statements.parquet",
    "r2://bucket/statements.parquet",
    "hf://datasets/user/repo/data.csv",
]

# The ways a session could try to lift the seal. `SET GLOBAL` and `RESET` are
# separate code paths in DuckDB from a plain `SET` — all must be refused.
#
# The PRAGMA forms are not decoration: `sql_query`'s validator allows queries
# beginning with PRAGMA, so they are literally reachable by an MCP agent. And
# re-enabling extension autoinstall is the live exfiltration path — it lets a
# session load an extension whose filesystem is NOT in `_DISABLED_FILESYSTEMS`
# (azure) and then read `az://`.
UNDO_ATTEMPTS = [
    "SET disabled_filesystems=''",
    "RESET disabled_filesystems",
    "SET GLOBAL disabled_filesystems=''",
    "SET lock_configuration=false",
    "SET autoload_known_extensions=true",
    "SET autoinstall_known_extensions=true",
    "SET allow_community_extensions=true",
    "PRAGMA disabled_filesystems=''",
    "PRAGMA autoinstall_known_extensions=true",
    "PRAGMA autoload_known_extensions=true",
    "PRAGMA allow_community_extensions=true",
]


@pytest.fixture()
def write_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """A real encrypted read-write Database — the path `profile create` takes."""
    database = Database(
        tmp_path / "seal.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


@pytest.fixture()
def read_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """A real encrypted read-only Database — the handle MCP agents query."""
    db_path = tmp_path / "seal_ro.duckdb"
    builder = Database(
        db_path,
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    builder.execute("CREATE TABLE app.seal_probe (id INTEGER)")
    builder.execute("INSERT INTO app.seal_probe VALUES (1)")
    builder.close()

    database = Database(
        db_path,
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=True,
    )
    yield database
    database.close()


def test_encrypted_write_succeeds_under_seal(write_db: Database) -> None:
    """The seal must not cost us encrypted writes.

    This is the regression guard for the duckdb upper bound: an encrypted write
    needs the OpenSSL crypto module, so if the seal ever stops loading httpfs
    this fails with "read-only crypto module loaded" — the exact symptom that
    was previously misdiagnosed as "DuckDB 1.5.4 is broken".
    """
    write_db.execute("CREATE TABLE app.seal_probe (id INTEGER, note TEXT)")
    write_db.execute("INSERT INTO app.seal_probe VALUES (1, 'written under seal')")
    write_db.execute("CHECKPOINT")

    rows = write_db.execute("SELECT id, note FROM app.seal_probe").fetchall()
    assert rows == [(1, "written under seal")]


def test_disabled_filesystems_covers_every_registered_fs(write_db: Database) -> None:
    """The disable list must name every remote filesystem httpfs registers.

    `_DISABLED_FILESYSTEMS` is a hand-maintained constant; this is the tripwire
    that keeps it honest. `conn.list_filesystems()` returns exactly the
    extension-registered filesystems (the remote ones httpfs brings — HTTP, S3,
    HuggingFace today) and never the built-in LocalFileSystem the encrypted DB
    file needs. If a future DuckDB/httpfs registers a fourth remote filesystem,
    this set grows and the assertion fails in CI — a red build instead of a
    silently-open scheme reaching the network. This is why the seal can keep an
    explicit, reviewable constant rather than clever runtime logic that might
    accidentally disable local access.
    """
    registered = set(write_db.conn.list_filesystems())
    disabled = set(_DISABLED_FILESYSTEMS.split(","))

    # Guard the guard: if httpfs stopped registering filesystems the assertion
    # below would pass vacuously and hide a regression in the fixture or seal.
    assert registered, "httpfs registered no filesystems — seal/fixture changed"
    # The local filesystem must never appear here — disabling it would block the
    # encrypted DB file. It doesn't (list_filesystems omits built-ins), but pin it.
    assert "LocalFileSystem" not in registered

    missing = registered - disabled
    assert not missing, (
        f"httpfs registers filesystem(s) absent from _DISABLED_FILESYSTEMS: "
        f"{sorted(missing)}. Add each to the constant in database.py — an "
        f"undisabled remote filesystem is open network egress on the agent handle."
    )


def test_write_connection_has_crypto_extension_loaded(write_db: Database) -> None:
    """Httpfs IS loaded on a write connection — the source comment says so now.

    Pinned because it is the whole reason the filesystem disable below exists.
    If a future DuckDB restores built-in write crypto and this flips to False,
    the seal's `LOAD httpfs` (and this test) can be deleted.
    """
    loaded = write_db.execute(
        "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'httpfs'"
    ).fetchone()
    assert loaded is not None
    assert loaded[0] is True


@pytest.mark.parametrize("url", REMOTE_URLS)
def test_remote_filesystem_refused_on_write_connection(
    write_db: Database, url: str
) -> None:
    """DuckDB refuses every remote scheme before it opens a socket."""
    with pytest.raises(duckdb.PermissionException, match="disabled by configuration"):
        write_db.execute(f"SELECT * FROM read_csv('{url}')")  # noqa: S608 — fixed test URLs, not user input


@pytest.mark.parametrize("url", REMOTE_URLS)
def test_remote_filesystem_refused_on_read_only_connection(
    read_db: Database, url: str
) -> None:
    """The agent-facing handle is the one that matters most."""
    with pytest.raises(duckdb.PermissionException, match="disabled by configuration"):
        read_db.execute(f"SELECT * FROM read_csv('{url}')")  # noqa: S608 — fixed test URLs, not user input


@pytest.mark.parametrize("sql", UNDO_ATTEMPTS)
def test_agent_cannot_undo_the_seal(read_db: Database, sql: str) -> None:
    """`lock_configuration` is what makes the read handle a boundary, not advice.

    The read-only connection is the one that executes agent-supplied SQL
    (`sql_query` opens `get_database(read_only=True)`), so it is the one that
    must be un-unlockable.
    """
    with pytest.raises(
        duckdb.InvalidInputException, match="the configuration has been locked"
    ):
        read_db.execute(sql)


def test_loading_httpfs_does_not_restore_remote_access(read_db: Database) -> None:
    """The sharp edge: `LOAD` is NOT gated by `lock_configuration`.

    A session can still load httpfs — DuckDB permits it. What it cannot do is
    re-enable the filesystems, because `disabled_filesystems` is enforced at
    lookup time. So loading httpfs buys the agent nothing. If a future DuckDB
    evaluates the disable list only at registration time, this test fails and
    the seal needs rethinking.
    """
    read_db.execute("LOAD httpfs")

    with pytest.raises(duckdb.PermissionException, match="disabled by configuration"):
        read_db.execute("SELECT * FROM read_csv('https://example.com/x.csv')")


def test_write_connection_cannot_re_enable_a_disabled_filesystem(
    write_db: Database,
) -> None:
    """Write connections are unlocked, but the filesystem disable still holds.

    They must stay unlocked — DuckDB issues `SET
    current_transaction_invalidation_policy` for DDL-with-a-DEFAULT inside an
    explicit transaction, which is exactly what MigrationRunner does, and a
    locked configuration refuses it. That costs us nothing here: DuckDB treats
    `disabled_filesystems` as one-way regardless of the lock, so even an
    unlocked connection cannot hand back the remote filesystem.
    """
    with pytest.raises(duckdb.InvalidInputException, match="cannot be re-enabled"):
        write_db.execute("SET disabled_filesystems=''")


def test_write_connection_runs_ddl_with_default_inside_a_transaction(
    write_db: Database,
) -> None:
    """Regression guard for the seal-vs-migrations conflict.

    `ALTER TABLE ... ADD COLUMN ... DEFAULT` inside `BEGIN`/`COMMIT` makes DuckDB
    set `current_transaction_invalidation_policy`. If someone later adds
    `lock_configuration=true` to the write path, every migration that backfills
    a column dies here with "the configuration has been locked" — which is how
    this was caught.
    """
    write_db.execute("CREATE TABLE app.seal_ddl (id INTEGER)")
    write_db.execute("INSERT INTO app.seal_ddl VALUES (1)")

    write_db.execute("BEGIN TRANSACTION")
    write_db.execute(
        "ALTER TABLE app.seal_ddl "
        "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    )
    write_db.execute("COMMIT")

    rows = write_db.execute(
        "SELECT count(*) FROM app.seal_ddl WHERE updated_at IS NOT NULL"
    ).fetchone()
    assert rows is not None
    assert rows[0] == 1


def test_load_crypto_extension_failure_raises_database_crypto_error() -> None:
    """A failed INSTALL/LOAD httpfs is wrapped, not leaked raw.

    The documented failure path — a machine with no cached httpfs and no network
    on its first encrypted write — must surface as ``DatabaseCryptoError`` (which
    ``classify_user_error`` maps to a clean CLI/MCP message), and the half-open
    connection must be closed so it can't leak.
    """
    conn = MagicMock()
    conn.execute.side_effect = duckdb.Error("HTTP 404 fetching httpfs")

    with pytest.raises(DatabaseCryptoError, match="httpfs"):
        _load_crypto_extension(conn)

    conn.close.assert_called_once()
