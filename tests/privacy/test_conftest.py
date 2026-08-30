"""Regression coverage for privacy test fixtures."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from moneybin.database import Database
from moneybin.secrets import SecretStore


@pytest.fixture()
def database_init_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[list[dict[str, bool | None]], None, None]:
    """Record Database constructor options used by the privacy fixture."""
    calls: list[dict[str, bool | None]] = []
    original_init = Database.__init__

    def record_init(
        self: Database,
        db_path: Path,
        *,
        read_only: bool,
        secret_store: SecretStore | None = None,
        no_auto_upgrade: bool | None = None,
        assume_initialized: bool = False,
    ) -> None:
        calls.append({
            "read_only": read_only,
            "no_auto_upgrade": no_auto_upgrade,
            "assume_initialized": assume_initialized,
        })
        original_init(
            self,
            db_path,
            read_only=read_only,
            secret_store=secret_store,
            no_auto_upgrade=no_auto_upgrade,
            assume_initialized=assume_initialized,
        )

    monkeypatch.setattr(Database, "__init__", record_init)
    yield calls


def test_populated_db_reopens_a_template_without_schema_initialization(
    database_init_calls: list[dict[str, bool | None]],
    populated_db: Database,
) -> None:
    """The per-test privacy DB must not rebuild its production schema."""
    assert database_init_calls[-1]["assume_initialized"] is True
    assert populated_db.conn.execute("SELECT 1").fetchone() == (1,)
