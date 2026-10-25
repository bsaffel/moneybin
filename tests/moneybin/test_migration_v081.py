"""V081: add recipient_kind, terms_version, approved_via to app.ai_consent_grants."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V081__add_consent_recipient_kind import migrate
from tests.moneybin.migration_helpers import column_exists, insert_rows, run_migration

_NEW = ("recipient_kind", "terms_version", "approved_via")


@pytest.fixture()
def pre_v081_db(db: Database) -> Database:
    """app.ai_consent_grants in its V022 shape, holding one backend grant."""
    db.execute("DROP TABLE app.ai_consent_grants")
    db.execute("""CREATE TABLE app.ai_consent_grants (
        grant_id VARCHAR PRIMARY KEY, feature_category VARCHAR NOT NULL,
        backend VARCHAR NOT NULL, consent_mode VARCHAR NOT NULL,
        granted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        revoked_at TIMESTAMPTZ, grant_prompt TEXT NOT NULL
    )""")
    insert_rows(
        db,
        "app",
        "ai_consent_grants",
        ("grant_id", "feature_category", "backend", "consent_mode", "grant_prompt"),
        [("g1", "smart-import-parsing", "anthropic", "persistent", "text")],
    )
    return db


def test_v081_adds_columns_and_backfills(pre_v081_db: Database) -> None:
    for column in _NEW:
        assert not column_exists(pre_v081_db, "app", "ai_consent_grants", column)
    run_migration(pre_v081_db, migrate)
    row = pre_v081_db.execute(
        "SELECT recipient_kind, terms_version, approved_via FROM app.ai_consent_grants"
    ).fetchone()
    assert row == ("ai_backend", None, None)


def test_v081_is_idempotent(pre_v081_db: Database) -> None:
    run_migration(pre_v081_db, migrate)
    run_migration(pre_v081_db, migrate)
    assert column_exists(pre_v081_db, "app", "ai_consent_grants", "recipient_kind")
