"""Tests for ConsentRepo — audited writes to app.ai_consent_grants.

Phase 1 covers the schema; Phase 2 adds the repository CRUD + audit tests.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.tables import AI_CONSENT_GRANTS


def test_consent_grants_table_exists(db: Database) -> None:
    cols = db.execute(
        "SELECT column_name FROM duckdb_columns() "
        "WHERE schema_name = ? AND table_name = ?",
        [AI_CONSENT_GRANTS.schema, AI_CONSENT_GRANTS.name],
    ).fetchall()
    names = {c[0] for c in cols}
    assert names == {
        "grant_id",
        "feature_category",
        "backend",
        "consent_mode",
        "granted_at",
        "revoked_at",
        "grant_prompt",
    }
