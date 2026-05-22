"""Tests for ConsentRepo — audited writes to app.ai_consent_grants.

Phase 1 covers the schema; Phase 2 adds the repository CRUD + audit tests.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.consent import ConsentMode
from moneybin.repositories.consent_repo import ConsentRepo
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


def test_grant_inserts_and_pairs_audit(db: Database) -> None:
    repo = ConsentRepo(db)
    grant, created = repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="Allow sharing transaction details with Anthropic?",
        actor="cli.privacy_grant",
    )
    assert created is True
    assert grant.feature_category == "mcp-data-sharing"
    assert grant.backend == "anthropic"
    assert grant.revoked_at is None

    audit = db.conn.execute(
        "SELECT action, target_table, target_id, actor "
        "FROM app.audit_log WHERE target_id = ?",
        [grant.grant_id],
    ).fetchone()
    assert audit == (
        "consent.grant",
        "ai_consent_grants",
        grant.grant_id,
        "cli.privacy_grant",
    )


def test_grant_is_idempotent_per_category_backend(db: Database) -> None:
    repo = ConsentRepo(db)
    first, first_created = repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli.privacy_grant",
    )
    second, second_created = repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli.privacy_grant",
    )
    assert first_created is True
    assert second_created is False
    assert second.grant_id == first.grant_id
    assert len(repo.list_active()) == 1
    # Idempotent grant emits no second audit row.
    audit_count = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'consent.grant'"
    ).fetchone()
    assert audit_count == (1,)


def test_revoke_sets_revoked_at_and_audits(db: Database) -> None:
    repo = ConsentRepo(db)
    repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli.privacy_grant",
    )
    count = repo.revoke(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        actor="cli.privacy_revoke",
    )
    assert count == 1
    assert repo.list_active() == []
    assert len(repo.list_all()) == 1  # revoked row retained
    audit = db.conn.execute(
        "SELECT action FROM app.audit_log WHERE action = 'consent.revoke'"
    ).fetchone()
    assert audit == ("consent.revoke",)


def test_revoke_missing_is_noop(db: Database) -> None:
    repo = ConsentRepo(db)
    assert (
        repo.revoke(
            feature_category="mcp-data-sharing", backend="anthropic", actor="cli"
        )
        == 0
    )


def test_revoke_all(db: Database) -> None:
    repo = ConsentRepo(db)
    repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli",
    )
    repo.grant(
        feature_category="ml-categorization",
        backend="openai",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli",
    )
    assert repo.revoke_all(actor="cli.privacy_revoke_all") == 2
    assert repo.list_active() == []


def test_revoke_all_audit_before_image_is_pre_update(db: Database) -> None:
    """revoke_all must capture the before-image BEFORE setting revoked_at.

    Guards the ordering bug: if `before` is fetched after the UPDATE, it
    already shows revoked_at set and the audit trail loses the active->revoked
    transition.
    """
    repo = ConsentRepo(db)
    repo.grant(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        grant_prompt="p",
        actor="cli",
    )
    repo.revoke_all(actor="cli.privacy_revoke_all")
    row = db.conn.execute(
        "SELECT before_value, after_value FROM app.audit_log "
        "WHERE action = 'consent.revoke'"
    ).fetchone()
    assert row is not None
    import json

    before = json.loads(row[0])
    after = json.loads(row[1])
    assert before["revoked_at"] is None  # before-image: still active
    assert after["revoked_at"] is not None  # after-image: revoked
