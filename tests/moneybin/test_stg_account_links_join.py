"""Integration tests: staging account models translate source-native → canonical.

B1: stg_tabular__accounts, stg_ofx__accounts, stg_plaid__accounts each
LEFT JOIN app.account_links so that ``account_id`` is the canonical opaque id
and ``source_account_key`` holds the source-native identifier.

Seeding strategy: INSERT directly into raw.* + app.account_links (bypassing
AccountLinksRepo to avoid the audit-log pairing, which is not the subject of
these tests). Then materialize via sqlmesh and assert the projected columns.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_accepted_source_native(
    db: Database,
    *,
    link_id: str,
    account_id: str,
    ref_value: str,
    source_type: str,
    source_origin: str,
) -> None:
    """Seed one accepted source_native row in app.account_links."""
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, ?, ?, 'accepted', 'auto', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [link_id, account_id, ref_value, source_type, source_origin],
    )


@pytest.mark.slow
def test_stg_tabular_translates_to_canonical_account_id(db: Database) -> None:
    """stg_tabular__accounts: account_id becomes canonical; native key in source_account_key."""
    native_key = "native-tab-acct-001"
    canonical_id = "canontab000001"

    db.execute(
        """
        INSERT INTO raw.tabular_accounts
            (account_id, account_name, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES (?, 'Checking', '/tmp/test.csv', 'csv', 'test_bank_tab',
                'imp-tab-001', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-tab-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin="test_bank_tab",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_tabular__accounts
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_tabular__accounts"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_plaid_translates_to_canonical_account_id(db: Database) -> None:
    """stg_plaid__accounts: account_id becomes canonical; native key in source_account_key."""
    native_key = "plaid-acc-native-001"
    canonical_id = "canonplaid00001"

    db.execute(
        """
        INSERT INTO raw.plaid_accounts
            (account_id, account_type, institution_name, source_file,
             source_type, source_origin, extracted_at, loaded_at)
        VALUES (?, 'depository', 'Test Bank Plaid', 'sync_job_001',
                'plaid', 'plaid-item-origin-001', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-plaid-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="plaid",
        source_origin="plaid-item-origin-001",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_plaid__accounts
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_plaid__accounts"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_ofx_translates_to_canonical_account_id(db: Database) -> None:
    """stg_ofx__accounts: account_id becomes canonical; native key in source_account_key."""
    native_key = "ofx-acctid-00001"
    canonical_id = "canonofx0000001"

    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, account_type, institution_org, source_file,
             source_type, source_origin, extracted_at, loaded_at)
        VALUES (?, 'CHECKING', 'Test Bank OFX', '/tmp/test.ofx',
                'ofx', 'test_bank_ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="ofx",
        source_origin="test_bank_ofx",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_ofx__accounts
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_ofx__accounts"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )
