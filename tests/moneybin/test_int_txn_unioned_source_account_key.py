"""Integration test: source_account_key carries through prep.int_transactions__unioned.

B3: int_transactions__unioned must project source_account_key (the source-native account
identifier) so that B4 can re-key transaction_id off the immutable source identity.

Seeds a single OFX transaction row with a link in app.account_links and materializes the
view via sqlmesh; asserts source_account_key is present and holds the native key.
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
def test_int_transactions_unioned_carries_source_account_key(
    db: Database,
) -> None:
    """source_account_key propagates from stg_ofx__transactions through the union view."""
    native_key = "ofx-union-acct-001"
    canonical_id = "canonunion00001"

    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, source_file, extracted_at, loaded_at, import_id,
             source_type, source_origin)
        VALUES ('fitid-union-001', ?, 'DEBIT', CURRENT_TIMESTAMP,
                -25.00, '/tmp/test_union.ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                'imp-union-001', 'ofx', 'test_bank_union')
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-union-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="ofx",
        source_origin="test_bank_union",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.int_transactions__unioned
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.int_transactions__unioned"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )
