"""Integration tests: staging transaction + balance models translate source-native → canonical.

B2: stg_ofx__transactions, stg_tabular__transactions, stg_plaid__transactions,
stg_manual__transactions, stg_ofx__balances each LEFT JOIN app.account_links so
that ``account_id`` is the canonical opaque id and ``source_account_key`` holds
the source-native identifier.

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
    status: str = "accepted",
) -> None:
    """Seed one accepted source_native row in app.account_links."""
    db.execute(
        """
        INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type,
             source_origin, status, decided_by, decided_at)
        VALUES (?, ?, 'source_native', ?, ?, ?, ?, 'auto', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [link_id, account_id, ref_value, source_type, source_origin, status],
    )


@pytest.mark.slow
def test_stg_ofx_transactions_translates_to_canonical_account_id(
    db: Database,
) -> None:
    """stg_ofx__transactions: account_id becomes canonical; native key in source_account_key."""
    native_key = "ofx-txn-acct-001"
    canonical_id = "canonofxtxn0001"

    db.execute(
        """
        INSERT INTO raw.ofx_transactions
            (source_transaction_id, account_id, transaction_type, date_posted,
             amount, source_file, extracted_at, loaded_at, import_id,
             source_type, source_origin)
        VALUES ('fitid-001', ?, 'DEBIT', CURRENT_TIMESTAMP,
                -10.00, '/tmp/test.ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                'imp-ofx-txn-001', 'ofx', 'test_bank_ofx_txn')
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-txn-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="ofx",
        source_origin="test_bank_ofx_txn",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_ofx__transactions
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_ofx__transactions"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_tabular_transactions_translates_to_canonical_account_id(
    db: Database,
) -> None:
    """stg_tabular__transactions: account_id becomes canonical; native key in source_account_key."""
    native_key = "tab-txn-acct-001"
    canonical_id = "canontabtxn0001"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at, loaded_at)
        VALUES ('tab-txn-id-001', ?, '2024-01-15', -50.00, 'Test purchase',
                '/tmp/test.csv', 'csv', 'test_bank_tab_txn',
                'imp-tab-txn-001', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-tab-txn-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin="test_bank_tab_txn",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_tabular__transactions"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_pre_fix_pinned_row_authoritative(
    db: Database,
) -> None:
    """A corrected copy must not double-count a legacy pinned transaction.

    The production change this catches is removing the legacy-pinned suppression:
    both raw rows would then reach staging under different source-account keys.
    """
    canonical_id = "canonical-pinned-01"
    native_key = "native-statement-01"
    source_file = "pinned-statement.csv"
    source_origin = "legacy-pinned-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-pinned-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-statement-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
            native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-pinned-statement-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-pinned-statement-01",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY source_account_key
        """,
        [source_file],
    ).fetchall()

    assert rows == [(canonical_id,)]
    raw_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_file = ?",
        [source_file],
    ).fetchone()
    assert raw_count == (2,)


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_active_legacy_row_when_twin_is_deleted(
    db: Database,
) -> None:
    """A deleted corrected reimport cannot hide its active legacy twin."""
    canonical_id = "canonical-deleted-01"
    native_key = "native-deleted-01"
    source_file = "deleted-twin-statement.csv"
    source_origin = "deleted-twin-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at,
             loaded_at, deleted_from_source_at)
        VALUES
            ('canonical-deleted-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL),
            ('native-deleted-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
            native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-deleted-twin-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-deleted-twin-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        """,
        [source_file],
    ).fetchall()

    assert rows == [(canonical_id,)]


@pytest.mark.slow
def test_stg_tabular_transactions_preserves_legacy_transaction_curation(
    db: Database,
) -> None:
    """Curation stays attached to the retained legacy transaction id."""
    canonical_id = "canonical-curation-01"
    native_key = "native-curation-01"
    source_file = "curated-statement.csv"
    source_origin = "legacy-curation-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-curation-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-curation-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
            native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-curated-statement-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-curated-statement-01",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    legacy_transaction = db.execute(
        """
        SELECT transaction_id
        FROM prep.int_transactions__matched
        WHERE source_transaction_id = ?
        """,
        [f"{canonical_id}:source-001"],
    ).fetchone()
    assert legacy_transaction is not None
    db.execute(
        """
        INSERT INTO app.transaction_notes (note_id, transaction_id, text, author)
        VALUES ('note-legacy-curation', ?, 'Keep this note', 'test')
        """,  # noqa: S608  # test fixture
        [legacy_transaction[0]],
    )

    curated_rows = db.execute(
        """
        SELECT t.transaction_id, n.text
        FROM core.fct_transactions AS t
        JOIN app.transaction_notes AS n ON n.transaction_id = t.transaction_id
        WHERE t.account_id = ?
        """,
        [canonical_id],
    ).fetchall()

    assert curated_rows == [(legacy_transaction[0], "Keep this note")]


@pytest.mark.slow
def test_stg_tabular_transactions_does_not_cross_suppress_a_reused_path(
    db: Database,
) -> None:
    """A legacy pin cannot hide a later statement for another account."""
    first_account = "canonical-first-01"
    second_account = "canonical-second-01"
    second_native_key = "native-second-01"
    source_file = "reused-statement.csv"
    source_origin = "reused-path-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-first-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-second-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'second-account-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            first_account,
            source_file,
            source_origin,
            second_native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-reused-path-first",
        account_id=first_account,
        ref_value=first_account,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-reused-path-second",
        account_id=second_account,
        ref_value=second_native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY source_account_key
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (first_account, first_account),
        (second_account, second_native_key),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_preserves_same_account_path_reuse_after_a_merge(
    db: Database,
) -> None:
    """A reversed self-map cannot prove that a later path reuse is a duplicate."""
    losing_account = "canonical-loser-01"
    winning_account = "canonical-winner-01"
    native_key = "native-merged-01"
    source_file = "merged-statement.csv"
    source_origin = "merged-legacy-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, description,
             source_file, source_type, source_origin, import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-loser-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-merged-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            losing_account,
            source_file,
            source_origin,
            native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-merged-legacy-reversed",
        account_id=losing_account,
        ref_value=losing_account,
        source_type="csv",
        source_origin=source_origin,
        status="reversed",
    )
    _insert_accepted_source_native(
        db,
        link_id="link-merged-legacy-current",
        account_id=winning_account,
        ref_value=losing_account,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-merged-native-current",
        account_id=winning_account,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY source_account_key
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (winning_account, losing_account),
        (winning_account, native_key),
    ]


@pytest.mark.slow
def test_stg_plaid_transactions_translates_to_canonical_account_id(
    db: Database,
) -> None:
    """stg_plaid__transactions: account_id becomes canonical; native key in source_account_key."""
    native_key = "plaid-txn-acct-001"
    canonical_id = "canonplaidtxn01"

    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, transaction_date, amount, source_file,
             source_type, source_origin, extracted_at, loaded_at)
        VALUES ('plaid-txn-id-001', ?, '2024-01-15', 25.00, 'sync_job_001',
                'plaid', 'plaid-item-txn-001', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-plaid-txn-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="plaid",
        source_origin="plaid-item-txn-001",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_plaid__transactions
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_plaid__transactions"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_manual_transactions_translates_to_canonical_account_id(
    db: Database,
) -> None:
    """stg_manual__transactions: account_id becomes canonical; native key in source_account_key."""
    native_key = "manual-acct-00001"
    canonical_id = "canonmanual00001"

    db.execute(
        """
        INSERT INTO raw.manual_transactions
            (source_transaction_id, account_id, transaction_date, amount,
             description, import_id, created_by)
        VALUES ('manual_abc123def456', ?, '2024-01-15', -30.00,
                'Manual test entry', 'imp-manual-001', 'cli')
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-manual-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="manual",
        source_origin="user",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_manual__transactions
        WHERE source_account_key = ?
        ORDER BY created_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_manual__transactions"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )


@pytest.mark.slow
def test_stg_ofx_balances_translates_to_canonical_account_id(
    db: Database,
) -> None:
    """stg_ofx__balances: account_id becomes canonical; native key in source_account_key."""
    native_key = "ofx-bal-acct-001"
    canonical_id = "canonofxbal0001"

    db.execute(
        """
        INSERT INTO raw.ofx_balances
            (account_id, statement_end_date, ledger_balance, ledger_balance_date,
             source_file, extracted_at, loaded_at, import_id, source_type, source_origin)
        VALUES (?, '2024-01-31', 1000.00, '2024-01-31',
                '/tmp/test.ofx', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP,
                'imp-bal-001', 'ofx', 'test_bank_ofx_bal')
        """,  # noqa: S608  # test fixture
        [native_key],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-ofx-bal-001",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="ofx",
        source_origin="test_bank_ofx_bal",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db.execute(
        """
        SELECT account_id, source_account_key
        FROM prep.stg_ofx__balances
        WHERE source_account_key = ?
        ORDER BY loaded_at DESC LIMIT 1
        """,
        [native_key],
    ).fetchone()

    assert row is not None, "No row found in prep.stg_ofx__balances"
    assert row[0] == canonical_id, (
        f"account_id: expected {canonical_id!r}, got {row[0]!r}"
    )
    assert row[1] == native_key, (
        f"source_account_key: expected {native_key!r}, got {row[1]!r}"
    )
