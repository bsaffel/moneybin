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

from datetime import date
from decimal import Decimal

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
    """Seed one source_native row in app.account_links."""
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
            ('canonical-pinned-01:source-002', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-statement-01:source-001', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-statement-01:source-002', ?, DATE '2024-01-15', -50.00,
             'Test purchase', ?, 'csv', ?, 'corrected-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
            canonical_id,
            source_file,
            source_origin,
            native_key,
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

    assert rows == [(canonical_id,), (canonical_id,)]
    raw_count = db.execute(
        "SELECT COUNT(*) FROM raw.tabular_transactions WHERE source_file = ?",
        [source_file],
    ).fetchone()
    assert raw_count == (4,)


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_idless_rows_with_different_amount_identity(
    db: Database,
) -> None:
    """Different parsed amount strings are distinct content-hash identities."""
    canonical_id = "canonical-amount-identity-01"
    native_key = "native-amount-identity-01"
    source_file = "amount-identity-statement.csv"
    source_origin = "amount-identity-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, amount, original_amount,
             description, source_file, source_type, source_origin, import_id,
             extracted_at, loaded_at)
        VALUES
            ('csv-legacy-amount-identity', ?, DATE '2024-01-15', 5.00, '5',
             'Same-day purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('csv-corrected-amount-identity', ?, DATE '2024-01-15', 5.00, '5.00',
             'Same-day purchase', ?, 'csv', ?, 'corrected-import',
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
        link_id="link-amount-identity-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-amount-identity-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_account_key, original_amount
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY source_account_key
        """,
        [source_file],
    ).fetchall()

    assert rows == [(canonical_id, "5"), (native_key, "5.00")]


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_distinct_source_transaction_ids(
    db: Database,
) -> None:
    """Matching content does not make distinct source transactions duplicates."""
    canonical_id = "canonical-source-id-01"
    native_key = "native-source-id-01"
    source_file = "source-id-statement.csv"
    source_origin = "source-id-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-source-id-01:legacy-001', ?, 'legacy-001',
             DATE '2024-01-15', -50.00, 'Test purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-source-id-01:later-002', ?, 'later-002',
             DATE '2024-01-15', -50.00, 'Test purchase', ?, 'csv', ?,
             'later-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
        link_id="link-source-id-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-source-id-native",
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

    assert rows == [(canonical_id,), (native_key,)]


@pytest.mark.slow
def test_stg_tabular_transactions_projects_corrected_stable_source_id_values(
    db: Database,
) -> None:
    """A corrected stable source ID keeps the legacy identity and new values.

    The production change this catches is projecting the legacy row's stale
    values after the corrected source ID has been retired.
    """
    canonical_id = "canonical-stable-source-id-01"
    native_key = "native-stable-source-id-01"
    source_file = "stable-source-id-statement.csv"
    source_origin = "stable-source-id-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-stable-source-id-01:source-001', ?, 'source-001',
             DATE '2024-01-15', -50.00, 'Original purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-stable-source-id-01:source-001', ?, 'source-001',
             DATE '2024-01-16', -55.00, 'Corrected purchase', ?, 'csv', ?,
             'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
        link_id="link-stable-source-id-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-stable-source-id-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT
            source_account_key,
            transaction_id,
            source_transaction_id,
            transaction_date,
            amount,
            description
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (
            canonical_id,
            "canonical-stable-source-id-01:source-001",
            "source-001",
            date(2024, 1, 16),
            Decimal("-55.00"),
            "Corrected purchase",
        )
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_legacy_identity_when_path_changes(
    db: Database,
) -> None:
    """A differently spelled path cannot rotate a retained legacy identity."""
    canonical_id = "canonical-path-reimport-01"
    native_key = "native-path-reimport-01"
    source_id = "source-001"
    original_path = "path-reimport-original.csv"
    later_path = "path-reimport-later.csv"
    source_origin = "path-reimport-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-path-reimport-01:source-001', ?, ?, DATE '2024-01-15',
             -50.00, 'Original purchase', ?, 'csv', ?, 'legacy-import',
             TIMESTAMP '2024-01-15 00:00:00', TIMESTAMP '2024-01-15 00:00:00'),
            ('native-path-reimport-01:source-001', ?, ?, DATE '2024-01-16',
             -55.00, 'Corrected original purchase', ?, 'csv', ?,
             'corrected-original-import', TIMESTAMP '2024-01-16 00:00:00',
             TIMESTAMP '2024-01-16 00:00:00'),
            ('native-path-reimport-01:source-001', ?, ?, DATE '2024-01-17',
             -60.00, 'Corrected later purchase', ?, 'csv', ?,
             'corrected-later-import', TIMESTAMP '2024-01-17 00:00:00',
             TIMESTAMP '2024-01-17 00:00:00')
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_id,
            original_path,
            source_origin,
            native_key,
            source_id,
            original_path,
            source_origin,
            native_key,
            source_id,
            later_path,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-path-reimport-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-path-reimport-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_file, source_account_key, transaction_id, description
        FROM prep.stg_tabular__transactions
        WHERE source_file IN (?, ?)
        ORDER BY source_file
        """,
        [later_path, original_path],
    ).fetchall()

    assert rows == [
        (
            later_path,
            native_key,
            "native-path-reimport-01:source-001",
            "Corrected later purchase",
        ),
        (
            original_path,
            canonical_id,
            "canonical-path-reimport-01:source-001",
            "Corrected original purchase",
        ),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_deduplicates_ordinary_reimports_across_paths(
    db: Database,
) -> None:
    """An ordinary stable-ID re-import keeps only its newest raw row."""
    transaction_id = "ordinary-path-reimport-001"
    account_id = "ordinary-path-account-001"
    earlier_path = "ordinary-path-earlier.csv"
    later_path = "ordinary-path-later.csv"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            (?, ?, 'source-001', DATE '2024-01-15', -50.00, 'Earlier purchase',
             ?, 'csv', 'ordinary-path-reimport-test', 'earlier-import',
             TIMESTAMP '2024-01-15 00:00:00', TIMESTAMP '2024-01-15 00:00:00'),
            (?, ?, 'source-001', DATE '2024-01-16', -55.00, 'Later purchase',
             ?, 'csv', 'ordinary-path-reimport-test', 'later-import',
             TIMESTAMP '2024-01-16 00:00:00', TIMESTAMP '2024-01-16 00:00:00')
        """,  # noqa: S608  # test fixture
        [
            transaction_id,
            account_id,
            earlier_path,
            transaction_id,
            account_id,
            later_path,
        ],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_file, description
        FROM prep.stg_tabular__transactions
        WHERE transaction_id = ?
        """,
        [transaction_id],
    ).fetchall()

    assert rows == [(later_path, "Later purchase")]


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_same_ids_from_distinct_source_origins(
    db: Database,
) -> None:
    """Source origins scope tabular IDs that otherwise collide."""
    transaction_id = "cross-origin-transaction-001"
    account_id = "cross-origin-account-001"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            (?, ?, 'source-001', DATE '2024-01-15', -50.00, 'First origin',
             'first-origin.csv', 'csv', 'first-origin', 'first-import',
             TIMESTAMP '2024-01-15 00:00:00', TIMESTAMP '2024-01-16 00:00:00'),
            (?, ?, 'source-001', DATE '2024-01-15', -50.00, 'Second origin',
             'second-origin.csv', 'csv', 'second-origin', 'second-import',
             TIMESTAMP '2024-01-15 00:00:00', TIMESTAMP '2024-01-16 00:00:00')
        """,  # noqa: S608  # test fixture
        [transaction_id, account_id, transaction_id, account_id],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_origin, description
        FROM prep.stg_tabular__transactions
        WHERE transaction_id = ?
        ORDER BY source_origin
        """,
        [transaction_id],
    ).fetchall()

    assert rows == [
        ("first-origin", "First origin"),
        ("second-origin", "Second origin"),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_breaks_same_load_timestamp_ties_by_extracted_at(
    db: Database,
) -> None:
    """A same-scope re-import keeps the row from the later extraction."""
    transaction_id = "same-load-timestamp-transaction-001"
    account_id = "same-load-timestamp-account-001"
    loaded_at = "2024-01-17 00:00:00"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            (?, ?, 'source-001', DATE '2024-01-15', -50.00, 'Earlier extraction',
             'earlier-extraction.csv', 'csv', 'same-load-timestamp', 'first-import',
             TIMESTAMP '2024-01-15 00:00:00', ?),
            (?, ?, 'source-001', DATE '2024-01-16', -55.00, 'Later extraction',
             'later-extraction.csv', 'csv', 'same-load-timestamp', 'second-import',
             TIMESTAMP '2024-01-16 00:00:00', ?)
        """,  # noqa: S608  # test fixture
        [transaction_id, account_id, loaded_at, transaction_id, account_id, loaded_at],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_file, description
        FROM prep.stg_tabular__transactions
        WHERE transaction_id = ?
        """,
        [transaction_id],
    ).fetchall()

    assert rows == [("later-extraction.csv", "Later extraction")]


@pytest.mark.slow
def test_stg_tabular_transactions_breaks_exact_timestamp_ties_by_source_file(
    db: Database,
) -> None:
    """An otherwise equal re-import has a stable source-file winner."""
    transaction_id = "exact-timestamp-transaction-001"
    account_id = "exact-timestamp-account-001"
    observed_at = "2024-01-17 00:00:00"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            (?, ?, 'source-001', DATE '2024-01-15', -50.00, 'Z file',
             'z-file.csv', 'csv', 'exact-timestamp', 'first-import', ?, ?),
            (?, ?, 'source-001', DATE '2024-01-16', -55.00, 'A file',
             'a-file.csv', 'csv', 'exact-timestamp', 'second-import', ?, ?)
        """,  # noqa: S608  # test fixture
        [
            transaction_id,
            account_id,
            observed_at,
            observed_at,
            transaction_id,
            account_id,
            observed_at,
            observed_at,
        ],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_file, description
        FROM prep.stg_tabular__transactions
        WHERE transaction_id = ?
        """,
        [transaction_id],
    ).fetchall()

    assert rows == [("a-file.csv", "A file")]


@pytest.mark.slow
def test_stg_tabular_transactions_pairs_duplicate_stable_source_ids_by_occurrence(
    db: Database,
) -> None:
    """A duplicate stable ID retires only one legacy/corrected pair.

    The production change this catches is joining every repeated legacy ID to the
    one corrected row, which projects corrected values under every legacy ID.
    """
    canonical_id = "canonical-duplicate-stable-id-01"
    native_key = "native-duplicate-stable-id-01"
    source_file = "duplicate-stable-id-statement.csv"
    source_origin = "duplicate-stable-id-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-duplicate-stable-id-01:legacy-001', ?, 'repeat-001',
             DATE '2024-01-15', -50.00, 'Original purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('canonical-duplicate-stable-id-01:legacy-002', ?, 'repeat-001',
             DATE '2024-01-16', -55.00, 'Second legacy purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-duplicate-stable-id-01:corrected-001', ?, 'repeat-001',
             DATE '2024-01-17', -60.00, 'Corrected purchase', ?, 'csv', ?,
             'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
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
        link_id="link-duplicate-stable-id-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-duplicate-stable-id-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT transaction_date, amount, description
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY transaction_date
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (date(2024, 1, 16), Decimal("-55.00"), "Second legacy purchase"),
        (date(2024, 1, 17), Decimal("-60.00"), "Corrected purchase"),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_matches_matcher_endpoint_grain(
    db: Database,
) -> None:
    """A decision protects every row governed by the matcher's node identity."""
    canonical_id = "canonical-origin-match-01"
    native_key = "native-origin-match-01"
    source_file = "origin-match-statement.csv"
    source_origin = "origin-match-current"
    other_origin = "origin-match-other"
    corrected_transaction_id = f"{native_key}:source-001"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-origin-match-01:source-001', ?, ?, DATE '2024-01-15',
             -50.00, 'Original purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            (?, ?, ?, DATE '2024-01-16', -55.00, 'Corrected purchase', ?,
             'csv', ?, 'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            corrected_transaction_id,
            source_file,
            source_origin,
            corrected_transaction_id,
            native_key,
            corrected_transaction_id,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-origin-match-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-origin-match-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b, account_id,
            confidence_score, match_signals, match_type, match_tier, match_status,
            match_reason, decided_by, decided_at
        ) VALUES ('origin-mismatch-pair', ?, 'csv', ?, 'other-source-001', 'csv',
                  ?, ?, 0.95, '{}', 'dedup', '3', 'accepted', NULL, 'user',
                  CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [corrected_transaction_id, other_origin, other_origin, canonical_id],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_account_key, transaction_id, description
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (
            canonical_id,
            "canonical-origin-match-01:source-001",
            "Original purchase",
        ),
        (
            native_key,
            corrected_transaction_id,
            "Corrected purchase",
        ),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_blank_id_match_endpoints(
    db: Database,
) -> None:
    """A match decision retains a content-paired corrected twin without a source ID."""
    canonical_id = "canonical-blank-match-01"
    native_key = "native-blank-match-01"
    source_file = "blank-match-statement.csv"
    source_origin = "blank-match-test"
    corrected_transaction_id = "native-blank-match-01:source-001"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-blank-match-01:source-001', ?, NULL, DATE '2024-01-15',
             -50.00, 'Matched purchase', ?, 'csv', ?, 'legacy-import',
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            (?, ?, NULL, DATE '2024-01-15', -50.00, 'Matched purchase', ?,
             'csv', ?, 'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            canonical_id,
            source_file,
            source_origin,
            corrected_transaction_id,
            native_key,
            source_file,
            source_origin,
        ],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-blank-match-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-blank-match-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b, account_id,
            confidence_score, match_signals, match_type, match_tier, match_status,
            match_reason, decided_by, decided_at
        ) VALUES ('blank-match-pair', ?, 'csv', ?, 'other-source-001', 'csv', ?,
                  ?, 0.95, '{}', 'dedup', '3', 'accepted', NULL, 'user',
                  CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            corrected_transaction_id,
            source_origin,
            source_origin,
            canonical_id,
        ],
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    retained = db.execute(
        """
        SELECT source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_account_key = ?
        """,
        [native_key],
    ).fetchall()

    assert retained == [(native_key,)]


@pytest.mark.slow
def test_stg_tabular_transactions_does_not_pair_blank_source_ids(
    db: Database,
) -> None:
    """Blank source IDs use content identity instead of a stable-ID pairing."""
    canonical_id = "canonical-blank-source-id-01"
    native_key = "native-blank-source-id-01"
    source_file = "blank-source-id-statement.csv"
    source_origin = "blank-source-id-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-blank-source-id-01:legacy-001', ?, ' ',
             DATE '2024-01-15', -50.00, 'Legacy purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-blank-source-id-01:corrected-001', ?, ' ',
             DATE '2024-01-16', -55.00, 'Corrected purchase', ?, 'csv', ?,
             'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
        link_id="link-blank-source-id-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-blank-source-id-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db.execute(
        """
        SELECT source_account_key, transaction_id
        FROM prep.stg_tabular__transactions
        WHERE source_file = ?
        ORDER BY transaction_id
        """,
        [source_file],
    ).fetchall()

    assert rows == [
        (canonical_id, "canonical-blank-source-id-01:legacy-001"),
        (native_key, "native-blank-source-id-01:corrected-001"),
    ]


@pytest.mark.slow
def test_stg_tabular_transactions_keeps_distinct_original_date_strings(
    db: Database,
) -> None:
    """Different raw date strings are distinct content-hash identities."""
    canonical_id = "canonical-raw-date-01"
    native_key = "native-raw-date-01"
    source_file = "raw-date-statement.csv"
    source_origin = "raw-date-test"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, transaction_date, original_date_str,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES
            ('canonical-raw-date-01:legacy-001', ?, DATE '2024-01-15',
             '2024-01-15', -50.00, 'Test purchase', ?, 'csv', ?,
             'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
            ('native-raw-date-01:later-002', ?, DATE '2024-01-15',
             '01/15/2024', -50.00, 'Test purchase', ?, 'csv', ?,
             'later-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
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
        link_id="link-raw-date-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )
    _insert_accepted_source_native(
        db,
        link_id="link-raw-date-native",
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

    assert rows == [(canonical_id,), (native_key,)]


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
@pytest.mark.parametrize(
    "proposal_status", ["tracking", "pending", "approved", "rejected", "superseded"]
)
def test_stg_tabular_transactions_keeps_curated_corrected_duplicates(
    db: Database,
    proposal_status: str,
) -> None:
    """An upgrade never hides a corrected duplicate with an app reference."""
    canonical_id = "canonical-upgrade-curation-01"
    native_key = "native-upgrade-curation-01"
    source_file = "upgrade-curation-statement.csv"
    source_origin = "upgrade-curation-test"
    source_ids = ["category", "note", "tag", "split", "decision", "alias", "proposal"]

    for source_id in source_ids:
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, source_transaction_id, transaction_date,
                 amount, description, source_file, source_type, source_origin,
                 import_id, extracted_at, loaded_at)
            VALUES (?, ?, ?, DATE '2024-01-15', -50.00, 'Test purchase', ?,
                    'csv', ?, 'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture
            [
                f"{native_key}:{source_id}",
                native_key,
                source_id,
                source_file,
                source_origin,
            ],
        )
    _insert_accepted_source_native(
        db,
        link_id="link-upgrade-curation-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    corrected_ids = dict(
        db.execute(
            """
            SELECT source_transaction_id, transaction_id
            FROM prep.int_transactions__matched
            WHERE account_id = ?
            """,
            [canonical_id],
        ).fetchall()
    )
    corrected_source_ids = {
        source_id: f"{native_key}:{source_id}" for source_id in source_ids
    }
    assert set(corrected_ids) == set(corrected_source_ids.values())
    db.execute(
        """
        INSERT INTO app.transaction_categories (transaction_id, category)
        VALUES (?, 'Food')
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["category"]]],
    )
    db.execute(
        """
        INSERT INTO app.transaction_notes (note_id, transaction_id, text, author)
        VALUES ('note-upgrade-curation', ?, 'Keep this note', 'test')
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["note"]]],
    )
    db.execute(
        """
        INSERT INTO app.transaction_tags (transaction_id, tag, applied_by)
        VALUES (?, 'keep', 'test')
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["tag"]]],
    )
    db.execute(
        """
        INSERT INTO app.transaction_splits
            (split_id, transaction_id, amount, created_by)
        VALUES ('split-upgrade-curation', ?, -50.00, 'test')
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["split"]]],
    )
    db.execute(
        """
        INSERT INTO app.categorization_decisions
            (decision_id, transaction_id, attempt_number, status, category_revision)
        VALUES ('decision-upgrade-curation', ?, 1, 'pending', 0)
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["decision"]]],
    )
    db.execute(
        """
        INSERT INTO app.transaction_id_aliases
            (old_transaction_id, new_transaction_id, created_at)
        VALUES ('superseded-upgrade-alias', ?, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [corrected_ids[corrected_source_ids["alias"]]],
    )
    db.execute(
        """
        INSERT INTO app.proposed_rules
            (proposed_rule_id, merchant_pattern, category, status, sample_txn_ids)
        VALUES ('proposal-upgrade-curation', 'Test purchase', 'Food', ?, ARRAY[?])
        """,  # noqa: S608  # test fixture
        [proposal_status, corrected_ids[corrected_source_ids["proposal"]]],
    )

    for source_id in source_ids:
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, source_transaction_id, transaction_date,
                 amount, description, source_file, source_type, source_origin,
                 import_id, extracted_at, loaded_at)
            VALUES (?, ?, ?, DATE '2024-01-15', -50.00, 'Test purchase', ?,
                    'csv', ?, 'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture
            [
                f"{canonical_id}:{source_id}",
                canonical_id,
                source_id,
                source_file,
                source_origin,
            ],
        )
    _insert_accepted_source_native(
        db,
        link_id="link-upgrade-curation-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    retained_ids = {
        row[0]
        for row in db.execute(
            """
            SELECT transaction_id
            FROM core.fct_transactions
            WHERE transaction_id IN (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                corrected_ids[corrected_source_ids[source_id]]
                for source_id in source_ids
            ],
        ).fetchall()
    }

    assert retained_ids == set(corrected_ids.values())
    legacy_rows = db.execute(
        """
        SELECT COUNT(*)
        FROM prep.stg_tabular__transactions
        WHERE source_account_key = ? AND source_file = ?
        """,
        [canonical_id, source_file],
    ).fetchone()

    assert legacy_rows == (len(source_ids),)


@pytest.mark.slow
@pytest.mark.parametrize("match_status", ["pending", "accepted", "rejected"])
def test_stg_tabular_transactions_keeps_corrected_transfer_endpoints(
    db: Database,
    match_status: str,
) -> None:
    """An upgrade keeps corrected twins used by a terminal transfer decision."""
    debit_account_id = "canonical-upgrade-transfer-debit"
    credit_account_id = "canonical-upgrade-transfer-credit"
    debit_native_key = "native-upgrade-transfer-debit"
    credit_native_key = "native-upgrade-transfer-credit"
    source_origin = "upgrade-transfer-test"
    transfer_legs = [
        (debit_account_id, debit_native_key, "transfer-debit", -50.00),
        (credit_account_id, credit_native_key, "transfer-credit", 50.00),
    ]

    for canonical_id, native_key, source_id, amount in transfer_legs:
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, source_transaction_id, transaction_date,
                 amount, description, source_file, source_type, source_origin,
                 import_id, extracted_at, loaded_at)
            VALUES (?, ?, ?, DATE '2024-01-15', ?, 'Transfer', ?, 'csv', ?,
                    'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture
            [
                f"{native_key}:{source_id}",
                native_key,
                source_id,
                amount,
                f"{native_key}.csv",
                source_origin,
            ],
        )
        _insert_accepted_source_native(
            db,
            link_id=f"link-upgrade-transfer-native-{source_id}",
            account_id=canonical_id,
            ref_value=native_key,
            source_type="csv",
            source_origin=source_origin,
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    corrected_ids = dict(
        db.execute(
            """
            SELECT source_transaction_id, transaction_id
            FROM prep.int_transactions__matched
            WHERE source_transaction_id IN (?, ?)
            """,
            [
                f"{debit_native_key}:transfer-debit",
                f"{credit_native_key}:transfer-credit",
            ],
        ).fetchall()
    )
    debit_source_id = f"{debit_native_key}:transfer-debit"
    credit_source_id = f"{credit_native_key}:transfer-credit"
    assert set(corrected_ids) == {debit_source_id, credit_source_id}

    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES ('upgrade-transfer-pair', ?, 'csv', ?, ?, 'csv', ?, ?, 0.95, '{}',
                  'transfer', '4', ?, ?, NULL, 'user', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            debit_source_id,
            source_origin,
            credit_source_id,
            source_origin,
            debit_account_id,
            credit_account_id,
            match_status,
        ],
    )

    for canonical_id, native_key, source_id, amount in transfer_legs:
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, source_transaction_id, transaction_date,
                 amount, description, source_file, source_type, source_origin,
                 import_id, extracted_at, loaded_at)
            VALUES (?, ?, ?, DATE '2024-01-15', ?, 'Transfer', ?, 'csv', ?,
                    'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,  # noqa: S608  # test fixture
            [
                f"{canonical_id}:{source_id}",
                canonical_id,
                source_id,
                amount,
                f"{native_key}.csv",
                source_origin,
            ],
        )
        _insert_accepted_source_native(
            db,
            link_id=f"link-upgrade-transfer-legacy-{source_id}",
            account_id=canonical_id,
            ref_value=canonical_id,
            source_type="csv",
            source_origin=source_origin,
        )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    if match_status == "accepted":
        transfer = db.execute(
            """
            SELECT debit_transaction_id, credit_transaction_id
            FROM core.bridge_transfers
            WHERE transfer_id = 'upgrade-transfer-pair'
            """
        ).fetchone()

        assert transfer == (
            corrected_ids[debit_source_id],
            corrected_ids[credit_source_id],
        )
    else:
        retained = {
            row[0]
            for row in db.execute(
                """
                SELECT source_account_key
                FROM prep.stg_tabular__transactions
                WHERE source_account_key IN (?, ?)
                """,
                [debit_native_key, credit_native_key],
            ).fetchall()
        }

        assert retained == {debit_native_key, credit_native_key}


@pytest.mark.slow
@pytest.mark.parametrize("match_status", ["pending", "accepted", "rejected"])
def test_stg_tabular_transactions_keeps_corrected_dedup_secondary(
    db: Database,
    match_status: str,
) -> None:
    """An upgrade keeps a corrected twin referenced by a terminal dedup decision."""
    canonical_id = "canonical-upgrade-dedup-01"
    native_key = "native-upgrade-dedup-01"
    source_id = "dedup-secondary"
    source_file = "upgrade-dedup-statement.csv"
    source_origin = "upgrade-dedup-test"
    corrected_source_id = f"{native_key}:{source_id}"
    legacy_source_id = f"{canonical_id}:{source_id}"

    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES (?, ?, ?, DATE '2024-01-15', -50.00, 'Test purchase', ?, 'csv', ?,
                'corrected-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [corrected_source_id, native_key, source_id, source_file, source_origin],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-upgrade-dedup-native",
        account_id=canonical_id,
        ref_value=native_key,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            match_status, match_reason, decided_by, decided_at
        ) VALUES ('upgrade-dedup-pair', ?, 'csv', ?, ?, 'csv', ?, ?, 0.95, '{}',
                  'dedup', '3', ?, NULL, 'user', CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [
            legacy_source_id,
            source_origin,
            corrected_source_id,
            source_origin,
            canonical_id,
            match_status,
        ],
    )
    db.execute(
        """
        INSERT INTO raw.tabular_transactions
            (transaction_id, account_id, source_transaction_id, transaction_date,
             amount, description, source_file, source_type, source_origin,
             import_id, extracted_at, loaded_at)
        VALUES (?, ?, ?, DATE '2024-01-15', -50.00, 'Test purchase', ?, 'csv', ?,
                'legacy-import', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test fixture
        [legacy_source_id, canonical_id, source_id, source_file, source_origin],
    )
    _insert_accepted_source_native(
        db,
        link_id="link-upgrade-dedup-legacy",
        account_id=canonical_id,
        ref_value=canonical_id,
        source_type="csv",
        source_origin=source_origin,
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    retained = db.execute(
        """
        SELECT source_account_key
        FROM prep.stg_tabular__transactions
        WHERE source_account_key = ? AND source_transaction_id = ?
        """,
        [native_key, source_id],
    ).fetchall()

    assert retained == [(native_key,)]


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
