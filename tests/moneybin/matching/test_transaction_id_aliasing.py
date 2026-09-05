"""Curation survives a canonical-transaction-id re-key (ADR-015 alias forwarding).

``core.fct_transactions.transaction_id`` is content-derived from the dedup
group's *anchor* member, so it changes whenever the anchor changes: a
more-stable source joining the group, or Plaid re-minting an id as a pending
transaction posts. Both are exercised here through the production entry points
(``MatchingService.set_status`` and ``MatchingService.run``), not by calling the
forwarding helper directly — the defect in #406 was precisely that nothing
production-side ever called it.

The harness runs the real ``prep.int_transactions__matched`` model SQL against a
stub ``prep.int_transactions__unioned`` table, the same approach
``test_int_matched_model.py`` uses, so the ids under test are the ones the
shipped model derives rather than ones the test invents.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import SQLMESH_ROOT, Database
from moneybin.errors import UserError
from moneybin.matching.persistence import get_match_decision
from moneybin.metrics.registry import TRANSACTION_CURATION_FORWARDED_TOTAL
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.repositories.transaction_categories_repo import TransactionCategoriesRepo
from moneybin.repositories.transaction_notes_repo import TransactionNotesRepo
from moneybin.repositories.transaction_splits_repo import TransactionSplitsRepo
from moneybin.repositories.transaction_tags_repo import TransactionTagsRepo
from moneybin.services.matching_service import MatchingService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService

_MODEL_FILE = SQLMESH_ROOT / "models" / "prep" / "int_transactions__matched.sql"

_ACCOUNT = "acct_canonical"
_ACCOUNT_KEY = "src_acct_0001"
_ORIGIN = "testbank"

_UNIONED_STUB_DDL = """\
CREATE TABLE IF NOT EXISTS prep.int_transactions__unioned (
    source_transaction_id VARCHAR NOT NULL,
    account_id            VARCHAR NOT NULL,
    source_account_key    VARCHAR,
    transaction_date      DATE,
    authorized_date       DATE,
    amount                DECIMAL(18, 2),
    description           VARCHAR,
    original_description  VARCHAR,
    merchant_name         VARCHAR,
    merchant_entity_id    VARCHAR,
    memo                  VARCHAR,
    category              VARCHAR,
    subcategory           VARCHAR,
    category_detailed     VARCHAR,
    plaid_category        VARCHAR,
    category_confidence   VARCHAR,
    payment_channel       VARCHAR,
    transaction_type      VARCHAR,
    check_number          VARCHAR,
    is_pending            BOOLEAN,
    pending_transaction_id VARCHAR,
    location_address      VARCHAR,
    location_city         VARCHAR,
    location_region       VARCHAR,
    location_postal_code  VARCHAR,
    location_country      VARCHAR,
    location_latitude     DOUBLE,
    location_longitude    DOUBLE,
    currency_code         VARCHAR,
    source_type           VARCHAR,
    source_origin         VARCHAR,
    source_file           VARCHAR,
    source_extracted_at   TIMESTAMP,
    loaded_at             TIMESTAMP
);
"""


def _matched_view_sql() -> str:
    """Turn the shipped model file into a CREATE VIEW over the stub table."""
    raw = _MODEL_FILE.read_text()
    body = re.sub(r"^MODEL\s*\(.*?\);\s*", "", raw, flags=re.DOTALL).strip()
    return f"CREATE OR REPLACE VIEW prep.int_transactions__matched AS\n{body}"


def _canonical_id(source_type: str, source_transaction_id: str) -> str:
    """Derive the ADR-015 source-identity hash from first principles."""
    raw = f"{source_type}|{_ORIGIN}|{_ACCOUNT_KEY}|{source_transaction_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@pytest.fixture()
def matched_db(db: Database) -> Generator[Database, None, None]:
    """A test database carrying the real matched model over a stub unioned table."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(_UNIONED_STUB_DDL)
    db.execute(_matched_view_sql())
    yield db


def _insert_source_row(
    db: Database,
    *,
    source_transaction_id: str,
    source_type: str,
    source_file: str,
    pending_transaction_id: str | None = None,
    description: str = "Coffee Roasters",
    amount: str = "-12.34",
    txn_date: date = date(2024, 3, 15),
) -> None:
    db.execute(
        """
        INSERT INTO prep.int_transactions__unioned (
            source_transaction_id, account_id, source_account_key,
            transaction_date, amount, description, currency_code,
            source_type, source_origin, source_file, is_pending,
            pending_transaction_id, loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'USD', ?, ?, ?, FALSE, ?, CURRENT_TIMESTAMP)
        """,  # noqa: S608  # test input, not executing user SQL
        [
            source_transaction_id,
            _ACCOUNT,
            _ACCOUNT_KEY,
            txn_date,
            amount,
            description,
            source_type,
            _ORIGIN,
            source_file,
            pending_transaction_id,
        ],
    )


def _categorize(
    db: Database, transaction_id: str, *, category: str, by: str = "user"
) -> None:
    """Categorize one transaction, naming the authority that wrote it."""
    TransactionCategoriesRepo(db).set(
        transaction_id,
        category=category,
        subcategory="Coffee",
        category_id=None,
        categorized_by=by,
        actor="cli",
    )


def _curate(db: Database, transaction_id: str) -> None:
    """Attach one of every kind of user curation to ``transaction_id``."""
    _categorize(db, transaction_id, category="Food & Drink")
    TransactionNotesRepo(db).add(
        transaction_id=transaction_id,
        note_id="note00000001",
        text="reimbursable",
        actor="cli",
    )
    TransactionTagsRepo(db).add(transaction_id=transaction_id, tag="work", actor="cli")
    TransactionSplitsRepo(db).insert(
        split_id="split0000001",
        transaction_id=transaction_id,
        amount=Decimal("-12.34"),
        category="Food & Drink",
        subcategory="Coffee",
        category_id=None,
        note=None,
        ord=0,
        actor="cli",
    )


_CURATION_TABLES = (
    "transaction_categories",
    "transaction_notes",
    "transaction_tags",
    "transaction_splits",
)


def _curation_ids(db: Database) -> dict[str, list[str]]:
    """Every transaction id each curation table currently points at."""
    return {
        table: [
            str(row[0])
            for row in db.execute(
                f"SELECT transaction_id FROM app.{table} ORDER BY transaction_id"  # noqa: S608  # code-supplied table names
            ).fetchall()
        ]
        for table in _CURATION_TABLES
    }


def _categories(db: Database) -> dict[str, tuple[str, str]]:
    """``transaction_id -> (category, categorized_by)`` for every categorized row."""
    return {
        str(txn): (str(category), str(by))
        for txn, category, by in db.execute(
            "SELECT transaction_id, category, categorized_by "
            "FROM app.transaction_categories"
        ).fetchall()
    }


def _tags(db: Database) -> set[tuple[str, str]]:
    """Every ``(transaction_id, tag)`` pair currently stored."""
    return {
        (str(txn), str(tag))
        for txn, tag in db.execute(
            "SELECT transaction_id, tag FROM app.transaction_tags"
        ).fetchall()
    }


def _aliases(db: Database) -> dict[str, str]:
    return {
        str(old): str(new)
        for old, new in db.execute(
            "SELECT old_transaction_id, new_transaction_id "
            "FROM app.transaction_id_aliases"
        ).fetchall()
    }


def _forwarded_count() -> float:
    """Read the committed curation-forwarding counter."""
    return TRANSACTION_CURATION_FORWARDED_TOTAL._value.get()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # no public counter getter


def _canonical_ids(db: Database) -> set[str]:
    return {
        str(row[0])
        for row in db.execute(
            "SELECT DISTINCT transaction_id FROM prep.int_transactions__matched"
        ).fetchall()
    }


def _seed_pending_dedup(
    db: Database,
    *,
    match_id: str,
    side_a: tuple[str, str] = ("tabular", "csv_1234"),
    side_b: tuple[str, str] = ("ofx", "ofx_5678"),
) -> None:
    MatchDecisionsRepo(db).insert(
        match_id=match_id,
        source_transaction_id_a=side_a[1],
        source_type_a=side_a[0],
        source_origin_a=_ORIGIN,
        source_transaction_id_b=side_b[1],
        source_type_b=side_b[0],
        source_origin_b=_ORIGIN,
        account_id=_ACCOUNT,
        confidence_score=0.91,
        match_signals={},
        match_tier="3",
        match_status="pending",
        decided_by="auto",
        actor="system",
    )


class TestMergeRekeyForwardsCuration:
    """A confirmed dedup merge that re-anchors the group must not orphan curation."""

    @pytest.mark.unit
    def test_confirming_a_merge_writes_an_alias_and_moves_curation(
        self, matched_db: Database
    ) -> None:
        """The tabular id is superseded by the OFX anchor; curation follows it."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        assert _canonical_ids(matched_db) == {old_id}
        _curate(matched_db, old_id)

        # A more-stable source backfills the same transaction and the matcher
        # proposes the pair; the user confirms it.
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(matched_db, match_id="match0000001")
        MatchingService(matched_db).set_status(
            "match0000001", status="accepted", actor="cli"
        )

        new_id = _canonical_id("ofx", "ofx_5678")
        assert _canonical_ids(matched_db) == {new_id}, "one canonical row survives"
        assert _aliases(matched_db) == {old_id: new_id}
        assert _curation_ids(matched_db) == {
            "transaction_categories": [new_id],
            "transaction_notes": [new_id],
            "transaction_tags": [new_id],
            "transaction_splits": [new_id],
        }

    @pytest.mark.unit
    def test_lower_stability_twin_joining_leaves_the_id_and_curation_alone(
        self, matched_db: Database
    ) -> None:
        """The OFX anchor keeps the id, so nothing is aliased and nothing moves."""
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        anchor_id = _canonical_id("ofx", "ofx_5678")
        _curate(matched_db, anchor_id)

        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _seed_pending_dedup(matched_db, match_id="match0000002")
        MatchingService(matched_db).set_status(
            "match0000002", status="accepted", actor="cli"
        )

        assert _canonical_ids(matched_db) == {anchor_id}
        # The tabular member's own hash is superseded even though the anchor
        # did not move — a consumer holding it must still resolve.
        assert _aliases(matched_db) == {_canonical_id("tabular", "csv_1234"): anchor_id}
        assert _curation_ids(matched_db)["transaction_categories"] == [anchor_id]

    @pytest.mark.unit
    def test_successive_rekeys_chain_and_curation_lands_on_the_live_id(
        self, matched_db: Database
    ) -> None:
        """Two re-keys in a row: the map chains, the curation ends up on the anchor.

        The alias map is append-only, so the second re-key cannot rewrite the
        first alias's target. It appends ``mid -> new`` instead, leaving
        ``old -> mid -> new`` resolvable as a chain — the case ADR-015 leaves
        open as "alias-chain-collapse". The curation is not chained: it is moved
        each time, so it always sits on the id ``core`` currently carries.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        csv_id = _canonical_id("tabular", "csv_1234")
        _curate(matched_db, csv_id)

        # A manual entry (minted id, rank 1) outranks the content hash (rank 2).
        _insert_source_row(
            matched_db,
            source_transaction_id="manual_2345",
            source_type="manual",
            source_file="manual",
        )
        _seed_pending_dedup(
            matched_db,
            match_id="match0000007",
            side_a=("tabular", "csv_1234"),
            side_b=("manual", "manual_2345"),
        )
        MatchingService(matched_db).set_status(
            "match0000007", status="accepted", actor="cli"
        )
        manual_id = _canonical_id("manual", "manual_2345")
        assert _canonical_ids(matched_db) == {manual_id}
        assert _aliases(matched_db) == {csv_id: manual_id}

        # Then the native-id source backfills the same transaction (rank 0).
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(
            matched_db,
            match_id="match0000008",
            side_a=("manual", "manual_2345"),
            side_b=("ofx", "ofx_5678"),
        )
        MatchingService(matched_db).set_status(
            "match0000008", status="accepted", actor="cli"
        )

        ofx_id = _canonical_id("ofx", "ofx_5678")
        assert _canonical_ids(matched_db) == {ofx_id}
        assert _aliases(matched_db) == {csv_id: manual_id, manual_id: ofx_id}
        assert _curation_ids(matched_db) == {
            "transaction_categories": [ofx_id],
            "transaction_notes": [ofx_id],
            "transaction_tags": [ofx_id],
            "transaction_splits": [ofx_id],
        }

    @pytest.mark.unit
    def test_audit_undo_of_the_merge_operation_still_reverses_the_decision(
        self, matched_db: Database
    ) -> None:
        """The forwarding takes its own operation id so the merge stays undoable.

        ``system_audit_undo`` reverses an operation as a whole, and
        ``TransactionIdAliasesRepo`` refuses to undo an alias row — the map is
        append-only. Recording the alias under the caller's operation would make
        that refusal fire on the merge itself, so a confirmed merge could no
        longer be reversed through the audit trail.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        _curate(matched_db, old_id)
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(matched_db, match_id="match0000011")

        # One operation per surface call, the way handle_cli_errors binds it.
        with operation() as merge_op:
            MatchingService(matched_db).set_status(
                "match0000011", status="accepted", actor="cli"
            )
        assert _aliases(matched_db), "the merge must have re-keyed something"

        UndoService(matched_db).undo(merge_op, actor="cli")

        row = get_match_decision(matched_db, "match0000011")
        assert row is not None
        assert row["match_status"] == "pending"
        assert _canonical_ids(matched_db) == {
            old_id,
            _canonical_id("ofx", "ofx_5678"),
        }

    @pytest.mark.unit
    def test_rejecting_a_proposal_writes_no_alias(self, matched_db: Database) -> None:
        """A rejected pair never merges, so no id is superseded."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(matched_db, match_id="match0000003")
        MatchingService(matched_db).set_status(
            "match0000003", status="rejected", actor="cli"
        )

        assert _aliases(matched_db) == {}
        assert len(_canonical_ids(matched_db)) == 2


class TestUndoingAMergeRestoresCuration:
    """Reversing a merge must hand each transaction back the curation it carried.

    A merge moves the superseded id's curation onto the survivor, and the two
    collision branches *delete* a row outright when both halves carried a
    category, or the same tag. Reversing the merge revives both transactions, so
    leaving the curation where the merge put it strands the user's edit on a
    transaction they never edited and loses the deleted half for good — from an
    operation the product advertises as reversible.

    The alias rows are not part of that: the map is append-only by design, and
    every assertion below re-checks that they survive the undo untouched.
    """

    @pytest.mark.unit
    def test_undoing_a_merge_returns_curation_to_the_id_it_was_written_on(
        self, matched_db: Database
    ) -> None:
        """The CSV row loses the anchor election, so its curation moved — and returns.

        Categories, notes, tags and splits all rode onto the OFX anchor when the
        merge was confirmed. Reversing it revives the CSV transaction, and the
        edits the user made against *that* transaction come back with it.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        _curate(matched_db, old_id)
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(matched_db, match_id="match0000009")
        service = MatchingService(matched_db)
        service.set_status("match0000009", status="accepted", actor="cli")
        new_id = _canonical_id("ofx", "ofx_5678")
        assert _curation_ids(matched_db) == dict.fromkeys(_CURATION_TABLES, [new_id])

        service.undo("match0000009", actor="cli")

        assert _canonical_ids(matched_db) == {old_id, new_id}
        assert _curation_ids(matched_db) == dict.fromkeys(_CURATION_TABLES, [old_id])
        assert _aliases(matched_db) == {old_id: new_id}

    @pytest.mark.unit
    def test_a_category_the_tiebreak_dropped_from_the_anchor_is_restored(
        self, matched_db: Database
    ) -> None:
        """Two ``user`` edits tie, so the anchor's row is deleted to make room.

        ``transaction_id`` is the whole primary key of
        ``app.transaction_categories``, so only one of the two survives the
        merge. Nothing but the undo can bring the other back.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        _categorize(matched_db, old_id, category="Food & Drink", by="user")
        _categorize(matched_db, new_id, category="Travel", by="user")

        service = MatchingService(matched_db)
        _seed_pending_dedup(matched_db, match_id="match0000030")
        service.set_status("match0000030", status="accepted", actor="cli")
        assert _categories(matched_db) == {new_id: ("Food & Drink", "user")}

        service.undo("match0000030", actor="cli")

        assert _categories(matched_db) == {
            old_id: ("Food & Drink", "user"),
            new_id: ("Travel", "user"),
        }
        assert _aliases(matched_db) == {old_id: new_id}

    @pytest.mark.unit
    def test_a_category_the_tiebreak_dropped_from_the_superseded_id_is_restored(
        self, matched_db: Database
    ) -> None:
        """The other collision branch: the anchor outranks, so the old row is dropped.

        ``provider_native`` sits below ``user`` on the ``SOURCE_PRIORITY``
        ladder, so the merge deletes the superseded row rather than moving it —
        a delete the undo has to reverse just the same.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        _categorize(matched_db, old_id, category="Food & Drink", by="provider_native")
        _categorize(matched_db, new_id, category="Travel", by="user")

        service = MatchingService(matched_db)
        _seed_pending_dedup(matched_db, match_id="match0000031")
        service.set_status("match0000031", status="accepted", actor="cli")
        assert _categories(matched_db) == {new_id: ("Travel", "user")}

        service.undo("match0000031", actor="cli")

        assert _categories(matched_db) == {
            old_id: ("Food & Drink", "provider_native"),
            new_id: ("Travel", "user"),
        }
        assert _aliases(matched_db) == {old_id: new_id}

    @pytest.mark.unit
    def test_an_identical_tag_collapsed_by_the_merge_is_restored(
        self, matched_db: Database
    ) -> None:
        """``(transaction_id, tag)`` is the PK, so one of two identical tags is deleted.

        The surviving pair is indistinguishable from either original, which is
        exactly why the undo cannot infer the deleted one back — it has to
        reverse the delete.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        TransactionTagsRepo(matched_db).add(
            transaction_id=old_id, tag="work", actor="cli"
        )
        TransactionTagsRepo(matched_db).add(
            transaction_id=new_id, tag="work", actor="cli"
        )

        service = MatchingService(matched_db)
        _seed_pending_dedup(matched_db, match_id="match0000032")
        service.set_status("match0000032", status="accepted", actor="cli")
        assert _tags(matched_db) == {(new_id, "work")}

        service.undo("match0000032", actor="cli")

        assert _tags(matched_db) == {(old_id, "work"), (new_id, "work")}
        assert _aliases(matched_db) == {old_id: new_id}

    @pytest.mark.unit
    def test_curation_written_on_the_anchor_is_untouched_by_the_undo(
        self, matched_db: Database
    ) -> None:
        """The anchor keeps its id through the merge, so no repoint ever runs.

        Not a regression test for the restore — nothing moved, so there is
        nothing to move back. It pins the other half of the contract: the undo
        must not drag an untouched edit off the transaction that owns it.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        anchor_id = _canonical_id("ofx", "ofx_5678")
        _curate(matched_db, anchor_id)
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _seed_pending_dedup(matched_db, match_id="match0000033")
        service = MatchingService(matched_db)
        service.set_status("match0000033", status="accepted", actor="cli")
        assert _curation_ids(matched_db) == dict.fromkeys(_CURATION_TABLES, [anchor_id])

        service.undo("match0000033", actor="cli")

        assert _curation_ids(matched_db) == dict.fromkeys(_CURATION_TABLES, [anchor_id])
        assert _aliases(matched_db) == {_canonical_id("tabular", "csv_1234"): anchor_id}


class TestPendingPostedRekeyForwardsCuration:
    """Plaid re-mints ``transaction_id`` when a pending transaction posts."""

    @pytest.mark.unit
    def test_posted_row_forwards_the_removed_pending_id(
        self, matched_db: Database
    ) -> None:
        """The posted row's ``pending_transaction_id`` is the forwarding pointer."""
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_pend_1234",
            source_type="plaid",
            source_file="plaid_sync",
        )
        pending_id = _canonical_id("plaid", "plaid_pend_1234")
        _curate(matched_db, pending_id)

        # Plaid removes the pending row and delivers a posted one under a new id.
        matched_db.execute(
            "DELETE FROM prep.int_transactions__unioned "
            "WHERE source_transaction_id = 'plaid_pend_1234'"
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_post_5678",
            source_type="plaid",
            source_file="plaid_sync",
            pending_transaction_id="plaid_pend_1234",
        )
        posted_id = _canonical_id("plaid", "plaid_post_5678")
        assert _canonical_ids(matched_db) == {posted_id}

        MatchingService(matched_db).run(actor="system")

        assert _aliases(matched_db) == {pending_id: posted_id}
        assert _curation_ids(matched_db) == {
            "transaction_categories": [posted_id],
            "transaction_notes": [posted_id],
            "transaction_tags": [posted_id],
            "transaction_splits": [posted_id],
        }

    @pytest.mark.unit
    def test_the_forwarded_counter_counts_curation_rows_not_audit_events(
        self, matched_db: Database
    ) -> None:
        """``_curate`` writes four rows, so one re-key forwards exactly four.

        A repoint emits an event per row *identity* it changes, and a
        primary-key move changes two — the counter answers "how much of the
        user's curation rode along", so it must not follow the event count.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_pend_1234",
            source_type="plaid",
            source_file="plaid_sync",
        )
        _curate(matched_db, _canonical_id("plaid", "plaid_pend_1234"))
        matched_db.execute(
            "DELETE FROM prep.int_transactions__unioned "
            "WHERE source_transaction_id = 'plaid_pend_1234'"
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_post_5678",
            source_type="plaid",
            source_file="plaid_sync",
            pending_transaction_id="plaid_pend_1234",
        )
        before = _forwarded_count()

        MatchingService(matched_db).run(actor="system")

        assert _forwarded_count() - before == 4.0

    @pytest.mark.unit
    def test_a_still_live_pending_row_is_not_aliased_away(
        self, matched_db: Database
    ) -> None:
        """A pending row Plaid has not removed is still a transaction of its own.

        The amounts and dates differ so the two rows cannot be deduped into one
        group: if they merged, the pending arm's ``old_id`` would equal the
        group's canonical id and the ``old_id <> new_id`` filter — not the
        still-live guard — would be what kept the alias out.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_pend_1234",
            source_type="plaid",
            source_file="plaid_sync",
            amount="-12.34",
            txn_date=date(2024, 3, 15),
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="plaid_post_5678",
            source_type="plaid",
            source_file="plaid_sync_2",
            pending_transaction_id="plaid_pend_1234",
            description="Hardware Store",
            amount="-98.76",
            txn_date=date(2024, 4, 20),
        )
        pending_id = _canonical_id("plaid", "plaid_pend_1234")
        posted_id = _canonical_id("plaid", "plaid_post_5678")
        assert _canonical_ids(matched_db) == {pending_id, posted_id}, (
            "the two rows must stay separate for the still-live guard to be "
            "the only thing that can suppress the alias"
        )

        MatchingService(matched_db).run(actor="system")

        assert _canonical_ids(matched_db) == {pending_id, posted_id}
        assert _aliases(matched_db) == {}


class TestForwardingIsIdempotentAndConflictSafe:
    """Re-running the forwarding, and forwarding onto an already-curated id."""

    @pytest.mark.unit
    def test_second_run_writes_no_duplicate_alias(self, matched_db: Database) -> None:
        """The append-only map tolerates repeated matcher runs."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        _seed_pending_dedup(matched_db, match_id="match0000004")
        MatchingService(matched_db).set_status(
            "match0000004", status="accepted", actor="cli"
        )
        first = _aliases(matched_db)

        MatchingService(matched_db).run(actor="system")

        assert _aliases(matched_db) == first

    @pytest.mark.unit
    def test_user_category_outranks_a_provider_one_on_the_surviving_id(
        self, matched_db: Database
    ) -> None:
        """Both sides curated: the higher-authority row wins the single PK."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        TransactionCategoriesRepo(matched_db).set(
            old_id,
            category="Food & Drink",
            subcategory="Coffee",
            category_id=None,
            categorized_by="user",
            actor="cli",
        )
        TransactionCategoriesRepo(matched_db).set(
            new_id,
            category="Shopping",
            subcategory=None,
            category_id=None,
            categorized_by="ai",
            actor="system",
        )

        _seed_pending_dedup(matched_db, match_id="match0000005")
        MatchingService(matched_db).set_status(
            "match0000005", status="accepted", actor="cli"
        )

        rows: list[Any] = matched_db.execute(
            "SELECT transaction_id, category, categorized_by "
            "FROM app.transaction_categories"
        ).fetchall()
        assert rows == [(new_id, "Food & Drink", "user")]

    @pytest.mark.unit
    def test_a_provider_category_does_not_displace_the_survivor_s_user_edit(
        self, matched_db: Database
    ) -> None:
        """The mirror of the case above: the survivor already holds the user edit."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        TransactionCategoriesRepo(matched_db).set(
            old_id,
            category="Shopping",
            subcategory=None,
            category_id=None,
            categorized_by="ai",
            actor="system",
        )
        TransactionCategoriesRepo(matched_db).set(
            new_id,
            category="Food & Drink",
            subcategory="Coffee",
            category_id=None,
            categorized_by="user",
            actor="cli",
        )

        _seed_pending_dedup(matched_db, match_id="match0000010")
        MatchingService(matched_db).set_status(
            "match0000010", status="accepted", actor="cli"
        )

        rows: list[Any] = matched_db.execute(
            "SELECT transaction_id, category, categorized_by "
            "FROM app.transaction_categories"
        ).fetchall()
        assert rows == [(new_id, "Food & Drink", "user")]

    @pytest.mark.unit
    def test_equal_authority_moves_the_superseded_category(
        self, matched_db: Database
    ) -> None:
        """A tie admits the incoming row, the way `upsert_guarded` does.

        Two `user` edits are both the user's, and neither the id nor the ladder
        ranks one above the other. The two write paths onto this table must
        answer that the same way or the table has two precedence rules.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        TransactionCategoriesRepo(matched_db).set(
            old_id,
            category="Food & Drink",
            subcategory="Coffee",
            category_id=None,
            categorized_by="user",
            actor="cli",
        )
        TransactionCategoriesRepo(matched_db).set(
            new_id,
            category="Shopping",
            subcategory=None,
            category_id=None,
            categorized_by="user",
            actor="cli",
        )

        _seed_pending_dedup(matched_db, match_id="match0000011")
        MatchingService(matched_db).set_status(
            "match0000011", status="accepted", actor="cli"
        )

        rows: list[Any] = matched_db.execute(
            "SELECT transaction_id, category, categorized_by "
            "FROM app.transaction_categories"
        ).fetchall()
        assert rows == [(new_id, "Food & Drink", "user")]

    @pytest.mark.unit
    def test_duplicate_tag_on_the_surviving_id_is_collapsed(
        self, matched_db: Database
    ) -> None:
        """Forwarding a tag the destination already carries must not violate the PK."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        _insert_source_row(
            matched_db,
            source_transaction_id="ofx_5678",
            source_type="ofx",
            source_file="statement.qfx",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        new_id = _canonical_id("ofx", "ofx_5678")
        TransactionTagsRepo(matched_db).add(
            transaction_id=old_id, tag="work", actor="cli"
        )
        TransactionTagsRepo(matched_db).add(
            transaction_id=new_id, tag="work", actor="cli"
        )

        _seed_pending_dedup(matched_db, match_id="match0000006")
        MatchingService(matched_db).set_status(
            "match0000006", status="accepted", actor="cli"
        )

        assert _curation_ids(matched_db)["transaction_tags"] == [new_id]


def _operation_of(db: Database, action: str) -> str:
    """The single operation id that emitted ``action``."""
    rows = db.execute(
        "SELECT DISTINCT operation_id FROM app.audit_log WHERE action = ?",
        [action],
    ).fetchall()
    assert len(rows) == 1, f"expected exactly one {action} operation, found {rows}"
    return str(rows[0][0])


def _merge_the_csv_row_onto_an_ofx_anchor(db: Database, *, match_id: str) -> str:
    """Land the OFX twin, propose the pair, confirm it; return the surviving id."""
    _insert_source_row(
        db,
        source_transaction_id="ofx_5678",
        source_type="ofx",
        source_file="statement.qfx",
    )
    _seed_pending_dedup(db, match_id=match_id)
    MatchingService(db).set_status(match_id, status="accepted", actor="cli")
    return _canonical_id("ofx", "ofx_5678")


class TestARepointBlocksUndoOfTheEditItMoved:
    """The row moved, so undoing the edit that wrote it must refuse, not no-op.

    ``UndoService._cascade_blockers`` joins the audit log on exact
    ``(target_schema, target_table, target_id)``. When a repoint names only the
    surviving id, the move is invisible from the superseded id's side: the
    original edit reports ``can_undo=True``, its undo deletes by the old id,
    matches zero rows, and reports success while the moved row stands. The
    documented walk is "undo the blocker, then the original undoes cleanly" —
    which requires the repoint to *be* a blocker.
    """

    @pytest.mark.unit
    def test_undoing_the_original_categorization_is_blocked_by_the_repoint(
        self, matched_db: Database
    ) -> None:
        """``transaction_categories`` keys its audit on the transaction id itself."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        with operation() as edit_op:
            TransactionCategoriesRepo(matched_db).set(
                old_id,
                category="Food & Drink",
                subcategory="Coffee",
                category_id=None,
                categorized_by="user",
                actor="cli",
            )

        new_id = _merge_the_csv_row_onto_an_ofx_anchor(
            matched_db, match_id="match0000020"
        )
        repoint_op = _operation_of(matched_db, "category.repoint_transaction")

        detail = UndoService(matched_db).get(edit_op)
        assert detail.undo_blocked_by == [repoint_op]
        assert detail.can_undo is False

        with pytest.raises(UserError) as excinfo:
            UndoService(matched_db).undo(edit_op, actor="cli")
        assert excinfo.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert _curation_ids(matched_db)["transaction_categories"] == [new_id], (
            "the refusal must leave the moved row exactly where the repoint put it"
        )

    @pytest.mark.unit
    def test_undoing_the_original_tag_is_blocked_by_the_repoint(
        self, matched_db: Database
    ) -> None:
        """The non-colliding tag branch keys its audit on ``transaction_id:tag``.

        Its colliding branch already audits the *old* key, so only the branch
        that moves the row can hide from the cascade check.
        """
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        with operation() as edit_op:
            TransactionTagsRepo(matched_db).add(
                transaction_id=old_id, tag="work", actor="cli"
            )

        new_id = _merge_the_csv_row_onto_an_ofx_anchor(
            matched_db, match_id="match0000021"
        )
        repoint_op = _operation_of(matched_db, "tag.repoint_transaction")

        detail = UndoService(matched_db).get(edit_op)
        assert detail.undo_blocked_by == [repoint_op]
        assert detail.can_undo is False

        with pytest.raises(UserError) as excinfo:
            UndoService(matched_db).undo(edit_op, actor="cli")
        assert excinfo.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert _curation_ids(matched_db)["transaction_tags"] == [new_id]

    @pytest.mark.unit
    def test_a_repointed_note_already_blocks_its_own_undo(
        self, matched_db: Database
    ) -> None:
        """``note_id`` survives the move, so the cascade check already sees it."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        with operation() as edit_op:
            TransactionNotesRepo(matched_db).add(
                transaction_id=old_id,
                note_id="note00000001",
                text="reimbursable",
                actor="cli",
            )

        new_id = _merge_the_csv_row_onto_an_ofx_anchor(
            matched_db, match_id="match0000022"
        )
        repoint_op = _operation_of(matched_db, "note.repoint_transaction")

        detail = UndoService(matched_db).get(edit_op)
        assert detail.undo_blocked_by == [repoint_op]
        assert detail.can_undo is False
        assert _curation_ids(matched_db)["transaction_notes"] == [new_id]

    @pytest.mark.unit
    def test_a_repointed_split_already_blocks_its_own_undo(
        self, matched_db: Database
    ) -> None:
        """``split_id`` survives the move, so the cascade check already sees it."""
        _insert_source_row(
            matched_db,
            source_transaction_id="csv_1234",
            source_type="tabular",
            source_file="export.csv",
        )
        old_id = _canonical_id("tabular", "csv_1234")
        with operation() as edit_op:
            TransactionSplitsRepo(matched_db).insert(
                split_id="split0000001",
                transaction_id=old_id,
                amount=Decimal("-12.34"),
                category="Food & Drink",
                subcategory="Coffee",
                category_id=None,
                note=None,
                ord=0,
                actor="cli",
            )

        new_id = _merge_the_csv_row_onto_an_ofx_anchor(
            matched_db, match_id="match0000023"
        )
        repoint_op = _operation_of(matched_db, "split.repoint_transaction")

        detail = UndoService(matched_db).get(edit_op)
        assert detail.undo_blocked_by == [repoint_op]
        assert detail.can_undo is False
        assert _curation_ids(matched_db)["transaction_splits"] == [new_id]
