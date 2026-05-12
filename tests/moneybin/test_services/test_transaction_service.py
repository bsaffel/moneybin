# tests/moneybin/test_services/test_transaction_service.py
"""Tests for TransactionService."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services._validators import InvalidSlugError
from moneybin.services.audit_service import AuditService
from moneybin.services.transaction_service import (
    ManualBatchResult,
    ManualEntryRawResult,
    Note,
    RecurringResult,
    RecurringTransaction,
    Split,
    TagRenameResult,
    TransactionService,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def transaction_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with core + app tables and test transactions."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test transactions: 3 "Coffee Shop" across months for recurring
    conn.execute("""
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES
        ('T1', 'A1', '2026-04-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-04-10', CURRENT_TIMESTAMP,
         2026, 4, 10, 3, '2026-04', '2026-Q2'),
        ('T2', 'A1', '2026-04-15', 5000.00, 5000.00, 'income',
         'Employer Inc', 'CREDIT', false, 'USD', 'ofx',
         '2026-04-15', CURRENT_TIMESTAMP,
         2026, 4, 15, 1, '2026-04', '2026-Q2'),
        ('T3', 'A1', '2026-03-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-03-10', CURRENT_TIMESTAMP,
         2026, 3, 10, 1, '2026-03', '2026-Q1'),
        ('T4', 'A1', '2026-02-10', -50.00, 50.00, 'expense',
         'Coffee Shop', 'DEBIT', false, 'USD', 'ofx',
         '2026-02-10', CURRENT_TIMESTAMP,
         2026, 2, 10, 1, '2026-02', '2026-Q1')
    """)  # noqa: S608  # test input, not executing SQL

    # Categorize one transaction
    conn.execute("""
        INSERT INTO app.transaction_categories
            (transaction_id, category, subcategory, categorized_at,
             categorized_by)
        VALUES
        ('T1', 'Food & Drink', 'Coffee Shops', CURRENT_TIMESTAMP, 'user')
    """)  # noqa: S608  # test input, not executing SQL

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


class TestRecurring:
    """Tests for TransactionService.recurring()."""

    @pytest.mark.unit
    def test_returns_recurring_result(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        assert isinstance(result, RecurringResult)
        assert len(result.transactions) == 1

    @pytest.mark.unit
    def test_recurring_fields(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        rec = result.transactions[0]
        assert isinstance(rec, RecurringTransaction)
        assert rec.description == "Coffee Shop"
        assert rec.occurrence_count == 3
        assert rec.avg_amount == Decimal("-50.00")

    @pytest.mark.unit
    def test_min_occurrences_filter(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        # With min_occurrences=4, Coffee Shop (3 occurrences) excluded
        result = service.recurring(min_occurrences=4)
        assert len(result.transactions) == 0

    @pytest.mark.unit
    def test_to_envelope_sensitivity_medium(self, transaction_db: Database) -> None:
        service = TransactionService(transaction_db)
        result = service.recurring(min_occurrences=3)
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "medium"


class TestEmptyResults:
    """Tests for service behavior with no data in tables."""

    @pytest.fixture()
    def empty_db(self, tmp_path: Path) -> Generator[Database, None, None]:
        mock_store = MagicMock()
        mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
        database = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_store,
            no_auto_upgrade=True,
        )
        create_core_tables_raw(database.conn)
        db_module._database_instance = database  # type: ignore[attr-defined]
        yield database
        db_module._database_instance = None  # type: ignore[attr-defined]
        database.close()

    @pytest.mark.unit
    def test_get_empty_db(self, empty_db: Database) -> None:
        from moneybin.services.transaction_service import TransactionGetResult

        service = TransactionService(empty_db)
        result = service.get()
        assert isinstance(result, TransactionGetResult)
        assert result.transactions == []
        assert result.next_cursor is None

    @pytest.mark.unit
    def test_recurring_empty_db(self, empty_db: Database) -> None:
        service = TransactionService(empty_db)
        result = service.recurring()
        assert isinstance(result, RecurringResult)
        assert result.transactions == []


class TestNotes:
    """Tests for TransactionService note operations (multi-note shape)."""

    @pytest.fixture()
    def audit_service(self, transaction_db: Database) -> AuditService:
        return AuditService(transaction_db)

    @pytest.fixture()
    def txn_service(
        self, transaction_db: Database, audit_service: AuditService
    ) -> TransactionService:
        return TransactionService(transaction_db, audit=audit_service)

    @pytest.fixture()
    def sample_transaction_id(self) -> str:
        # T1 is inserted by the transaction_db fixture
        return "T1"

    @pytest.mark.unit
    def test_add_note_writes_row_and_emits_audit(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        note = txn_service.add_note(
            sample_transaction_id, "checked statement", actor="cli"
        )
        assert isinstance(note, Note)
        assert note.note_id and len(note.note_id) == 12
        assert note.transaction_id == sample_transaction_id
        assert note.text == "checked statement"
        assert note.author == "cli"
        assert note.created_at  # populated from DB default

        events = audit_service.list_events(
            action_pattern="note.add", target_id=sample_transaction_id
        )
        assert len(events) == 1
        assert events[0].after_value == {
            "note_id": note.note_id,
            "text": "checked statement",
            "author": "cli",
        }
        assert events[0].before_value is None
        assert events[0].target_table == "transaction_notes"
        assert events[0].target_schema == "app"

    @pytest.mark.unit
    def test_add_note_rejects_overlong_text(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        with pytest.raises(ValueError):
            txn_service.add_note(sample_transaction_id, "x" * 2001, actor="cli")

    @pytest.mark.unit
    def test_add_note_rejects_empty_text(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        with pytest.raises(ValueError):
            txn_service.add_note(sample_transaction_id, "", actor="cli")

    @pytest.mark.unit
    def test_edit_note_updates_text_and_emits_audit(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        note = txn_service.add_note(sample_transaction_id, "v1", actor="cli")
        edited = txn_service.edit_note(note.note_id, "v2", actor="cli")
        assert edited.text == "v2"
        assert edited.note_id == note.note_id
        events = audit_service.list_events(action_pattern="note.edit")
        assert len(events) == 1
        assert events[0].before_value == {"text": "v1"}
        assert events[0].after_value == {"text": "v2"}
        assert events[0].target_id == sample_transaction_id

    @pytest.mark.unit
    def test_edit_note_missing_raises_lookup(
        self, txn_service: TransactionService
    ) -> None:
        with pytest.raises(LookupError):
            txn_service.edit_note("doesnotexist", "anything", actor="cli")

    @pytest.mark.unit
    def test_delete_note_emits_audit_with_after_null(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        note = txn_service.add_note(sample_transaction_id, "doomed", actor="mcp")
        txn_service.delete_note(note.note_id, actor="mcp")
        events = audit_service.list_events(action_pattern="note.delete")
        assert len(events) == 1
        assert events[0].after_value is None
        assert events[0].before_value == {
            "note_id": note.note_id,
            "text": "doomed",
            "author": "mcp",
        }
        assert events[0].target_id == sample_transaction_id
        # Row is gone
        assert txn_service.list_notes(sample_transaction_id) == []

    @pytest.mark.unit
    def test_delete_note_missing_raises_lookup(
        self, txn_service: TransactionService
    ) -> None:
        with pytest.raises(LookupError):
            txn_service.delete_note("doesnotexist", actor="cli")

    @pytest.mark.unit
    def test_list_notes_returns_chronological(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        n1 = txn_service.add_note(sample_transaction_id, "first", actor="cli")
        n2 = txn_service.add_note(sample_transaction_id, "second", actor="cli")
        notes = txn_service.list_notes(sample_transaction_id)
        assert [n.note_id for n in notes] == [n1.note_id, n2.note_id]
        assert [n.text for n in notes] == ["first", "second"]

    @pytest.mark.unit
    def test_list_notes_empty_for_unknown_transaction(
        self, txn_service: TransactionService
    ) -> None:
        assert txn_service.list_notes("nope") == []

    @pytest.mark.unit
    def test_lazy_audit_service_default(self, transaction_db: Database) -> None:
        # When constructed without an explicit audit service, one is built lazily
        # so all existing call sites continue to work.
        service = TransactionService(transaction_db)
        note = service.add_note("T1", "lazy default", actor="cli")
        assert note.text == "lazy default"


class TestTags:
    """Tests for TransactionService tag operations (Req 13–16)."""

    @pytest.fixture()
    def audit_service(self, transaction_db: Database) -> AuditService:
        return AuditService(transaction_db)

    @pytest.fixture()
    def txn_service(
        self, transaction_db: Database, audit_service: AuditService
    ) -> TransactionService:
        return TransactionService(transaction_db, audit=audit_service)

    @pytest.fixture()
    def sample_transaction_id(self) -> str:
        return "T1"

    @pytest.fixture()
    def txns_with_shared_tag(self, txn_service: TransactionService) -> list[str]:
        # T1, T2, T3 (three transactions all tagged "foo") for rename test.
        ids = ["T1", "T2", "T3"]
        for txn_id in ids:
            txn_service.add_tags(txn_id, ["foo"], actor="cli")
        return ids

    @pytest.mark.unit
    def test_add_tags_validates_pattern(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        with pytest.raises(InvalidSlugError):
            txn_service.add_tags(sample_transaction_id, ["Bad Tag"], actor="cli")

    @pytest.mark.unit
    def test_add_tags_invalid_does_not_mutate(
        self,
        txn_service: TransactionService,
        sample_transaction_id: str,
    ) -> None:
        # 'foo' is valid, 'Bad Tag' is not — validation up front means no
        # partial state should land.
        with pytest.raises(InvalidSlugError):
            txn_service.add_tags(sample_transaction_id, ["foo", "Bad Tag"], actor="cli")
        assert txn_service.list_tags(sample_transaction_id) == []

    @pytest.mark.unit
    def test_add_tags_returns_only_added(
        self,
        txn_service: TransactionService,
        sample_transaction_id: str,
    ) -> None:
        added = txn_service.add_tags(sample_transaction_id, ["foo", "bar"], actor="cli")
        assert sorted(added) == ["bar", "foo"]
        # Re-add: foo already present, baz is new
        added2 = txn_service.add_tags(
            sample_transaction_id, ["foo", "baz"], actor="cli"
        )
        assert added2 == ["baz"]
        assert txn_service.list_tags(sample_transaction_id) == [
            "bar",
            "baz",
            "foo",
        ]

    @pytest.mark.unit
    def test_add_tags_idempotent_marks_audit_noop(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.add_tags(sample_transaction_id, ["foo"], actor="cli")
        txn_service.add_tags(sample_transaction_id, ["foo"], actor="cli")
        events = audit_service.list_events(action_pattern="tag.add")
        noops = [e for e in events if e.context_json and e.context_json.get("noop")]
        assert len(noops) == 1
        # The non-noop add still recorded a normal tag.add event
        normals = [e for e in events if not e.context_json]
        assert len(normals) == 1

    @pytest.mark.unit
    def test_remove_tags_absent_marks_audit_noop(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        removed = txn_service.remove_tags(
            sample_transaction_id, ["never-applied"], actor="cli"
        )
        assert removed == []
        events = audit_service.list_events(action_pattern="tag.remove")
        assert len(events) == 1
        assert events[0].context_json == {"noop": True}

    @pytest.mark.unit
    def test_set_tags_diff_only_writes_delta(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.set_tags(sample_transaction_id, ["foo", "bar"], actor="mcp")
        # Drop 'tag.add' events recorded by the first set_tags so we only see
        # the delta of the second call.
        first_add_count = len(audit_service.list_events(action_pattern="tag.add"))
        result = txn_service.set_tags(
            sample_transaction_id, ["bar", "baz"], actor="mcp"
        )
        assert result == ["bar", "baz"]
        assert txn_service.list_tags(sample_transaction_id) == ["bar", "baz"]

        adds = audit_service.list_events(action_pattern="tag.add")
        removes = audit_service.list_events(action_pattern="tag.remove")
        # foo and bar added in the first call, baz added in the second — third
        # add event since the start of the test.
        assert len(adds) == first_add_count + 1
        new_add = next(e for e in adds if (e.after_value or {}).get("tag") == "baz")
        assert new_add.before_value is None
        # foo removed (the one and only remove)
        assert len(removes) == 1
        assert (removes[0].before_value or {}).get("tag") == "foo"
        assert removes[0].context_json is None

    @pytest.mark.unit
    def test_set_tags_validates_before_mutating(
        self,
        txn_service: TransactionService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.add_tags(sample_transaction_id, ["foo"], actor="mcp")
        with pytest.raises(InvalidSlugError):
            txn_service.set_tags(sample_transaction_id, ["bar", "Bad Tag"], actor="mcp")
        # Pre-existing state untouched
        assert txn_service.list_tags(sample_transaction_id) == ["foo"]

    @pytest.mark.unit
    def test_rename_tag_emits_parent_plus_per_row_children(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        txns_with_shared_tag: list[str],
    ) -> None:
        result = txn_service.rename_tag("foo", "bar", actor="cli")
        assert isinstance(result, TagRenameResult)
        assert result.row_count == len(txns_with_shared_tag)

        chain = audit_service.chain_for(result.parent_audit_id)
        assert len(chain) == 1 + len(txns_with_shared_tag)
        children = [e for e in chain if e.audit_id != result.parent_audit_id]
        assert all(e.parent_audit_id == result.parent_audit_id for e in children)
        assert all(e.action == "tag.rename_row" for e in children)
        assert {e.target_id for e in children} == set(txns_with_shared_tag)

        parent = next(e for e in chain if e.audit_id == result.parent_audit_id)
        assert parent.action == "tag.rename"
        assert parent.target_id is None
        assert parent.before_value == {"old_tag": "foo"}
        assert parent.after_value == {
            "new_tag": "bar",
            "row_count": len(txns_with_shared_tag),
        }

        # All rows now carry the new tag, none the old.
        for txn_id in txns_with_shared_tag:
            assert "bar" in txn_service.list_tags(txn_id)
            assert "foo" not in txn_service.list_tags(txn_id)

    @pytest.mark.unit
    def test_list_distinct_tags_counts_applications(
        self, txn_service: TransactionService
    ) -> None:
        txn_service.add_tags("T1", ["foo", "bar"], actor="cli")
        txn_service.add_tags("T2", ["foo"], actor="cli")
        txn_service.add_tags("T3", ["foo"], actor="cli")
        assert txn_service.list_distinct_tags() == [("bar", 1), ("foo", 3)]

    @pytest.mark.unit
    def test_list_tags_empty_for_unknown_transaction(
        self, txn_service: TransactionService
    ) -> None:
        assert txn_service.list_tags("nope") == []


class TestSplits:
    """Tests for TransactionService split operations (Req 17–21)."""

    @pytest.fixture()
    def audit_service(self, transaction_db: Database) -> AuditService:
        return AuditService(transaction_db)

    @pytest.fixture()
    def txn_service(
        self, transaction_db: Database, audit_service: AuditService
    ) -> TransactionService:
        return TransactionService(transaction_db, audit=audit_service)

    @pytest.fixture()
    def sample_transaction_id(self) -> str:
        # T1 from the transaction_db fixture (amount -50.00).
        return "T1"

    @pytest.fixture()
    def sample_transaction_id_amount_minus_100(self, transaction_db: Database) -> str:
        # Insert a parent transaction with amount=-100.00 for balance tests.
        transaction_db.conn.execute(
            """
            INSERT INTO core.fct_transactions (
                transaction_id, account_id, transaction_date, amount,
                amount_absolute, transaction_direction, description,
                transaction_type, is_pending, currency_code, source_type,
                source_extracted_at, loaded_at,
                transaction_year, transaction_month, transaction_day,
                transaction_day_of_week, transaction_year_month,
                transaction_year_quarter
            ) VALUES
            ('TSPLIT', 'A1', '2026-04-20', -100.00, 100.00, 'expense',
             'Big Box Store', 'DEBIT', false, 'USD', 'ofx',
             '2026-04-20', CURRENT_TIMESTAMP,
             2026, 4, 20, 6, '2026-04', '2026-Q2')
            """  # noqa: S608  # test input, not executing SQL
        )
        return "TSPLIT"

    @pytest.mark.unit
    def test_add_split_inserts_with_auto_incrementing_ord(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        s1 = txn_service.add_split(
            sample_transaction_id,
            Decimal("-30.00"),
            category="Supplies",
            actor="cli",
        )
        s2 = txn_service.add_split(
            sample_transaction_id,
            Decimal("-20.00"),
            category="Gas",
            actor="cli",
        )
        s3 = txn_service.add_split(
            sample_transaction_id, Decimal("-10.00"), category="Misc", actor="cli"
        )
        assert isinstance(s1, Split)
        assert s1.ord == 0
        assert s2.ord == 1
        assert s3.ord == 2
        assert s1.amount == Decimal("-30.00")
        assert s1.split_id and len(s1.split_id) == 12

        splits = txn_service.list_splits(sample_transaction_id)
        assert [s.split_id for s in splits] == [s1.split_id, s2.split_id, s3.split_id]
        assert [s.ord for s in splits] == [0, 1, 2]

    @pytest.mark.unit
    def test_add_split_emits_audit(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        s = txn_service.add_split(
            sample_transaction_id,
            Decimal("-25.50"),
            category="Coffee",
            actor="cli",
        )
        events = audit_service.list_events(
            action_pattern="split.add", target_id=sample_transaction_id
        )
        assert len(events) == 1
        assert events[0].before_value is None
        assert events[0].after_value == {
            "split_id": s.split_id,
            "amount": "-25.50",
            "category": "Coffee",
        }
        assert events[0].target_table == "transaction_splits"
        assert events[0].target_schema == "app"

    @pytest.mark.unit
    def test_remove_split_emits_audit_with_before(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        s = txn_service.add_split(
            sample_transaction_id,
            Decimal("-10.00"),
            category="Coffee",
            actor="cli",
        )
        txn_service.remove_split(s.split_id, actor="cli")
        events = audit_service.list_events(action_pattern="split.remove")
        assert len(events) == 1
        assert events[0].after_value is None
        assert events[0].before_value == {
            "split_id": s.split_id,
            "amount": "-10.00",
            "category": "Coffee",
        }
        assert events[0].target_id == sample_transaction_id
        assert txn_service.list_splits(sample_transaction_id) == []

    @pytest.mark.unit
    def test_remove_split_missing_raises_lookup(
        self, txn_service: TransactionService
    ) -> None:
        with pytest.raises(LookupError):
            txn_service.remove_split("doesnotexist", actor="cli")

    @pytest.mark.unit
    def test_clear_splits_removes_all_and_emits_one_event(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.add_split(
            sample_transaction_id, Decimal("-10.00"), category="A", actor="cli"
        )
        txn_service.add_split(
            sample_transaction_id, Decimal("-20.00"), category="B", actor="cli"
        )
        txn_service.clear_splits(sample_transaction_id, actor="cli")
        assert txn_service.list_splits(sample_transaction_id) == []
        events = audit_service.list_events(action_pattern="split.clear")
        assert len(events) == 1
        assert events[0].before_value == {"split_count": 2}
        assert events[0].after_value is None
        assert events[0].target_id == sample_transaction_id

    @pytest.mark.unit
    def test_clear_splits_noop_when_empty(
        self,
        txn_service: TransactionService,
        audit_service: AuditService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.clear_splits(sample_transaction_id, actor="cli")
        events = audit_service.list_events(action_pattern="split.clear")
        assert events == []

    @pytest.mark.unit
    def test_set_splits_replaces_all_atomically(
        self,
        txn_service: TransactionService,
        sample_transaction_id: str,
    ) -> None:
        txn_service.add_split(
            sample_transaction_id, Decimal("-10.00"), category="A", actor="cli"
        )
        result = txn_service.set_splits(
            sample_transaction_id,
            [
                {
                    "amount": Decimal("-50.00"),
                    "category": "B",
                    "subcategory": None,
                    "note": None,
                }
            ],
            actor="mcp",
        )
        assert len(result) == 1
        assert result[0].amount == Decimal("-50.00")
        assert result[0].category == "B"
        assert result[0].ord == 0
        listed = txn_service.list_splits(sample_transaction_id)
        assert [(s.amount, s.category) for s in listed] == [(Decimal("-50.00"), "B")]

    @pytest.mark.unit
    def test_set_splits_validates_before_mutating(
        self,
        txn_service: TransactionService,
        sample_transaction_id: str,
    ) -> None:
        # Seed an existing split that should remain untouched on validation failure.
        txn_service.add_split(
            sample_transaction_id, Decimal("-10.00"), category="Keep", actor="cli"
        )
        with pytest.raises(ValueError):
            txn_service.set_splits(
                sample_transaction_id,
                [
                    {"amount": Decimal("-5.00"), "category": "ok"},
                    {"amount": 7.0, "category": "bad-float"},  # not Decimal
                ],
                actor="mcp",
            )
        listed = txn_service.list_splits(sample_transaction_id)
        assert [(s.amount, s.category) for s in listed] == [(Decimal("-10.00"), "Keep")]

    @pytest.mark.unit
    def test_splits_balance_returns_signed_residual(
        self,
        txn_service: TransactionService,
        sample_transaction_id_amount_minus_100: str,
    ) -> None:
        txn_service.add_split(
            sample_transaction_id_amount_minus_100,
            Decimal("-60.00"),
            category="A",
            actor="cli",
        )
        # parent -100, children -60 → residual = -100 - (-60) = -40
        residual = txn_service.splits_balance(sample_transaction_id_amount_minus_100)
        assert residual == Decimal("-40.00")
        assert isinstance(residual, Decimal)

    @pytest.mark.unit
    def test_splits_balance_zero_when_balanced(
        self,
        txn_service: TransactionService,
        sample_transaction_id_amount_minus_100: str,
    ) -> None:
        txn_service.add_split(
            sample_transaction_id_amount_minus_100,
            Decimal("-60.00"),
            category="A",
            actor="cli",
        )
        txn_service.add_split(
            sample_transaction_id_amount_minus_100,
            Decimal("-40.00"),
            category="B",
            actor="cli",
        )
        assert txn_service.splits_balance(
            sample_transaction_id_amount_minus_100
        ) == Decimal("0.00")

    @pytest.mark.unit
    def test_splits_balance_no_children_equals_parent(
        self,
        txn_service: TransactionService,
        sample_transaction_id_amount_minus_100: str,
    ) -> None:
        # No splits → residual = parent.amount
        assert txn_service.splits_balance(
            sample_transaction_id_amount_minus_100
        ) == Decimal("-100.00")

    @pytest.mark.unit
    def test_splits_balance_missing_parent_raises_lookup(
        self, txn_service: TransactionService
    ) -> None:
        with pytest.raises(LookupError):
            txn_service.splits_balance("nope")

    @pytest.mark.unit
    def test_list_splits_honors_ord(
        self, txn_service: TransactionService, sample_transaction_id: str
    ) -> None:
        s1 = txn_service.add_split(
            sample_transaction_id, Decimal("-1.00"), category="x", actor="cli"
        )
        s2 = txn_service.add_split(
            sample_transaction_id, Decimal("-2.00"), category="y", actor="cli"
        )
        splits = txn_service.list_splits(sample_transaction_id)
        assert [s.ord for s in splits] == [0, 1]
        assert [s.split_id for s in splits] == [s1.split_id, s2.split_id]

    @pytest.mark.unit
    def test_list_splits_empty_for_unknown_transaction(
        self, txn_service: TransactionService
    ) -> None:
        assert txn_service.list_splits("nope") == []


class TestManualEntry:
    """Tests for ``TransactionService.create_manual_batch`` (Task 7a)."""

    @staticmethod
    def _seed_account(database: Database, account_id: str = "A1") -> None:
        database.conn.execute(
            "INSERT INTO core.dim_accounts (account_id) VALUES (?)",
            [account_id],
        )

    @staticmethod
    def _entry(**overrides: object) -> dict[str, object]:
        base: dict[str, object] = {
            "account_id": "A1",
            "amount": Decimal("-12.34"),
            "transaction_date": "2026-04-15",
            "description": "Coffee Shop",
        }
        base.update(overrides)
        return base

    @pytest.mark.unit
    def test_create_manual_batch_writes_one_import_log_row(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        result = service.create_manual_batch(
            [self._entry(), self._entry(amount=Decimal("99.00"))],
            actor="cli",
        )
        assert isinstance(result, ManualBatchResult)
        assert len(result.results) == 2
        assert all(
            isinstance(r, ManualEntryRawResult)
            and r.source_transaction_id.startswith("manual_")
            for r in result.results
        )

        log_rows = transaction_db.conn.execute(
            "SELECT source_type, format_name FROM raw.import_log WHERE import_id = ?",
            [result.import_id],
        ).fetchall()
        assert log_rows == [("manual", "manual_entry")]

        manual_rows = transaction_db.conn.execute(
            "SELECT source_transaction_id, import_id "
            "FROM raw.manual_transactions WHERE import_id = ?",
            [result.import_id],
        ).fetchall()
        assert len(manual_rows) == 2
        assert {r[0] for r in manual_rows} == {
            r.source_transaction_id for r in result.results
        }
        assert {r[1] for r in manual_rows} == {result.import_id}

    @pytest.mark.unit
    def test_create_manual_batch_emits_one_manual_create_audit(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        result = service.create_manual_batch(
            [self._entry(), self._entry()], actor="cli"
        )

        audit_rows = transaction_db.conn.execute(
            "SELECT action, target_id, after_value FROM app.audit_log "
            "WHERE action = 'manual.create'"
        ).fetchall()
        assert len(audit_rows) == 1
        action, target_id, after_value = audit_rows[0]
        assert action == "manual.create"
        assert target_id == result.import_id
        import json as _json

        assert _json.loads(after_value) == {"row_count": 2}

    @pytest.mark.unit
    def test_create_manual_batch_rejects_whole_batch_on_validation_failure(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        with pytest.raises(ValueError, match=r"entries\[1\]\.account_id"):
            service.create_manual_batch(
                [
                    self._entry(),
                    self._entry(account_id="GHOST"),
                    self._entry(),
                ],
                actor="cli",
            )
        # No raw rows and no import_log row should have been written.
        manual_count = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM raw.manual_transactions"
        ).fetchone()
        assert manual_count is not None
        assert manual_count[0] == 0
        log_count = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM raw.import_log WHERE source_type = 'manual'"
        ).fetchone()
        assert log_count is not None
        assert log_count[0] == 0

    @pytest.mark.unit
    def test_create_manual_batch_rejects_size_zero(
        self, transaction_db: Database
    ) -> None:
        service = TransactionService(transaction_db)
        with pytest.raises(ValueError, match="batch size"):
            service.create_manual_batch([], actor="cli")

    @pytest.mark.unit
    def test_create_manual_batch_rejects_size_above_100(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        oversize = [self._entry() for _ in range(101)]
        with pytest.raises(ValueError, match="batch size"):
            service.create_manual_batch(oversize, actor="cli")
        # Size check fires before any DB mutation.
        log_count = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM raw.import_log WHERE source_type = 'manual'"
        ).fetchone()
        assert log_count is not None
        assert log_count[0] == 0

    @pytest.mark.unit
    @pytest.mark.unit
    def test_create_manual_batch_with_category_writes_user_categorization(
        self, transaction_db: Database
    ) -> None:
        import hashlib

        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        result = service.create_manual_batch(
            [self._entry(category="Food & Drink", subcategory="Coffee Shops")],
            actor="cli",
        )
        # Raw row's category column stays NULL (categorization lives in app.).
        raw_rows = transaction_db.conn.execute(
            "SELECT category, subcategory FROM raw.manual_transactions "
            "WHERE import_id = ?",
            [result.import_id],
        ).fetchall()
        assert raw_rows == [(None, None)]

        # ManualEntryRawResult exposes the predicted gold key, and it matches
        # the SQLMesh int_transactions__matched fallback hash.
        entry_result = result.results[0]
        expected_txn_id = hashlib.sha256(
            f"manual|{entry_result.source_transaction_id}|A1".encode()
        ).hexdigest()[:16]
        assert entry_result.transaction_id == expected_txn_id

        # User-category row keyed on the predicted gold transaction_id.
        cat_rows = transaction_db.conn.execute(
            "SELECT category, subcategory, categorized_by "
            "FROM app.transaction_categories WHERE transaction_id = ?",
            [entry_result.transaction_id],
        ).fetchall()
        assert cat_rows == [("Food & Drink", "Coffee Shops", "user")]

        # And a category.set audit event was emitted alongside manual.create.
        actions = [
            r[0]
            for r in transaction_db.conn.execute(
                "SELECT action FROM app.audit_log ORDER BY occurred_at"
            ).fetchall()
        ]
        assert "manual.create" in actions
        assert "category.set" in actions

    @pytest.mark.unit
    def test_create_manual_batch_without_category_writes_no_categorization(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        result = service.create_manual_batch(
            [self._entry(), self._entry(amount=Decimal("9.99"))],
            actor="cli",
        )
        cat_count = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM app.transaction_categories WHERE transaction_id IN (?, ?)",
            [r.transaction_id for r in result.results],
        ).fetchone()
        assert cat_count is not None and cat_count[0] == 0
        cat_set = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM app.audit_log WHERE action = 'category.set'"
        ).fetchone()
        assert cat_set is not None and cat_set[0] == 0

    @pytest.mark.unit
    def test_create_manual_batch_skips_blank_category_string(
        self, transaction_db: Database
    ) -> None:
        self._seed_account(transaction_db)
        service = TransactionService(transaction_db)
        result = service.create_manual_batch([self._entry(category="   ")], actor="cli")
        cat_count = transaction_db.conn.execute(
            "SELECT COUNT(*) FROM app.transaction_categories WHERE transaction_id = ?",
            [result.results[0].transaction_id],
        ).fetchone()
        assert cat_count is not None and cat_count[0] == 0
