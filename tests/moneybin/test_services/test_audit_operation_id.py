"""operation_id capture + grouping on app.audit_log (REC-PR1).

Verifies the capture spine: every audit row gets a NOT-NULL ``operation_id``,
rows written inside one ``operation()`` block share it (including writes from
real repositories, which need no changes), bare writes each get their own, and
a caller-supplied id (the self-heal form) flows through.

These run against the ``db`` fixture, which runs ``init_schemas`` but not
migrations — i.e. the fresh-install path where the schema file carries the
column. The existing-DB upgrade path is covered by ``test_migration_v023``.
"""

from __future__ import annotations

import re

import pytest

from moneybin.database import Database
from moneybin.repositories.user_categories_repo import UserCategoriesRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation

_OP_ID = re.compile(r"^op_[0-9a-f]{32}$")


@pytest.fixture()
def audit(db: Database) -> AuditService:
    return AuditService(db)


def _record(audit: AuditService, action: str = "test.write") -> None:
    audit.record_audit_event(
        action=action,
        target=("app", "transaction_tags", "txn_1"),
        before=None,
        after={"tag": "x"},
        actor="cli",
    )


def _op_ids(db: Database, *, action: str | None = None) -> list[str]:
    where = "WHERE action = ?" if action else ""
    params = [action] if action else []
    return [
        r[0]
        for r in db.execute(
            f"SELECT operation_id FROM app.audit_log {where} "  # noqa: S608  # static
            "ORDER BY occurred_at, audit_id",
            params,
        ).fetchall()
    ]


class TestCapture:
    """Every recorded event persists and returns a well-formed operation_id."""

    def test_persisted_row_has_operation_id_in_format(
        self, db: Database, audit: AuditService
    ) -> None:
        _record(audit)
        (op,) = _op_ids(db)
        assert _OP_ID.match(op)

    def test_returned_event_carries_matching_operation_id(
        self, db: Database, audit: AuditService
    ) -> None:
        event = audit.record_audit_event(
            action="test.write",
            target=("app", "transaction_tags", "txn_1"),
            before=None,
            after={"tag": "x"},
            actor="cli",
        )
        assert _OP_ID.match(event.operation_id)
        assert _op_ids(db) == [event.operation_id]


class TestGroupingWithinContext:
    """Writes inside one operation() block share one id; separate blocks differ."""

    def test_n_writes_in_one_context_share_one_id(
        self, db: Database, audit: AuditService
    ) -> None:
        with operation():
            _record(audit, "a")
            _record(audit, "b")
            _record(audit, "c")
        ops = _op_ids(db)
        assert len(ops) == 3
        assert len(set(ops)) == 1

    def test_separate_contexts_get_distinct_ids(
        self, db: Database, audit: AuditService
    ) -> None:
        with operation():
            _record(audit, "a")
        with operation():
            _record(audit, "b")
        assert len(set(_op_ids(db))) == 2


class TestRealRepoInheritsContext:
    """Repositories route through _emit_audit → record_audit_event unchanged.

    Two repo writes in one operation() are siblings: they share one
    operation_id but have no parent_audit_id link — the flat-group primitive
    that operation_id adds on top of the parent_audit_id causal tree.
    """

    def test_repo_writes_in_one_context_share_id_without_parent_link(
        self, db: Database
    ) -> None:
        repo = UserCategoriesRepo(db)
        with operation():
            repo.insert(category="Foo", actor="cli")
            repo.insert(category="Bar", actor="cli")
        rows = db.execute(
            "SELECT operation_id, parent_audit_id FROM app.audit_log "
            "WHERE action = 'user_category.insert'"
        ).fetchall()
        assert len(rows) == 2
        assert len({r[0] for r in rows}) == 1  # one shared operation_id
        assert all(r[1] is None for r in rows)  # no parent-child link


class TestNoContextFallback:
    """A bare write outside any operation() is still its own valid operation."""

    def test_bare_write_gets_valid_id(self, db: Database, audit: AuditService) -> None:
        _record(audit)
        (op,) = _op_ids(db)
        assert _OP_ID.match(op)

    def test_two_bare_writes_get_distinct_ids(
        self, db: Database, audit: AuditService
    ) -> None:
        _record(audit, "a")
        _record(audit, "b")
        assert len(set(_op_ids(db))) == 2


class TestCustomId:
    """A caller-supplied id (self-heal recipes, later REC-PR) flows through."""

    def test_self_heal_form_is_persisted_verbatim(
        self, db: Database, audit: AuditService
    ) -> None:
        custom = "op_self_heal_drift_0123456789abcdef0123456789abcdef"
        with operation(custom):
            _record(audit)
        assert _op_ids(db) == [custom]
