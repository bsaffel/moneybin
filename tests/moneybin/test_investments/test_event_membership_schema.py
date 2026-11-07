"""Immutable investment membership and its audited transactional owner."""

from dataclasses import replace
from importlib import import_module

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation


def member():
    from moneybin.investments.event_membership import EventMember, MembershipBinding

    return EventMember(
        event_group_id="111111111111",
        investment_transaction_id="222222222222",
        source_event_key="source_event",
        source_type="manual",
        source_origin="manual",
        native_reference="native",
        observation_version="manual_revision",
        semantic_leg_role="buy",
        binding=MembershipBinding(
            account_id="account",
            security_id="security",
            effective_currency="USD",
            currency_source="account",
            account_generation="account_generation",
            security_generation="security_generation",
            currency_generation="currency_generation",
        ),
    )


def test_membership_schema_and_migration_keep_required_utc_bindings(
    db: Database,
) -> None:
    expected = {"investment_event_members", "investment_match_field_resolutions"}
    actual = {
        r[0]
        for r in db.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'app'"
        ).fetchall()
    }
    assert expected <= actual
    columns = dict(
        db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'investment_event_members'"
        ).fetchall()
    )
    assert columns["created_at"] == "TIMESTAMP WITH TIME ZONE"
    assert columns["retired_at"] == "TIMESTAMP WITH TIME ZONE"
    assert columns["projection_changed_at"] == "TIMESTAMP WITH TIME ZONE"
    for name in (
        "bound_account_id",
        "bound_effective_currency",
        "account_generation",
        "security_generation",
        "currency_generation",
        "observation_version",
    ):
        assert name in columns
    db.execute("DROP TABLE app.investment_match_field_resolutions")
    db.execute("DROP TABLE app.investment_event_members")
    import_module("moneybin.sql.migrations.V082__investment_membership").migrate(db)
    recreated = dict(
        db.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'app' AND table_name = 'investment_event_members'"
        ).fetchall()
    )
    assert recreated == columns


def test_membership_revisions_keep_exact_binding_and_audit(db: Database) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )

    repo = InvestmentEventMembersRepo(db)
    original = member()
    with operation() as op:
        first = repo.register(original, actor="test")
        with pytest.raises(UserError, match="active"):
            repo.register(
                replace(original, observation_version="new_revision"), actor="test"
            )
        repo.retire(first, actor="test")
        second = repo.register(
            replace(original, observation_version="new_revision"), actor="test"
        )
    active = repo.active_members()
    assert len(active) == 1
    assert active[0]["membership_id"] == second
    assert active[0]["investment_transaction_id"] == "222222222222"
    assert active[0]["observation_version"] == "new_revision"
    assert active[0]["bound_account_id"] == "account"
    history = repo.history()
    assert len(history) == 2
    old = next(row for row in history if row["membership_id"] == first)
    assert old["observation_version"] == "manual_revision"
    assert old["account_generation"] == "account_generation"
    assert old["retired_at"] is not None
    events = AuditService(db).events_for_operation(op)
    assert [e.action for e in events] == [
        "investment_membership.register",
        "investment_membership.retire",
        "investment_membership.register",
    ]
    assert events[1].before_value is not None
    assert events[1].after_value is not None
    assert events[1].before_value["retired_at"] is None
    assert events[1].after_value["observation_version"] == "manual_revision"


def test_membership_and_audit_roll_back_in_outer_transaction(db: Database) -> None:
    from moneybin.repositories.investment_event_members_repo import (
        InvestmentEventMembersRepo,
    )

    repo = InvestmentEventMembersRepo(db)
    with operation() as op:
        db.begin()
        try:
            repo.register(member(), actor="test", in_outer_txn=True)
            raise RuntimeError("after membership write")
        except RuntimeError:
            db.rollback()
    assert repo.history() == []
    assert AuditService(db).events_for_operation(op) == []


def test_field_resolution_preserves_selected_revision_and_audit(db: Database) -> None:
    from moneybin.investments.event_membership import FieldResolution
    from moneybin.repositories.investment_match_field_resolutions_repo import (
        InvestmentMatchFieldResolutionsRepo,
    )

    repo = InvestmentMatchFieldResolutionsRepo(db)
    with operation() as op:
        resolution_id = repo.record(
            FieldResolution(
                decision_id="decision",
                investment_transaction_id="222222222222",
                field_name="trade_date",
                source_type="manual",
                source_origin="manual",
                native_reference="native",
                observation_version="manual_revision",
            ),
            actor="test",
        )
    rows = repo.active_resolutions()
    assert len(rows) == 1
    assert rows[0]["resolution_id"] == resolution_id
    assert rows[0]["observation_version"] == "manual_revision"
    assert (
        AuditService(db).events_for_operation(op)[0].action
        == "investment_resolution.select"
    )
