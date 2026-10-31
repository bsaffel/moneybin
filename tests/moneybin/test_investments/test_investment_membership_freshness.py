"""Committed membership cannot be presented through stale dependent models."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
)


@pytest.fixture()
def published(comparison_db: Database, monkeypatch: pytest.MonkeyPatch) -> Database:
    seed_manual_event(comparison_db, "manual")
    install_comparison_models(comparison_db)
    InvestmentMembershipService(comparison_db).reconcile(actor="test")

    def dependent_models(_name: str) -> frozenset[str]:
        return frozenset({
            "core.fct_investment_transactions",
            "core.fct_realized_gains",
            "core.fct_investment_lots",
            "core.dim_holdings",
        })

    monkeypatch.setattr(
        "moneybin.sqlmesh_registry.relations_downstream_of", dependent_models
    )
    monkeypatch.setattr(
        "moneybin.sqlmesh_registry.materialized_model_names",
        lambda: frozenset({
            "core.fct_investment_transactions",
            "core.fct_realized_gains",
            "core.fct_investment_lots",
        }),
    )
    for name in (
        "fct_investment_transactions",
        "fct_investment_lots",
        "fct_realized_gains",
        "dim_holdings",
    ):
        comparison_db.execute(
            f"CREATE TABLE core.{name} (investment_transaction_id VARCHAR)"
        )
    comparison_db.execute("CREATE SCHEMA IF NOT EXISTS meta")
    comparison_db.execute(
        "CREATE TABLE meta.model_freshness (model_name VARCHAR, last_executed_at TIMESTAMPTZ)"
    )
    return comparison_db


def acknowledge(
    db: Database, *, omit: str | None = None, old: str | None = None
) -> None:
    db.execute("DELETE FROM meta.model_freshness")
    for name in (
        "core.fct_investment_transactions",
        "core.fct_realized_gains",
        "core.fct_investment_lots",
        "core.dim_holdings",
    ):
        if name == omit:
            continue
        # A read-through VIEW can have an old creation interval; tables must rebuild.
        stamp = datetime.now(UTC) + timedelta(seconds=1)
        if name in {old, "core.dim_holdings"}:
            stamp = datetime(2000, 1, 1, tzinfo=UTC)
        db.execute("INSERT INTO meta.model_freshness VALUES (?, ?)", [name, stamp])


@pytest.mark.parametrize("incomplete", ["absent", "missing", "never", "old"])
def test_complete_dependent_execution_is_required(
    published: Database, incomplete: str
) -> None:
    from moneybin.investments.freshness import (
        projection_pending,
        require_current_relations,
    )

    acknowledge(
        published,
        omit="core.fct_realized_gains" if incomplete == "missing" else None,
        old="core.fct_realized_gains" if incomplete == "old" else None,
    )
    if incomplete == "absent":
        published.execute("DELETE FROM meta.model_freshness")
    if incomplete == "never":
        published.execute(
            "UPDATE meta.model_freshness SET last_executed_at = NULL WHERE model_name = 'core.fct_realized_gains'"
        )
    assert projection_pending(published)
    with pytest.raises(UserError) as caught:
        require_current_relations(published, {"core.dim_holdings"})
    assert caught.value.code == "refresh_stale"
    assert "moneybin refresh --step transform" in str(caught.value.hint)


def test_recovery_requires_all_models_but_view_creation_is_not_execution(
    published: Database,
) -> None:
    from moneybin.investments.freshness import projection_pending

    assert projection_pending(published)
    acknowledge(published)
    assert not projection_pending(published)


def test_retirement_still_requires_a_rebuild(published: Database) -> None:
    from moneybin.investments.freshness import projection_pending

    members = InvestmentEventMembersRepo(published).active_members()
    for member in members:
        InvestmentEventMembersRepo(published).retire(
            member["membership_id"], actor="test"
        )
    assert not InvestmentEventMembersRepo(published).active_members()
    assert projection_pending(published)
    acknowledge(published)
    assert not projection_pending(published)


def test_read_through_price_arrival_does_not_invalidate_membership(
    published: Database,
) -> None:
    from moneybin.investments.freshness import projection_pending

    acknowledge(published)
    published.execute(
        """INSERT INTO raw.security_prices (
            provider_security_key, price_date, quote_currency, source_type,
            source_origin, close, price_basis, extracted_at, loaded_at
        ) VALUES ('native_security', DATE '2026-01-01', 'USD', 'plaid',
                  'origin', 100, 'raw', ?, ?)""",
        [datetime.now(UTC), datetime.now(UTC) + timedelta(seconds=2)],
    )
    assert not projection_pending(published)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 1 FROM core.fct_investment_transactions",
        "SELECT COUNT(*) FROM core.fct_realized_gains",
        "WITH x AS (SELECT * FROM core.dim_holdings) SELECT 1 FROM x",
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM core.fct_investment_transactions)",
        "SELECT * FROM reports.synthetic_nested",
    ],
)
def test_query_lineage_blocks_direct_indirect_and_nested_reads(
    published: Database, query: str
) -> None:
    from moneybin.investments.freshness import require_current_query

    published.execute("CREATE SCHEMA IF NOT EXISTS reports")
    published.execute(
        "CREATE VIEW reports.synthetic_nested AS SELECT 1 AS n FROM core.fct_investment_transactions"
    )
    with pytest.raises(UserError) as caught:
        require_current_query(published, query)
    assert caught.value.code == "refresh_stale"


def test_unrelated_reads_stay_available(published: Database) -> None:
    from moneybin.investments.freshness import (
        require_current_query,
        require_current_relations,
    )

    require_current_query(published, "SELECT 1 AS n")
    require_current_query(
        published,
        "WITH fct_investment_transactions AS (SELECT 1 AS n) SELECT n FROM fct_investment_transactions",
    )
    require_current_relations(published, {"core.dim_accounts"})


@pytest.mark.parametrize(
    "entry", ["sql", "report", "bundle", "list_events", "holdings", "lots", "gains"]
)
def test_shared_read_owners_refuse_stale_membership(
    published: Database, entry: str
) -> None:
    from unittest.mock import MagicMock

    from moneybin.exports.service import ExportService
    from moneybin.privacy.sql_query import execute_sql_query
    from moneybin.reports._framework.execute import execute_catalog_report
    from moneybin.services.investment_service import InvestmentService

    with pytest.raises(UserError) as caught:
        if entry == "sql":
            execute_sql_query(
                published, "SELECT 1 FROM core.fct_investment_transactions", max_rows=10
            )
        elif entry == "report":
            spec = MagicMock()
            spec.runner.return_value.sql = (
                "SELECT 1 FROM core.fct_investment_transactions"
            )
            spec.runner.return_value.params = []
            execute_catalog_report(spec, published, max_rows=10)
        elif entry == "bundle":
            ExportService(published).prepare_bundle(profile="test")
        else:
            getattr(InvestmentService(published), entry)()
    assert caught.value.code == "refresh_stale"


@pytest.mark.parametrize("batch", [False, True])
def test_postcommit_rebuild_failure_keeps_accepted_decision_durable(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, batch: bool
) -> None:
    from moneybin.protocol.write_contracts import InvestmentMatchDecisionRequest
    from moneybin.services.review_decisions_service import ReviewDecisionsService
    from tests.moneybin.test_investments.test_investment_match_acceptance import (
        registered_proposal,
    )

    matching, proposal, _ = registered_proposal(comparison_db)
    calls: list[str] = []

    def failed(database: Database, inputs: Any, **kwargs: Any) -> None:
        assert matching.history()[0]["status"] == "accepted"
        assert {
            r["decision_id"]
            for r in InvestmentEventMembersRepo(database).active_members()
        } == {proposal}
        calls.append(kwargs["committed_change"])
        raise UserError(
            "The investment decision was committed; rebuild failed.",
            code="refresh_model_failed",
        )

    monkeypatch.setattr("moneybin.services.restate.restate_after_write", failed)
    with pytest.raises(UserError, match="committed"):
        if batch:
            ReviewDecisionsService(comparison_db, actor="test").apply_ordinary([
                InvestmentMatchDecisionRequest(
                    kind="investment_match", decision_id=proposal, decision="accept"
                )
            ])
        else:
            matching.decide(proposal, decision="accept", actor="test")
    assert calls == ["investment decision"]
    assert matching.history()[0]["status"] == "accepted"
    assert len(InvestmentMembershipService(comparison_db).project()) == 1


@pytest.mark.parametrize("deferred_recovery", ["apply", "restate"])
def test_real_refresh_recovers_committed_acceptance_and_is_idempotent(
    db: Database, monkeypatch: pytest.MonkeyPatch, deferred_recovery: str
) -> None:
    from moneybin.investments.freshness import projection_pending
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from moneybin.services.transform_service import TransformService
    from tests.moneybin.test_stg_plaid_investments import (
        _link_security,  # pyright: ignore[reportPrivateUsage]  # reuse synthetic financial fixture
        _manual_investment_txn,  # pyright: ignore[reportPrivateUsage]  # reuse synthetic financial fixture
        _publish_investment_fixture,  # pyright: ignore[reportPrivateUsage]  # reuse synthetic financial fixture
        _raw_investment_txn,  # pyright: ignore[reportPrivateUsage]  # reuse synthetic financial fixture
    )

    _raw_investment_txn(db)
    _link_security(db, "sec_1", "canonical_security")
    _manual_investment_txn(
        db,
        account_id="acc_1",
        security_id="canonical_security",
        trade_date="2026-07-06",
        quantity="10",
        price="214.55",
        amount="-2145.50",
        fees="0",
    )
    _publish_investment_fixture(db)
    assert not projection_pending(db)
    matching = InvestmentMatchingService(db)
    assert len(matching.pending()) == 1
    proposal = matching.pending()[0]["proposal_id"]

    def fail(*args: Any, **kwargs: Any) -> None:
        raise UserError(
            "Decision committed; rebuild interrupted.", code="refresh_model_failed"
        )

    with monkeypatch.context() as patch:
        patch.setattr("moneybin.services.restate.restate_after_write", fail)
        with pytest.raises(UserError, match="committed"):
            matching.decide(proposal, decision="accept", actor="test")
    assert matching.history()[0]["status"] == "accepted"
    assert projection_pending(db)
    before = InvestmentEventMembersRepo(db).history()
    result = TransformService(db).apply()
    assert result.applied, result.error
    assert not projection_pending(db)
    assert db.execute(
        "SELECT COUNT(*) FROM core.fct_investment_transactions"
    ).fetchone() == (1,)
    assert InvestmentEventMembersRepo(db).history() == before

    # A deferred source delivery must not masquerade as current before reconciliation.
    from datetime import date
    from decimal import Decimal

    from moneybin.connectors.sync_models import SyncInvestmentTransaction
    from moneybin.extractors.plaid.extractor import PlaidExtractor

    arrived = datetime.now(UTC)
    incoming = SyncInvestmentTransaction(
        investment_transaction_id="incoming",
        account_id="acc_1",
        provider_item_id="item_1",
        security_id="sec_1",
        date=date(2026, 7, 7),
        name="New delivery",
        quantity=Decimal(1),
        amount=Decimal("214.55"),
        price=Decimal("214.55"),
        fees=Decimal(0),
        iso_currency_code="USD",
        type="buy",
        subtype="buy",
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # synthetic deferred delivery
        [incoming], "new_delivery", arrived, arrived
    )
    assert projection_pending(db)
    from moneybin.tables import ACCOUNT_SETTINGS

    recovered = (
        TransformService(db).apply()
        if deferred_recovery == "apply"
        else TransformService(db).restate_models([ACCOUNT_SETTINGS.full_name])
    )
    assert recovered.applied, recovered.error
    assert not projection_pending(db)
    assert any(
        row["native_reference"] == "incoming"
        for row in InvestmentEventMembersRepo(db).active_members()
    )


def test_populated_pre_cut_warehouse_is_not_current_without_membership_projection(
    db: Database,
) -> None:
    from moneybin.investments.freshness import (
        projection_pending,
        require_current_relations,
    )

    # A previous release's populated SQLMesh environment, before initial registration.
    db.execute("CREATE SCHEMA IF NOT EXISTS sqlmesh")
    db.execute("CREATE TABLE sqlmesh._environments (name VARCHAR)")
    db.execute("INSERT INTO sqlmesh._environments VALUES ('prod')")
    db.execute(
        "CREATE TABLE core.fct_investment_transactions (investment_transaction_id VARCHAR)"
    )
    db.execute(
        "INSERT INTO core.fct_investment_transactions VALUES ('legacy_source_id')"
    )
    assert not InvestmentEventMembersRepo(db).has_history()
    assert projection_pending(db)
    with pytest.raises(UserError) as caught:
        require_current_relations(db, {"core.fct_investment_transactions"})
    assert caught.value.code == "refresh_stale"


def test_successful_empty_profile_refresh_acknowledges_the_hard_cut(
    db: Database,
) -> None:
    from moneybin.investments.freshness import projection_pending
    from moneybin.services.transform_service import TransformService

    result = TransformService(db).apply()
    assert result.applied, result.error
    assert not InvestmentEventMembersRepo(db).has_history()
    assert not projection_pending(db)
