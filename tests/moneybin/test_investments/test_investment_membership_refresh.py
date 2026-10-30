"""Every full transform proves membership before executing Golden models."""

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.orchestration.refresh import refresh
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.investment_matching_service import InvestmentPlanResult
from moneybin.services.investment_membership_service import (
    MembershipReconciliationResult,
)
from moneybin.services.transform_service import ApplyResult, TransformService


def test_fresh_bootstrap_builds_inputs_without_golden_execution_or_membership(
    db: Database,
) -> None:
    TransformService(db).bootstrap_investment_inputs()
    tables = {
        f"{s}.{t}"
        for s, t in db.execute(
            "SELECT table_schema, table_name FROM information_schema.tables"
        ).fetchall()
    }
    assert {
        "prep.int_investment_events__headers",
        "prep.int_investment_events__legs",
        "prep.int_investment_events__evidence",
    } <= tables
    assert "core.fct_investment_transactions" not in tables
    assert InvestmentEventMembersRepo(db).history() == []
    if "meta.model_freshness" in tables:
        assert not db.execute("""SELECT 1 FROM meta.model_freshness
            WHERE model_name = 'core.fct_investment_transactions' AND last_executed_at IS NOT NULL""").fetchall()


def _pipeline_spies(
    monkeypatch: pytest.MonkeyPatch, events: list[str], failure: str | None = None
) -> None:
    def bootstrap(_self: object) -> None:
        events.append("bootstrap")
        if failure == "bootstrap":
            raise RuntimeError("injected bootstrap failure")

    def planner(_self: object, **kwargs: Any) -> InvestmentPlanResult:
        events.append("planner")
        if failure == "planner":
            raise RuntimeError("injected planner failure")
        return InvestmentPlanResult(
            proposed=0, stale=0, suppressed=0, pending_unique=0, pending_competing=0
        )

    def membership(_self: object, **kwargs: Any) -> MembershipReconciliationResult:
        events.append("membership")
        if failure == "membership":
            raise RuntimeError("injected membership failure")
        return MembershipReconciliationResult()

    def apply(_self: object) -> ApplyResult:
        events.append("apply")
        return ApplyResult(applied=True, duration_seconds=0.1)

    monkeypatch.setattr(
        TransformService, "bootstrap_investment_inputs", bootstrap, raising=False
    )
    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        planner,
    )
    monkeypatch.setattr(
        "moneybin.services.investment_membership_service.InvestmentMembershipService.reconcile",
        membership,
    )
    monkeypatch.setattr(TransformService, "_apply_sqlmesh", apply, raising=False)


def test_full_transform_order_is_bootstrap_plan_membership_apply(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _pipeline_spies(monkeypatch, events)
    result = refresh(db, steps=["transform"])
    assert result.applied
    assert events == ["bootstrap", "planner", "membership", "apply"]


@pytest.mark.parametrize("failure", ["bootstrap", "planner", "membership"])
def test_prerequisite_failure_prevents_golden_apply_and_acknowledgement(
    db: Database, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    events: list[str] = []
    _pipeline_spies(monkeypatch, events, failure)
    result = refresh(db, steps=["transform"])
    assert not result.applied
    stage = result.stage("investment_match")
    assert stage is not None and stage.ran and stage.error
    assert result.stage("transform") is None
    assert "apply" not in events
    assert InvestmentEventMembersRepo(db).history() == []


def test_planner_only_bootstraps_but_never_registers_or_applies(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _pipeline_spies(monkeypatch, events)
    result = refresh(db, steps=["investment_match"])
    assert not result.applied
    assert events == ["bootstrap", "planner"]
    assert InvestmentEventMembersRepo(db).history() == []


def test_public_apply_owns_prerequisites_without_calling_orchestration(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    _pipeline_spies(monkeypatch, events)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("service called orchestration")

    monkeypatch.setattr("moneybin.orchestration.refresh.refresh", forbidden)
    result = TransformService(db).apply()
    assert result.applied
    assert events == ["bootstrap", "planner", "membership", "apply"]


@pytest.mark.parametrize("failure", ["bootstrap", "planner", "membership"])
def test_public_apply_fails_closed_on_prerequisite_failure(
    db: Database, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    events: list[str] = []
    _pipeline_spies(monkeypatch, events, failure)

    result = TransformService(db).apply()

    assert not result.applied and result.error
    assert "apply" not in events
    assert InvestmentEventMembersRepo(db).history() == []


@pytest.mark.parametrize("entry", ["cli", "mcp", "import"])
def test_full_transform_entries_preserve_prerequisite_failure_owner(
    db: Database, monkeypatch: pytest.MonkeyPatch, entry: str
) -> None:
    import json
    from contextlib import contextmanager

    from moneybin.database import SchemaDriftError
    from moneybin.services.import_service import ImportService

    events: list[str] = []
    _pipeline_spies(monkeypatch, events, "planner")

    @contextmanager
    def database(**kwargs: Any):
        yield db

    monkeypatch.setattr("moneybin.database.get_database", database)
    if entry == "cli":
        from typer.testing import CliRunner

        from moneybin.cli.commands.transform import app

        result = CliRunner().invoke(app, ["apply", "--output", "json"])
        assert result.exit_code == 1, result.output
        data = json.loads(result.stdout)["data"]
        assert not data["applied"]
        assert any(
            stage["step"] == "investment_match" and stage["error"]
            for stage in data["stages"]
        )
        assert data.get("error") is None
    elif entry == "mcp":
        from moneybin.mcp.server import check_schema_at_boot

        def drift(_db: Database) -> set[str]:
            return {"core.dim_accounts"}

        monkeypatch.setattr("moneybin.database.check_core_schema_drift", drift)
        with pytest.raises(SchemaDriftError, match="investment_match"):
            check_schema_at_boot()
    else:
        with pytest.raises(RuntimeError, match="investment_match"):
            ImportService(db).run_transforms()
    assert events == ["bootstrap", "planner"]


def test_bootstrap_discovers_a_new_comparison_dependency_from_fresh_configuration(
    db: Database, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    import shutil
    from contextlib import contextmanager
    from pathlib import Path

    from sqlmesh.core.dialect import parse

    from moneybin.database import sqlmesh_context

    root = tmp_path / "sqlmesh"
    shutil.copytree(
        Path("src/moneybin/sqlmesh"),
        root,
        ignore=shutil.ignore_patterns(".cache", "__pycache__"),
    )
    model = root / "models/prep/int_investment_events__headers.sql"
    source = model.read_text()
    query = parse(source, default_dialect="duckdb")[-1]
    model.write_text(
        source[: source.index(");") + 2]
        + "\nSELECT original.* FROM ("
        + query.sql(dialect="duckdb")
        + ") AS original CROSS JOIN prep.synthetic_comparison_input"
    )
    (root / "models/prep/synthetic_comparison_input.sql").write_text(
        "MODEL (name prep.synthetic_comparison_input, kind VIEW); SELECT 1 AS n"
    )

    @contextmanager
    def context(database: Database):
        with sqlmesh_context(database, sqlmesh_root=root) as ctx:
            yield ctx

    monkeypatch.setattr("moneybin.services.transform_service.sqlmesh_context", context)
    TransformService(db).bootstrap_investment_inputs()
    assert db.execute("SELECT * FROM prep.synthetic_comparison_input").fetchall() == [
        (1,)
    ]
    assert not db.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'core' AND table_name = 'fct_investment_transactions'"
    ).fetchall()
    assert InvestmentEventMembersRepo(db).history() == []
