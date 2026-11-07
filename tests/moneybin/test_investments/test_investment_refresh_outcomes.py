"""CLI exit status and text counts honor the investment stage outcome."""

import json
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin import error_codes
from moneybin.adapters.refresh_adapters import refresh_envelope
from moneybin.cli.main import app
from moneybin.connectors.sync_models import PlaidSourceResult, PullResult
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.orchestration.refresh import (
    RefreshResult,
    StageOutcome,
    expand_steps,
    refresh,
    step_outcome,
)
from moneybin.protocol.envelope import ResponseEnvelope
from moneybin.services.transform_service import ApplyResult
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


@pytest.fixture
def refresh_database(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> Database:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)

    @contextmanager
    def database_context(
        *args: object, **kwargs: object
    ) -> Generator[Database, None, None]:
        yield comparison_db

    monkeypatch.setattr("moneybin.database.get_database", database_context)
    return comparison_db


@pytest.mark.parametrize("flags", [[], ["--output", "json"], ["--quiet"]])
def test_blocked_requested_transform_exits_nonzero(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    flags: list[str],
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected planner failure")

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    result = CliRunner().invoke(app, ["refresh", "--step", "transform", *flags])
    assert result.exit_code == 1, result.output


def test_normal_refresh_text_shows_investment_pending_counts(
    refresh_database: Database,
) -> None:
    result = CliRunner().invoke(app, ["refresh", "--step", "investment_match"])
    assert result.exit_code == 0, result.output
    assert "1 unique" in result.output
    assert "0 competing" in result.output


def _successful_apply(_service: object) -> ApplyResult:
    return ApplyResult(applied=True, duration_seconds=0.1)


def _unavailable_stage(_service: object, **_kwargs: object) -> StageOutcome:
    """Inject a skipped stage at the carrier boundary, without running bootstrap."""
    return StageOutcome(step="investment_match", ran=False)


@pytest.mark.parametrize(
    "steps,retry",
    [
        (["transform"], "transform"),
        (["investment_match"], "investment_match"),
        (["investment_match", "match"], "investment_match"),
    ],
)
def test_planner_crash_is_stage_owned_and_retry_retains_requested_scope(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    steps: list[str],
    retry: str,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("private payload must not escape")

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    apply = MagicMock(side_effect=AssertionError("blocked apply must not run"))
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService._apply_sqlmesh", apply
    )
    result = refresh(refresh_database, steps=steps)
    assert result.error is None
    assert result.applied is False
    assert result.duration_seconds is None
    stage = result.stage("investment_match")
    assert stage is not None and stage.ran and stage.error
    assert "private payload" not in stage.error
    assert result.stage("transform") is None
    apply.assert_not_called()
    envelope = refresh_envelope(result, requested=expand_steps(steps))
    retries = [
        a.arguments for a in envelope.recovery_actions or [] if a.tool == "refresh_run"
    ]
    assert {"steps": [retry]} in retries


@pytest.mark.parametrize(
    "steps,retry",
    [
        (["investment_match"], "investment_match"),
        (["investment_match", "match"], "investment_match"),
        (["transform"], "transform"),
    ],
)
def test_unavailable_comparison_inputs_offer_retry_without_inventing_counts(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    steps: list[str],
    retry: str,
) -> None:
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService._apply_sqlmesh",
        _successful_apply,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService.prepare_investments",
        _unavailable_stage,
    )
    result = refresh(comparison_db, steps=steps)
    stage = result.stage("investment_match")
    assert stage is not None
    assert (stage.ran, stage.error, stage.counts) == (False, None, {})
    envelope = refresh_envelope(result, requested=expand_steps(steps))
    assert {"steps": [retry]} in [
        a.arguments for a in envelope.recovery_actions or [] if a.tool == "refresh_run"
    ]


def _embedded_envelopes(
    outcome: RefreshResult,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> list[ResponseEnvelope[Any]]:
    from moneybin.mcp.tools import import_inbox, import_tools, sync
    from moneybin.services.import_service import BatchImportResult, PerFileResult
    from moneybin.services.inbox_service import InboxSyncResult

    steps = step_outcome(outcome)
    pull = PullResult(
        job_id="test",
        transactions_loaded=1,
        accounts_loaded=0,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        refresh_steps=steps,
        transforms_error=outcome.error,
    )
    # Exercise the actual serialization boundary used by this embedded carrier.
    pull = PullResult.model_validate_json(pull.model_dump_json())
    sync_service = MagicMock()
    sync_service.__enter__.return_value.pull.return_value = pull
    monkeypatch.setattr(
        sync, "_build_sync_service", lambda *, local_only=False: sync_service
    )
    batch = BatchImportResult(
        per_file=[PerFileResult(path="a.csv", status="imported", source_type="csv")],
        transforms_applied=outcome.applied,
        transforms_duration_seconds=outcome.duration_seconds,
        transforms_error=outcome.error,
        refresh_steps=steps,
    )
    service = MagicMock()
    service.import_files.return_value = batch
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService",
        MagicMock(return_value=service),
    )

    def validated_path(path: str) -> Path:
        return tmp_path / path

    monkeypatch.setattr(import_tools, "_validate_file_path", validated_path)
    monkeypatch.setattr(import_tools, "get_database", MagicMock())
    inbox = MagicMock()
    inbox.sync.return_value = InboxSyncResult(
        transforms_applied=outcome.applied,
        transforms_error=outcome.error,
        refresh_steps=steps,
    )
    monkeypatch.setattr(import_inbox, "InboxService", MagicMock(return_value=inbox))
    monkeypatch.setattr(import_inbox, "get_database", MagicMock())
    monkeypatch.setattr(import_inbox, "_uncategorized_count", lambda: 0)
    return [
        sync.sync_pull(),
        import_tools.import_files(["a.csv", "b.csv"]),
        import_inbox.import_inbox_sync(),
    ]


@pytest.mark.parametrize("failure", ["transient", "refusal", "pending"])
def test_real_planner_outcome_survives_embedded_carriers(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    if failure != "pending":

        def fail(*args: object, **kwargs: object) -> None:
            if failure == "refusal":
                raise UserError(
                    "Component exceeds safe bound",
                    code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
                )
            raise RuntimeError("injected transient error")

        monkeypatch.setattr(
            "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
            fail,
        )
    steps = ["transform"] if failure != "pending" else ["investment_match"]
    result = refresh(refresh_database, steps=steps)
    envelopes = [refresh_envelope(result, requested=expand_steps(steps))]
    envelopes.extend(_embedded_envelopes(result, monkeypatch, tmp_path))
    for envelope in envelopes:
        assert envelope.data is not None
        data = envelope.to_dict()["data"]
        assert data.get("error") is None and data.get("transforms_error") is None
        stage = next(s for s in data["stages"] if s["step"] == "investment_match")
        assert set(stage) == {"step", "ran", "counts", "error"}
        retries = [
            a.arguments
            for a in envelope.recovery_actions or []
            if a.tool == "refresh_run"
        ]
        prose = " ".join(envelope.actions)
        if failure == "transient":
            assert {"steps": ["transform"]} in retries
        elif failure == "refusal":
            assert retries == []
            assert "refresh_run" not in prose
            assert "unchanged" in prose
        else:
            assert stage["counts"]["pending_unique"] == 1
            assert 'reviews(kind="investment_matches", status="pending")' in prose


def test_component_refusal_cli_does_not_recommend_unchanged_retry(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> None:
        raise UserError(
            "Component exceeds safe bound",
            code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
        )

    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    result = CliRunner().invoke(app, ["refresh", "--step", "transform"])
    assert result.exit_code == 1
    assert "Re-run the failed step" not in result.output
    assert "unchanged" in result.output


def test_component_refusal_preserves_independent_cash_retry(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise UserError(
            "Component exceeds safe bound",
            code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
        )

    def fail_cash(*args: object, **kwargs: object) -> None:
        raise RuntimeError("cash matcher transient failure")

    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.run", fail_cash
    )
    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        refuse,
    )
    result = refresh(refresh_database, steps=["match", "transform"])
    envelopes = [
        refresh_envelope(result, requested=expand_steps(["match", "transform"]))
    ]
    envelopes.extend(_embedded_envelopes(result, monkeypatch, tmp_path))
    for envelope in envelopes:
        assert [
            a.arguments
            for a in envelope.recovery_actions or []
            if a.tool == "refresh_run"
        ] == [{"steps": ["match"]}]


def test_unavailable_inputs_keep_transform_recovery_through_embedded_carriers(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService._apply_sqlmesh",
        _successful_apply,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService.prepare_investments",
        _unavailable_stage,
    )
    result = refresh(comparison_db, steps=["transform"])
    for envelope in _embedded_envelopes(result, monkeypatch, tmp_path):
        data = envelope.to_dict()["data"]
        stage = next(s for s in data["stages"] if s["step"] == "investment_match")
        assert stage == {
            "step": "investment_match",
            "ran": False,
            "error": None,
            "counts": {},
        }
        assert {"steps": ["transform"]} in [
            a.arguments
            for a in envelope.recovery_actions or []
            if a.tool == "refresh_run"
        ]


@pytest.mark.parametrize("failure", ["transient", "refusal", "pending"])
@pytest.mark.parametrize("surface", ["sync", "import", "inbox"])
def test_cli_embedded_json_keeps_stage_recovery_and_pending_actions(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
    surface: str,
) -> None:
    from moneybin.services.import_service import BatchImportResult, PerFileResult
    from moneybin.services.inbox_service import InboxSyncResult

    if failure != "pending":

        def fail(*args: object, **kwargs: object) -> None:
            if failure == "refusal":
                raise UserError(
                    "Component exceeds safe bound",
                    code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
                )
            raise RuntimeError("injected transient failure")

        monkeypatch.setattr(
            "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
            fail,
        )
    result = refresh(
        refresh_database,
        steps=["investment_match"] if failure == "pending" else ["transform"],
    )
    outcome = step_outcome(result)
    pull = PullResult(
        job_id="test",
        ack_state="confirmed",
        setup_state="complete",
        refresh_state="complete" if failure == "pending" else "failed",
        transactions_loaded=1,
        accounts_loaded=0,
        balances_loaded=0,
        transactions_removed=0,
        institutions=[],
        refresh_steps=outcome,
        source_results=[
            PlaidSourceResult.model_validate({
                "provider": "plaid",
                "connection_id": "22222222-2222-4222-8222-222222222222",
                "credential_generation": 1,
                "outcomes": [
                    {
                        "account_id": "synthetic-account",
                        "native_scope": {
                            "kind": "plaid_item",
                            "provider_item_id": "synthetic-item",
                        },
                        "window": None,
                        "status": "complete",
                        "error_code": None,
                    }
                ],
            })
        ],
    )
    sync_service = MagicMock()
    sync_service.__enter__.return_value.pull.return_value = pull
    monkeypatch.setattr(
        "moneybin.cli.commands.sync._build_sync_service",
        lambda *, local_only=False: sync_service,
    )
    batch = BatchImportResult(
        per_file=[PerFileResult(path="a.csv", status="imported", source_type="csv")],
        transforms_applied=False,
        transforms_duration_seconds=None,
        refresh_steps=outcome,
    )
    import_service = MagicMock()
    import_service.import_files.return_value = batch
    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService",
        MagicMock(return_value=import_service),
    )
    inbox = MagicMock()
    inbox.sync.return_value = InboxSyncResult(refresh_steps=outcome)
    monkeypatch.setattr(
        "moneybin.cli.commands.import_inbox.InboxService", MagicMock(return_value=inbox)
    )
    commands = {
        "sync": ["sync", "pull"],
        "import": ["import", "files", str(tmp_path / "a.csv"), str(tmp_path / "b.csv")],
        "inbox": ["import", "inbox"],
    }
    cli = CliRunner().invoke(app, [*commands[surface], "--output", "json"])
    assert cli.exit_code == (0 if failure == "pending" else 1), cli.output
    envelope = json.loads(cli.stdout)
    stage = next(
        s for s in envelope["data"]["stages"] if s["step"] == "investment_match"
    )
    assert set(stage) == {"step", "ran", "counts", "error"}
    retries = [
        a["arguments"]
        for a in envelope.get("recovery_actions", [])
        if a["tool"] == "refresh_run"
    ]
    prose = " ".join(envelope["actions"])
    if failure == "transient":
        assert retries == [{"steps": ["transform"]}]
    elif failure == "refusal":
        assert retries == []
        assert "refresh_run" not in prose
        assert "unchanged" in prose
    else:
        assert 'reviews(kind="investment_matches", status="pending")' in prose


def test_skipped_planner_and_failed_apply_retry_transform_without_cash_retry(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def fail_cash(*args: object, **kwargs: object) -> None:
        raise RuntimeError("transient cash failure")

    def fail_apply(_service: object) -> ApplyResult:
        return ApplyResult(applied=False, duration_seconds=0.1, error="apply failed")

    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.run", fail_cash
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService._apply_sqlmesh",
        fail_apply,
    )
    monkeypatch.setattr(
        "moneybin.orchestration.refresh.TransformService.prepare_investments",
        _unavailable_stage,
    )
    result = refresh(comparison_db, steps=["match", "transform"])
    envelopes = [
        refresh_envelope(result, requested=expand_steps(["match", "transform"]))
    ]
    envelopes.extend(_embedded_envelopes(result, monkeypatch, tmp_path))
    for envelope in envelopes:
        assert [
            a.arguments
            for a in envelope.recovery_actions or []
            if a.tool == "refresh_run"
        ] == [{"steps": ["transform"]}]


@pytest.mark.parametrize("refusal", [False, True])
@pytest.mark.parametrize("surface", ["cli", "mcp"])
def test_actual_single_file_import_preserves_blocked_planner_stage(
    refresh_database: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    refusal: bool,
    surface: str,
) -> None:
    from moneybin.services.import_service import ImportResult

    def fail(*args: object, **kwargs: object) -> None:
        if refusal:
            raise UserError(
                "Component exceeds safe bound",
                code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
            )
        raise RuntimeError("injected transient failure")

    source = tmp_path / "single.csv"
    source.write_text("date,amount,description\n2026-01-01,-1,Test\n")

    def import_one(*_args: object, **_kwargs: object) -> ImportResult:
        return ImportResult(
            file_path=str(source),
            file_type="tabular",
            transactions=1,
            import_id="import-test",
        )

    def scoped_refresh(db: Database) -> RefreshResult:
        return refresh(db, steps=["transform"])

    monkeypatch.setattr(
        "moneybin.services.import_service.ImportService._import_one",
        import_one,
    )
    monkeypatch.setattr(
        "moneybin.services.import_service._refresh",
        scoped_refresh,
    )
    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.InvestmentMatchingService.run",
        fail,
    )
    if surface == "cli":
        cli = CliRunner().invoke(
            app, ["import", "files", str(source), "--output", "json"]
        )
        assert cli.exit_code == 1, cli.output
        assert cli.stdout.strip(), (
            "A durable import must retain its outcome and planner stage"
        )
        envelope = json.loads(cli.stdout)
    else:
        from moneybin.mcp.tools import import_tools

        @contextmanager
        def database_context(**_kwargs: object) -> Generator[Database, None, None]:
            yield refresh_database

        def validated_path(path: str) -> Path:
            return Path(path)

        monkeypatch.setattr(import_tools, "get_database", database_context)
        monkeypatch.setattr(import_tools, "_validate_file_path", validated_path)
        monkeypatch.setattr("moneybin.orchestration.refresh.refresh", scoped_refresh)
        envelope = import_tools.import_files([str(source)]).to_dict()
    assert envelope["data"]["imported_count"] == 1
    assert envelope["data"]["files"][0]["import_id"] == "import-test"
    assert envelope["data"].get("transforms_error") is None
    assert envelope["data"]["transforms_applied"] is False
    stage = next(
        s for s in envelope["data"]["stages"] if s["step"] == "investment_match"
    )
    assert stage["ran"] and stage["error"]
    retries = [
        a["arguments"]
        for a in envelope.get("recovery_actions", [])
        if a["tool"] == "refresh_run"
    ]
    assert retries == ([] if refusal else [{"steps": ["transform"]}])
