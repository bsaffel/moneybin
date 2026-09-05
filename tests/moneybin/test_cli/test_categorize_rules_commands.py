"""CLI wiring test for `moneybin transactions categorize rules create`.

Mirrors `test_auto_accept_allow_broad_forwards_true`
(test_categorize_auto_commands.py): `--allow-broad` on `rules create` must
forward through to `CategorizationService.create_rules(allow_broad=...)` —
the specificity-gate override itself lives in the service
(test_categorization_service_writes.py::TestCreateRulesUnselectiveContainsGate);
this test only pins the CLI-to-service wiring, which had no boundary test.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.categorization import (
    CategorizationRuleInput,
    ConflictDecision,
    ConflictDecisionResult,
)
from moneybin.services.categorization.applier import RuleCreationResult

runner = CliRunner()

_EXPECTED_ITEM = CategorizationRuleInput(
    name="Transfer TO",
    merchant_pattern="TO",
    category="Transfer",
    subcategory="Internal Transfer",
    match_type="contains",
)

_ARGS = [
    "rules",
    "create",
    "Transfer TO",
    "--pattern",
    "TO",
    "--category",
    "Transfer",
    "--subcategory",
    "Internal Transfer",
    "--match-type",
    "contains",
]


def _rule_result() -> RuleCreationResult:
    return RuleCreationResult(
        created=1, existing=0, skipped=0, error_details=[], rule_ids=["r1"]
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_create_allow_broad_forwards_true(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--allow-broad forwards allow_broad=True to create_rules()."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.create_rules.return_value = _rule_result()

    result = runner.invoke(app, [*_ARGS, "--allow-broad"])

    assert result.exit_code == 0, result.output
    svc.create_rules.assert_called_once_with(
        [_EXPECTED_ITEM], reapply=False, actor="cli", allow_broad=True
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_create_allow_broad_defaults_to_false(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Without the flag, create_rules() is called with allow_broad=False."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.create_rules.return_value = _rule_result()

    result = runner.invoke(app, _ARGS)

    assert result.exit_code == 0, result.output
    svc.create_rules.assert_called_once_with(
        [_EXPECTED_ITEM], reapply=False, actor="cli", allow_broad=False
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_apply_runs_only_the_rules_engine(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """The rules subcommand must not apply merchants or provider-native data."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.categorize_run.return_value = {
        "applied_by_method": {"rules": 2},
        "total_applied": 2,
    }

    result = runner.invoke(app, ["rules", "apply"])

    assert result.exit_code == 0, result.output
    svc.categorize_run.assert_called_once_with(methods=["rules"])
    svc.categorize_pending.assert_not_called()


# --- rules resolve (MB-124) -------------------------------------------------


def _resolution(
    conflict_id: str = "conf_aaaaaaaaaaaaaaaa",
    resolution: str = "replace",
    rule_id: str | None = "r2",
) -> ConflictDecisionResult:
    return ConflictDecisionResult(
        conflict_id=conflict_id,
        resolution=resolution,  # pyright: ignore[reportArgumentType]  # test literal
        rule_id=rule_id,
        superseded_rule_id="r1" if resolution == "replace" else None,
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_forwards_one_replace_decision(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.resolve_rule_conflicts.return_value = [_resolution()]

    result = runner.invoke(
        app, ["rules", "resolve", "conf_aaaaaaaaaaaaaaaa", "--replace", "--yes"]
    )

    assert result.exit_code == 0, result.output
    svc.resolve_rule_conflicts.assert_called_once_with(
        [
            ConflictDecision(
                conflict_id="conf_aaaaaaaaaaaaaaaa",
                resolution="replace",
                priority=None,
            )
        ],
        actor="cli",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_forwards_the_reprioritize_priority(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.resolve_rule_conflicts.return_value = [_resolution(resolution="reprioritize")]

    result = runner.invoke(
        app,
        [
            "rules",
            "resolve",
            "conf_aaaaaaaaaaaaaaaa",
            "--reprioritize",
            "10",
            "--yes",
        ],
    )

    assert result.exit_code == 0, result.output
    svc.resolve_rule_conflicts.assert_called_once_with(
        [
            ConflictDecision(
                conflict_id="conf_aaaaaaaaaaaaaaaa",
                resolution="reprioritize",
                priority=10,
            )
        ],
        actor="cli",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_reads_a_batch_file(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, tmp_path: Path
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.resolve_rule_conflicts.return_value = [
        _resolution(),
        _resolution("conf_bbbbbbbbbbbbbbbb", "cancel", None),
    ]
    batch = tmp_path / "resolutions.json"
    batch.write_text(
        json.dumps([
            {"conflict_id": "conf_aaaaaaaaaaaaaaaa", "resolution": "replace"},
            {"conflict_id": "conf_bbbbbbbbbbbbbbbb", "resolution": "cancel"},
        ])
    )

    result = runner.invoke(
        app, ["rules", "resolve", "--from-file", str(batch), "--yes"]
    )

    assert result.exit_code == 0, result.output
    svc.resolve_rule_conflicts.assert_called_once_with(
        [
            ConflictDecision(
                conflict_id="conf_aaaaaaaaaaaaaaaa",
                resolution="replace",
                priority=None,
            ),
            ConflictDecision(
                conflict_id="conf_bbbbbbbbbbbbbbbb",
                resolution="cancel",
                priority=None,
            ),
        ],
        actor="cli",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_refuses_two_resolutions(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(
        app,
        ["rules", "resolve", "conf_aaaaaaaaaaaaaaaa", "--replace", "--cancel", "-y"],
    )

    assert result.exit_code == 2, result.output
    mock_svc_cls.return_value.resolve_rule_conflicts.assert_not_called()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_refuses_no_resolution(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(app, ["rules", "resolve", "conf_aaaaaaaaaaaaaaaa", "-y"])

    assert result.exit_code == 2, result.output
    mock_svc_cls.return_value.resolve_rule_conflicts.assert_not_called()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_declined_confirmation_changes_nothing(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A human who says no must not have the batch applied anyway."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(
        app, ["rules", "resolve", "conf_aaaaaaaaaaaaaaaa", "--replace"], input="n\n"
    )

    assert result.exit_code == 0, result.output
    mock_svc_cls.return_value.resolve_rule_conflicts.assert_not_called()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_resolve_missing_batch_file_exits_two(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, tmp_path: Path
) -> None:
    mock_get_db.return_value.__enter__.return_value = MagicMock()

    result = runner.invoke(
        app,
        ["rules", "resolve", "--from-file", str(tmp_path / "absent.json"), "-y"],
    )

    assert result.exit_code == 2, result.output
    mock_svc_cls.return_value.resolve_rule_conflicts.assert_not_called()


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.rules.get_database")
def test_rules_create_reports_a_conflict_without_failing(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A refused rule is a decision to make, not a command that failed."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.create_rules.return_value = RuleCreationResult(
        created=0,
        existing=0,
        skipped=0,
        error_details=[],
        rule_ids=[],
        conflicts=1,
        conflict_ids=["conf_aaaaaaaaaaaaaaaa"],
        conflict_details=[
            {
                "conflict_id": "conf_aaaaaaaaaaaaaaaa",
                "name": "Transfer TO",
                "existing_rule_id": "r1",
                "reason": "Rule r1 already matches this pattern.",
            }
        ],
    )

    result = runner.invoke(app, [*_ARGS, "--output", "json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "conflict"
    assert payload["data"]["conflicts"] == 1
