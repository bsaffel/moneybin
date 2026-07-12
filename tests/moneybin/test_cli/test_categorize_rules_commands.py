"""CLI wiring test for `moneybin transactions categorize rules create`.

Mirrors `test_auto_accept_allow_broad_forwards_true`
(test_categorize_auto_commands.py): `--allow-broad` on `rules create` must
forward through to `CategorizationService.create_rules(allow_broad=...)` —
the specificity-gate override itself lives in the service
(test_categorization_service_writes.py::TestCreateRulesUnselectiveContainsGate);
this test only pins the CLI-to-service wiring, which had no boundary test.
"""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.categorization import CategorizationRuleInput
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
