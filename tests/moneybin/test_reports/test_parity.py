"""Surface parity: catalog reports match the documented CLI and MCP contracts.

Locks the migrated surface — one generic MCP runner, ergonomic CLI commands,
and stable catalog IDs — against regression. Runner logic is covered by
test_definitions; masking/tier by test_execute.

The second half covers R7 of ``docs/specs/reports-dynamic.md``: a user-created
report and a built-in must reach their rows through the *same* functions in the
same order, and produce envelopes of the same shape. Asserted on the traversed
path rather than on matching output, because two forked paths can agree on one
fixture's output and diverge on the next.
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any, cast
from unittest.mock import MagicMock, patch

import typer
from fastmcp import Client, FastMCP
from typer.testing import CliRunner

from moneybin.cli.commands import reports as reports_commands
from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.catalog import ReportCatalog, get_report_catalog
from moneybin.reports._framework.contract import (
    Binding,
    ParamSpec,
    ReportQuery,
    ReportSpec,
)
from moneybin.reports._framework.dynamic import spec_from_row
from moneybin.reports._framework.execute import ReportResult
from moneybin.reports._framework.introspect import build_spec
from moneybin.reports._framework.registry import (
    register_generic_reports_tool,
    register_reports_cli,
    spec_of,
)
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.repositories.user_reports_repo import UserReportsRepo
from moneybin.services.user_reports_service import UserReportsService
from moneybin.tables import TableRef
from tests.moneybin.test_reports._metadata import TEST_SEMANTICS, output_columns

REPORTS_APP = reports_commands.app

_EXPECTED_CLI = {
    "cashflow",
    "spending",
    "recurring",
    "merchants",
    "large-transactions",
    "balance-drift",
}
_EXPECTED_CATALOG_CLI = {
    "core:balance_drift": "balance-drift",
    "core:cashflow": "cashflow",
    "core:large_transactions": "large-transactions",
    "core:merchants": "merchants",
    "core:networth": "networth",
    "core:networth_history": "networth-history",
    "core:recurring": "recurring",
    "core:spending": "spending",
}
#: R5's tier-spanning catalog/runner plus the CLI-only lifecycle verbs. These
#: share the group namespace with the generated per-report commands, so a report
#: named after one of them would be shadowed — locked below.
_EXPECTED_LIFECYCLE_CLI = {
    "list",
    "run",
    "explain",
    "create",
    "set",
    "delete",
    "reclassify",
}


def registered_report_command_names(app: typer.Typer) -> set[str]:
    """Return the public report command names registered on one Typer app."""
    return {command.name for command in app.registered_commands if command.name}


def _result(records: list[dict[str, object]]) -> ReportResult:
    return ReportResult(
        records=records,
        columns=list(records[0]) if records else [],
        output_classes={"value": DataClass.AGGREGATE},
        tier=Tier.LOW,
        total_count=len(records),
        truncated=False,
    )


async def test_mcp_surface_matches_expected_set() -> None:
    mcp = FastMCP("parity")
    register_generic_reports_tool(mcp)
    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}
    assert set(tools) == {"reports"}
    assert set(tools["reports"].inputSchema["properties"]) == {
        "report_id",
        "parameters",
        "limit",
    }


def test_cli_surface_matches_expected_set() -> None:
    app = typer.Typer()
    register_reports_cli(ALL_REPORTS, app)
    names = registered_report_command_names(app)
    assert names == _EXPECTED_CLI


def test_every_catalog_report_has_an_ergonomic_cli_command() -> None:
    assert registered_report_command_names(REPORTS_APP) == (
        set(_EXPECTED_CATALOG_CLI.values()) | _EXPECTED_LIFECYCLE_CLI
    )


def test_no_report_name_is_shadowed_by_a_lifecycle_verb() -> None:
    """A report named ``run`` would lose its generated command to the verb.

    Both live in one Typer group, and the group registers the verbs first, so a
    collision resolves silently in the verb's favour — the report would simply
    stop being invocable by name from the CLI.
    """
    report_commands = {
        report.name.replace("_", "-") for report in get_report_catalog().list()
    }

    assert report_commands & _EXPECTED_LIFECYCLE_CLI == set()


def test_catalog_ids_map_one_to_one_to_public_cli_commands() -> None:
    mapping = {
        report.report_id: report.name.replace("_", "-")
        for report in get_report_catalog().list()
    }
    assert mapping == _EXPECTED_CATALOG_CLI
    assert len(set(mapping.values())) == len(mapping)


def test_networth_preserves_flags_and_executes_through_catalog() -> None:
    help_result = CliRunner().invoke(REPORTS_APP, ["networth", "--help"])
    assert help_result.exit_code == 0, help_result.output
    assert "--as-of" in help_result.output
    assert "--account" in help_result.output
    assert "--as-of-date" not in help_result.output

    database = MagicMock()
    database_context = MagicMock()
    database_context.__enter__.return_value = database
    with (
        patch(
            "moneybin.cli.commands.reports.networth.get_database",
            return_value=database_context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
        patch("moneybin.cli.commands.reports.networth.render_or_json"),
    ):
        mock_catalog.return_value.execute.return_value = _result([{"value": 1}])
        result = CliRunner().invoke(
            REPORTS_APP,
            [
                "networth",
                "--as-of",
                "2026-07-01",
                "--account",
                "acct-a",
                "--account",
                "acct-b",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_catalog.return_value.execute.assert_called_once_with(
        database,
        report_id="core:networth",
        parameters={
            "as_of": "2026-07-01",
            "account_ids": ["acct-a", "acct-b"],
        },
        limit=1_000_000,
    )


def test_networth_history_preserves_flags_and_executes_through_catalog() -> None:
    help_result = CliRunner().invoke(REPORTS_APP, ["networth-history", "--help"])
    assert help_result.exit_code == 0, help_result.output
    assert "--from" in help_result.output
    assert "--to" in help_result.output
    assert "--interval" in help_result.output
    assert "--from-date" not in help_result.output
    assert "--to-date" not in help_result.output

    database = MagicMock()
    database_context = MagicMock()
    database_context.__enter__.return_value = database
    with (
        patch(
            "moneybin.cli.commands.reports.networth.get_database",
            return_value=database_context,
        ),
        patch("moneybin.reports._framework.catalog.get_report_catalog") as mock_catalog,
        patch("moneybin.cli.commands.reports.networth.render_or_json"),
    ):
        mock_catalog.return_value.execute.return_value = _result([{"value": 1}])
        result = CliRunner().invoke(
            REPORTS_APP,
            [
                "networth-history",
                "--from",
                "2026-01-01",
                "--to",
                "2026-07-01",
                "--interval",
                "weekly",
                "--output",
                "json",
            ],
        )

    assert result.exit_code == 0, result.output
    mock_catalog.return_value.execute.assert_called_once_with(
        database,
        report_id="core:networth_history",
        parameters={
            "from_date": "2026-01-01",
            "to_date": "2026-07-01",
            "interval": "weekly",
        },
        limit=1_000_000,
    )


def test_every_report_targets_a_reports_view() -> None:
    for runner in ALL_REPORTS:
        view = spec_of(runner).view
        # A decorated report is graph-backed by definition; only a dynamic
        # report's view is None, and those never enter ALL_REPORTS.
        assert view is not None
        assert view.schema == "reports"


# ---------------------------------------------------------------------------
# R7 — execution parity between the user tier and a built-in
# ---------------------------------------------------------------------------

#: Every shared step, as the module that *binds* the name — patching the
#: definition site leaves the caller's already-imported reference untouched.
_SHARED_STEPS = (
    ("moneybin.reports._framework.catalog", "execute_catalog_report"),
    ("moneybin.reports._framework.execute", "classify_columns"),
    ("moneybin.reports._framework.catalog", "redact_catalog_execution"),
    ("moneybin.reports._framework.execute", "redact_records"),
)

#: What one report execution traverses, in order. ``redact_records`` appears
#: twice: once for the result rows, once for the effective-parameter metadata
#: (one call per supplied parameter), and both tiers owe both.
_EXPECTED_PATH = [
    "execute_catalog_report",
    "classify_columns",
    "redact_catalog_execution",
    "redact_records",
    "redact_records",
]

_PARITY_CLASSES = {
    "account_id": DataClass.RECORD_ID,
    "routing_number": DataClass.ROUTING_NUMBER,
}
_PARITY_PARAMETERS = {"acct": "021000021"}


def _builtin_accounts(db: Database, *, acct: str) -> ReportQuery:
    """Accounts holding one routing number.

    Args:
        db: Open read-only database connection.
        acct: Routing number to filter on.
    """
    return ReportQuery(
        "SELECT account_id, routing_number FROM core.dim_accounts "
        "WHERE routing_number = ? ORDER BY account_id",
        [Binding(acct, DataClass.ROUTING_NUMBER)],
    )


def _builtin_spec() -> ReportSpec:
    """A decorator-tier spec: declared classes, positional binding, real view."""
    return build_spec(
        _builtin_accounts,
        report_id="test:accounts",
        name="accounts",
        view=TableRef("reports", "test_summary"),
        classes=_PARITY_CLASSES,
        parameter_classes={"acct": DataClass.ROUTING_NUMBER},
        columns=output_columns(_PARITY_CLASSES),
        semantics=TEST_SEMANTICS,
    )


def _saved_spec(db: Database) -> ReportSpec:
    """The same question saved as a user report: stored SQL, by-name binding.

    Deliberately different from the built-in in every way R5 and R8 permit — a
    template instead of a runner, ``$acct`` instead of ``?``, a derived class map
    instead of a declared one. What must not differ is the path below.
    """
    UserReportsService(db).create(
        name="saved_accounts",
        query_sql=(
            "SELECT account_id, routing_number FROM core.dim_accounts "
            "WHERE routing_number = $acct ORDER BY account_id"
        ),
        description="Accounts holding one routing number.",
        params=(
            ParamSpec(
                name="acct",
                annotation=str,
                default=None,
                required=True,
                help="",
                # Derived from the column it filters, never read from here.
                data_class=DataClass.UNRESOLVED,
            ),
        ),
        actor="cli",
    )
    row = UserReportsRepo(db).find_by_name("saved_accounts")
    assert row is not None
    return spec_from_row(db, row).spec


@contextmanager
def _recorded_path() -> Generator[list[str]]:
    """Record each shared step by name, in the order execution reaches it."""
    calls: list[str] = []
    patches = [
        patch(f"{module}.{name}", _recorder(calls, module, name))
        for module, name in _SHARED_STEPS
    ]
    for active in patches:
        active.start()
    try:
        yield calls
    finally:
        for active in reversed(patches):
            active.stop()


def _recorder(calls: list[str], module: str, name: str) -> Callable[..., Any]:
    """Wrap the real function so calling it appends its name to ``calls``."""
    from importlib import import_module

    real = getattr(import_module(module), name)

    def recording(*args: Any, **kwargs: Any) -> Any:
        calls.append(name)
        return real(*args, **kwargs)

    return recording


def _skeleton(value: Any) -> Any:
    """The type shape of one envelope, with every leaf replaced by its type name.

    Compares structure without comparing values: the two tiers legitimately
    differ in ``report_id`` and in the SQL they ran, and a value-wise assertion
    would only be satisfiable by making the fixtures identical — which is the
    thing R7 is not allowed to assume.
    """
    if isinstance(value, dict):
        entries = cast(dict[str, Any], value)
        return {key: _skeleton(entries[key]) for key in sorted(entries)}
    if isinstance(value, list):
        return [_skeleton(item) for item in cast(list[Any], value)]
    return type(value).__name__


def test_a_saved_report_and_a_builtin_traverse_the_same_execution_path(
    saved_db: Database,
) -> None:
    """Both tiers reach their rows through one path, and it is the expected one.

    Asserting only that the two recordings agree would pass if both tiers forked
    the same wrong way, so the path is pinned by name as well.
    """
    catalog = ReportCatalog([_builtin_spec(), _saved_spec(saved_db)])

    with _recorded_path() as builtin_path:
        catalog.execute(
            saved_db,
            report_id="test:accounts",
            parameters=_PARITY_PARAMETERS,
            limit=10,
        )
    with _recorded_path() as saved_path:
        catalog.execute(
            saved_db,
            report_id="saved_accounts",
            parameters=_PARITY_PARAMETERS,
            limit=10,
        )

    assert builtin_path == _EXPECTED_PATH
    assert saved_path == builtin_path


def test_a_saved_report_and_a_builtin_build_structurally_identical_envelopes(
    saved_db: Database,
) -> None:
    """One envelope shape, one class list, one masking outcome, both tiers."""
    catalog = ReportCatalog([_builtin_spec(), _saved_spec(saved_db)])

    results = [
        catalog.execute(
            saved_db, report_id=handle, parameters=_PARITY_PARAMETERS, limit=10
        )
        for handle in ("test:accounts", "saved_accounts")
    ]
    # `to_dict()` rather than the dataclass: it is what both surfaces put on the
    # wire, so a field that never reaches a caller cannot fake parity here.
    builtin, saved = (result.to_envelope().to_dict() for result in results)

    assert _skeleton(saved) == _skeleton(builtin)
    assert results[1].classes_returned == results[0].classes_returned
    # Same question, same rows, same column masked — the built-in from a declared
    # class, the saved report from one nobody declared.
    assert [row["routing_number"] for row in results[1].records] == ["*****"]
    assert [row["account_id"] for row in results[1].records] == [
        row["account_id"] for row in results[0].records
    ]
