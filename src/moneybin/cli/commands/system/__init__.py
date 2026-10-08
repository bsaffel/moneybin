"""system — system and data status meta-view."""

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database

from . import audit as _audit
from . import doctor as _doctor

app = typer.Typer(
    help="System and data status",
    no_args_is_help=True,
)

app.add_typer(_audit.app, name="audit")
app.command(
    name="doctor",
    help="Run pipeline integrity checks across all invariants",
)(_doctor.doctor_command)


@app.command("status")
def system_status(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # status is data-only; nothing to suppress
    no_pager: bool = no_pager_option,
) -> None:
    """Show data inventory and pending review queue counts.

    `--output json` returns the same `data` and sensitivity as the MCP call
    `system_status(sections=["overview", "exports"])`.
    """
    from moneybin.exports.service import ExportService
    from moneybin.services.system_service import SystemService

    if output == OutputFormat.JSON:
        _emit_status_json(output)
        return

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            s = SystemService(db).status()
            exports = ExportService(db).status()

    min_d, max_d = s.transactions_date_range
    transaction_range = (
        f"{s.transactions_count} ({min_d} – {max_d})" if s.transactions_count else "0"
    )
    pairs = [
        ("Accounts", str(s.accounts_count)),
        ("Transactions", transaction_range),
        ("Last import", str(s.last_import_at or "never")),
        ("Matches pending", str(s.matches_pending)),
        ("Uncategorized", str(s.categorize_pending)),
    ]
    for destination in exports.destinations:
        state = "ready" if destination.ready else "not ready"
        access = "read-write" if destination.write_capable else "read-only"
        value = f"{destination.kind}; {state}; {access}"
        if destination.reasons:
            value = f"{value}; reasons: {', '.join(destination.reasons)}"
        pairs.append((f"Export {destination.name}", value))
    emit_human_result(
        compose_human_result([build_summary(pairs, title="System status")]),
        policy=get_terminal_policy(no_pager=no_pager),
        finite_read=True,
        no_pager=no_pager,
    )


def _emit_status_json(output: OutputFormat) -> None:
    """Emit the overview and exports sections in the MCP tool's sectioned shape."""
    from moneybin.adapters import system_status_adapters as status_adapters
    from moneybin.config import get_settings
    from moneybin.exports.service import ExportService
    from moneybin.privacy.classified_envelope import build_classified_envelope
    from moneybin.privacy.payloads.system import (
        OverviewStatus,
        SystemStatusCoarsePayload,
        SystemStatusSection,
    )
    from moneybin.services.system_service import SystemService, database_connections

    with handle_cli_errors(
        cli_actor="system_status", payload_type=SystemStatusCoarsePayload
    ):
        # Snapshot connections before opening, as the MCP tool does, so this
        # command's own read never appears in the list.
        db_connections = status_adapters.database_connections_info(
            database_connections(get_settings().database.path)
        )
        with get_database(read_only=True) as db:
            service = SystemService(db)
            status = service.status()
            gsheet = status_adapters.gsheet_info(service.gsheet_connections())
            readiness = ExportService(db).status()

    sections: list[SystemStatusSection] = [
        OverviewStatus(
            overview=status_adapters.overview_payload(status, gsheet, db_connections)
        ),
        status_adapters.exports_status(readiness),
    ]
    envelope = build_classified_envelope(
        SystemStatusCoarsePayload(sections=sections),
        contract_type=[type(section) for section in sections],
        total_count=len(sections),
        returned_count=len(sections),
    )
    render_or_json(
        envelope,
        output,
        cli_actor="system_status",
        classes_returned=envelope.classes_returned,
    )
