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
from moneybin.protocol.envelope import build_envelope

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
    """Show data inventory and pending review queue counts."""
    from moneybin.exports.service import ExportService
    from moneybin.services.system_service import SystemService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            s = SystemService(db).status()
            exports = ExportService(db).status()

    min_d, max_d = s.transactions_date_range
    if output == OutputFormat.JSON:
        from moneybin.privacy.payloads.system import (
            SystemStatusCLIPayload,
            SystemStatusExportDestination,
        )

        render_or_json(
            build_envelope(
                data=SystemStatusCLIPayload(
                    accounts_count=s.accounts_count,
                    transactions_count=s.transactions_count,
                    transactions_date_range=[
                        min_d.isoformat() if min_d else None,
                        max_d.isoformat() if max_d else None,
                    ],
                    last_import_at=(
                        s.last_import_at.isoformat() if s.last_import_at else None
                    ),
                    matches_pending=s.matches_pending,
                    categorize_pending=s.categorize_pending,
                    exports=[
                        SystemStatusExportDestination(
                            name=destination.name,
                            kind=destination.kind,
                            ready=destination.ready,
                            write_capable=destination.write_capable,
                            reasons=list(destination.reasons),
                        )
                        for destination in exports.destinations
                    ],
                )
            ),
            output,
            cli_actor="system_status",
        )
        return

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
