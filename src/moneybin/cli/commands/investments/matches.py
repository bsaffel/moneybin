"""Bounded review-only investment matching through the refresh operation."""

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import render_note, render_rows, render_summary
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.reviews import InvestmentMatchDetails
from moneybin.privacy.redaction import redact_typed

app = typer.Typer(help="Plan investment matches for review", no_args_is_help=True)


def _inspect(status: str, output: OutputFormat, quiet: bool) -> None:
    from moneybin.adapters.investment_matching_adapters import investment_review_view
    from moneybin.protocol.envelope import build_envelope

    with handle_cli_errors(cli_actor=f"investments_matches_{status}"):
        with get_database(read_only=True) as db:
            view = investment_review_view(
                db, "pending" if status == "pending" else "history"
            )
        envelope = build_envelope(data=view, sensitivity="high")
        render_or_json(envelope, output, cli_actor=f"investments_matches_{status}")
        if output == OutputFormat.TEXT:
            for row in view.rows:
                # render_or_json's TEXT path applies no redaction by design —
                # the caller owns what it displays. Legs carry account_id,
                # which can be a raw user-authored key for an unlinked manual
                # account (see InvestmentLegRecord), so mask it the same way
                # the JSON path does before printing anything.
                details = redact_typed(
                    row.details, None, declared_type=InvestmentMatchDetails
                )
                render_summary(
                    [
                        ("Proposal", row.decision_id),
                        ("Confidence", details.confidence_band),
                    ],
                    title=row.summary,
                )
                for leg in details.legs:
                    render_note(
                        f"Leg {leg.leg_role} — {leg.source_type} / {leg.source_origin}",
                        quiet=quiet,
                    )
                    render_rows(["Field", "Observed value"], leg.model_dump().items())
                render_note("Evidence", quiet=quiet)
                for evidence in details.evidence:
                    render_rows(["Field", "Evidence"], evidence.model_dump().items())
                for conflict in details.field_choices:
                    render_summary([
                        ("Field choice", conflict.field),
                        ("Conflict", conflict.conflict_id),
                    ])
                    render_rows(
                        ["Choice", "Observed value"],
                        [
                            (choice.choice_id, choice.value)
                            for choice in conflict.choices
                        ],
                    )
                if details.alternatives:
                    render_note("Competing alternatives", quiet=quiet)
                    render_rows(
                        ["Source events"],
                        [(" / ".join(members),) for members in details.alternatives],
                    )
                render_note("Downstream effects", quiet=quiet)
                render_rows(["Effect", "Value"], details.downstream_effects.items())
                for prior in details.supersession:
                    render_note("Prior Match and curation", quiet=quiet)
                    render_rows(["Context", "Value"], prior.model_dump().items())
            if not view.rows:
                render_note(f"No {status} investment Proposals.", quiet=quiet)


@app.command("pending")
def investments_matches_pending(
    output: OutputFormat = output_option, quiet: bool = quiet_option
) -> None:
    """Inspect pending Proposals with their issued review evidence."""
    _inspect("pending", output, quiet)


@app.command("history")
def investments_matches_history(
    output: OutputFormat = output_option, quiet: bool = quiet_option
) -> None:
    """Inspect historical Proposal evidence without rerunning matching."""
    _inspect("history", output, quiet)


@app.command("run")
def investments_matches_run(
    output: OutputFormat = output_option, quiet: bool = quiet_option
) -> None:
    """Plan whole-event Proposals without changing the Golden ledger."""
    from moneybin.adapters.refresh_adapters import refresh_envelope
    from moneybin.orchestration.refresh import refresh

    with handle_cli_errors(cli_actor="investments_matches_run"):
        with get_database(read_only=False) as db:
            result = refresh(db, steps=["investment_match"], actor="cli")
    envelope = refresh_envelope(result, requested=frozenset({"investment_match"}))
    render_or_json(envelope, output, cli_actor="investments_matches_run")
    stage = result.stage("investment_match")
    if stage is not None and stage.error:
        render_note(stage.error, warn=True)
        raise typer.Exit(1)
    if output != OutputFormat.JSON:
        count = (
            stage.counts.get("pending_unique", 0)
            + stage.counts.get("pending_competing", 0)
            if stage is not None and stage.ran
            else None
        )
        render_note(
            "Comparison inputs are not ready; run refresh first."
            if count is None
            else f"{count} investment Proposals prepared for review.",
            quiet=quiet,
        )
