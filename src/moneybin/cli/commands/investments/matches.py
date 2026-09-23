"""Bounded review-only investment matching through the refresh operation."""

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
    render_or_json,
    wide_option,
)
from moneybin.cli.render import build_summary, compose_human_result
from moneybin.cli.utils import get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.reviews import InvestmentMatchDetails
from moneybin.privacy.redaction import redact_typed

app = typer.Typer(help="Plan investment matches for review", no_args_is_help=True)


def _inspect(
    status: str,
    output: OutputFormat,
    *,
    quiet: bool,
    wide: bool,
    no_pager: bool,
) -> None:
    from moneybin.adapters.investment_matching_adapters import investment_review_view
    from moneybin.protocol.envelope import build_envelope

    with handle_cli_errors(cli_actor=f"investments_matches_{status}"):
        with get_database(read_only=True) as db:
            view = investment_review_view(
                db, "pending" if status == "pending" else "history"
            )
        envelope = build_envelope(data=view, sensitivity="high")
        render_or_json(envelope, output, cli_actor=f"investments_matches_{status}")
        if output == OutputFormat.JSON:
            return

        policy = get_terminal_policy(no_pager=no_pager)
        title = (
            "Pending investment Proposals"
            if status == "pending"
            else "Investment Proposal history"
        )
        if not view.rows:
            empty_scope = "pending" if status == "pending" else "historical"
            emit_human_result(
                compose_human_result(
                    [
                        build_summary(
                            [("Result", f"No {empty_scope} investment Proposals.")],
                            title=title,
                        )
                    ],
                    disclosures=(
                        ()
                        if quiet
                        else (
                            "Run 'moneybin investments matches run' to prepare "
                            "investment Proposals.",
                        )
                    ),
                ),
                policy=policy,
                finite_read=True,
                no_pager=no_pager,
                wide=wide,
            )
            return

        parts: list[object] = [
            build_summary(
                [
                    (
                        "Scope",
                        f"All {len(view.rows)} "
                        f"{'pending' if status == 'pending' else 'historical'} "
                        "investment Proposals.",
                    )
                ],
                title=title,
            )
        ]
        for row in view.rows:
            # render_or_json's TEXT path applies no redaction by design — the
            # caller owns what it displays. Legs can carry a raw, user-authored
            # account_id, so redact the typed detail before any field is formatted.
            details = redact_typed(
                row.details, None, declared_type=InvestmentMatchDetails
            )
            parts.append(
                build_summary(
                    [
                        ("Proposal", row.decision_id),
                        ("Status", row.status),
                        ("Confidence", details.confidence_band),
                    ],
                    title=row.summary,
                )
            )
            for leg in details.legs:
                parts.append(
                    build_summary(
                        [(key, str(value)) for key, value in leg.model_dump().items()],
                        title=(
                            f"Leg {leg.leg_role} — "
                            f"{leg.source_type} / {leg.source_origin}"
                        ),
                    )
                )
            for evidence in details.evidence:
                parts.append(
                    build_summary(
                        [
                            (key, str(value))
                            for key, value in evidence.model_dump().items()
                        ],
                        title="Evidence",
                    )
                )
            for conflict in details.field_choices:
                parts.append(
                    build_summary(
                        [
                            ("Field choice", conflict.field),
                            ("Conflict", conflict.conflict_id),
                        ],
                        title="Field choice",
                    )
                )
                for choice in conflict.choices:
                    parts.append(
                        build_summary(
                            [
                                (key, str(value))
                                for key, value in choice.model_dump().items()
                            ],
                            title="Choice",
                        )
                    )
            if details.alternatives:
                parts.append(
                    build_summary(
                        [
                            ("Source events", " / ".join(members))
                            for members in details.alternatives
                        ],
                        title="Competing alternatives",
                    )
                )
            parts.append(
                build_summary(
                    [
                        (key, str(value))
                        for key, value in details.downstream_effects.items()
                    ],
                    title="Downstream effects",
                )
            )
            for prior in details.supersession:
                parts.append(
                    build_summary(
                        [
                            (key, str(value))
                            for key, value in prior.model_dump().items()
                        ],
                        title="Prior Match and curation",
                    )
                )
        emit_human_result(
            compose_human_result(parts),
            policy=policy,
            finite_read=True,
            no_pager=no_pager,
            wide=wide,
        )


@app.command("pending")
def investments_matches_pending(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Inspect pending Proposals with their issued review evidence."""
    _inspect("pending", output, quiet=quiet, wide=wide, no_pager=no_pager)


@app.command("history")
def investments_matches_history(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    wide: bool = wide_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Inspect historical Proposal evidence without rerunning matching."""
    _inspect("history", output, quiet=quiet, wide=wide, no_pager=no_pager)


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
    if output == OutputFormat.JSON:
        render_or_json(envelope, output, cli_actor="investments_matches_run")
        return
    stage = result.stage("investment_match")
    if stage is not None and stage.error:
        typer.echo(stage.error, err=True)
        raise typer.Exit(1)
    count = (
        stage.count("pending_unique") + stage.count("pending_competing")
        if stage is not None and stage.ran
        else None
    )
    emit_human_result(
        compose_human_result([
            build_summary(
                [
                    (
                        "Result",
                        "Comparison inputs are not ready; run refresh first."
                        if count is None
                        else f"{count} investment Proposals prepared for review.",
                    )
                ],
                title="Investment match planning",
            )
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
