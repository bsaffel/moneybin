"""merchants links — review-queue commands for merchant entity id binding.

Subcommands: pending, set, history, run.
Mirrors `accounts links` — thin wrappers over MerchantLinksService.

`merchants links undo` is deliberately NOT registered: deferred to the
M1L audit-undo consumer, same as `accounts links undo`.
"""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import (
    OutputFormat,
    emit_human_result,
    no_pager_option,
    output_option,
    quiet_option,
)
from moneybin.cli.render import build_rows, build_summary, compose_human_result
from moneybin.cli.utils import confidence_cell, get_terminal_policy, handle_cli_errors
from moneybin.database import get_database
from moneybin.privacy.payloads.merchants import (
    MerchantLinksHistoryPayload,
    MerchantLinksPendingPayload,
    MerchantLinksRunPayload,
)
from moneybin.protocol.envelope import build_envelope
from moneybin.services.merchant_links_service import MerchantLinksService

app = typer.Typer(
    help="Review and manage merchant-link binding decisions",
    no_args_is_help=True,
)
logger = logging.getLogger(__name__)


@app.command("pending")
def links_pending(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """List pending merchant-link decisions, grouped by provider entity id.

    Shows provider entity ids with candidate merchant proposals. Each group
    lists the candidate decision_id, merchant_id, canonical name, and
    confidence. Use `merchants links set` to decide each group.
    """
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            svc = MerchantLinksService(db, actor="cli")
            groups = svc.pending()
            n_pending = svc.count_pending()

    payload = MerchantLinksPendingPayload.from_service(groups, n_pending)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_pending",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary(
            [("Pending decisions", str(n_pending))], title="Merchant-link decisions"
        )
    ]
    for group in groups:
        parts.extend([
            build_summary([
                ("Provider entity", group.ref_value),
                ("Provider merchant", group.provider_merchant_name or "-"),
                ("Source", group.source_type),
                ("Candidates", str(len(group.candidates))),
            ]),
            build_rows(
                ["decision id", "merchant id", "confidence", "canonical name"],
                [
                    (
                        candidate.decision_id,
                        candidate.candidate_merchant_id,
                        confidence_cell(candidate.confidence),
                        candidate.candidate_canonical_name or "-",
                    )
                    for candidate in group.candidates
                ],
                numeric=("confidence",),
                terminal=policy,
            ),
        ])
    disclosures = (
        (("Next: moneybin merchants links set <decision-id> --into <merchant-id>"),)
        if groups
        else ("No pending decisions. Next: moneybin merchants links run",)
    )
    emit_human_result(
        compose_human_result(parts, disclosures=disclosures),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("set")
def links_set(
    decision_id: str = typer.Argument(
        ..., help="Decision ID to act on (from `merchants links pending`)"
    ),
    into: str | None = typer.Option(
        None,
        "--into",
        help="Accept: bind this provider entity id to the decision's candidate merchant_id (confirming safety check)",
    ),
    new: bool = typer.Option(
        False,
        "--new",
        help="Reject all candidates; resolver mints a new merchant on its next categorization pass",
    ),
) -> None:
    """Accept (bind) or reject a pending merchant-link decision.

    Pass exactly one of:
      --into <candidate_merchant_id>   bind the provider entity id to this merchant
      --new                            reject; resolver mints a new merchant on next run

    Examples:
      moneybin merchants links set dec001 --into merch0001aa
      moneybin merchants links set dec001 --new
    """
    if into is not None and new:
        logger.error("❌ --into and --new are mutually exclusive")
        raise typer.Exit(2)
    # Truthiness, not `is None`: an empty `--into ""` is not a valid merchant id
    # and must not silently fall through to the bind path.
    if not into and not new:
        logger.error("❌ Specify either --into <merchant_id> or --new")
        raise typer.Exit(2)

    target_merchant_id: str | None = into if not new else None

    with handle_cli_errors():
        with get_database(read_only=False) as db:
            MerchantLinksService(db, actor="cli").set(
                decision_id, target_merchant_id=target_merchant_id, decided_by="user"
            )

    receipt = [
        ("Decision", decision_id),
        ("Outcome", "bound to merchant" if target_merchant_id else "rejected"),
    ]
    if target_merchant_id:
        receipt.append(("Merchant ID", target_merchant_id))
    emit_human_result(
        compose_human_result([
            build_summary(receipt, title="Merchant-link decision recorded")
        ]),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )


@app.command("history")
def links_history(
    limit: int = typer.Option(50, "--limit", "-n", min=1, help="Max records to show"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
    no_pager: bool = no_pager_option,
) -> None:
    """Show recent merchant-link decisions (all statuses), newest first."""
    with handle_cli_errors():
        with get_database(read_only=True) as db:
            rows = MerchantLinksService(db, actor="cli").history(limit=limit)

    payload = MerchantLinksHistoryPayload.from_rows(rows)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_history",
        )
        return

    policy = get_terminal_policy(no_pager=no_pager)
    parts: list[object] = [
        build_summary([("Maximum records", str(limit))], title="Merchant-link history")
    ]
    if payload.decisions:
        parts.append(
            build_rows(
                [
                    "decision id",
                    "provider entity",
                    "provider merchant",
                    "source",
                    "merchant id",
                    "status",
                    "decided by",
                    "decided at",
                    "signal",
                    "confidence",
                ],
                [
                    (
                        decision.decision_id,
                        decision.ref_value,
                        decision.provider_merchant_name or "-",
                        decision.source_type,
                        decision.candidate_merchant_id,
                        decision.status,
                        decision.decided_by,
                        decision.decided_at or "-",
                        decision.signal,
                        confidence_cell(decision.confidence),
                    )
                    for decision in payload.decisions
                ],
                numeric=("confidence",),
                terminal=policy,
            )
        )
    else:
        parts.append(build_summary([("Result", "No merchant-link decisions found.")]))
    emit_human_result(
        compose_human_result(
            parts,
            disclosures=(
                () if payload.decisions else ("Next: moneybin merchants links run",)
            ),
        ),
        policy=policy,
        finite_read=True,
        no_pager=no_pager,
    )


@app.command("run")
def links_run(
    output: OutputFormat = output_option,
) -> None:
    """Harvest existing categorization facts into merchant-link bindings.

    Binds provider entity ids that point unambiguously to a single canonical
    merchant (recorded immediately, no review), and routes one-id-many-merchant
    conflicts to the pending review queue. Reports the two outcomes distinctly.

    Run this after importing transactions with merchant_entity_id data to
    surface binding decisions for review.
    """
    with handle_cli_errors():
        with get_database(read_only=False) as db:
            result = MerchantLinksService(db, actor="cli").run()

    payload = MerchantLinksRunPayload(bound=result.bound, conflicts=result.conflicts)

    if output == OutputFormat.JSON:
        from moneybin.cli.output import render_or_json

        render_or_json(
            build_envelope(data=payload),
            output,
            cli_actor="merchants_links_run",
        )
        return

    emit_human_result(
        compose_human_result(
            [
                build_summary(
                    [
                        ("Bindings recorded", str(result.bound)),
                        ("Conflicts queued", str(result.conflicts)),
                    ],
                    title="Merchant-link harvest complete",
                )
            ],
            disclosures=(
                ("Next: moneybin merchants links pending",) if result.conflicts else ()
            ),
        ),
        policy=get_terminal_policy(),
        finite_read=False,
        receipt=True,
    )
