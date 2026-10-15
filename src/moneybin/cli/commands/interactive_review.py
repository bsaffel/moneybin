"""Explicit, bounded terminal decisions over the shipped review queues."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import typer

from moneybin.cli.output import emit_human_result
from moneybin.cli.prompts import Choice, choose_required
from moneybin.cli.render import build_summary, compose_human_result, format_money
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.errors import UserError

if TYPE_CHECKING:
    from moneybin.cli.terminal import TerminalPolicy

_QUEUES = ("matches", "categorize", "account-links", "merchant-links", "security-links")


@dataclass(frozen=True)
class ReviewCandidate:
    """An explicitly selectable decision and its own identity target."""

    decision_id: str
    target_id: str
    label: str


class SavedDecisionError(Exception):
    """A decision committed but its required follow-up failed."""


@dataclass(frozen=True)
class ReviewItem:
    """A displayed decision and the evidence its authorization covers."""

    queue: str
    item_id: str
    evidence: tuple[tuple[str, str], ...]
    candidates: tuple[ReviewCandidate, ...] = ()


def load_items(type_: str, limit: int | None = None) -> list[ReviewItem]:
    """Read pending decisions without creating categorization attempts."""
    from moneybin.services.account_links_service import AccountLinksService
    from moneybin.services.categorization import CategorizationService
    from moneybin.services.matching_service import MatchingService
    from moneybin.services.merchant_links_service import MerchantLinksService
    from moneybin.services.security_links_service import SecurityLinksService

    items: list[ReviewItem] = []
    queues = _QUEUES if type_ == "all" else (type_,)
    with get_database(read_only=True) as db:
        for queue in queues:
            remaining = None if limit is None else limit - len(items)
            if remaining == 0:
                break
            pending: list[ReviewItem] = []
            if queue == "matches":
                match_service = MatchingService(db)
                for row in match_service.get_pending(limit=remaining):
                    evidence = [
                        (key.replace("_", " "), str(value))
                        for key, value in sorted(row.items())
                    ]
                    for side in ("a", "b"):
                        account_id = row.get("account_id_b") if side == "b" else None
                        observation = match_service.get_source_observation(
                            source_type=str(row[f"source_type_{side}"]),
                            source_transaction_id=str(
                                row[f"source_transaction_id_{side}"]
                            ),
                            account_id=str(account_id or row["account_id"]),
                        )
                        if observation is None:
                            evidence.append((
                                f"Transaction {side.upper()}",
                                "Source observation unavailable; refresh before deciding",
                            ))
                            continue
                        evidence.extend(
                            (
                                f"Transaction {side.upper()} {key.replace('_', ' ')}",
                                format_money(value, "flow", minus="-")
                                if key == "amount"
                                else str(value or "-"),
                            )
                            for key, value in observation.items()
                        )
                    pending.append(
                        ReviewItem(queue, str(row["match_id"]), tuple(evidence))
                    )
            elif queue == "categorize":
                svc = CategorizationService(db)
                rows = svc.list_uncategorized_transactions(limit=None) or []
                attempts = svc.project_pending_review_attempts([
                    str(row["transaction_id"]) for row in rows
                ])
                for row in rows:
                    attempt = attempts.get(str(row["transaction_id"]))
                    if attempt is None:
                        continue
                    pending.append(
                        ReviewItem(
                            queue,
                            str(attempt["decision_id"]),
                            tuple(
                                (
                                    key.replace("_", " "),
                                    format_money(value, "flow")
                                    if key == "amount"
                                    else str(value),
                                )
                                for key, value in sorted(row.items())
                                if key not in {"age_days", "priority_score"}
                            ),
                        )
                    )
            elif queue == "account-links":
                from moneybin.cli.commands.accounts.links import overlap_cell

                for group in AccountLinksService(db).pending():
                    candidates = sorted(group.candidates, key=lambda c: c.decision_id)
                    pending.append(
                        ReviewItem(
                            queue,
                            group.provisional_account_id,
                            (
                                (
                                    "Provisional account",
                                    group.provisional_display_name or "Unnamed account",
                                ),
                                ("Transactions affected", str(group.transactions)),
                                *(
                                    (
                                        f"Candidate [{c.decision_id}]",
                                        f"{c.candidate_display_name} [{c.candidate_account_id}]; {c.signal}; {overlap_cell(c.overlap)}",
                                    )
                                    for c in candidates
                                ),
                            ),
                            candidates=tuple(
                                ReviewCandidate(
                                    c.decision_id,
                                    c.candidate_account_id,
                                    c.candidate_display_name,
                                )
                                for c in candidates
                            ),
                        )
                    )
            elif queue == "merchant-links":
                for merchant_group in MerchantLinksService(db).pending():
                    merchants = sorted(
                        merchant_group.candidates, key=lambda c: c.decision_id
                    )
                    pending.append(
                        ReviewItem(
                            queue,
                            f"{merchant_group.source_type}:{merchant_group.ref_value}",
                            (
                                ("Provider entity", merchant_group.ref_value),
                                (
                                    "Provider merchant",
                                    merchant_group.provider_merchant_name or "-",
                                ),
                                ("Source", merchant_group.source_type),
                                *(
                                    (
                                        f"Candidate [{c.decision_id}]",
                                        f"{c.candidate_canonical_name} [{c.candidate_merchant_id}]; confidence {c.confidence}",
                                    )
                                    for c in merchants
                                ),
                            ),
                            candidates=tuple(
                                ReviewCandidate(
                                    c.decision_id,
                                    c.candidate_merchant_id,
                                    c.candidate_canonical_name,
                                )
                                for c in merchants
                            ),
                        )
                    )
            else:
                from moneybin.price_sources import FEED_KEY_REF_KINDS

                for security_group in SecurityLinksService(db).pending():
                    securities = sorted(
                        security_group.candidates, key=lambda c: c.decision_id
                    )
                    pending.append(
                        ReviewItem(
                            queue,
                            f"{security_group.source_type}:{security_group.ref_kind}:{security_group.ref_value}",
                            (
                                ("Reference kind", security_group.ref_kind),
                                ("Provider reference", security_group.ref_value),
                                (
                                    "Provider ticker",
                                    security_group.provider_ticker or "-",
                                ),
                                ("Provider name", security_group.provider_name or "-"),
                                ("Source", security_group.source_type),
                                (
                                    "Accept effect",
                                    "Bind price-feed key; delete nothing"
                                    if security_group.ref_kind in FEED_KEY_REF_KINDS
                                    else "Merge: move references, tax lots, manual ledger rows and price marks; delete provisional security",
                                ),
                                *(
                                    (
                                        f"Candidate [{c.decision_id}]",
                                        f"{c.candidate_ticker or '-'} / {c.candidate_name or '-'} [{c.candidate_security_id}]; reason {c.match_reason or '-'}; confidence {c.confidence}",
                                    )
                                    for c in securities
                                ),
                            ),
                            candidates=tuple(
                                ReviewCandidate(
                                    c.decision_id,
                                    c.candidate_security_id,
                                    c.candidate_name
                                    or c.candidate_ticker
                                    or "Candidate",
                                )
                                for c in securities
                            ),
                        )
                    )
            if queue not in {"matches", "categorize"}:
                pending.sort(key=lambda item: item.item_id)
            items.extend(pending if remaining is None else pending[:remaining])
    return items


def _show(pairs: list[tuple[str, str]], title: str, policy: TerminalPolicy) -> None:
    emit_human_result(
        compose_human_result([build_summary(pairs, title=title)]),
        policy=policy,
        finite_read=False,
        receipt=True,
    )


def run_review(type_: str, limit: int, policy: TerminalPolicy) -> None:
    """Count displayed items, including skips, and retain partial receipts."""
    shown = saved = skipped = stale = 0
    outcome = "complete"
    try:
        with handle_cli_errors(cli_actor="review"):
            selection = load_items(type_, limit)[:limit]
            for item in selection:
                shown += 1
                _show([("Decision", item.item_id), *item.evidence], item.queue, policy)
                reject_label = {
                    "account-links": "Keep standalone (reject all candidates)",
                    "merchant-links": "New merchant (reject all candidates)",
                }.get(item.queue, "Reject")
                action = choose_required(
                    None,
                    choices=(
                        Choice("accept", "Accept"),
                        Choice("reject", reject_label),
                        Choice("skip", "Skip"),
                        Choice("quit", "Quit"),
                    ),
                    flag="decision",
                    policy=policy,
                )
                if action == "quit":
                    outcome = "quit"
                    break
                if action == "skip":
                    skipped += 1
                    continue
                live = {row.item_id: row for row in load_items(item.queue)}
                if live.get(item.item_id) != item:
                    stale += 1
                    typer.echo(
                        "Decision changed since display; skipped. Refresh the review queue."
                    )
                    continue
                try:
                    if apply_decision(item, action, policy):
                        saved += 1
                        _show(
                            [("Item", item.item_id), ("Decision", action)],
                            "Review decision saved",
                            policy,
                        )
                    else:
                        stale += 1
                except SavedDecisionError:
                    saved += 1
                    raise typer.Exit(1) from None
                except UserError as exc:
                    from moneybin import error_codes

                    if (
                        item.queue == "matches"
                        and exc.code == error_codes.REFRESH_MODEL_FAILED
                    ):
                        from moneybin.matching.persistence import get_match_decision

                        with get_database(read_only=True) as db:
                            committed = get_match_decision(db, item.item_id)
                        if committed is not None:
                            state = str(committed["match_status"])
                            if state == (
                                "accepted" if action == "accept" else "rejected"
                            ):
                                saved += 1
                            _show(
                                [("Match", item.item_id), ("Committed status", state)],
                                "Match decision committed; FX accounting stale",
                                policy,
                            )
                    raise
    except (typer.Abort, KeyboardInterrupt, EOFError):
        outcome = "cancelled"
        raise typer.Abort() from None
    except typer.Exit as exc:
        outcome = "cancelled" if exc.exit_code == 0 else "stopped"
        raise
    except BaseException:
        outcome = "stopped"
        raise
    finally:
        _show(
            [
                ("Session", outcome),
                ("Items shown", str(shown)),
                ("Decisions saved", str(saved)),
                ("Skipped", str(skipped)),
                ("Stale items", str(stale)),
            ],
            "Review session",
            policy,
        )


def apply_decision(item: ReviewItem, action: str, policy: TerminalPolicy) -> bool:
    """Use each domain's existing decision service and confirmation boundary."""
    target: str | None = None
    category: dict[str, str | bool | None] | None = None
    decision_id = item.item_id
    if item.candidates:
        if action == "accept" or item.queue == "security-links":
            selected = choose_required(
                None,
                choices=tuple(
                    Choice(
                        c.decision_id,
                        f"{c.label} [{c.target_id}] (decision {c.decision_id})",
                    )
                    for c in item.candidates
                ),
                flag="identity target",
                policy=policy,
            )
            candidate = next(c for c in item.candidates if c.decision_id == selected)
        else:
            # The service rejects every sibling in this explicitly rejected group.
            candidate = item.candidates[0]
        decision_id = candidate.decision_id
        if action == "accept":
            target = candidate.target_id
    if item.queue == "categorize" and action == "accept":
        from moneybin.services.categorization import CategorizationService

        with get_database(read_only=True) as db:
            categories = CategorizationService(db).get_active_categories()
        category_id = choose_required(
            None,
            choices=tuple(
                Choice(
                    str(row["category_id"]),
                    f"{row['category']} / {row['subcategory'] or '-'} [{row['category_id']}]",
                )
                for row in categories
            ),
            flag="category",
            policy=policy,
        )
        category = next(
            row for row in categories if str(row["category_id"]) == category_id
        )
    # The last prompt can span a concurrent decision/import, so re-read again.
    if item not in load_items(item.queue):
        typer.echo("Decision changed while choosing its target; no decision saved.")
        return False
    if item.queue == "account-links":
        from moneybin.cli.commands.accounts.links import (
            confirm_merge,
            merge_drift_check,
            report_rematch,
        )
        from moneybin.services.account_links_service import AccountLinksService

        approved = confirm_merge(decision_id, target) if target is not None else None
        with get_database(read_only=False) as db:
            rematch = AccountLinksService(db, actor="cli").set(
                decision_id,
                target_account_id=target,
                decided_by="user",
                verify_accept=merge_drift_check(db, decision_id, approved),
            )
        for note in report_rematch(rematch):
            typer.echo(note)
        # The merge is durable even when its follow-up refresh fails.
        if rematch is not None and rematch.error is not None:
            typer.echo("Account decision saved; follow-up rebuild failed.")
            raise SavedDecisionError()
        return True
    with get_database(read_only=False) as db:
        if item.queue == "matches":
            from moneybin.cli.utils import warn_transfers_retired
            from moneybin.matching.reconciliation import RETIRED_SIDES_COLLAPSED
            from moneybin.services.matching_service import MatchingService

            result = MatchingService(db).set_status(
                item.item_id,
                status="accepted" if action == "accept" else "rejected",
                actor="cli",
            )
            warn_transfers_retired(
                result.transfers_retired,
                cause=RETIRED_SIDES_COLLAPSED,
                rematch_follow_up=True,
            )
            if result.match_status != (
                "accepted" if action == "accept" else "rejected"
            ):
                typer.echo(f"Requested decision did not stand: {result.match_status}.")
                raise typer.Exit(1)
        elif item.queue == "categorize":
            from moneybin.protocol.write_contracts import CategorizationDecisionRequest
            from moneybin.services.review_decisions_service import (
                ReviewDecisionsService,
            )

            request = (
                CategorizationDecisionRequest(
                    kind="categorization",
                    decision_id=item.item_id,
                    decision="accept" if action == "accept" else "reject",
                )
                if category is None
                else CategorizationDecisionRequest(
                    kind="categorization",
                    decision_id=item.item_id,
                    decision="accept",
                    category=cast(str, category["category"]),
                    subcategory=cast(str | None, category["subcategory"]),
                )
            )
            ReviewDecisionsService(db, actor="cli").apply_ordinary([request])
        elif item.queue == "merchant-links":
            from moneybin.services.merchant_links_service import MerchantLinksService

            MerchantLinksService(db, actor="cli").set(
                decision_id, target_merchant_id=target, decided_by="user"
            )
        else:
            from moneybin.services.security_links_service import SecurityLinksService

            service = SecurityLinksService(db, actor="cli")
            if target is not None:
                result_kind = service.accept(
                    decision_id, into=target, decided_by="user"
                )
                typer.echo(f"Security decision saved: {result_kind}.")
            else:
                service.reject_merge(decision_id, decided_by="user")
    return True
