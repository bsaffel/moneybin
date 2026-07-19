"""Transactions categorize namespace tools — rules, categorization, auto-rules."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Literal

from fastmcp import FastMCP

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import get_database
from moneybin.errors import RecoveryAction, UserError
from moneybin.mcp._registration import register
from moneybin.mcp.adapters.categorize_adapters import (
    auto_accept_envelope,
    auto_review_envelope,
)
from moneybin.mcp.confirmation import (
    ConfirmationBinding,
    ConfirmationGrant,
    grant_confirmation_or_raise,
)
from moneybin.mcp.decorator import mcp_tool
from moneybin.mcp.write_contracts import CategorizationRuleTarget
from moneybin.privacy.payloads.categorize import (
    AutoAcceptPayload,
    AutoReviewPayload,
    AutoStatsPayload,
    CategorizationRulesCoarsePayload,
    CategorizationRulesCurrentView,
    CategorizationRulesHistoryView,
    CategorizationRulesSetPayload,
    CategorizationRuleStateResult,
    CategorizeCommitPayload,
    CategorizeRulesPayload,
    CategorizeRunPayload,
    CategorizeStatsPayload,
    CategorizeStatsWithAutoPayload,
    CatPendingPayload,
    ImproveAiPayload,
    PendingTxnRow,
    RulesCreatePayload,
    RulesDeletePayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.account_service import AccountService
from moneybin.services.auto_rule_service import AutoRuleService
from moneybin.services.categorization import (
    CategorizationResult,
    CategorizationService,
    validate_items,
    validate_rule_items,
)
from moneybin.services.categorization.applier import (
    RuleStateTarget,
    RuleTargetPlan,
)
from moneybin.services.mutation_context import current_operation_id

logger = logging.getLogger(__name__)


def transactions_categorize_rules() -> ResponseEnvelope[CategorizeRulesPayload]:
    """List all categorization rules.

    Returns rule ID, name, pattern, match type, category, priority,
    and active status. Rules are applied in priority order during import.
    """
    with get_database(read_only=True) as db:
        payload = CategorizationService(db).list_rules()
    return build_envelope(
        data=payload,
        actions=[
            "Use transactions_categorize_rules_set to declare rule target states",
        ],
    )


def _to_rule_state_target(target: CategorizationRuleTarget) -> RuleStateTarget:
    """Translate the strict MCP target contract into the service-owned type."""
    matcher = target.matcher
    return RuleStateTarget(
        rule_id=target.rule_id,
        state=target.state,
        merchant_pattern=matcher.value if matcher is not None else None,
        match_type=matcher.type if matcher is not None else None,
        min_amount=matcher.min_amount if matcher is not None else None,
        max_amount=matcher.max_amount if matcher is not None else None,
        account_id=matcher.account_id if matcher is not None else None,
        category=target.category,
        subcategory=target.subcategory,
        priority=target.priority,
    )


def _rule_targets_binding(
    rules: list[CategorizationRuleTarget], plan: RuleTargetPlan
) -> ConfirmationBinding:
    """Bind a destructive batch to validated targets and resolved rule IDs."""
    return ConfirmationBinding(
        arguments={
            "rules": [rule.model_dump(mode="json") for rule in rules],
            "live_states": [
                {
                    "rule_id": item.rule_id,
                    "action": item.action,
                    "before_digest": item.before_digest,
                }
                for item in plan.items
            ],
        },
        resolved_ids=plan.resolved_ids,
        actor="mcp",
        profile=get_settings().profile,
        authorization_context="local-profile",
        operation_kind="transactions_categorize_rules_set",
        blast_radius={
            "rules": len(rules),
            "changed_rules": len(plan.changed),
            "deleted_rules": sum(item.action == "delete" for item in plan.items),
        },
    )


def _preview_rule_targets(rules: list[CategorizationRuleTarget]) -> RuleTargetPlan:
    """Validate target-state resolution without opening a mutation transaction."""
    with get_database(read_only=True) as db:
        return CategorizationService(db).plan_rule_targets([
            _to_rule_state_target(rule) for rule in rules
        ])


def _apply_rule_targets(
    rules: list[CategorizationRuleTarget],
    plan: RuleTargetPlan,
    *,
    grant: ConfirmationGrant | None,
    expected_binding: ConfirmationBinding,
) -> list[CategorizationRuleStateResult]:
    """Re-preflight and write every changed rule within one audited transaction."""
    with get_database(read_only=False) as db:
        service = CategorizationService(db)

        def verify(live_plan: RuleTargetPlan) -> None:
            binding = _rule_targets_binding(rules, live_plan)
            if grant is not None:
                grant.verify(binding)
            elif binding.canonical_bytes() != expected_binding.canonical_bytes():
                raise UserError(
                    "Categorization rule state changed after preflight.",
                    code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
                )

        result = service.apply_rule_targets(plan, actor="mcp", verify=verify)
    return [
        CategorizationRuleStateResult(
            rule_id=item.rule_id,
            state=item.state,
            changed=item.changed,
        )
        for item in result
    ]


@mcp_tool(domain="categorize", read_only=False, destructive=True, idempotent=True)
async def transactions_categorize_rules_set_coarse(
    rules: list[CategorizationRuleTarget],
    confirmation_token: str | None = None,
) -> ResponseEnvelope[CategorizationRulesSetPayload]:
    """Atomically declare complete categorization-rule target states."""
    if not rules:
        raise UserError(
            "rules must contain at least one target.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    plan = await asyncio.to_thread(_preview_rule_targets, rules)
    expected_binding = _rule_targets_binding(rules, plan)
    if confirmation_token is not None and not plan.destructive:
        raise UserError(
            "confirmation_token is only valid when removing a present rule.",
            code=error_codes.MUTATION_INVALID_INPUT,
        )
    grant: ConfirmationGrant | None = None
    if plan.destructive or confirmation_token is not None:
        grant = await grant_confirmation_or_raise(
            binding=expected_binding if confirmation_token is None else None,
            message=(
                "Remove the selected categorization rule(s)? Their full prior "
                "state is retained in the audit log and can be restored with "
                "system_audit_undo(operation_id)."
            ),
            confirmation_token=confirmation_token,
        )
    results = await asyncio.to_thread(
        _apply_rule_targets,
        rules,
        plan,
        grant=grant,
        expected_binding=expected_binding,
    )
    operation_id = current_operation_id()
    return build_envelope(
        data=CategorizationRulesSetPayload(
            results=results,
            operation_id=operation_id,
        ),
        recovery_actions=[
            RecoveryAction(
                tool="system_audit_undo",
                arguments={"operation_id": operation_id},
                rationale="Restore the audited rule-state mutation.",
                confidence="certain",
                idempotent=False,
            )
        ],
    )


@mcp_tool(domain="categorize")
def transactions_categorize_rules_coarse(
    view: Literal["active", "inactive", "history"] = "active",
) -> ResponseEnvelope[CategorizationRulesCoarsePayload]:
    """Read active, inactive, or complete historical categorization-rule states."""
    with get_database(read_only=True) as db:
        service = CategorizationService(db)
        if view == "history":
            payload: CategorizationRulesCoarsePayload = CategorizationRulesHistoryView(
                kind="history",
                events=service.list_rule_history(),
            )
        else:
            payload = CategorizationRulesCurrentView(
                kind=view,
                rules=service.list_rule_snapshots(active=view == "active"),
            )
    return build_envelope(data=payload)


def register_categorization_coarse_reads(mcp: FastMCP) -> None:
    """Register the standard categorization-rule read projection."""
    register(
        mcp,
        transactions_categorize_rules_coarse,
        "transactions_categorize_rules",
        "Read active, inactive, or audit-backed historical categorization rules. "
        "Use view='history' to include prior and deleted states; statistics "
        "are available from system_status(sections=['categorization']).",
        privacy_actor="transactions_categorize_rules",
    )


def register_categorization_coarse_writes(mcp: FastMCP) -> None:
    """Register the standard declarative categorization-rule write."""
    register(
        mcp,
        transactions_categorize_rules_set_coarse,
        "transactions_categorize_rules_set",
        "Atomically declare categorization rules present, inactive, or absent. "
        "Present requires matcher, category, and priority; inactive and absent "
        "require rule_id and forbid replacement fields. The tool advertises its "
        "maximum destructive risk, but asks for exact payload-bound confirmation "
        "only before a present rule is hard-deleted. Rule removal is recoverable "
        "with system_audit_undo(operation_id).",
        privacy_actor="transactions_categorize_rules_set",
    )


def transactions_categorize_stats(
    include_auto: bool = False,
) -> ResponseEnvelope[CategorizeStatsPayload | CategorizeStatsWithAutoPayload]:
    """Get categorization coverage statistics.

    Returns total transactions, categorized count, uncategorized count,
    percentage categorized, breakdown by categorization source, and
    plaid_unmapped — the count of Plaid transactions whose PFC code has no
    category-source-bridge mapping yet (omitted when no Plaid data is
    present).

    The source breakdown carries one bucket per persisted ``categorized_by``
    value (``user``, ``rule``, ``auto_rule``, ``migration``, ``ml``,
    ``provider_native``, ``ai``) plus a reporting-only ``merchant_map``
    bucket: rows written via merchant-pattern matching are split out of
    ``rule`` here so the count reconciles with transactions_categorize_rules'
    rule list, but the persisted ``categorized_by`` value on those rows is
    still ``rule``.

    Args:
        include_auto: When True, also return auto-rule health metrics
            (active auto-rules, pending proposals, transactions categorized
            by auto-rules). The response ``data`` becomes a
            ``CategorizeStatsWithAutoPayload`` (``{overall: {...}, auto: {...}}``)
            instead of the flat ``CategorizeStatsPayload`` shape. Default
            False returns the flat overall shape.
    """
    with get_database(read_only=True) as db:
        overall = CategorizationService(db).stats()
        if not include_auto:
            return build_envelope(
                data=overall.to_payload(),
                actions=[
                    "Use reviews(kind='categorization') for uncategorized transactions"
                ],
            )
        auto_data = AutoRuleService(db).stats()
    # include_auto=True: composite of overall coverage + auto-rule health, as a
    # typed payload so the annotation matches the runtime shape and the privacy
    # middleware derives the tier from real fields.
    return build_envelope(
        data=CategorizeStatsWithAutoPayload(
            overall=overall.to_payload(),
            auto=AutoStatsPayload(
                active_auto_rules=auto_data.active_auto_rules,
                pending_proposals=auto_data.pending_proposals,
                transactions_categorized=auto_data.transactions_categorized,
            ),
        ),
        actions=[
            "Use reviews(kind='categorization') for uncategorized transactions",
            "Use transactions_categorize_rules(view='history') to inspect "
            "persisted rule state changes",
        ],
    )


def transactions_categorize_pending(
    limit: int = 50,
    sort: Literal["date", "impact"] = "date",
    min_amount: Decimal = Decimal("0"),
    account: str | None = None,
) -> ResponseEnvelope[CatPendingPayload]:
    """Find transactions that have not been categorized yet.

    Returns uncategorized transactions from the curator-impact view (excludes
    transfer pairs and archived accounts). Use this to identify transactions
    that need manual or AI-assisted categorization.

    Amounts use the accounting convention: negative = expense, positive = income;
    transfers exempt. Amounts are in the currency named by
    ``summary.display_currency``.

    Args:
        limit: Maximum number of results (default 50, max 1000).
        sort: ``date`` (most recent first, default) or ``impact`` (ABS(amount)
            * age_days — highest-value/oldest transactions first).
        min_amount: Filter to ABS(amount) >= this value. Default 0 returns all.
        account: Filter to a specific account; accepts ``account_id`` or
            case-insensitive display_name. Ambiguous matches raise. Default
            None returns all accounts.
    """
    with get_database(read_only=True) as db:
        account_id: str | None = None
        if account is not None:
            account_id = AccountService(db).resolve_strict(account)
        records = CategorizationService(db).list_uncategorized_transactions(
            limit=min(limit, 1000),
            sort=sort,
            min_amount=min_amount,
            account_id=account_id,
        )
    if records is None:
        return build_envelope(
            data=CatPendingPayload(transactions=[]),
            actions=["Import data first using import_files"],
        )
    payload = CatPendingPayload(
        transactions=[
            PendingTxnRow(
                transaction_id=r["transaction_id"],
                transaction_date=str(r["txn_date"])
                if r.get("txn_date") is not None
                else None,
                amount=float(r["amount"]) if r.get("amount") is not None else None,
                description=r.get("description"),
                memo=None,
                account_id=r.get("account_id"),
                age_days=int(r["age_days"]) if r.get("age_days") is not None else None,
                pending_transfer_match=bool(r.get("pending_transfer_match", False)),
            )
            for r in records
        ]
    )
    actions = [
        "Use transactions_categorize_commit to commit categorizations for these transactions",
        "Use transactions_categorize_rules_set to set up automatic categorization",
    ]
    flagged = sum(1 for r in records if r.get("pending_transfer_match"))
    if flagged:
        actions.append(
            f"{flagged} of these have an unresolved transfer match. Categorizing "
            "a transfer leg double-counts it against the eventual pair — resolve "
            "them first with reviews(kind='matches') and reviews_decide."
        )
    return build_envelope(
        data=payload,
        actions=actions,
    )


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_commit(
    items: Sequence[Mapping[str, str | None]],
) -> ResponseEnvelope[CategorizeCommitPayload]:
    """Commit externally-decided categorizations for a batch of transactions.

    Each item should have ``transaction_id``, ``category``, and optionally
    ``subcategory`` and ``canonical_merchant_name``. Transactions that
    already have a category are overwritten (subject to source-precedence
    rules).

    Also auto-creates exemplar-only merchant mappings from each row's
    normalized match_text so future rows with the same match_text are
    categorized automatically via the merchant matcher. When
    ``canonical_merchant_name`` is provided, multiple rows with different
    match_text values are merged under one merchant identity by appending
    exemplars rather than spawning per-row merchants.

    Typical caller: an LLM that received redacted rows from
    transactions_categorize_assist, proposed categorizations, the user
    reviewed, and the LLM now persists the accepted decisions.

    Args:
        items: List of dicts with transaction_id, category, optional
            subcategory, and optional canonical_merchant_name.
    """
    if not items:
        empty = CategorizationResult(applied=0, skipped=0, errors=0, error_details=[])
        return build_envelope(
            data=empty.to_payload(),
            total_count=0,
            actions=[
                "Use transactions_categorize_rules to review auto-created rules",
                "Use reviews(kind='categorization') to fetch the next batch",
            ],
        )

    validated, parse_errors = validate_items(items)
    with get_database(read_only=False) as db:
        result = CategorizationService(db).categorize_items(validated)
    result.merge_parse_errors(parse_errors)
    return build_envelope(
        data=result.to_payload(),
        total_count=len(items),
        actions=[
            "Use transactions_categorize_rules to review auto-created rules",
            "Use reviews(kind='categorization') to fetch the next batch",
        ],
    )


def transactions_categorize_rules_create(
    rules: list[dict[str, str | float | int | None]],
    reapply: bool = False,
    allow_broad: bool = False,
) -> ResponseEnvelope[RulesCreatePayload]:
    """Create multiple categorization rules in one call.

    Each rule should have ``name``, ``merchant_pattern``, and ``category``.
    Optional fields: ``subcategory``, ``match_type`` (default 'contains'),
    ``min_amount``, ``max_amount``, ``account_id``, ``priority`` (default 100).

    A ``contains`` rule whose ``merchant_pattern`` is too short to
    discriminate (below ``auto_rule_min_contains_length``, default 4 chars —
    e.g. `contains "TO"` matches STORE, AUTO, TOTAL) is refused rather than
    created: it would silently relabel unrelated transactions across the
    ledger. The refused item is not inserted, counted in ``skipped``, and
    explained in ``error_details``. Fix by using ``match_type="exact"`` for
    a short pattern, or pass ``allow_broad=True`` to accept the risk.

    Args:
        rules: List of rule dicts.
        reapply: If True, retroactively apply the new rules to all
            uncategorized transactions after the inserts commit. Default
            False; only future categorizations are affected.
        allow_broad: If True, bypass the unselective-``contains`` refusal
            above. Only set this after confirming the short pattern is
            intentional — it is not the same override as auto-rule review's
            ``allow_broad`` (that gate is breadth-vs-evidence; this one is a
            fixed specificity floor).
    """
    validated, parse_errors = validate_rule_items(rules)
    with get_database(read_only=False) as db:
        result = CategorizationService(db).create_rules(
            validated, reapply=reapply, actor="mcp", allow_broad=allow_broad
        )
    result.merge_parse_errors(parse_errors)
    return build_envelope(
        data=result.to_payload(),
        total_count=len(rules),
        actions=[
            "Use transactions_categorize_rules to review all rules",
        ],
    )


def transactions_categorize_rules_delete(
    rule_id: str, reapply: bool = False
) -> ResponseEnvelope[RulesDeletePayload]:
    """Soft-delete a categorization rule by setting it inactive.

    The rule remains in the database but will no longer be applied
    during auto-categorization.

    Args:
        rule_id: The rule ID to deactivate.
        reapply: If True, run categorize_pending after the deactivation so
            rows previously covered by lower-priority sources have a chance
            to be re-evaluated. Default False; existing categorizations are
            left untouched.
    """
    with get_database(read_only=False) as db:
        deactivated = CategorizationService(db).deactivate_rule(
            rule_id, reapply=reapply, actor="mcp"
        )
    if not deactivated:
        raise UserError(f"Rule {rule_id} not found", code="RULE_NOT_FOUND")
    return build_envelope(
        data=RulesDeletePayload(rule_id=rule_id, action="deactivated")
    )


def transactions_categorize_auto_review(
    limit: int | None = None,
) -> ResponseEnvelope[AutoReviewPayload]:
    """List pending auto-rule proposals.

    Returns proposed categorization rules awaiting review, including
    sample matching transactions and trigger counts.

    Args:
        limit: Maximum number of proposals to return. Defaults to the
            configured ``auto_rule_list_default_limit`` (100). The envelope
            ``summary.has_more`` flag indicates whether more proposals exist
            beyond the returned page.
    """
    with get_database(read_only=True) as db:
        result = AutoRuleService(db).review(limit=limit)
    return auto_review_envelope(result)


def transactions_categorize_auto_accept(
    accept: list[str] | None = None,
    reject: list[str] | None = None,
    allow_broad: bool = False,
) -> ResponseEnvelope[AutoAcceptPayload]:
    """Accept or reject auto-rule proposals by ID.

    Accepted proposals become active rules and immediately categorize
    matching transactions. Writes app.categorization_rules and
    app.transaction_categories; revert accepted rules with
    transactions_categorize_rules_delete (rejected proposals cannot be
    un-rejected).

    Args:
        accept: Proposal IDs to accept and promote to active rules.
        reject: Proposal IDs to reject and dismiss.
        allow_broad: Required to accept a proposal that
            transactions_categorize_auto_review flagged ``is_broad`` — one whose
            ``estimated_match_count`` far exceeds the evidence behind it. Without
            this, such proposals are skipped rather than promoted. Review
            ``estimated_match_count`` before setting it: a broad rule
            recategorizes every matching transaction at once, and a wrong
            Transfer label also removes those rows from spend reports.
    """
    with get_database(read_only=False) as db:
        result = AutoRuleService(db).accept(
            accept=accept or [],
            reject=reject or [],
            actor="mcp",
            allow_broad=allow_broad,
        )
    return auto_accept_envelope(result)


@mcp_tool(domain="categorize", read_only=False)
def transactions_categorize_run(
    methods: list[Literal["rules", "merchants"]] | None = None,
    operation: Literal["categorize", "improve_ai"] = "categorize",
) -> ResponseEnvelope[CategorizeRunPayload | ImproveAiPayload]:
    """Run categorization or upgrade AI guesses to provider-native categories.

    ``operation="categorize"`` runs deterministic engines over uncategorized
    transactions. ``rules`` applies active user-authored pattern rules and
    ``merchants`` applies the stored merchant catalog. Engines run in the
    order given; the canonical order takes an optimized shared-scan path.

    ``operation="improve_ai"`` revisits only transactions currently labeled
    by AI and upgrades those with a confident provider-native category. It
    forbids ``methods`` because rules/merchant engines target a different
    transaction population. Amounts use the accounting convention: negative
    = expense, positive = income; transfers exempt.

    Args:
        operation: ``categorize`` (default) or ``improve_ai``.
        methods: Engines to run in the listed order. Defaults to
            ["rules", "merchants"]. Valid only for ``categorize``.
    """
    if operation == "improve_ai":
        if methods is not None:
            raise UserError(
                "methods is valid only when operation='categorize'.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        with get_database(read_only=False) as db:
            count = CategorizationService(db).improve_ai_categories()
        return build_envelope(
            data=ImproveAiPayload(upgraded_count=count),
            sensitivity="low",
            actions=[
                "Use system_status(sections=['categorization']) to check coverage",
            ],
        )

    with get_database(read_only=False) as db:
        data = CategorizationService(db).categorize_run(methods=methods)
    payload = CategorizeRunPayload(
        applied_by_method=data["applied_by_method"], total_applied=data["total_applied"]
    )
    return build_envelope(
        data=payload,
        actions=[
            "Use system_status(sections=['categorization']) to check coverage",
            "Use reviews(kind='categorization') for remaining rows",
        ],
    )


def transactions_categorize_improve_ai() -> ResponseEnvelope[ImproveAiPayload]:
    """Re-categorize AI-guessed transactions to confident provider-native categories.

    Reverse-looks-up every transaction currently ``categorized_by='ai'``
    against the Plaid category bridge; upgrades it to ``provider_native``
    only when the bridge match is at MEDIUM confidence or higher. Only
    rewrites rows currently ``categorized_by='ai'`` — user, rule, and
    merchant categorizations are never overwritten. Writes
    app.transaction_categories; revert by re-categorizing the transaction
    (a user edit wins at priority 1). Returns the count of transactions
    upgraded.
    """
    with get_database(read_only=False) as db:
        count = CategorizationService(db).improve_ai_categories()
    return build_envelope(
        data=ImproveAiPayload(upgraded_count=count),
        sensitivity="low",
        actions=[
            "Use system_status(sections=['categorization']) to check coverage",
        ],
    )


def register_transactions_categorize_tools(mcp: FastMCP) -> None:
    """Register the standard categorization read and write boundaries."""
    register_categorization_coarse_reads(mcp)
    register(
        mcp,
        transactions_categorize_commit,
        "transactions_categorize_commit",
        "Commit a caller-reviewed categorization batch to "
        "app.transaction_categories and app.user_merchants. Re-categorize a "
        "transaction to replace a prior decision.",
    )
    register(
        mcp,
        transactions_categorize_run,
        "transactions_categorize_run",
        "Run deterministic categorization engines or upgrade confident AI "
        "guesses to provider-native categories. operation='categorize' accepts "
        "methods; operation='improve_ai' forbids it. Amounts use the accounting "
        "convention: negative = expense, positive = income; transfers exempt.",
    )
    register_categorization_coarse_writes(mcp)
