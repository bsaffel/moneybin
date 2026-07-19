"""Atomic preflight and dispatch for consolidated review decisions."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal, cast

import duckdb
from pydantic import JsonValue

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.persistence import get_match_decision
from moneybin.mcp.write_contracts import (
    AccountLinkDecisionRequest,
    CategorizationDecisionRequest,
    IdentityDecisionRequest,
    MatchDecisionRequest,
    MerchantLinkDecisionRequest,
    ReviewDecisionRequest,
    SecurityLinkDecisionRequest,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services._text import build_match_inputs
from moneybin.services.account_links_service import AccountLinksService
from moneybin.services.categorization import CategorizationService
from moneybin.services.merchant_links_service import MerchantLinksService
from moneybin.services.security_links_service import SecurityLinksService
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    CATEGORIES,
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MERCHANT_LINK_DECISIONS,
    MERCHANT_LINKS,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
    TRANSACTION_CATEGORIES,
    USER_MERCHANTS,
)

_IdentityRequest = (
    AccountLinkDecisionRequest
    | MerchantLinkDecisionRequest
    | SecurityLinkDecisionRequest
)


@dataclass(frozen=True, slots=True)
class OrdinaryDecisionPlanItem:
    """One fully resolved ordinary review decision."""

    request: CategorizationDecisionRequest | MatchDecisionRequest
    changed: bool
    status: str
    category_changed: bool = False
    merchant_changed: bool = False
    merchant_id: str | None = None
    match_text: str | None = None


@dataclass(frozen=True, slots=True)
class IdentityDecisionPlanItem:
    """One resolved identity target with exact persisted before-state."""

    request: _IdentityRequest
    changed: bool
    status: Literal["accepted", "rejected"]
    source_id: str
    target_id: str
    group_key: tuple[str, ...]
    before_state: JsonValue
    blast_radius: dict[str, int]


@dataclass(frozen=True, slots=True)
class IdentityDecisionPlan:
    """Complete ordered identity batch plan."""

    items: tuple[IdentityDecisionPlanItem, ...]

    @property
    def changed_count(self) -> int:
        """Return the number of material decision transitions."""
        return sum(item.changed for item in self.items)

    @property
    def destructive(self) -> bool:
        """Return whether any material branch accepts an identity merge."""
        return any(
            item.changed and item.request.decision == "accept" for item in self.items
        )


def _json_safe(value: Any) -> JsonValue:
    """Normalize database values into deterministic confirmation JSON."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (date, datetime, Decimal)):
        return str(value)
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {str(key): _json_safe(item) for key, item in mapping.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return str(value)


def _query_json_rows(
    db: Database,
    sql: str,
    params: list[Any],
) -> list[dict[str, JsonValue]]:
    """Return complete selected rows as stable JSON-shaped dictionaries."""
    cursor = db.execute(sql, params)
    columns = [str(description[0]) for description in cursor.description]
    return [
        {column: _json_safe(value) for column, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def _history_row(
    rows: list[dict[str, Any]],
    decision_id: str,
    *,
    kind: str,
) -> dict[str, Any]:
    """Find one complete decision row or raise a sanitized not-found error."""
    row = next(
        (candidate for candidate in rows if candidate["decision_id"] == decision_id),
        None,
    )
    if row is None:
        raise UserError(
            f"No {kind} decision exists for this id.",
            code=error_codes.MUTATION_NOT_FOUND,
        )
    return row


def _count_rows(db: Database, sql: str, params: list[Any]) -> int:
    """Return a best-effort entity count for a confirmation blast radius."""
    try:
        row = db.execute(sql, params).fetchone()
    except duckdb.CatalogException:
        return 0
    return int(row[0]) if row else 0


class ReviewDecisionsService:
    """Preflight and atomically dispatch consolidated identity decisions."""

    def __init__(self, db: Database, *, actor: str) -> None:
        """Bind the batch service to one database connection and audit actor."""
        self._db = db
        self._actor = actor

    @staticmethod
    def _ordinary_error(
        *,
        index: int,
        request: CategorizationDecisionRequest | MatchDecisionRequest,
        code: str,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "index": index,
            "kind": request.kind,
            "decision_id": request.decision_id,
            "code": code,
            "reason": reason,
        }

    def _prepare_categorization(
        self,
        request: CategorizationDecisionRequest,
    ) -> OrdinaryDecisionPlanItem:
        row = self._db.execute(
            f"""
            SELECT tc.category, tc.subcategory, tx.description, tx.memo
            FROM {FCT_TRANSACTIONS.full_name} AS tx
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} AS tc
              ON tc.transaction_id = tx.transaction_id
            WHERE tx.transaction_id = ?
            LIMIT 1
            """,  # noqa: S608  # TableRef constants + parameterized value
            [request.decision_id],
        ).fetchone()
        if row is None:
            raise UserError(
                "No transaction exists for this categorization decision.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        current_category = cast(str | None, row[0])
        current_subcategory = cast(str | None, row[1])
        if request.decision == "reject":
            if current_category is not None:
                raise UserError(
                    "The categorization decision is already decided.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            return OrdinaryDecisionPlanItem(
                request=request,
                changed=False,
                status="rejected",
            )

        category_row = self._db.execute(
            f"""
            SELECT category_id
            FROM {CATEGORIES.full_name}
            WHERE category = ?
              AND subcategory IS NOT DISTINCT FROM ?
              AND is_active
            LIMIT 1
            """,  # noqa: S608  # TableRef constant + parameterized values
            [request.category, request.subcategory],
        ).fetchone()
        if category_row is None:
            raise UserError(
                "The requested category target does not exist or is inactive.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        category_changed = current_category is None
        if current_category is not None:
            if not (
                current_category == request.category
                and current_subcategory == request.subcategory
            ):
                raise UserError(
                    "The categorization decision is already decided differently.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            category_changed = False

        merchant_id: str | None = None
        merchant_changed = False
        match_text: str | None = None
        if request.canonical_merchant_name is not None:
            match_text, _description, _memo = build_match_inputs(
                cast(str | None, row[2]),
                cast(str | None, row[3]),
            )
            if match_text:
                categorization = CategorizationService(self._db)
                merchant_id = categorization.find_review_merchant(
                    request.canonical_merchant_name,
                    category=cast(str, request.category),
                    subcategory=request.subcategory,
                )
                if merchant_id is None:
                    merchant_changed = True
                else:
                    exemplar_row = self._db.execute(
                        f"""
                        SELECT list_contains(exemplars, ?)
                        FROM {USER_MERCHANTS.full_name}
                        WHERE merchant_id = ?
                        """,  # noqa: S608  # TableRef constant + parameterized values
                        [match_text, merchant_id],
                    ).fetchone()
                    merchant_changed = not bool(exemplar_row and exemplar_row[0])
        return OrdinaryDecisionPlanItem(
            request=request,
            changed=category_changed or merchant_changed,
            status="accepted",
            category_changed=category_changed,
            merchant_changed=merchant_changed,
            merchant_id=merchant_id,
            match_text=match_text,
        )

    def _prepare_match(
        self,
        request: MatchDecisionRequest,
    ) -> OrdinaryDecisionPlanItem:
        row = get_match_decision(self._db, request.decision_id)
        if row is None:
            raise UserError(
                "No match decision exists for this id.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        target = "accepted" if request.decision == "accept" else "rejected"
        current = str(row["match_status"])
        if current == target:
            return OrdinaryDecisionPlanItem(
                request=request,
                changed=False,
                status=target,
            )
        if current != "pending":
            raise UserError(
                "The match decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        return OrdinaryDecisionPlanItem(
            request=request,
            changed=True,
            status=target,
        )

    def plan_ordinary(
        self,
        decisions: list[ReviewDecisionRequest],
    ) -> tuple[OrdinaryDecisionPlanItem, ...]:
        """Resolve an entire ordered ordinary batch and report every invalid id."""
        if not decisions:
            raise UserError(
                "decisions must contain at least one review decision.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        errors: list[dict[str, Any]] = []
        prepared: list[OrdinaryDecisionPlanItem] = []
        seen: set[tuple[str, str]] = set()
        for index, request in enumerate(decisions):
            key = (request.kind, request.decision_id)
            if key in seen:
                errors.append(
                    self._ordinary_error(
                        index=index,
                        request=request,
                        code=error_codes.MUTATION_INVALID_INPUT,
                        reason="The same decision appears more than once in the batch.",
                    )
                )
                continue
            seen.add(key)
            try:
                item = (
                    self._prepare_categorization(request)
                    if isinstance(request, CategorizationDecisionRequest)
                    else self._prepare_match(request)
                )
            except UserError as exc:
                errors.append(
                    self._ordinary_error(
                        index=index,
                        request=request,
                        code=exc.code,
                        reason=exc.message,
                    )
                )
            else:
                prepared.append(item)
        if errors:
            raise UserError(
                "Review decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={"errors": errors},
            )
        return tuple(prepared)

    def apply_ordinary(
        self,
        decisions: list[ReviewDecisionRequest],
    ) -> tuple[OrdinaryDecisionPlanItem, ...]:
        """Revalidate and atomically apply an ordinary decision batch."""
        initial = self.plan_ordinary(decisions)
        if not any(item.changed for item in initial):
            raise UserError(
                "Every review decision is already satisfied or has no persisted change.",
                code=error_codes.MUTATION_NOTHING_TO_DO,
            )
        self._db.begin()
        try:
            live = self.plan_ordinary(decisions)
            category_service = CategorizationService(self._db)
            match_repo = MatchDecisionsRepo(self._db)
            for item in live:
                if not item.changed:
                    continue
                request = item.request
                if isinstance(request, CategorizationDecisionRequest):
                    category_service.apply_review_categorization_in_active_txn(
                        request.decision_id,
                        category=cast(str, request.category),
                        subcategory=request.subcategory,
                        canonical_merchant_name=request.canonical_merchant_name,
                        match_text=item.match_text,
                        existing_merchant_id=item.merchant_id,
                        category_changed=item.category_changed,
                        merchant_changed=item.merchant_changed,
                        actor=self._actor,
                    )
                else:
                    match_repo.update_status(
                        request.decision_id,
                        status=item.status,
                        decided_by="user",
                        actor=self._actor,
                        in_outer_txn=True,
                    )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        return live

    def _prepare_account(
        self,
        request: AccountLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = AccountLinksService(self._db, actor=self._actor)
        decision = _history_row(
            service.history(limit=None),
            request.decision_id,
            kind="account-link",
        )
        source_id = str(decision["provisional_account_id"])
        candidate_id = str(decision["candidate_account_id"])
        target_id = request.target_id or source_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted account-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The account-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    target_account_id=cast(str, request.target_id),
                )
                source_id = impact.provisional_account_id
                target_id = impact.candidate_account_id
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {ACCOUNT_LINK_DECISIONS.full_name}
            WHERE (provisional_account_id = ? OR candidate_account_id = ?)
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constants + parameterized values
            [source_id, source_id],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {ACCOUNT_LINKS.full_name}
            WHERE account_id = ? AND status = 'accepted'
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        transactions = _count_rows(
            self._db,
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} WHERE account_id = ?",  # noqa: S608  # TableRef constant
            [source_id],
        )
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=("account", source_id),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "accepted_links": links,
            }),
            blast_radius={
                "accounts": 2 if request.decision == "accept" else 1,
                "merchants": 0,
                "securities": 0,
                "transactions": transactions if request.decision == "accept" else 0,
                "lots": 0,
            },
        )

    def _prepare_merchant(
        self,
        request: MerchantLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = MerchantLinksService(self._db, actor=self._actor)
        decision = _history_row(
            service.history(limit=None),
            request.decision_id,
            kind="merchant-link",
        )
        source_id = (
            f"{decision['source_type']}:merchant_entity_id:{decision['ref_value']}"
        )
        candidate_id = str(decision["candidate_merchant_id"])
        target_id = request.target_id or source_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted merchant-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The merchant-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    target_merchant_id=cast(str, request.target_id),
                )
                target_id = impact.candidate_merchant_id
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MERCHANT_LINK_DECISIONS.full_name}
            WHERE source_type = ? AND ref_value = ?
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_value"]],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MERCHANT_LINKS.full_name}
            WHERE source_type = ? AND ref_value = ?
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_value"]],
        )
        transactions = _count_rows(
            self._db,
            f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name} WHERE merchant_id = ?",  # noqa: S608  # TableRef constant
            [target_id],
        )
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=(
                "merchant",
                str(decision["source_type"]),
                str(decision["ref_value"]),
            ),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "existing_links": links,
            }),
            blast_radius={
                "accounts": 0,
                "merchants": 1,
                "securities": 0,
                "transactions": transactions,
                "lots": 0,
            },
        )

    def _prepare_security(
        self,
        request: SecurityLinkDecisionRequest,
    ) -> IdentityDecisionPlanItem:
        service = SecurityLinksService(self._db, actor=self._actor)
        decision = _history_row(
            service.history(limit=None),
            request.decision_id,
            kind="security-link",
        )
        binding = self._db.execute(
            f"""
            SELECT security_id FROM {SECURITY_LINKS.full_name}
            WHERE ref_kind = ? AND ref_value = ? AND source_type = ?
              AND status = 'accepted'
            LIMIT 1
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["ref_kind"], decision["ref_value"], decision["source_type"]],
        ).fetchone()
        source_id = str(binding[0]) if binding is not None else request.decision_id
        candidate_id = str(decision["candidate_security_id"])
        target_id = request.target_id or source_id
        status: Literal["accepted", "rejected"] = (
            "accepted" if request.decision == "accept" else "rejected"
        )
        current = str(decision["status"])
        selection_disposal_ids: tuple[str, ...] = ()
        if current == status:
            if request.decision == "accept" and request.target_id != candidate_id:
                raise UserError(
                    "target_id does not match the accepted security-link candidate.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            changed = False
        elif current != "pending":
            raise UserError(
                "The security-link decision is already decided differently.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        else:
            changed = True
            if request.decision == "accept":
                impact = service.accept_impact(
                    request.decision_id,
                    into=cast(str, request.target_id),
                )
                source_id = impact.provisional_security_id
                target_id = impact.candidate_security_id
                selection_disposal_ids = impact.lot_selection_disposal_ids
        decisions = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITY_LINK_DECISIONS.full_name}
            WHERE source_type = ? AND ref_kind = ? AND ref_value = ?
              AND status = 'pending' AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [decision["source_type"], decision["ref_kind"], decision["ref_value"]],
        )
        links = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITY_LINKS.full_name}
            WHERE security_id = ? AND status = 'accepted'
            ORDER BY link_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        securities = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {SECURITIES.full_name}
            WHERE security_id IN (?, ?)
            ORDER BY security_id
            """,  # noqa: S608  # TableRef constant + parameterized values
            [source_id, target_id],
        )
        manual = _query_json_rows(
            self._db,
            f"""
            SELECT * FROM {MANUAL_INVESTMENT_TRANSACTIONS.full_name}
            WHERE security_id = ?
            ORDER BY source_transaction_id
            """,  # noqa: S608  # TableRef constant + parameterized value
            [source_id],
        )
        selections = [
            row
            for disposal_id in selection_disposal_ids
            for row in _query_json_rows(
                self._db,
                f"""
                SELECT * FROM {LOT_SELECTIONS.full_name}
                WHERE investment_transaction_id = ?
                ORDER BY lot_id
                """,  # noqa: S608  # TableRef constant + parameterized value
                [disposal_id],
            )
        ]
        transactions = _count_rows(
            self._db,
            f"SELECT COUNT(*) FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} "
            "WHERE security_id = ?",  # noqa: S608  # TableRef constant
            [source_id],
        ) + len(manual)
        lots = _count_rows(
            self._db,
            f"SELECT COUNT(*) FROM {FCT_INVESTMENT_LOTS.full_name} "
            "WHERE security_id = ?",  # noqa: S608  # TableRef constant
            [source_id],
        )
        return IdentityDecisionPlanItem(
            request=request,
            changed=changed,
            status=status,
            source_id=source_id,
            target_id=target_id,
            group_key=(
                "security",
                str(decision["source_type"]),
                str(decision["ref_kind"]),
                str(decision["ref_value"]),
            ),
            before_state=_json_safe({
                "decision": _json_safe(decision),
                "affected_decisions": decisions,
                "accepted_links": links,
                "securities": securities,
                "manual_investment_transactions": manual,
                "lot_selections": selections,
            }),
            blast_radius={
                "accounts": 0,
                "merchants": 0,
                "securities": 2 if request.decision == "accept" else 1,
                "transactions": transactions if request.decision == "accept" else 0,
                "lots": lots if request.decision == "accept" else 0,
            },
        )

    def plan_identity(
        self,
        decisions: list[IdentityDecisionRequest],
    ) -> IdentityDecisionPlan:
        """Resolve a complete ordered identity batch before its first write."""
        if not decisions:
            raise UserError(
                "decisions must contain at least one identity decision.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        errors: list[dict[str, Any]] = []
        prepared: list[IdentityDecisionPlanItem] = []
        seen_ids: set[tuple[str, str]] = set()
        seen_groups: set[tuple[str, ...]] = set()
        for index, request in enumerate(decisions):
            key = (request.kind, request.decision_id)
            if key in seen_ids:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": error_codes.MUTATION_INVALID_INPUT,
                    "reason": "The same decision appears more than once in the batch.",
                })
                continue
            seen_ids.add(key)
            try:
                if isinstance(request, AccountLinkDecisionRequest):
                    item = self._prepare_account(request)
                elif isinstance(request, MerchantLinkDecisionRequest):
                    item = self._prepare_merchant(request)
                else:
                    item = self._prepare_security(request)
            except UserError as exc:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": exc.code,
                    "reason": exc.message,
                })
                continue
            if item.group_key in seen_groups:
                errors.append({
                    "index": index,
                    "kind": request.kind,
                    "decision_id": request.decision_id,
                    "code": error_codes.MUTATION_INVALID_INPUT,
                    "reason": (
                        "Multiple decisions target one identity review group; "
                        "submit exactly one terminal decision for that group."
                    ),
                })
                continue
            seen_groups.add(item.group_key)
            prepared.append(item)
        if errors:
            raise UserError(
                "Identity decision preflight failed.",
                code=error_codes.MUTATION_INVALID_INPUT,
                details={"errors": errors},
            )
        return IdentityDecisionPlan(items=tuple(prepared))

    def apply_identity(
        self,
        decisions: list[IdentityDecisionRequest],
        *,
        verify: Callable[[IdentityDecisionPlan], None],
    ) -> IdentityDecisionPlan:
        """Re-preflight then apply every identity decision in one transaction."""
        self._db.begin()
        try:
            plan = self.plan_identity(decisions)
            verify(plan)
            if plan.changed_count == 0:
                raise UserError(
                    "Every identity decision is already satisfied.",
                    code=error_codes.MUTATION_NOTHING_TO_DO,
                )
            account_service = AccountLinksService(self._db, actor=self._actor)
            merchant_service = MerchantLinksService(self._db, actor=self._actor)
            security_service = SecurityLinksService(self._db, actor=self._actor)
            for item in plan.items:
                if not item.changed:
                    continue
                request = item.request
                decided_by = "user" if request.decision == "accept" else "auto"
                if isinstance(request, AccountLinkDecisionRequest):
                    account_service.set(
                        request.decision_id,
                        target_account_id=request.target_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                elif isinstance(request, MerchantLinkDecisionRequest):
                    merchant_service.set(
                        request.decision_id,
                        target_merchant_id=request.target_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                elif request.decision == "accept":
                    security_service.accept_merge(
                        request.decision_id,
                        into=cast(str, request.target_id),
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
                else:
                    security_service.reject_merge(
                        request.decision_id,
                        decided_by=decided_by,
                        in_outer_txn=True,
                    )
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        account_changed = any(
            item.changed and isinstance(item.request, AccountLinkDecisionRequest)
            for item in plan.items
        )
        merchant_outcomes = tuple(
            item.status
            for item in plan.items
            if item.changed and isinstance(item.request, MerchantLinkDecisionRequest)
        )
        security_outcomes = tuple(
            item.status
            for item in plan.items
            if item.changed and isinstance(item.request, SecurityLinkDecisionRequest)
        )
        if account_changed:
            account_service.record_committed_outer_decisions()
        if merchant_outcomes:
            merchant_service.record_committed_outer_outcomes(merchant_outcomes)
        if security_outcomes:
            security_service.record_committed_outer_outcomes(security_outcomes)
        return plan
