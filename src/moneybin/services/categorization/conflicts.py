"""Rule-conflict detection and resolution.

Two rules whose canonical matchers compare equal fire on exactly the same
transactions. When they also disagree about the category, priority and creation
order silently pick the winner and the loser is fully shadowed — a rule the user
was told was created has no effect. This module refuses that: creation records
the refused proposal in ``app.rule_conflicts`` and hands the user an explicit
decision.

It sits at the package leaves beside ``_shared`` so both creation paths — the
batch ``create_rules_core`` and the declarative ``plan_rule_targets`` — can
import it without a cycle through the applier.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    RULE_CONFLICT_RESOLVED_TOTAL,
    RULE_CONFLICTS_PENDING,
    RULE_CREATE_CONFLICT_BLOCKED_TOTAL,
)
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo
from moneybin.repositories.rule_conflicts_repo import (
    ConflictResolution,
    RuleConflictsRepo,
)
from moneybin.services.categorization._shared import (
    MatcherKey,
    canonical_matcher_key,
    matcher_digest,
    resolve_category_id,
)
from moneybin.tables import CATEGORIZATION_RULES, RULE_CONFLICTS

logger = logging.getLogger(__name__)

#: Columns every conflict read projects, in DDL order.
_CONFLICT_SELECT = """
    conflict_id, matcher_digest, existing_rule_id, existing_rule_updated_at,
    existing_name, existing_category, existing_subcategory, existing_priority,
    proposed_name, proposed_merchant_pattern, proposed_match_type,
    proposed_min_amount, proposed_max_amount, proposed_account_id,
    proposed_category, proposed_subcategory, proposed_priority,
    proposed_created_by, status, resolution, resolved_rule_id,
    detected_at, resolved_at
"""


@dataclass(frozen=True, slots=True)
class ActiveRule:
    """One active rule, reduced to what conflict detection needs."""

    rule_id: str
    name: str
    category: str
    subcategory: str | None
    priority: int
    #: The matcher's own tie-break: ``fetch_active_rules`` orders
    #: ``priority ASC, created_at ASC``, so this is what decides between twins
    #: at equal priority. ``rule_id`` is a random uuid4 hex and orders nothing.
    created_at: datetime
    updated_at: datetime
    key: MatcherKey


@dataclass(frozen=True, slots=True)
class RuleConflict:
    """A proposed rule refused because an active rule owns the same matcher."""

    conflict_id: str
    matcher_digest: str
    existing_rule_id: str
    existing_name: str
    existing_category: str
    existing_subcategory: str | None
    existing_priority: int
    existing_rule_updated_at: datetime
    proposed_name: str
    proposed_merchant_pattern: str
    proposed_match_type: str
    proposed_min_amount: Decimal | float | None
    proposed_max_amount: Decimal | float | None
    proposed_account_id: str | None
    proposed_category: str
    proposed_subcategory: str | None
    proposed_priority: int
    proposed_created_by: str

    @property
    def winner_rule_id(self) -> str:
        """The rule that would have won had both been active.

        Lower priority number wins; the existing rule wins a tie because it was
        created first and the matcher is identical, so nothing else separates
        them. This is an explanation of live behaviour, not a decision — the
        proposal is not activated either way.
        """
        return self.existing_rule_id

    def why(self) -> str:
        """One sentence naming the disagreement, for a human or an agent."""
        existing = _label(self.existing_category, self.existing_subcategory)
        proposed = _label(self.proposed_category, self.proposed_subcategory)
        return (
            f"Rule {self.existing_rule_id} already matches this pattern and "
            f"assigns {existing}; the proposal assigns {proposed}."
        )


def _label(category: str, subcategory: str | None) -> str:
    """Render a category pair the way both surfaces show it."""
    return f"{category} / {subcategory}" if subcategory else category


def conflict_identity(
    *,
    existing_rule_id: str,
    existing_rule_updated_at: object,
    digest: str,
    proposed_category: str,
    proposed_subcategory: str | None,
) -> str:
    """Return the deterministic id for one conflict.

    A content hash (``.claude/rules/identifiers.md`` strategy 2) rather than a
    fresh UUID: re-submitting the same refused rule is one conflict awaiting
    one decision, so it must land on the row that is already queued. The
    existing rule's ``updated_at`` is part of the input, so editing that rule
    yields a different id — the recorded conflict describes a rule state that
    no longer exists and is superseded rather than silently reused.
    """
    raw = "|".join([
        existing_rule_id,
        str(existing_rule_updated_at),
        digest,
        proposed_category,
        proposed_subcategory or "",
    ])
    return f"conf_{hashlib.sha256(raw.encode()).hexdigest()[:16]}"


def load_active_rules(
    db: Database, *, match_type: str | None = None
) -> list[ActiveRule]:
    """Read active rules and lift each into its canonical matcher key.

    ``match_type`` narrows the read; the canonical comparison still happens in
    Python because case folding and amount-grain normalization are defined
    there and must not be re-spelled in SQL where the two could drift.
    """
    sql = (
        "SELECT rule_id, name, merchant_pattern, match_type, min_amount, "
        "max_amount, account_id, category, subcategory, priority, updated_at, "
        "created_at "
        f"FROM {CATEGORIZATION_RULES.full_name} WHERE is_active = true"  # noqa: S608  # TableRef constant
    )
    params: list[object] = []
    if match_type is not None:
        sql += " AND match_type = ?"
        params.append(match_type)
    rows = db.execute(sql, params).fetchall()
    return [
        ActiveRule(
            rule_id=str(row[0]),
            name=str(row[1]),
            category=str(row[7]),
            subcategory=str(row[8]) if row[8] is not None else None,
            priority=int(row[9] or 0),
            updated_at=row[10],
            created_at=row[11],
            key=canonical_matcher_key(
                merchant_pattern=str(row[2]),
                match_type=str(row[3]),
                min_amount=row[4],
                max_amount=row[5],
                account_id=str(row[6]) if row[6] is not None else None,
            ),
        )
        for row in rows
    ]


def detect_conflict(
    active: Sequence[ActiveRule],
    *,
    key: MatcherKey,
    name: str,
    merchant_pattern: str,
    match_type: str,
    min_amount: Decimal | float | None,
    max_amount: Decimal | float | None,
    account_id: str | None,
    category: str,
    subcategory: str | None,
    priority: int,
    created_by: str,
) -> RuleConflict | None:
    """Return the conflict this proposal creates, or ``None``.

    ``None`` covers both the idempotent case (an active rule with this matcher
    *and* this output — the caller returns that rule unchanged) and the clear
    case (no active rule owns the matcher). Only a matcher twin that assigns a
    different category is a conflict.

    When several active rules already share the matcher, the one the matcher
    itself would pick is reported — lowest priority number, then oldest — so
    the rule named is the one currently deciding the category. ``rule_id``
    only breaks a same-timestamp tie the matcher leaves open.
    """
    twins = [rule for rule in active if rule.key == key]
    if not twins:
        return None
    if any(
        rule.category == category and rule.subcategory == subcategory for rule in twins
    ):
        return None
    winner = min(twins, key=lambda rule: (rule.priority, rule.created_at, rule.rule_id))
    digest = matcher_digest(key)
    return RuleConflict(
        conflict_id=conflict_identity(
            existing_rule_id=winner.rule_id,
            existing_rule_updated_at=winner.updated_at,
            digest=digest,
            proposed_category=category,
            proposed_subcategory=subcategory,
        ),
        matcher_digest=digest,
        existing_rule_id=winner.rule_id,
        existing_name=winner.name,
        existing_category=winner.category,
        existing_subcategory=winner.subcategory,
        existing_priority=winner.priority,
        existing_rule_updated_at=winner.updated_at,
        proposed_name=name,
        proposed_merchant_pattern=merchant_pattern,
        proposed_match_type=match_type,
        proposed_min_amount=min_amount,
        proposed_max_amount=max_amount,
        proposed_account_id=account_id,
        proposed_category=category,
        proposed_subcategory=subcategory,
        proposed_priority=priority,
        proposed_created_by=created_by,
    )


def record_conflicts(
    db: Database,
    conflicts: Sequence[RuleConflict],
    *,
    actor: str,
    surface: str,
    in_outer_txn: bool = False,
) -> None:
    """Persist refused proposals so the review surface can offer a decision.

    The refusal metric increments here rather than at each detection site:
    ``plan_rule_targets`` runs twice per write (preview, then re-plan inside
    the transaction), so counting detections would double every conflict the
    declarative path finds. This runs once per conflict that actually lands.
    """
    if not conflicts:
        return
    RULE_CREATE_CONFLICT_BLOCKED_TOTAL.labels(surface=surface).inc(len(conflicts))
    repo = RuleConflictsRepo(db)
    for conflict in conflicts:
        repo.prune_stale(
            conflict.existing_rule_id,
            keep_updated_at=conflict.existing_rule_updated_at,
            actor=actor,
            in_outer_txn=in_outer_txn,
        )
        repo.set(
            conflict_id=conflict.conflict_id,
            matcher_digest=conflict.matcher_digest,
            existing_rule_id=conflict.existing_rule_id,
            existing_rule_updated_at=conflict.existing_rule_updated_at,
            existing_name=conflict.existing_name,
            existing_category=conflict.existing_category,
            existing_subcategory=conflict.existing_subcategory,
            existing_priority=conflict.existing_priority,
            proposed_name=conflict.proposed_name,
            proposed_merchant_pattern=conflict.proposed_merchant_pattern,
            proposed_match_type=conflict.proposed_match_type,
            proposed_min_amount=conflict.proposed_min_amount,
            proposed_max_amount=conflict.proposed_max_amount,
            proposed_account_id=conflict.proposed_account_id,
            proposed_category=conflict.proposed_category,
            proposed_subcategory=conflict.proposed_subcategory,
            proposed_priority=conflict.proposed_priority,
            proposed_created_by=conflict.proposed_created_by,
            actor=actor,
            in_outer_txn=in_outer_txn,
        )
    logger.info(f"Recorded {len(conflicts)} categorization rule conflict(s)")


@dataclass(frozen=True, slots=True)
class ConflictDecision:
    """One user decision about one recorded conflict."""

    conflict_id: str
    resolution: ConflictResolution
    priority: int | None = None


@dataclass(frozen=True, slots=True)
class ConflictDecisionResult:
    """What one resolution committed.

    ``superseded_rule_ids`` is a list because ``replace`` clears every rule
    that can still shadow the proposal, not only the one the conflict was
    recorded against — a prior ``reprioritize`` can leave several active rules
    sharing the matcher.
    """

    conflict_id: str
    resolution: ConflictResolution
    rule_id: str | None
    superseded_rule_ids: list[str]


class RuleConflictsService:
    """Read and resolve the categorization rule-conflict queue."""

    def __init__(self, db: Database) -> None:
        """Bind to an open Database."""
        self._db = db

    # -- Reads --

    def list_pending(self) -> list[dict[str, Any]]:
        """Return conflicts still describing live rule state, oldest first.

        The join on ``updated_at`` is the staleness filter: a conflict recorded
        against a rule that has since been edited describes a comparison that
        no longer holds, so it never reaches a reviewer.
        """
        rows = self._db.execute(
            f"""
            SELECT {_CONFLICT_SELECT}
            FROM {RULE_CONFLICTS.full_name} AS c
            WHERE c.status = 'pending'
              AND EXISTS (
                  SELECT 1 FROM {CATEGORIZATION_RULES.full_name} AS r
                  WHERE r.rule_id = c.existing_rule_id
                    AND r.is_active = true
                    AND r.updated_at = c.existing_rule_updated_at
              )
            ORDER BY c.detected_at ASC, c.conflict_id ASC
            """  # noqa: S608  # TableRef constants, no user input interpolated
        ).fetchall()
        return [_conflict_row(row) for row in rows]

    def list_history(self) -> list[dict[str, Any]]:
        """Return settled conflicts, newest decision first."""
        rows = self._db.execute(
            f"""
            SELECT {_CONFLICT_SELECT}
            FROM {RULE_CONFLICTS.full_name}
            WHERE status = 'resolved'
            ORDER BY resolved_at DESC, conflict_id ASC
            """  # noqa: S608  # TableRef constant, no user input interpolated
        ).fetchall()
        return [_conflict_row(row) for row in rows]

    def count_pending(self) -> int:
        """Return the exact number of live pending conflicts."""
        count = len(self.list_pending())
        RULE_CONFLICTS_PENDING.set(count)
        return count

    def count_history(self) -> int:
        """Return the exact number of settled conflicts."""
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {RULE_CONFLICTS.full_name} WHERE status = 'resolved'"  # noqa: S608  # TableRef constant
        ).fetchone()
        return int(row[0]) if row else 0

    def fetch_pending(self, conflict_id: str) -> dict[str, Any] | None:
        """Return one live pending conflict, or ``None`` when it is gone or stale."""
        return next(
            (row for row in self.list_pending() if row["conflict_id"] == conflict_id),
            None,
        )

    # -- Writes --

    def decide(
        self,
        decisions: Sequence[ConflictDecision],
        *,
        actor: str,
    ) -> list[ConflictDecisionResult]:
        """Apply an atomic batch of conflict resolutions.

        Every decision is re-resolved against live state *inside* the write
        transaction, so a conflict that went stale between review and
        confirmation is refused rather than applied against a rule the user
        never saw.
        """
        if not decisions:
            raise UserError(
                "decisions must contain at least one conflict resolution.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        seen: set[str] = set()
        for decision in decisions:
            if decision.conflict_id in seen:
                raise UserError(
                    "The same conflict appears more than once in the batch.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            seen.add(decision.conflict_id)
            if decision.resolution == "reprioritize" and decision.priority is None:
                raise UserError(
                    "reprioritize requires an explicit priority.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            if decision.resolution != "reprioritize" and decision.priority is not None:
                raise UserError(
                    "priority is only valid for reprioritize.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )

        self._db.begin()
        try:
            results = [self._apply_one(decision, actor=actor) for decision in decisions]
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        for result in results:
            RULE_CONFLICT_RESOLVED_TOTAL.labels(resolution=result.resolution).inc()
        RuleConflictsRepo(self._db).refresh_pending_gauge()
        return results

    def _apply_one(
        self, decision: ConflictDecision, *, actor: str
    ) -> ConflictDecisionResult:
        """Resolve one conflict against live state inside the caller's transaction."""
        live = self.fetch_pending(decision.conflict_id)
        if live is None:
            raise UserError(
                "The conflict is already decided, or the rule it describes has "
                "changed since it was recorded.",
                code=error_codes.TAXONOMY_RULE_CONFLICT_STALE,
                details={"conflict_id": decision.conflict_id},
            )
        conflicts_repo = RuleConflictsRepo(self._db)
        rules_repo = CategorizationRulesRepo(self._db)
        rule_id: str | None = None
        superseded: list[str] = []

        if decision.resolution != "cancel":
            if decision.resolution == "replace":
                superseded = self._deactivate_matcher_twins(
                    rules_repo, live, actor=actor
                )
            priority = (
                decision.priority
                if decision.priority is not None
                else int(live["proposed_priority"])
            )
            category = str(live["proposed_category"])
            subcategory = live["proposed_subcategory"]
            insert = rules_repo.insert(
                name=str(live["proposed_name"]),
                merchant_pattern=str(live["proposed_merchant_pattern"]),
                match_type=str(live["proposed_match_type"]),
                min_amount=live["proposed_min_amount"],
                max_amount=live["proposed_max_amount"],
                account_id=live["proposed_account_id"],
                category=category,
                subcategory=subcategory,
                category_id=resolve_category_id(self._db, category, subcategory),
                priority=priority,
                created_by=str(live["proposed_created_by"]),
                actor=actor,
                in_outer_txn=True,
            )
            rule_id = insert.target_id

        conflicts_repo.resolve(
            decision.conflict_id,
            resolution=decision.resolution,
            resolved_rule_id=rule_id,
            actor=actor,
            in_outer_txn=True,
        )
        return ConflictDecisionResult(
            conflict_id=decision.conflict_id,
            resolution=decision.resolution,
            rule_id=rule_id,
            superseded_rule_ids=superseded,
        )

    def _deactivate_matcher_twins(
        self,
        rules_repo: CategorizationRulesRepo,
        live: dict[str, Any],
        *,
        actor: str,
    ) -> list[str]:
        """Deactivate every active rule that can still shadow the proposal.

        The recorded conflict names one winner, but a prior ``reprioritize``
        deliberately leaves several active rules sharing a matcher. Replacing
        only the recorded winner would activate a proposal an older twin still
        shadows — the exact outcome this module exists to refuse — so the live
        set is re-read here rather than trusted from detection time.
        """
        key = canonical_matcher_key(
            merchant_pattern=str(live["proposed_merchant_pattern"]),
            match_type=str(live["proposed_match_type"]),
            min_amount=live["proposed_min_amount"],
            max_amount=live["proposed_max_amount"],
            account_id=live["proposed_account_id"],
        )
        superseded: list[str] = []
        for twin in load_active_rules(self._db):
            if twin.key != key:
                continue
            event = rules_repo.deactivate(
                twin.rule_id,
                actor=actor,
                context={"reason": "rule_conflict_replaced"},
                in_outer_txn=True,
            )
            if event is not None:
                superseded.append(twin.rule_id)
        return superseded


def _conflict_row(row: tuple[Any, ...]) -> dict[str, Any]:
    """Lift one DuckDB conflict row into a keyed dict at the service boundary."""
    return {
        "conflict_id": str(row[0]),
        "matcher_digest": str(row[1]),
        "existing_rule_id": str(row[2]),
        "existing_rule_updated_at": row[3],
        "existing_name": str(row[4]),
        "existing_category": str(row[5]),
        "existing_subcategory": row[6],
        "existing_priority": int(row[7]),
        "proposed_name": str(row[8]),
        "proposed_merchant_pattern": str(row[9]),
        "proposed_match_type": str(row[10]),
        "proposed_min_amount": row[11],
        "proposed_max_amount": row[12],
        "proposed_account_id": row[13],
        "proposed_category": str(row[14]),
        "proposed_subcategory": row[15],
        "proposed_priority": int(row[16]),
        "proposed_created_by": str(row[17]),
        "status": str(row[18]),
        "resolution": row[19],
        "resolved_rule_id": row[20],
        "detected_at": row[21],
        "resolved_at": row[22],
    }
