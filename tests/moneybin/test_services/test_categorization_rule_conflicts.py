"""Rule-conflict detection and resolution (MB-124 / #420).

Two rules whose canonical matchers are equal fire on the same transactions.
When they disagree about the category, priority and creation order silently
pick the winner and the loser is fully shadowed. These tests pin the refusal,
the recorded conflict, and each of the three resolutions.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from prometheus_client import REGISTRY

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.categorization import (
    CategorizationRuleInput,
    CategorizationService,
    ConflictDecision,
    RuleStateTarget,
)
from moneybin.services.categorization._shared import canonical_matcher_key
from moneybin.services.categorization.conflicts import (
    ActiveRule,
    detect_conflict,
    load_active_rules,
)
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture(autouse=True)
def _core_tables(db: Database) -> None:  # pyright: ignore[reportUnusedFunction]
    create_core_tables(db)


def _rule(
    name: str,
    *,
    pattern: str = "STARBUCKS",
    category: str = "Food & Drink",
    subcategory: str | None = None,
    match_type: str = "contains",
    priority: int = 100,
) -> CategorizationRuleInput:
    return CategorizationRuleInput(
        name=name,
        merchant_pattern=pattern,
        category=category,
        subcategory=subcategory,
        match_type=match_type,  # pyright: ignore[reportArgumentType]  # test literals
        priority=priority,
    )


def _active_rule_count(db: Database) -> int:
    row = db.execute(
        "SELECT COUNT(*) FROM app.categorization_rules WHERE is_active"
    ).fetchone()
    assert row is not None
    return int(row[0])


class TestConflictDetection:
    """Same matcher + different output refuses instead of shadowing."""

    @pytest.mark.unit
    def test_same_matcher_same_output_stays_idempotent(self, db: Database) -> None:
        service = CategorizationService(db)
        first = service.create_rules([_rule("Starbucks")])
        second = service.create_rules([_rule("Starbucks renamed")])

        assert second.created == 0
        assert second.conflicts == 0
        assert second.existing == 1
        assert second.rule_ids == first.rule_ids

    @pytest.mark.unit
    def test_same_matcher_different_category_is_a_conflict(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", category="Food & Drink")])
        result = service.create_rules([_rule("Coffee", category="Travel")])

        assert result.created == 0
        assert result.conflicts == 1
        assert result.skipped == 0, "a conflict is awaiting a decision, not failed"
        assert _active_rule_count(db) == 1
        assert result.conflict_ids[0].startswith("conf_")

    @pytest.mark.unit
    def test_same_matcher_different_subcategory_is_a_conflict(
        self, db: Database
    ) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", subcategory="Coffee Shops")])
        result = service.create_rules([_rule("Coffee", subcategory="Restaurants")])

        assert result.conflicts == 1
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_case_variant_pattern_is_the_same_matcher(self, db: Database) -> None:
        """A lowercase twin must conflict, not create a second shadowed rule."""
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", pattern="STARBUCKS")])
        result = service.create_rules([
            _rule("Coffee lower", pattern="  starbucks ", category="Travel")
        ])

        assert result.conflicts == 1
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_different_matcher_creates_a_second_rule(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", pattern="STARBUCKS")])
        result = service.create_rules([
            _rule("Peets", pattern="PEETS", category="Travel")
        ])

        assert result.created == 1
        assert result.conflicts == 0
        assert _active_rule_count(db) == 2

    @pytest.mark.unit
    def test_batch_conflicts_against_its_own_earlier_row(self, db: Database) -> None:
        """Two outputs for one matcher in one batch: the second is refused."""
        result = CategorizationService(db).create_rules([
            _rule("Coffee", category="Food & Drink"),
            _rule("Coffee travel", category="Travel"),
        ])

        assert result.created == 1
        assert result.conflicts == 1
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_conflict_row_binds_to_the_existing_rules_updated_at(
        self, db: Database
    ) -> None:
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee")])
        service.create_rules([_rule("Coffee travel", category="Travel")])

        row = db.execute(
            "SELECT existing_rule_id, existing_rule_updated_at, status "
            "FROM app.rule_conflicts"
        ).fetchone()
        assert row is not None
        assert row[0] == created.rule_ids[0]
        assert row[2] == "pending"
        live = db.execute(
            "SELECT updated_at FROM app.categorization_rules WHERE rule_id = ?",
            [created.rule_ids[0]],
        ).fetchone()
        assert live is not None
        assert row[1] == live[0]

    @pytest.mark.unit
    def test_redetecting_the_same_conflict_updates_one_row(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee")])
        first = service.create_rules([_rule("Coffee travel", category="Travel")])
        second = service.create_rules([_rule("Coffee travel", category="Travel")])

        assert first.conflict_ids == second.conflict_ids
        row = db.execute("SELECT COUNT(*) FROM app.rule_conflicts").fetchone()
        assert row is not None
        assert row[0] == 1

    @pytest.mark.unit
    def test_conflict_emits_a_paired_audit_row(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee")])
        result = service.create_rules([_rule("Coffee travel", category="Travel")])

        rows = db.execute(
            "SELECT action FROM app.audit_log WHERE target_id = ?",
            [result.conflict_ids[0]],
        ).fetchall()
        assert [row[0] for row in rows] == ["rule_conflict.set"]


class TestConflictQueue:
    """What the review surface reads."""

    @pytest.mark.unit
    def test_pending_queue_names_both_sides(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", category="Food & Drink")])
        service.create_rules([_rule("Coffee travel", category="Travel")])

        pending = service.list_rule_conflicts()
        assert len(pending) == 1
        assert pending[0]["existing_category"] == "Food & Drink"
        assert pending[0]["proposed_category"] == "Travel"
        assert service.count_rule_conflicts() == 1

    @pytest.mark.unit
    def test_editing_the_existing_rule_drops_the_conflict(self, db: Database) -> None:
        """The conflict described a comparison that no longer holds."""
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee")])
        service.create_rules([_rule("Coffee travel", category="Travel")])
        assert service.count_rule_conflicts() == 1

        db.execute(
            "UPDATE app.categorization_rules "
            "SET priority = 50, updated_at = updated_at + INTERVAL 1 SECOND "
            "WHERE rule_id = ?",
            [created.rule_ids[0]],
        )

        assert service.list_rule_conflicts() == []

    @pytest.mark.unit
    def test_deactivating_the_existing_rule_drops_the_conflict(
        self, db: Database
    ) -> None:
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee")])
        service.create_rules([_rule("Coffee travel", category="Travel")])

        db.execute(
            "UPDATE app.categorization_rules SET is_active = false WHERE rule_id = ?",
            [created.rule_ids[0]],
        )

        assert service.list_rule_conflicts() == []


class TestConflictResolution:
    """replace / reprioritize / cancel, and the staleness refusal."""

    def _one_conflict(self, db: Database) -> tuple[str, str]:
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee", category="Food & Drink")])
        result = service.create_rules([
            _rule("Coffee travel", category="Travel", priority=100)
        ])
        return created.rule_ids[0], result.conflict_ids[0]

    @pytest.mark.unit
    def test_replace_supersedes_the_existing_rule(self, db: Database) -> None:
        existing_id, conflict_id = self._one_conflict(db)
        service = CategorizationService(db)

        [result] = service.resolve_rule_conflicts(
            [ConflictDecision(conflict_id=conflict_id, resolution="replace")],
            actor="cli",
        )

        assert result.superseded_rule_ids == [existing_id]
        assert result.rule_id is not None
        row = db.execute(
            "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?",
            [existing_id],
        ).fetchone()
        assert row is not None
        assert row[0] is False
        new_row = db.execute(
            "SELECT category, is_active FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [result.rule_id],
        ).fetchone()
        assert new_row == ("Travel", True)

    @pytest.mark.unit
    def test_reprioritize_activates_both_at_the_stated_priority(
        self, db: Database
    ) -> None:
        existing_id, conflict_id = self._one_conflict(db)
        service = CategorizationService(db)

        [result] = service.resolve_rule_conflicts(
            [
                ConflictDecision(
                    conflict_id=conflict_id, resolution="reprioritize", priority=10
                )
            ],
            actor="cli",
        )

        assert result.superseded_rule_ids == []
        row = db.execute(
            "SELECT priority, is_active FROM app.categorization_rules "
            "WHERE rule_id = ?",
            [result.rule_id],
        ).fetchone()
        assert row == (10, True)
        still_active = db.execute(
            "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?",
            [existing_id],
        ).fetchone()
        assert still_active is not None
        assert still_active[0] is True

    @pytest.mark.unit
    def test_cancel_changes_no_rule(self, db: Database) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        service = CategorizationService(db)

        [result] = service.resolve_rule_conflicts(
            [ConflictDecision(conflict_id=conflict_id, resolution="cancel")],
            actor="cli",
        )

        assert result.rule_id is None
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_resolution_settles_the_conflict_and_leaves_history(
        self, db: Database
    ) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        service = CategorizationService(db)
        service.resolve_rule_conflicts(
            [ConflictDecision(conflict_id=conflict_id, resolution="cancel")],
            actor="cli",
        )

        assert service.list_rule_conflicts() == []
        history = service.list_rule_conflict_history()
        assert [row["resolution"] for row in history] == ["cancel"]
        assert service.count_rule_conflict_history() == 1

    @pytest.mark.unit
    def test_resolution_emits_a_paired_audit_row(self, db: Database) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        CategorizationService(db).resolve_rule_conflicts(
            [ConflictDecision(conflict_id=conflict_id, resolution="cancel")],
            actor="cli",
        )

        # `audit_id` is a UUID4 and both rows can share one `occurred_at`, so
        # neither orders the pair — the resolve row's own before/after is what
        # says which mutation it recorded.
        rows = db.execute(
            "SELECT action, before_value, after_value FROM app.audit_log "
            "WHERE target_id = ?",
            [conflict_id],
        ).fetchall()
        assert sorted(row[0] for row in rows) == [
            "rule_conflict.resolve",
            "rule_conflict.set",
        ]
        resolve = next(row for row in rows if row[0] == "rule_conflict.resolve")
        assert json.loads(resolve[1])["status"] == "pending"
        assert json.loads(resolve[2])["status"] == "resolved"

    @pytest.mark.unit
    def test_stale_conflict_is_refused(self, db: Database) -> None:
        existing_id, conflict_id = self._one_conflict(db)
        db.execute(
            "UPDATE app.categorization_rules "
            "SET priority = 50, updated_at = updated_at + INTERVAL 1 SECOND "
            "WHERE rule_id = ?",
            [existing_id],
        )

        with pytest.raises(UserError) as exc:
            CategorizationService(db).resolve_rule_conflicts(
                [ConflictDecision(conflict_id=conflict_id, resolution="replace")],
                actor="cli",
            )
        assert exc.value.code == error_codes.TAXONOMY_RULE_CONFLICT_STALE

    @pytest.mark.unit
    def test_deciding_the_same_conflict_twice_is_refused(self, db: Database) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        service = CategorizationService(db)
        service.resolve_rule_conflicts(
            [ConflictDecision(conflict_id=conflict_id, resolution="cancel")],
            actor="cli",
        )

        with pytest.raises(UserError) as exc:
            service.resolve_rule_conflicts(
                [ConflictDecision(conflict_id=conflict_id, resolution="cancel")],
                actor="cli",
            )
        assert exc.value.code == error_codes.TAXONOMY_RULE_CONFLICT_STALE

    @pytest.mark.unit
    def test_batch_is_atomic(self, db: Database) -> None:
        """One stale member rolls back the whole batch, including a good one."""
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee A", pattern="STARBUCKS")])
        service.create_rules([_rule("Coffee B", pattern="PEETS")])
        good = service.create_rules([
            _rule("Coffee A travel", pattern="STARBUCKS", category="Travel")
        ])
        service.create_rules([
            _rule("Coffee B travel", pattern="PEETS", category="Travel")
        ])
        before = _active_rule_count(db)

        with pytest.raises(UserError):
            service.resolve_rule_conflicts(
                [
                    ConflictDecision(
                        conflict_id=good.conflict_ids[0], resolution="replace"
                    ),
                    ConflictDecision(
                        conflict_id="conf_does_not_exist", resolution="cancel"
                    ),
                ],
                actor="cli",
            )

        assert _active_rule_count(db) == before
        assert service.count_rule_conflicts() == 2

    @pytest.mark.unit
    def test_reprioritize_requires_a_priority(self, db: Database) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        with pytest.raises(UserError) as exc:
            CategorizationService(db).resolve_rule_conflicts(
                [ConflictDecision(conflict_id=conflict_id, resolution="reprioritize")],
                actor="cli",
            )
        assert exc.value.code == error_codes.MUTATION_INVALID_INPUT

    @pytest.mark.unit
    def test_priority_is_only_valid_for_reprioritize(self, db: Database) -> None:
        _existing_id, conflict_id = self._one_conflict(db)
        with pytest.raises(UserError) as exc:
            CategorizationService(db).resolve_rule_conflicts(
                [
                    ConflictDecision(
                        conflict_id=conflict_id, resolution="replace", priority=10
                    )
                ],
                actor="cli",
            )
        assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


class TestRuleTargetConflicts:
    """The declarative target-state path uses the same matcher identity."""

    @pytest.mark.unit
    def test_present_target_conflicting_with_an_active_rule_is_refused(
        self, db: Database
    ) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", category="Food & Drink")])

        plan = service.plan_rule_targets([
            RuleStateTarget(
                rule_id=None,
                state="present",
                merchant_pattern="starbucks",
                match_type="contains",
                category="Travel",
                priority=100,
            )
        ])

        assert len(plan.conflicts) == 1
        assert plan.conflicts[0].proposed_category == "Travel"

    @pytest.mark.unit
    def test_apply_refuses_a_conflicting_plan(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee", category="Food & Drink")])
        plan = service.plan_rule_targets([
            RuleStateTarget(
                rule_id=None,
                state="present",
                merchant_pattern="STARBUCKS",
                match_type="contains",
                category="Travel",
                priority=100,
            )
        ])

        with pytest.raises(UserError) as exc:
            service.apply_rule_targets(plan, actor="mcp")
        assert exc.value.code == error_codes.TAXONOMY_RULE_CONFLICT
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_a_rule_does_not_conflict_with_itself(self, db: Database) -> None:
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee", category="Food & Drink")])

        plan = service.plan_rule_targets([
            RuleStateTarget(
                rule_id=created.rule_ids[0],
                state="present",
                merchant_pattern="STARBUCKS",
                match_type="contains",
                category="Travel",
                priority=100,
            )
        ])

        assert plan.conflicts == ()


class TestSpecificityFloorPrecedesConflicts:
    """An unselective pattern is refused outright, never queued for `replace`."""

    @pytest.mark.unit
    def test_unselective_pattern_is_skipped_not_queued(self, db: Database) -> None:
        """Queueing it would let `replace` activate it without allow_broad."""
        service = CategorizationService(db)
        service.create_rules(
            [_rule("Short", pattern="TO", category="Food & Drink")],
            allow_broad=True,
        )

        result = service.create_rules([
            _rule("Short travel", pattern="TO", category="Travel")
        ])

        assert result.skipped == 1
        assert result.conflicts == 0
        assert service.count_rule_conflicts() == 0
        assert _active_rule_count(db) == 1

    @pytest.mark.unit
    def test_allow_broad_still_reaches_conflict_detection(self, db: Database) -> None:
        """Isolates the ordering: with the floor waived, the same pair conflicts."""
        service = CategorizationService(db)
        service.create_rules(
            [_rule("Short", pattern="TO", category="Food & Drink")],
            allow_broad=True,
        )

        result = service.create_rules(
            [_rule("Short travel", pattern="TO", category="Travel")],
            allow_broad=True,
        )

        assert result.skipped == 0
        assert result.conflicts == 1


class TestRefusalMetric:
    """Both creation paths count their refusals, and neither double-counts."""

    @staticmethod
    def _blocked(surface: str) -> float:
        return (
            REGISTRY.get_sample_value(
                "moneybin_rule_create_conflict_blocked_total",
                {"surface": surface},
            )
            or 0.0
        )

    @pytest.mark.unit
    def test_create_rules_counts_one_refusal(self, db: Database) -> None:
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee")])
        before = self._blocked("create_rules")

        service.create_rules([_rule("Coffee travel", category="Travel")])

        assert self._blocked("create_rules") == before + 1

    @pytest.mark.unit
    def test_rule_targets_count_once_despite_planning_twice(self, db: Database) -> None:
        """`plan_rule_targets` runs on preview and again inside the write."""
        service = CategorizationService(db)
        service.create_rules([_rule("Coffee")])
        before = self._blocked("rule_targets")

        plan = service.plan_rule_targets([
            RuleStateTarget(
                rule_id=None,
                state="present",
                merchant_pattern="STARBUCKS",
                match_type="contains",
                category="Travel",
                priority=100,
            )
        ])
        service.plan_rule_targets([item.target for item in plan.items])
        service.record_rule_conflicts(plan.conflicts, actor="mcp")

        assert self._blocked("rule_targets") == before + 1


class TestTieBreakMatchesTheRuntimeMatcher:
    """Which twin the conflict names must be the one that decides the category."""

    @pytest.mark.unit
    def test_equal_priority_ties_break_on_created_at(self) -> None:
        """`fetch_active_rules` orders `priority ASC, created_at ASC`.

        `rule_id` is a random uuid4 hex, so ordering on it names an arbitrary
        twin as the rule deciding today — and `replace` then deactivates it.
        """
        key = canonical_matcher_key(merchant_pattern="STARBUCKS", match_type="contains")
        # rule_ids chosen so `min` on rule_id picks the *newer* rule.
        older = ActiveRule(
            rule_id="zzzzzzzzzzzz",
            name="Older",
            category="Food & Drink",
            subcategory=None,
            priority=100,
            created_at=datetime(2026, 1, 1, 12, 0, 0),
            updated_at=datetime(2026, 1, 1, 12, 0, 0),
            key=key,
        )
        newer = ActiveRule(
            rule_id="aaaaaaaaaaaa",
            name="Newer",
            category="Travel",
            subcategory=None,
            priority=100,
            created_at=datetime(2026, 6, 1, 12, 0, 0),
            updated_at=datetime(2026, 6, 1, 12, 0, 0),
            key=key,
        )

        conflict = detect_conflict(
            [newer, older],
            key=key,
            name="Proposal",
            merchant_pattern="STARBUCKS",
            match_type="contains",
            min_amount=None,
            max_amount=None,
            account_id=None,
            category="Shopping",
            subcategory=None,
            priority=100,
            created_by="ai",
        )

        assert conflict is not None
        assert conflict.existing_rule_id == "zzzzzzzzzzzz"

    @pytest.mark.unit
    def test_load_active_rules_carries_created_at(self, db: Database) -> None:
        """The tie-break is only as good as the column it reads."""
        CategorizationService(db).create_rules([_rule("Coffee")])

        [loaded] = load_active_rules(db)

        assert loaded.created_at is not None


class TestBatchFinalState:
    """A target batch must be judged against the state it will leave behind."""

    @pytest.mark.unit
    def test_two_present_targets_sharing_a_matcher_are_refused(
        self, db: Database
    ) -> None:
        """Neither exists yet, so pre-batch state alone sees no twin.

        Refused rather than queued: `app.rule_conflicts` records a decision
        about an *active* rule, and here neither side would exist.
        """
        service = CategorizationService(db)

        with pytest.raises(UserError) as exc:
            service.plan_rule_targets([
                RuleStateTarget(
                    rule_id=None,
                    state="present",
                    merchant_pattern="STARBUCKS",
                    match_type="contains",
                    category="Food & Drink",
                    priority=100,
                ),
                RuleStateTarget(
                    rule_id=None,
                    state="present",
                    merchant_pattern="STARBUCKS",
                    match_type="contains",
                    category="Travel",
                    priority=200,
                ),
            ])

        assert exc.value.code == error_codes.MUTATION_INVALID_INPUT
        assert _active_rule_count(db) == 0

    @pytest.mark.unit
    def test_a_case_variant_target_pair_is_refused_too(self, db: Database) -> None:
        """The duplicate-natural-key check compares raw text; this one canonicalizes."""
        service = CategorizationService(db)

        with pytest.raises(UserError) as exc:
            service.plan_rule_targets([
                RuleStateTarget(
                    rule_id=None,
                    state="present",
                    merchant_pattern="STARBUCKS",
                    match_type="contains",
                    category="Food & Drink",
                    priority=100,
                ),
                RuleStateTarget(
                    rule_id=None,
                    state="present",
                    merchant_pattern="starbucks",
                    match_type="contains",
                    category="Travel",
                    priority=200,
                ),
            ])

        assert exc.value.code == error_codes.MUTATION_INVALID_INPUT

    @pytest.mark.unit
    def test_a_target_does_not_conflict_with_a_rule_the_batch_deactivates(
        self, db: Database
    ) -> None:
        """The twin is gone by the time the new rule is live."""
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee", category="Food & Drink")])

        plan = service.plan_rule_targets([
            RuleStateTarget(rule_id=created.rule_ids[0], state="inactive"),
            RuleStateTarget(
                rule_id=None,
                state="present",
                merchant_pattern="STARBUCKS",
                match_type="contains",
                category="Travel",
                priority=100,
            ),
        ])

        assert plan.conflicts == ()


class TestReplaceClearsEveryLiveTwin:
    """`reprioritize` leaves two active twins; `replace` must clear both."""

    @staticmethod
    def _two_active_twins(db: Database) -> tuple[str, str]:
        """Return (older_rule_id, reprioritized_rule_id) sharing one matcher."""
        service = CategorizationService(db)
        first = service.create_rules([
            _rule("Coffee", category="Food & Drink", priority=100)
        ])
        queued = service.create_rules([
            _rule("Coffee travel", category="Travel", priority=100)
        ])
        [second] = service.resolve_rule_conflicts(
            [
                ConflictDecision(
                    conflict_id=queued.conflict_ids[0],
                    resolution="reprioritize",
                    priority=50,
                )
            ],
            actor="cli",
        )
        assert second.rule_id is not None
        return first.rule_ids[0], second.rule_id

    @pytest.mark.unit
    def test_replace_deactivates_every_rule_that_could_still_shadow(
        self, db: Database
    ) -> None:
        older_id, reprioritized_id = self._two_active_twins(db)
        service = CategorizationService(db)
        # priority 200 loses to the older twin at 100 — if that twin survives
        # the replace, the rule the user was told won is fully shadowed.
        queued = service.create_rules([
            _rule("Coffee shopping", category="Shopping", priority=200)
        ])
        assert queued.conflicts == 1

        [result] = service.resolve_rule_conflicts(
            [
                ConflictDecision(
                    conflict_id=queued.conflict_ids[0], resolution="replace"
                )
            ],
            actor="cli",
        )

        assert result.rule_id is not None
        active = {
            str(row[0])
            for row in db.execute(
                "SELECT rule_id FROM app.categorization_rules WHERE is_active"
            ).fetchall()
        }
        assert active == {result.rule_id}
        assert set(result.superseded_rule_ids) == {older_id, reprioritized_id}


class TestStalePruningKeepsHistory:
    """`reviews(kind='rule_conflicts', status='history')` advertises settled rows."""

    @pytest.mark.unit
    def test_resolved_rows_survive_a_later_stale_prune(self, db: Database) -> None:
        service = CategorizationService(db)
        created = service.create_rules([_rule("Coffee", category="Food & Drink")])
        first = service.create_rules([_rule("Coffee travel", category="Travel")])
        service.resolve_rule_conflicts(
            [ConflictDecision(conflict_id=first.conflict_ids[0], resolution="cancel")],
            actor="cli",
        )
        assert service.count_rule_conflict_history() == 1

        # Edit the existing rule, then queue a fresh conflict against it. The
        # stale prune fires for the new row and must not take the settled one.
        db.execute(
            "UPDATE app.categorization_rules "
            "SET priority = 50, updated_at = updated_at + INTERVAL 1 SECOND "
            "WHERE rule_id = ?",
            [created.rule_ids[0]],
        )
        second = service.create_rules([_rule("Coffee travel", category="Travel")])

        assert second.conflicts == 1
        assert service.count_rule_conflict_history() == 1
        assert [row["resolution"] for row in service.list_rule_conflict_history()] == [
            "cancel"
        ]
