"""Rule-conflict detection and resolution (MB-124 / #420).

Two rules whose canonical matchers are equal fire on the same transactions.
When they disagree about the category, priority and creation order silently
pick the winner and the loser is fully shadowed. These tests pin the refusal,
the recorded conflict, and each of the three resolutions.
"""

from __future__ import annotations

import json

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

        assert result.superseded_rule_id == existing_id
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

        assert result.superseded_rule_id is None
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
