"""Transactional standalone topology and exact Golden investment projection."""

import json
from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal
from uuid import uuid4

from moneybin import error_codes
from moneybin.database import Database, has_column
from moneybin.errors import UserError
from moneybin.investments.event_curation import (
    CurationRemap,
    Selection,
    preflight_membership_transition,
)
from moneybin.investments.event_fingerprints import fingerprint
from moneybin.investments.event_membership import (
    EventMember,
    FieldResolution,
    MembershipBinding,
)
from moneybin.investments.event_openings import ALGORITHM_VERSION, reconstruct_openings
from moneybin.investments.event_projection import (
    load_member_evidence,
    project_members,
    read_rows,
    source_revision,
)
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.investment_match_field_resolutions_repo import (
    InvestmentMatchFieldResolutionsRepo,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.tables import (
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    DIM_ACCOUNTS,
    FCT_INVESTMENT_TRANSACTIONS,
    INVESTMENT_EVENT_LEGS,
    INVESTMENT_EVENT_MEMBERS,
    INVESTMENT_MATCH_DECISIONS,
    LOT_SELECTIONS,
    SECURITIES,
)

if TYPE_CHECKING:
    from moneybin.investments.source_choice import SourceChoicePreview


@dataclass(frozen=True)
class MembershipReconciliationResult:
    """Committed standalone transition counts, never acceptance authority."""

    registered: int = 0
    advanced: int = 0
    retired: int = 0


def _identity(row: dict[str, Any]) -> tuple[str, str, str]:
    return row["source_type"], row["source_origin"], row["native_reference"]


def _member_row(member: EventMember, *, now: datetime) -> dict[str, Any]:
    row = asdict(member)
    binding = row.pop("binding")
    return {
        **row,
        "bound_account_id": binding["account_id"],
        "bound_security_id": binding["security_id"],
        "bound_effective_currency": binding["effective_currency"],
        **{
            key: binding[key]
            for key in (
                "currency_source",
                "account_generation",
                "security_generation",
                "currency_generation",
            )
        },
        "created_at": now,
        "projection_changed_at": now,
        "retired_at": None,
    }


class InvestmentMembershipService:
    """Publish complete source-event successors without revising captured evidence."""

    def __init__(self, db: Database) -> None:
        """Compose audited membership and selected-field repositories."""
        self._db = db
        self._members = InvestmentEventMembersRepo(db)
        self._resolutions = InvestmentMatchFieldResolutionsRepo(db)

    def project(self) -> list[dict[str, Any]]:
        """Read exact published evidence without current routing substitution."""
        return self._visible_ledger(
            self._project(
                self._members.active_members(), self._resolutions.active_resolutions()
            )
        )

    def _visible_ledger(
        self,
        ledger: list[dict[str, Any]],
        overrides: dict[str, dict[str, Any] | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Apply the shipped source-choice gate to projection and curation alike."""
        settings = {
            r["account_id"]: r
            for r in read_rows(
                self._db,
                f"SELECT account_id, investment_source_type, investment_source_type_changed_at FROM {ACCOUNT_SETTINGS.full_name}",
            )
        }  # TableRef constant
        for identity, before in (overrides or {}).items():
            if before is None:
                settings.pop(identity, None)
            else:
                settings[identity] = before
        result: list[dict[str, Any]] = []
        for row in ledger:
            setting = settings.get(row["account_id"], {})
            choice = setting.get("investment_source_type")
            if choice is not None and row["source_type"] != choice:
                continue
            stamp = setting.get("investment_source_type_changed_at")
            if isinstance(stamp, str):
                from moneybin.temporal import parse_utc

                stamp = parse_utc(stamp)
            result.append({
                **row,
                "updated_at": max(row["updated_at"], stamp)
                if stamp
                else row["updated_at"],
            })
        return result

    def _project(
        self,
        members: list[dict[str, Any]],
        resolutions: list[dict[str, Any]],
        *,
        source_choices: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        if not members:
            return []
        evidence = load_member_evidence(self._db)
        history = {r["membership_id"]: r for r in self._members.history()}
        choices = {r["resolution_id"]: r for r in self._resolutions.history()}
        for member in members:
            if member["member_kind"] != "opening_lot_reconstruction":
                continue
            context = json.loads(member["reconstruction_evidence"])
            if context["algorithm_version"] != ALGORITHM_VERSION:
                raise UserError(
                    "Opening reconstruction algorithm is unavailable.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
            try:
                sources = [
                    {**history[mid], "retired_at": None}
                    for mid in context["source_member_ids"]
                ]
                selected = [choices[rid] for rid in context["resolution_ids"]]
            except KeyError:
                raise UserError(
                    "Opening reconstruction evidence is incomplete.",
                    code=error_codes.RECOVERY_NO_PATH,
                ) from None
            rows, _contexts = reconstruct_openings(
                self._db,
                sources,
                evidence,
                selected,
                snapshots=[context["snapshot"]],
                guards=context["split_revisions"],
                source_choices=context.get("source_choices", {}),
            )
            row = next(
                (
                    r
                    for r in rows
                    if r["lot_key"] == context["lot_key"]
                    and r["source_security_key"] == context["source_security_key"]
                ),
                None,
            )
            if row is None:
                raise UserError(
                    "Exact opening reconstruction is unavailable.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
            evidence[source_revision(member)] = {**row, "trade_date_basis": "explicit"}
        return list(
            project_members(
                members,
                evidence,
                resolutions,
                source_choices=(
                    self._source_choices() if source_choices is None else source_choices
                ),
            )
        )

    def preview_source_choice(
        self, account_id: str, source: str | None
    ) -> "SourceChoicePreview":
        """Prove a source switch and describe its exact financial impact without writes."""
        from moneybin.investments.source_choice import SourceChoicePreview

        if source not in (None, "manual", "plaid"):
            raise UserError(
                "Invalid investment source type.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        from moneybin.investments.freshness import require_current_relations
        from moneybin.services.restate import _has_prod_environment  # pyright: ignore[reportPrivateUsage]  # same established SQLMesh presence boundary as freshness

        if _has_prod_environment(self._db):
            require_current_relations(self._db, [FCT_INVESTMENT_TRANSACTIONS.full_name])
        active = self._members.active_members() if self._members.has_history() else []
        if not active:
            return SourceChoicePreview(
                account_id,
                source,
                {},
                CurationRemap({}, {}),
                fingerprint("investment_source_choice", [account_id, source]),
            )
        transition = self.build_membership_transition(
            [row for row in active if row["member_kind"] == "source_event"],
            proposed_settings={account_id: {"investment_source_type": source}},
        )
        fields = (
            "account_id",
            "security_id",
            "source_type",
            "source_origin",
            "currency_code",
            "type",
            "subtype",
            "trade_date",
            "settlement_date",
            "original_acquisition_date",
            "quantity",
            "price",
            "amount",
            "fees",
        )
        known = {row["investment_transaction_id"] for row in active}
        provisional = {
            row["investment_transaction_id"]: "new opening " + row["source_event_key"]
            for row in transition.proposed
            if row["investment_transaction_id"] not in known
        }

        def values(ledger: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
            return {
                provisional.get(
                    row["investment_transaction_id"], row["investment_transaction_id"]
                ): {field: row.get(field) for field in fields}
                for row in ledger
                if row["account_id"] == account_id
            }

        before, after = (
            values(transition.current_ledger),
            values(transition.proposed_ledger),
        )
        changes: dict[str, dict[str, tuple[Any, Any]]] = {}
        for identity in sorted(before.keys() | after.keys()):
            old, new = before.get(identity, {}), after.get(identity, {})
            changed = {
                field: (old.get(field), new.get(field))
                for field in fields
                if old.get(field) != new.get(field)
            }
            if identity not in before or identity not in after:
                changed["included"] = (identity in before, identity in after)
            if changed:
                changes[identity] = changed
        return SourceChoicePreview(
            account_id,
            source,
            changes,
            transition.curation,
            fingerprint(
                "investment_source_choice",
                [
                    account_id,
                    source,
                    sorted(active, key=lambda row: row["membership_id"]),
                    sorted(
                        self._resolutions.active_resolutions(),
                        key=lambda row: row["resolution_id"],
                    ),
                    read_rows(
                        self._db,
                        f"SELECT * FROM {ACCOUNT_SETTINGS.full_name} ORDER BY account_id",
                    ),
                    before,
                    after,
                    transition.curation.before,
                ],
            ),
        )

    def _source_choices(
        self, overrides: dict[str, dict[str, Any] | None] | None = None
    ) -> dict[str, str]:
        choices = {
            row["account_id"]: row["investment_source_type"]
            for row in read_rows(
                self._db,
                f"SELECT account_id, investment_source_type FROM {ACCOUNT_SETTINGS.full_name}",
            )
            if row["investment_source_type"] is not None
        }  # fixed TableRef
        for identity, setting in (overrides or {}).items():
            choice = (setting or {}).get("investment_source_type")
            if choice is None:
                choices.pop(identity, None)
            else:
                choices[identity] = choice
        return choices

    def source_member(
        self,
        leg: dict[str, Any],
        *,
        event_group_id: str,
        investment_transaction_id: str,
        decision_id: str | None,
    ) -> dict[str, Any]:
        """Bind a currently verified Proposal leg before any publication."""
        return {
            **_member_row(
                EventMember(
                    event_group_id=event_group_id,
                    investment_transaction_id=investment_transaction_id,
                    source_event_key=leg["source_event_key"],
                    source_type=leg["source_type"],
                    source_origin=leg["source_origin"],
                    native_reference=leg["native_reference"],
                    observation_version=leg["observation_version"],
                    semantic_leg_role=leg["leg_role"],
                    binding=self._binding(leg),
                    decision_id=decision_id,
                ),
                now=datetime.now(UTC),
            ),
            "membership_id": uuid4().hex[:12],
        }

    def _matches_current_binding(
        self, member: dict[str, Any], row: dict[str, Any] | None
    ) -> bool:
        if row is None or any(
            member[key] != row.get(field)
            for key, field in (
                ("source_event_key", "source_event_key"),
                ("observation_version", "observation_version"),
                ("semantic_leg_role", "leg_role"),
                ("bound_account_id", "account_id"),
                ("bound_security_id", "security_id"),
                ("bound_effective_currency", "currency_code"),
                ("account_generation", "account_identity_generation"),
                ("security_generation", "security_identity_generation"),
            )
        ):
            return False
        return self._binding(row).currency_generation == member["currency_generation"]

    def invalidate_accepted(
        self, *, actor: str, parent_audit_id: str | None = None
    ) -> None:
        """Stale drifted Matches while preserving reviewed projection and reservations."""
        from moneybin.repositories.investment_match_decisions_repo import (
            InvestmentMatchDecisionsRepo,
        )

        current = {
            _identity(row): row
            for row in read_rows(
                self._db, f"SELECT * FROM {INVESTMENT_EVENT_LEGS.full_name}"
            )
        }  # TableRef constant
        decisions = InvestmentMatchDecisionsRepo(self._db)
        for member in self._members.active_members():
            if member["decision_id"] is None:
                continue
            row = current.get(_identity(member))
            changed = not self._matches_current_binding(member, row)
            if changed:
                decisions.mark_stale(
                    member["decision_id"],
                    actor=actor,
                    in_outer_txn=True,
                    parent_audit_id=parent_audit_id,
                )

    def locked_components(
        self, decisions: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Derive complete reservations and topology only from durable membership."""
        active = self._members.active_members()
        resolutions = self._resolutions.active_resolutions()
        selection_rows = read_rows(
            self._db,
            f"SELECT * FROM {LOT_SELECTIONS.full_name} ORDER BY investment_transaction_id, lot_id",
        )  # TableRef constant
        result: list[dict[str, Any]] = []
        live = {
            r["proposal_id"]: r
            for r in decisions
            if r["status"] == "accepted"
            or (r["status"] == "stale" and r["accepted_at"] is not None)
        }
        if any(
            m["decision_id"] is not None and m["decision_id"] not in live
            for m in active
        ):
            raise UserError(
                "Active investment membership decision linkage is incomplete.",
                code=error_codes.RECOVERY_NO_PATH,
            )
        for decision_id, row in sorted(live.items()):
            members = [m for m in active if m["decision_id"] == decision_id]
            reserved = sorted(_identity(m) for m in members)
            if (
                not members
                or set(reserved) != {_identity(leg) for leg in row["legs"]}
                or {m["source_event_key"] for m in members} != set(row["members"])
            ):
                raise UserError(
                    "Active investment membership topology is incomplete.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
            result.append({
                "decision_id": decision_id,
                "status": row["status"],
                "members": sorted(row["members"]),
                "reserved_rows": reserved,
                "membership": sorted(members, key=lambda m: m["membership_id"]),
                "field_resolutions": [
                    r for r in resolutions if r["decision_id"] == decision_id
                ],
                "curation_impact": selection_rows,
            })
        return result

    def _opening_binding(
        self,
        row: dict[str, Any],
        currency_overrides: dict[str, str | None] | None = None,
    ) -> MembershipBinding | None:
        account = self._db.execute(
            f"SELECT currency_code FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",
            [row["account_id"]],
        ).fetchone()  # TableRef, bound identity
        route = self._db.execute(
            f"""SELECT COUNT(*), MIN(account_id)
            FROM {ACCOUNT_LINKS.full_name} WHERE status = 'accepted'
            AND source_type = 'plaid' AND source_origin = ? AND ref_kind = 'source_native'
            AND ref_value = ?""",  # TableRef and bound native identity
            [row["source_origin"], row["source_account_key"]],
        ).fetchall()[0]
        setting = self._db.execute(
            f"SELECT currency_code FROM {ACCOUNT_SETTINGS.full_name} WHERE account_id = ?",  # fixed TableRef, bound identity
            [row["account_id"]],
        ).fetchone()
        inherited = (
            setting[0]
            if setting and setting[0] is not None
            else account[0]
            if account
            else None
        )
        if currency_overrides is not None and row["account_id"] in currency_overrides:
            inherited = currency_overrides[row["account_id"]]
        currency = row["currency_code"] or inherited
        security = self._db.execute(
            f"SELECT 1 FROM {SECURITIES.full_name} WHERE security_id = ?",  # TableRef and bound canonical identity
            [row["security_id"]],
        ).fetchone()
        if (
            account is None
            or security is None
            or route[0] != 1
            or route[1] != row["account_id"]
            or row["security_id"] is None
            or currency is None
        ):
            return None
        return self._binding({
            **row,
            "source_currency_code": row["currency_code"],
            "currency_code": currency,
            "account_identity_generation": self._routing_generation(
                "account_links",
                "source_native",
                row["source_account_key"],
                row["source_origin"],
            ),
            "security_identity_generation": self._routing_generation(
                "security_links", "plaid_security_id", row["source_security_key"], None
            ),
        })

    def _routing_generation(
        self, table: str, kind: str, native: str, origin: str | None
    ) -> str:
        row = self._db.execute(
            f"""SELECT COALESCE(SHA256(TO_JSON(ARRAY_AGG(audit_id ORDER BY audit_id))), SHA256(''))
            FROM {AUDIT_LOG.full_name} WHERE target_schema = 'app' AND target_table = ?
            AND COALESCE(after_value ->> '$.source_type', before_value ->> '$.source_type') = 'plaid'
            AND COALESCE(after_value ->> '$.ref_kind', before_value ->> '$.ref_kind') = ?
            AND COALESCE(after_value ->> '$.ref_value', before_value ->> '$.ref_value') = ?
            AND (? IS NULL OR COALESCE(after_value ->> '$.source_origin', before_value ->> '$.source_origin', '') = ?)""",  # TableRef, parameterized route identity
            [table, kind, native, origin, origin],
        ).fetchall()[0]
        return row[0]

    def _binding(self, row: dict[str, Any]) -> MembershipBinding:
        if not row["account_id"] or not row["currency_code"]:
            raise UserError(
                "Investment identity and currency must resolve before Golden publication.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        stamp = self._db.execute(
            f"""SELECT currency_code_changed_at FROM {ACCOUNT_SETTINGS.full_name} WHERE account_id = ?""",  # fixed TableRef, bound identity
            [row["account_id"]],
        ).fetchone()
        return MembershipBinding(
            account_id=row["account_id"],
            security_id=row["security_id"],
            effective_currency=row["currency_code"],
            currency_source="source"
            if row["source_currency_code"] is not None
            else "account",
            account_generation=row["account_identity_generation"],
            security_generation=row["security_identity_generation"],
            currency_generation=fingerprint(
                "investment_currency",
                [
                    row["currency_code"],
                    str(stamp[0])
                    if stamp
                    and stamp[0] is not None
                    and row["source_currency_code"] is None
                    else None,
                ],
            ),
        )

    def reconcile(self, *, actor: str) -> MembershipReconciliationResult:
        """Register current complete standalone topology under one write transaction."""
        self._db.begin()
        try:
            result = self._reconcile(actor=actor)
            self._db.commit()
            return result
        except BaseException:
            self._db.rollback()
            raise

    def require_removable_sources(
        self,
        *,
        source_type: str,
        source_origin: str | None = None,
        native_references: set[str] | None = None,
    ) -> None:
        """Refuse deletion of exact evidence still reserved by a reviewed Match."""
        if not self._members.has_history():
            return
        if any(
            row["decision_id"] is not None
            and row["source_type"] == source_type
            and (source_origin is None or row["source_origin"] == source_origin)
            and (
                native_references is None
                or row["native_reference"] in native_references
            )
            for row in self._members.active_members()
        ):
            raise UserError(
                "This source contains evidence used by an accepted investment Match.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                hint="Undo the affected Match acceptance before removing its source records.",
            )

    def removal_baseline(self) -> list[dict[str, Any]] | None:
        """Hold the complete pre-delete ledger in memory for the shared proof."""
        return self.project() if self._members.has_history() else None

    def reconcile_after_removal(
        self, baseline: list[dict[str, Any]] | None, *, actor: str
    ) -> None:
        """Retire removed standalone evidence inside the deletion owner's transaction."""
        if baseline is not None:
            self._reconcile(actor=actor, current_ledger_override=baseline)

    def _reconcile(
        self, *, actor: str, current_ledger_override: list[dict[str, Any]] | None = None
    ) -> MembershipReconciliationResult:
        self.invalidate_accepted(actor=actor)
        rows = read_rows(
            self._db,
            f"""SELECT * FROM {INVESTMENT_EVENT_LEGS.full_name}
            WHERE original_investment_transaction_id IS NOT NULL
              AND has_resolved_identity AND currency_code IS NOT NULL
            ORDER BY source_event_key, native_reference""",
        )  # fixed TableRef
        current: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            current[row["source_event_key"]].append(row)
        active = self._members.active_members()
        prior: dict[str, list[dict[str, Any]]] = defaultdict(list)
        reserved = {_identity(row) for row in active if row["decision_id"] is not None}
        for row in active:
            if row["decision_id"] is None and row["member_kind"] == "source_event":
                prior[row["source_event_key"]].append(row)
        retire: list[dict[str, Any]] = []
        register: list[EventMember] = []
        registered = advanced = retired = 0
        for key in sorted(set(prior) | set(current)):
            old, new = prior.get(key, []), current.get(key, [])
            if any(_identity(row) in reserved for row in new):
                continue
            same_shape = bool(old and new) and {
                (_identity(r), r["semantic_leg_role"]) for r in old
            } == {(_identity(r), r["leg_role"]) for r in new}
            old_by_identity = {_identity(row): row for row in old}
            bindings = {_identity(row): self._binding(row) for row in new}
            unchanged = same_shape and all(
                old_by_identity[_identity(row)]["observation_version"]
                == row["observation_version"]
                and all(
                    old_by_identity[_identity(row)].get(name) == value
                    for name, value in _member_row(
                        EventMember(
                            event_group_id="",
                            investment_transaction_id="",
                            source_event_key=key,
                            source_type=row["source_type"],
                            source_origin=row["source_origin"],
                            native_reference=row["native_reference"],
                            observation_version=row["observation_version"],
                            semantic_leg_role=row["leg_role"],
                            binding=bindings[_identity(row)],
                        ),
                        now=datetime.now(UTC),
                    ).items()
                    if name.startswith("bound_")
                    or name.endswith("generation")
                    or name == "currency_source"
                )
                for row in new
            )
            if unchanged:
                continue
            retire.extend(old)
            group_id = old[0]["event_group_id"] if same_shape else uuid4().hex[:12]
            for row in new:
                register.append(
                    EventMember(
                        event_group_id=group_id,
                        investment_transaction_id=old_by_identity[_identity(row)][
                            "investment_transaction_id"
                        ]
                        if same_shape
                        else uuid4().hex[:12],
                        source_event_key=key,
                        source_type=row["source_type"],
                        source_origin=row["source_origin"],
                        native_reference=row["native_reference"],
                        observation_version=row["observation_version"],
                        semantic_leg_role=row["leg_role"],
                        binding=bindings[_identity(row)],
                    )
                )
            if same_shape:
                advanced += len(new)
            else:
                registered += len(new)
                retired += len(old)
        now = datetime.now(UTC)
        retained = [
            r
            for r in active
            if r["membership_id"] not in {old["membership_id"] for old in retire}
            and r["member_kind"] == "source_event"
        ]
        proposed = retained + [
            {**_member_row(m, now=now), "membership_id": uuid4().hex[:12]}
            for m in register
        ]
        transition = self.build_membership_transition(
            proposed, current_ledger_override=current_ledger_override
        )
        old_openings = {
            r["native_reference"]: r
            for r in transition.current
            if r["member_kind"] == "opening_lot_reconstruction"
        }
        new_openings = {
            r["native_reference"]: r
            for r in transition.proposed
            if r["member_kind"] == "opening_lot_reconstruction"
        }
        registered += len(new_openings.keys() - old_openings.keys())
        retired += len(old_openings.keys() - new_openings.keys())
        advanced += sum(
            old_openings[key]["membership_id"] != new_openings[key]["membership_id"]
            for key in old_openings.keys() & new_openings.keys()
        )
        self.publish_transition(transition, actor=actor)
        from moneybin.services.investment_matching_service import (
            InvestmentMatchingService,
        )

        InvestmentMatchingService(self._db).invalidate_pending(actor=actor)
        return MembershipReconciliationResult(
            registered=registered, advanced=advanced, retired=retired
        )

    def plan_equivalence(
        self, *, kind: Literal["account", "security"], previous: str, proposed: str
    ) -> CurationRemap:
        """Preview a typed identity merge through the same complete-ledger proof."""
        mappings = {kind: {previous: proposed}}
        sources = [
            row
            for row in self._members.active_members()
            if row["member_kind"] == "source_event"
        ]
        revised: list[dict[str, Any]] = []
        for row in sources:
            binding = _from_row(row).binding
            original = binding
            binding = self._equivalent_binding(binding, mappings)
            if row["decision_id"] is not None:
                binding = replace(
                    binding, effective_currency=original.effective_currency
                )
            revised.append(
                {
                    **_member_row(
                        replace(_from_row(row), binding=binding), now=datetime.now(UTC)
                    ),
                    "membership_id": uuid4().hex[:12],
                }
                if binding != _from_row(row).binding
                else row
            )
        return self.build_membership_transition(
            revised, opening_equivalences=mappings
        ).curation

    def _equivalent_binding(
        self, binding: MembershipBinding, mappings: dict[str, dict[str, str]]
    ) -> MembershipBinding:
        account = mappings.get("account", {}).get(
            binding.account_id, binding.account_id
        )
        security = (
            mappings.get("security", {}).get(binding.security_id, binding.security_id)
            if binding.security_id is not None
            else None
        )
        currency = binding.effective_currency
        if account != binding.account_id and binding.currency_source == "account":
            row = self._db.execute(
                f"SELECT currency_code FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",  # fixed TableRef, bound survivor
                [account],
            ).fetchone()
            if row is None or row[0] is None:
                raise UserError(
                    "Surviving investment Account currency is unresolved.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            currency = row[0]
        return replace(
            binding,
            account_id=account,
            security_id=security,
            effective_currency=currency,
        )

    def rebind_dependency(
        self,
        events: list[AuditEvent],
        *,
        equivalences: dict[str, dict[str, str]] | None = None,
    ) -> None:
        """Preflight current dependency successors inside the dependency owner's transaction."""
        from moneybin.repositories.investment_match_decisions_repo import (
            InvestmentMatchDecisionsRepo,
        )
        from moneybin.services.investment_matching_service import (
            InvestmentMatchingService,
        )

        has_history = self._members.has_history()
        decisions = InvestmentMatchDecisionsRepo(self._db)
        pending = (
            [row for row in decisions.all_rows() if row["status"] == "pending"]
            if has_column(self._db, INVESTMENT_MATCH_DECISIONS, "proposal_id")
            else []
        )
        if not has_history and not pending:
            return
        if not has_column(self._db, INVESTMENT_EVENT_MEMBERS, "membership_id"):
            raise UserError(
                "Investment planning state requires a database upgrade before dependency changes.",
                code=error_codes.INFRA_DATABASE_UPGRADE_REQUIRED,
            )
        active = self._members.active_members()
        observed = read_rows(
            self._db, f"SELECT * FROM {INVESTMENT_EVENT_LEGS.full_name}"
        )  # fixed TableRef
        evidence = load_member_evidence(self._db)
        settings_before: dict[str, dict[str, Any] | None] = {}
        currency_changes: dict[str, str | None] = {}
        account_ids: set[str] = set()
        securities: set[str] = set()
        native_accounts: set[tuple[str, str]] = set()
        native_securities: set[str] = set()
        manual_rows: set[str] = set()
        for event in events:
            before, after = event.before_value or {}, event.after_value or {}
            if event.target_table == ACCOUNT_SETTINGS.name:
                identity = str(event.target_id)
                settings_before.setdefault(identity, event.before_value)
                account_ids.add(identity)
                if before.get("currency_code") != after.get("currency_code"):
                    currency_changes[identity] = after.get("currency_code")
            elif event.target_table == ACCOUNT_LINKS.name:
                account_ids.update(
                    str(row["account_id"])
                    for row in (before, after)
                    if row.get("account_id")
                )
                for row in (before, after):
                    if (
                        row.get("source_type") == "plaid"
                        and row.get("ref_kind") == "source_native"
                    ):
                        native_accounts.add((row["source_origin"], row["ref_value"]))
            else:
                securities.update(
                    str(row["security_id"])
                    for row in (before, after)
                    if row.get("security_id")
                )
                for row in (before, after):
                    if (
                        row.get("source_type") == "plaid"
                        and row.get("ref_kind") == "plaid_security_id"
                    ):
                        native_securities.add(row["ref_value"])
                    if (
                        row.get("source_type") == "manual"
                        and row.get("ref_kind") == "manual_investment_transaction_id"
                    ):
                        manual_rows.add(row["ref_value"])
        if any(value is None for value in currency_changes.values()):
            from moneybin.investments.opening_sql import model_query

            # The materialized Account still carries its old override until rebuild.
            fresh_accounts = {
                row["account_id"]: row["currency_code"]
                for row in read_rows(
                    self._db, model_query("int_accounts__merged").sql(dialect="duckdb")
                )
            }
            currency_changes = {
                identity: fresh_accounts.get(identity) if value is None else value
                for identity, value in currency_changes.items()
            }
        for row in observed:
            if (
                row["source_currency_code"] is None
                and row["account_id"] in currency_changes
            ):
                row["currency_code"] = currency_changes[row["account_id"]]

        def affected(row: dict[str, Any]) -> bool:
            identity = _identity(row)
            fact = evidence.get(source_revision(row), row)
            return (
                row.get("bound_account_id", row.get("account_id")) in account_ids
                or row.get("bound_security_id", row.get("security_id")) in securities
                or (identity[1], fact.get("source_account_key")) in native_accounts
                or fact.get("source_security_key") in native_securities
                or identity[0] == "manual"
                and identity[2] in manual_rows
            )

        impacted = {
            _identity(row)
            for row in active
            if row["member_kind"] == "source_event" and affected(row)
        }
        impacted.update(_identity(row) for row in observed if affected(row))
        current = {_identity(row): row for row in observed}
        actor = events[-1].actor
        for proposal in pending:
            if any(affected(leg) for leg in proposal["legs"]):
                decisions.mark_stale(
                    proposal["proposal_id"],
                    actor=actor,
                    in_outer_txn=True,
                    parent_audit_id=events[-1].audit_id,
                )
        if not has_history:
            return

        def equivalent(kind: str, identity: str | None) -> str | None:
            mapping = (equivalences or {}).get(kind, {})
            while identity is not None and identity in mapping:
                identity = mapping[identity]
            return identity

        for index, member in enumerate(active):
            if (
                member["member_kind"] != "source_event"
                or member["decision_id"] is None
                or _identity(member) not in impacted
            ):
                continue
            row = current.get(_identity(member))
            account = equivalent("account", member["bound_account_id"])
            security = equivalent("security", member["bound_security_id"])
            if (
                account != member["bound_account_id"]
                or security != member["bound_security_id"]
            ):
                if (
                    row is None
                    or row["account_id"] != account
                    or row["security_id"] != security
                ):
                    raise UserError(
                        "Equivalent investment dependencies did not publish complete routes.",
                        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                    )
                successor = {
                    **member,
                    "bound_account_id": account,
                    "bound_security_id": security,
                    "account_generation": row["account_identity_generation"],
                    "security_generation": row["security_identity_generation"],
                }
                if row["currency_code"] == member["bound_effective_currency"]:
                    successor["currency_generation"] = self._binding(
                        row
                    ).currency_generation
                member = {
                    **_member_row(_from_row(successor), now=datetime.now(UTC)),
                    "membership_id": uuid4().hex[:12],
                }
                active[index] = member
            if not self._matches_current_binding(member, row):
                decisions.mark_stale(
                    member["decision_id"],
                    actor=actor,
                    in_outer_txn=True,
                    parent_audit_id=events[-1].audit_id,
                )
        old_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        new_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for member in active:
            if (
                member["member_kind"] == "source_event"
                and member["decision_id"] is None
            ):
                old_groups[member["source_event_key"]].append(member)
        for row in observed:
            if (
                row["original_investment_transaction_id"] is not None
                and row["has_resolved_identity"]
                and row["currency_code"] is not None
            ):
                new_groups[row["source_event_key"]].append(row)
        while True:
            expanded = set(impacted)
            for rows in [*old_groups.values(), *new_groups.values()]:
                ids = {_identity(row) for row in rows}
                if ids & impacted:
                    expanded.update(ids)
            if expanded == impacted:
                break
            impacted = expanded
        retained = [
            member
            for member in active
            if member["member_kind"] == "source_event"
            and (member["decision_id"] is not None or _identity(member) not in impacted)
        ]
        reserved = {
            _identity(member) for member in active if member["decision_id"] is not None
        }
        for rows in new_groups.values():
            ids = {_identity(row) for row in rows}
            if not ids & impacted or ids & reserved:
                continue
            shape = {(_identity(row), row["leg_role"]) for row in rows}
            prior: list[dict[str, Any]] = next(
                (
                    group
                    for group in old_groups.values()
                    if {(_identity(row), row["semantic_leg_role"]) for row in group}
                    == shape
                ),
                list[dict[str, Any]](),
            )
            prior_by_identity = {_identity(row): row for row in prior}
            group_id = prior[0]["event_group_id"] if prior else uuid4().hex[:12]
            for row in rows:
                old = prior_by_identity.get(_identity(row))
                if old is not None and self._matches_current_binding(old, row):
                    retained.append(old)
                else:
                    retained.append(
                        self.source_member(
                            row,
                            event_group_id=group_id,
                            investment_transaction_id=old["investment_transaction_id"]
                            if old
                            else uuid4().hex[:12],
                            decision_id=None,
                        )
                    )
        transition = self.build_membership_transition(
            retained,
            prior_settings=settings_before,
            account_currency_overrides=currency_changes,
        )
        self.publish_transition(
            transition, actor=actor, parent_audit_id=events[-1].audit_id
        )
        InvestmentMatchingService(self._db).invalidate_pending(
            actor=actor, parent_audit_id=events[-1].audit_id
        )

    def prepare_reversal(
        self, acceptances: list[AuditEvent]
    ) -> tuple["MembershipTransition", dict[str, str]]:
        """Reconstruct former complete components using their normal current lifecycle."""
        from moneybin.repositories.investment_match_decisions_repo import (
            InvestmentMatchDecisionsRepo,
        )

        def refuse() -> UserError:
            return UserError(
                "Complete current investment successor topology cannot be proved.",
                code=error_codes.RECOVERY_NO_PATH,
            )

        required = {
            "prior_membership_ids",
            "new_membership_ids",
            "supersedes_decision_ids",
            "prior_resolution_ids",
            "new_resolution_ids",
        }
        if not acceptances or any(
            event.action != "investment_match.accept"
            or event.target_id is None
            or not required <= (event.context_json or {}).keys()
            for event in acceptances
        ):
            raise refuse()
        acceptance = acceptances[-1]
        if any(event.operation_id != acceptance.operation_id for event in acceptances):
            raise refuse()
        contexts = [event.context_json or {} for event in acceptances]
        for previous, following in zip(contexts, contexts[1:], strict=False):
            if set(previous["new_membership_ids"]) != set(
                following["prior_membership_ids"]
            ) or set(previous["new_resolution_ids"]) != set(
                following["prior_resolution_ids"]
            ):
                raise refuse()
        targets = {str(event.target_id) for event in acceptances}
        parents = {event.audit_id for event in acceptances}
        if len(targets) != len(acceptances):
            raise refuse()
        context = {
            "prior_membership_ids": contexts[0]["prior_membership_ids"],
            "new_membership_ids": contexts[-1]["new_membership_ids"],
            "prior_resolution_ids": contexts[0]["prior_resolution_ids"],
            "new_resolution_ids": contexts[-1]["new_resolution_ids"],
            "supersedes_decision_ids": [
                identity
                for item in contexts
                for identity in item["supersedes_decision_ids"]
            ],
        }
        history = {row["membership_id"]: row for row in self._members.history()}
        try:
            prior = [history[mid] for mid in context["prior_membership_ids"]]
            published = [history[mid] for mid in context["new_membership_ids"]]
        except KeyError as exc:
            raise refuse() from exc
        decisions = {
            row["proposal_id"]: row
            for row in InvestmentMatchDecisionsRepo(self._db).all_rows()
        }
        active = self._members.active_members()
        ratified = [row for row in active if row["decision_id"] in targets]
        if any(
            identity not in decisions
            or decisions[identity]["status"] not in {"accepted", "stale"}
            or decisions[identity]["accepted_at"] is None
            or not any(row["decision_id"] == identity for row in ratified)
            for identity in targets
        ):
            raise refuse()
        published_sources = [
            row
            for row in published
            if row["member_kind"] == "source_event" and row["decision_id"] in targets
        ]
        if {_identity(row) for row in ratified} != {
            _identity(row) for row in published_sources
        }:
            raise refuse()
        events = AuditService(self._db).events_for_operation(acceptance.operation_id)
        changed_ids = set(context["prior_membership_ids"]) - set(
            context["new_membership_ids"]
        )
        former = [
            row
            for row in prior
            if row["membership_id"] in changed_ids
            and row["member_kind"] == "source_event"
        ]
        if {_identity(row) for row in former} != {_identity(row) for row in ratified}:
            raise refuse()
        proposed = [
            row
            for row in active
            if row["member_kind"] == "source_event"
            and row["decision_id"] not in targets
        ]
        reserved = {
            _identity(row) for row in proposed if row["decision_id"] is not None
        }
        current = read_rows(
            self._db, f"SELECT * FROM {INVESTMENT_EVENT_LEGS.full_name}"
        )  # fixed TableRef
        by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in current:
            if (
                row["original_investment_transaction_id"] is not None
                and row["has_resolved_identity"]
                and row["currency_code"] is not None
            ):
                by_event[row["source_event_key"]].append(row)
        groups: dict[tuple[str | None, str], list[dict[str, Any]]] = defaultdict(list)
        for row in former:
            groups[row["decision_id"], row["event_group_id"]].append(row)
        statuses: dict[str, str] = {}
        resolutions = [
            row
            for row in self._resolutions.active_resolutions()
            if row["decision_id"] not in targets
        ]
        now = datetime.now(UTC)
        used_events: set[str] = set()
        for (prior_decision, _group), old in groups.items():
            if prior_decision is not None:
                if (
                    prior_decision not in context["supersedes_decision_ids"]
                    or prior_decision not in decisions
                    or decisions[prior_decision]["status"] != "reversed"
                ):
                    raise refuse()
                expected = {_identity(leg) for leg in decisions[prior_decision]["legs"]}
                if {_identity(row) for row in old} != expected:
                    raise refuse()
                initial = {_identity(row): row for row in published_sources}
                successors = {_identity(row): row for row in ratified}
                restored: list[dict[str, Any]] = []
                for member in old:
                    baseline, successor = (
                        initial[_identity(member)],
                        successors[_identity(member)],
                    )
                    if any(
                        baseline[key] != successor[key]
                        for key in (
                            "observation_version",
                            "source_event_key",
                            "semantic_leg_role",
                            "decision_id",
                        )
                    ):
                        raise refuse()
                    bound = dict(member)
                    # Within one accepted decision, binding advances are ratified
                    # equivalences; other identity changes retain reviewed rows.
                    for field, generation in (
                        ("bound_account_id", "account_generation"),
                        ("bound_security_id", "security_generation"),
                    ):
                        if (
                            member[field] == baseline[field]
                            and successor[field] != baseline[field]
                        ):
                            bound[field] = successor[field]
                            bound[generation] = successor[generation]
                            if (
                                field == "bound_account_id"
                                and member["bound_effective_currency"]
                                == baseline["bound_effective_currency"]
                                == successor["bound_effective_currency"]
                            ):
                                bound["currency_generation"] = successor[
                                    "currency_generation"
                                ]
                    restored.append(bound)
                proposed.extend(
                    {
                        **_member_row(_from_row(row), now=now),
                        "membership_id": uuid4().hex[:12],
                    }
                    for row in restored
                )
                observed = {_identity(row): row for row in current}
                stale = any(
                    not self._matches_current_binding(row, observed.get(_identity(row)))
                    for row in restored
                )
                statuses[prior_decision] = "stale" if stale else "accepted"
                if (
                    "prior_resolution_ids" not in context
                    or "new_resolution_ids" not in context
                ):
                    raise refuse()
                choice_history = {
                    row["resolution_id"]: row for row in self._resolutions.history()
                }
                if not set(context["prior_resolution_ids"]) <= choice_history.keys():
                    raise refuse()
                prior_choices = [
                    choice_history[rid]
                    for rid in context["prior_resolution_ids"]
                    if choice_history[rid]["decision_id"] == prior_decision
                ]
                keys = [
                    (row["investment_transaction_id"], row["field_name"])
                    for row in prior_choices
                ]
                if len(set(keys)) != len(keys):
                    raise refuse()
                resolutions.extend(
                    {
                        **row,
                        "resolution_id": uuid4().hex[:12],
                        "created_at": now,
                        "retired_at": None,
                    }
                    for row in prior_choices
                )
                continue
            identities = {_identity(row) for row in old}
            candidates = {
                key: rows
                for key, rows in by_event.items()
                if identities & {_identity(row) for row in rows}
            }
            if not candidates or any(
                reserved & {_identity(row) for row in rows}
                for rows in candidates.values()
            ):
                raise refuse()
            for key, rows in candidates.items():
                if key in used_events:
                    raise refuse()
                used_events.add(key)
                same = {(_identity(row), row["leg_role"]) for row in rows} == {
                    (_identity(row), row["semantic_leg_role"]) for row in old
                }
                group_id = old[0]["event_group_id"] if same else uuid4().hex[:12]
                old_by_identity = {_identity(row): row for row in old}
                for row in rows:
                    proposed.append(
                        self.source_member(
                            row,
                            event_group_id=group_id,
                            investment_transaction_id=old_by_identity[_identity(row)][
                                "investment_transaction_id"
                            ]
                            if same
                            else uuid4().hex[:12],
                            decision_id=None,
                        )
                    )
        prior_choice_ids = context.get("prior_resolution_ids")
        if prior_choice_ids is None:
            raise refuse()
        choice_history = {
            row["resolution_id"]: row for row in self._resolutions.history()
        }
        if not set(prior_choice_ids) <= choice_history.keys():
            raise refuse()
        prior_choices = [choice_history[rid] for rid in prior_choice_ids]
        curations = [
            event
            for event in events
            if event.target_table == LOT_SELECTIONS.name
            and event.parent_audit_id in parents
        ]
        overrides: dict[str, Selection] = {}
        last: dict[str, Selection] = {}
        for event in curations:
            if (
                event.before_value is None
                or event.after_value is None
                or event.target_id is None
                or any(
                    "selections" not in value
                    for value in (event.before_value, event.after_value)
                )
            ):
                raise refuse()
            before = sorted(
                (row["lot_id"], Decimal(row["quantity"]))
                for row in event.before_value["selections"]
            )
            after = sorted(
                (row["lot_id"], Decimal(row["quantity"]))
                for row in event.after_value["selections"]
            )
            if event.target_id in last and last[event.target_id] != before:
                raise refuse()
            overrides.setdefault(event.target_id, before)
            last[event.target_id] = after
        if any(
            LotSelectionsRepo(self._db).list_for_disposal(identity) != collection
            for identity, collection in last.items()
        ):
            raise refuse()
        mapping: dict[str, str] = {}
        for old in former:
            targets = {
                row["investment_transaction_id"]
                for row in proposed
                if _identity(row) == _identity(old)
                and row["semantic_leg_role"] == old["semantic_leg_role"]
            }
            if len(targets) == 1:
                mapping[old["investment_transaction_id"]] = targets.pop()
        origin = None
        if curations:
            affected_groups = {
                row["event_group_id"]
                for row in prior + published
                if row["membership_id"]
                in set(context["prior_membership_ids"])
                ^ set(context["new_membership_ids"])
            }
            origin = [
                row
                for row in self._project(active, self._resolutions.active_resolutions())
                if row["event_group_id"] not in affected_groups
            ]
            origin.extend(
                row
                for row in self._project(
                    [{**member, "retired_at": None} for member in prior], prior_choices
                )
                if row["event_group_id"] in affected_groups
            )
        return self.build_membership_transition(
            proposed,
            resolutions=resolutions,
            semantic_id_map=mapping,
            curation_overrides=overrides if curations else None,
            curation_origin_ledger=origin,
        ), statuses

    def undo_acceptances(self, acceptances: list[AuditEvent], *, actor: str) -> None:
        """Preflight the entire operation before publishing any inverse topology."""
        from moneybin.repositories.investment_match_decisions_repo import (
            InvestmentMatchDecisionsRepo,
        )
        from moneybin.services.investment_matching_service import (
            InvestmentMatchingService,
        )

        transition, statuses = self.prepare_reversal(acceptances)
        decisions = InvestmentMatchDecisionsRepo(self._db)
        roots = [
            decisions.restore_lifecycle(
                str(acceptance.target_id),
                status="reversed",
                actor=actor,
                undoes_operation_id=acceptance.operation_id,
            )
            for acceptance in reversed(acceptances)
        ]
        root = roots[0]
        for prior_id, status in statuses.items():
            decisions.restore_lifecycle(
                prior_id,
                status=status,
                actor=actor,
                parent_audit_id=root.audit_id,
                undoes_operation_id=acceptances[0].operation_id,
            )
        self.publish_transition(transition, actor=actor, parent_audit_id=root.audit_id)
        InvestmentMatchingService(self._db).invalidate_pending(
            actor=actor, parent_audit_id=root.audit_id
        )

    def build_membership_transition(
        self,
        proposed_sources: list[dict[str, Any]],
        *,
        resolutions: list[dict[str, Any]] | None = None,
        semantic_id_map: dict[str, str] | None = None,
        curation_overrides: dict[str, Selection] | None = None,
        curation_origin_ledger: list[dict[str, Any]] | None = None,
        prior_settings: dict[str, dict[str, Any] | None] | None = None,
        proposed_settings: dict[str, dict[str, Any] | None] | None = None,
        current_ledger_override: list[dict[str, Any]] | None = None,
        opening_equivalences: dict[str, dict[str, str]] | None = None,
        account_currency_overrides: dict[str, str | None] | None = None,
    ) -> "MembershipTransition":
        """Recompute opening topology before proving the complete proposed ledger."""
        active = self._members.active_members()
        resolutions = (
            self._resolutions.active_resolutions()
            if resolutions is None
            else resolutions
        )
        evidence = load_member_evidence(self._db)
        rows, contexts = reconstruct_openings(
            self._db,
            proposed_sources,
            evidence,
            resolutions,
            source_choices=self._source_choices(proposed_settings),
        )
        prior = {
            r["native_reference"]: r
            for r in active
            if r["member_kind"] == "opening_lot_reconstruction"
        }
        proposed = list(proposed_sources)
        now = datetime.now(UTC)
        mapping = dict(semantic_id_map or {})
        for row in rows:
            context = contexts[
                row["source_account_key"],
                row["source_security_key"],
                row["source_origin"],
            ]
            context = {
                **context,
                "source_security_key": row["source_security_key"],
                "lot_key": row["lot_key"],
            }
            key = fingerprint(
                "opening_key",
                [
                    row["source_origin"],
                    row["source_account_key"],
                    row["source_security_key"],
                    row["first_snapshot_source_file"],
                    row["lot_key"],
                ],
            )
            binding = self._opening_binding(row, account_currency_overrides)
            if binding is None:
                continue
            if opening_equivalences is not None:
                binding = self._equivalent_binding(binding, opening_equivalences)
            version = fingerprint("opening_revision", [context, asdict(binding)])
            old = prior.get(key)
            if old and old["observation_version"] == version:
                member = old
            else:
                member = {
                    **_member_row(
                        EventMember(
                            event_group_id=old["event_group_id"]
                            if old
                            else uuid4().hex[:12],
                            investment_transaction_id=old["investment_transaction_id"]
                            if old
                            else uuid4().hex[:12],
                            member_kind="opening_lot_reconstruction",
                            source_event_key=key,
                            source_type="plaid",
                            source_origin=row["source_origin"],
                            native_reference=key,
                            observation_version=version,
                            semantic_leg_role="acquisition",
                            binding=binding,
                            reconstruction_evidence=json.dumps(context, sort_keys=True),
                        ),
                        now=now,
                    ),
                    "membership_id": uuid4().hex[:12],
                }
            proposed.append(member)
            evidence[source_revision(member)] = {**row, "trade_date_basis": "explicit"}
            mapping[row["investment_transaction_id"]] = member[
                "investment_transaction_id"
            ]
        projected = self._visible_ledger(
            list(
                project_members(
                    proposed,
                    evidence,
                    resolutions,
                    source_choices=self._source_choices(proposed_settings),
                )
            ),
            proposed_settings,
        )
        current = (
            current_ledger_override
            if current_ledger_override is not None
            else self._visible_ledger(
                self._project(
                    active,
                    self._resolutions.active_resolutions(),
                    source_choices=self._source_choices(prior_settings),
                ),
                prior_settings,
            )
        )
        if (
            not active
            and self._db.execute(
                f"SELECT 1 FROM {LOT_SELECTIONS.full_name} LIMIT 1"
            ).fetchone()
        ):  # fixed TableRef
            if (
                self._db.execute(
                    """SELECT 1 FROM information_schema.tables
                WHERE table_catalog = current_database()
                  AND table_schema = ? AND table_name = ?""",
                    [
                        FCT_INVESTMENT_TRANSACTIONS.schema,
                        FCT_INVESTMENT_TRANSACTIONS.name,
                    ],
                ).fetchone()
                is None
            ):
                raise UserError(
                    "Cannot preserve pre-cut lot selections without the prior ledger.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
            current = read_rows(
                self._db, f"SELECT * FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}"
            )  # fixed TableRef
            for member in proposed_sources:
                observed = evidence[source_revision(member)]
                original = observed.get(
                    "original_investment_transaction_id",
                    observed.get("investment_transaction_id"),
                )
                if original is None:
                    raise UserError(
                        "Pre-cut source identity is unavailable.",
                        code=error_codes.RECOVERY_NO_PATH,
                    )
                mapping[original] = member["investment_transaction_id"]
        remap = preflight_membership_transition(
            self._db,
            current,
            projected,
            mapping,
            restored_selections=curation_overrides,
            restoration_ledger=self._visible_ledger(curation_origin_ledger)
            if curation_origin_ledger is not None
            else None,
        )
        return MembershipTransition(
            active, proposed, resolutions, current, projected, remap, mapping
        )

    def publish_transition(
        self,
        transition: "MembershipTransition",
        *,
        actor: str,
        parent_audit_id: str | None = None,
    ) -> None:
        """Publish a preflighted topology inside its owner's transaction."""
        proposed_ids = {r["membership_id"] for r in transition.proposed}
        active_ids = {r["membership_id"] for r in transition.current}
        for row in transition.current:
            if row["membership_id"] not in proposed_ids:
                self._members.retire(
                    row["membership_id"],
                    actor=actor,
                    in_outer_txn=True,
                    parent_audit_id=parent_audit_id,
                )
        for row in transition.proposed:
            if row["membership_id"] in active_ids:
                continue
            self._members.register(
                _from_row(row),
                membership_id=row["membership_id"],
                actor=actor,
                in_outer_txn=True,
                parent_audit_id=parent_audit_id,
            )
        old_resolutions = self._resolutions.active_resolutions()
        old_ids = {r["resolution_id"] for r in old_resolutions}
        new_ids = {r["resolution_id"] for r in transition.resolutions}
        for row in old_resolutions:
            if row["resolution_id"] not in new_ids:
                self._resolutions.retire(
                    row["resolution_id"],
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                    in_outer_txn=True,
                )
        for row in transition.resolutions:
            if row["resolution_id"] not in old_ids:
                self._resolutions.record(
                    FieldResolution(**{
                        key: row[key]
                        for key in (
                            "decision_id",
                            "investment_transaction_id",
                            "field_name",
                            "source_type",
                            "source_origin",
                            "native_reference",
                            "observation_version",
                        )
                    }),
                    resolution_id=row["resolution_id"],
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                    in_outer_txn=True,
                )
        for disposal, selections in transition.curation.replacements.items():
            LotSelectionsRepo(self._db).set_for_disposal(
                investment_transaction_id=disposal,
                selections=selections,
                actor=actor,
                in_outer_txn=True,
                parent_audit_id=parent_audit_id,
            )


@dataclass(frozen=True)
class MembershipTransition:
    """A complete ledger proof prepared before the first App publication."""

    current: list[dict[str, Any]]
    proposed: list[dict[str, Any]]
    resolutions: list[dict[str, Any]]
    current_ledger: list[dict[str, Any]]
    proposed_ledger: list[dict[str, Any]]
    curation: CurationRemap
    semantic_id_map: dict[str, str]


def _from_row(row: dict[str, Any]) -> EventMember:
    return EventMember(
        **{
            key: row[key]
            for key in (
                "event_group_id",
                "investment_transaction_id",
                "source_event_key",
                "source_type",
                "source_origin",
                "native_reference",
                "observation_version",
                "semantic_leg_role",
                "member_kind",
                "decision_id",
                "reconstruction_evidence",
            )
        },
        binding=MembershipBinding(
            account_id=row["bound_account_id"],
            security_id=row["bound_security_id"],
            effective_currency=row["bound_effective_currency"],
            currency_source=row["currency_source"],
            account_generation=row["account_generation"],
            security_generation=row["security_generation"],
            currency_generation=row["currency_generation"],
        ),
    )
