"""Transactional standalone topology and exact Golden investment projection."""

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.event_curation import (
    CurationRemap,
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
from moneybin.tables import (
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    DIM_ACCOUNTS,
    FCT_INVESTMENT_TRANSACTIONS,
    INVESTMENT_EVENT_LEGS,
    LOT_SELECTIONS,
)


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
        return self._project(
            self._members.active_members(), self._resolutions.active_resolutions()
        )

    def _project(
        self, members: list[dict[str, Any]], resolutions: list[dict[str, Any]]
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
        return list(project_members(members, evidence, resolutions))

    def source_member(
        self,
        leg: dict[str, Any],
        *,
        event_group_id: str,
        investment_transaction_id: str,
        decision_id: str,
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
            changed = row is None or any(
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
            )
            if row is not None and not changed:
                changed = (
                    self._binding(row).currency_generation
                    != member["currency_generation"]
                )
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

    def _opening_binding(self, row: dict[str, Any]) -> MembershipBinding | None:
        account = self._db.execute(
            f"SELECT currency_code FROM {DIM_ACCOUNTS.full_name} WHERE account_id = ?",
            [row["account_id"]],
        ).fetchone()  # TableRef, bound identity
        if account is None or row["security_id"] is None:
            return None
        return self._binding({
            **row,
            "source_currency_code": row["currency_code"],
            "currency_code": row["currency_code"] or (account[0] if account else None),
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
                    if stamp and row["source_currency_code"] is None
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

    def _reconcile(self, *, actor: str) -> MembershipReconciliationResult:
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
        transition = self.build_membership_transition(proposed)
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

    def build_membership_transition(
        self,
        proposed_sources: list[dict[str, Any]],
        *,
        resolutions: list[dict[str, Any]] | None = None,
        semantic_id_map: dict[str, str] | None = None,
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
            self._db, proposed_sources, evidence, resolutions
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
            binding = self._opening_binding(row)
            if binding is None:
                continue
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
        projected = list(project_members(proposed, evidence, resolutions))
        current = self._project(active, self._resolutions.active_resolutions())
        if (
            not active
            and self._db.execute(
                f"SELECT 1 FROM {LOT_SELECTIONS.full_name} LIMIT 1"
            ).fetchone()
        ):  # fixed TableRef
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
        remap = preflight_membership_transition(self._db, current, projected, mapping)
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
