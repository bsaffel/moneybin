"""The lifecycle capability behind ``moneybin reports create / set / delete``.

R5 of ``docs/specs/reports-dynamic.md``: one service owns save, update, rename,
archive, delete, and classification downgrade, and both surfaces reach the same
outcomes through it. Reading a saved report is *not* here — that is the shipped
``reports`` catalog/runner, which spans all three tiers.

Every mutation goes through :class:`UserReportsRepo` per Invariant 10; this
service composes the repo and never issues DML of its own.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics import registry as metrics
from moneybin.privacy.redaction import mask_strength
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import USER_REPORT_NAME, ParamSpec
from moneybin.reports._framework.derive import (
    class_fingerprint,
    derive_classification,
    with_downgrades,
)
from moneybin.reports._framework.dynamic import (
    declared_params,
    stored_params,
    unknown_semantics,
)
from moneybin.repositories.user_reports_repo import UNSET, Unset, UserReportsRepo
from moneybin.services.audit_service import AuditEvent
from moneybin.services.entity_reference import (
    EntityCandidate,
    ResolvedEntity,
    resolve_entity_reference,
)

logger = logging.getLogger(__name__)

ConfirmedVia = Literal["prompt", "flag"]
"""How a caller obtained the confirmation for a classification downgrade.

``prompt`` is a human answering the interactive confirm; ``flag`` is ``--yes``
supplied in the invocation. Recorded on the audit row because the surface and
actor are identical either way, so nothing else can tell an assistant supplying
the flag from the human the flag is supposed to represent.
"""


@dataclass(frozen=True, slots=True)
class SaveOutcome:
    """One completed save, with the notes R3 puts on the response.

    ``unresolved_columns`` is a note and never a gate: an unresolvable
    projection masks at run time but must not block the save.
    ``cleared_downgrades`` names approvals the mutation dropped, because a
    downgrade is a human judgment about one column of one query and carrying it
    onto rewritten SQL would be the same stale-authority failure one level down.
    """

    report_id: str
    name: str
    unresolved_columns: tuple[str, ...] = ()
    cleared_downgrades: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReclassifyOutcome:
    """One approved classification downgrade."""

    report_id: str
    column: str
    from_class: DataClass
    to_class: DataClass


def is_weaker_class(from_class: DataClass, to_class: DataClass) -> bool:
    """Whether ``to_class`` is a legitimate downgrade of ``from_class``.

    The tier must **strictly fall**, and masking may not strengthen. Requiring
    the tier to fall is what rejects an equal-tier weakening, which is the
    dangerous case a "neither component rises and at least one falls" rule
    admits: ``ROUTING_NUMBER → ACCOUNT_IDENTIFIER`` holds CRITICAL and drops
    masking from whole to partial, so every future run would render the real
    last four digits where every row previously showed ``'*****'``.

    The downgrade mechanism exists because derivation over-classifies *computed*
    columns — an author asserting "this z-score reveals no amount" makes a claim
    about information content. That argument is unavailable when both classes
    agree on the tier and differ only in transform, so no reason can waive it.
    Same rule ``.claude/rules/reports.md`` already applies to materialized
    reports at CI time; the runtime path gets the same guard, not a weaker one.
    """
    return to_class.tier < from_class.tier and mask_strength(to_class) <= mask_strength(
        from_class
    )


class UserReportsService:
    """Save, update, archive, delete, and downgrade user-created reports."""

    def __init__(self, db: Database) -> None:
        """Compose the audited repo over one open database connection."""
        self._db = db
        self._repo = UserReportsRepo(db)

    def rows(self, *, include_archived: bool = False) -> list[dict[str, Any]]:
        """Stored rows in stable ``report_id`` order, JSON columns decoded."""
        return self._repo.list(include_archived=include_archived)

    def resolve(self, handle: str) -> dict[str, Any]:
        """Resolve a ``report_id`` or name to its stored row.

        Uses the shared reference ladder — stable ID, then exact name, then
        unambiguous normalized name — so an exact ``report_id`` always wins. That
        ordering is what delivers R5's collision-recovery promise: a report whose
        name is contested across tiers still has an id its owner can type.
        Archived reports resolve too; archiving hides a report from the catalog
        rather than revoking access to it.
        """
        rows = self._repo.list(include_archived=True)
        resolution = resolve_entity_reference(
            handle,
            (
                EntityCandidate(
                    entity_id=str(row["report_id"]), display_name=str(row["name"])
                )
                for row in rows
            ),
        )
        if not isinstance(resolution, ResolvedEntity):
            candidates = getattr(resolution, "candidate_ids", ())
            if candidates:
                raise UserError(
                    "Report reference is ambiguous.",
                    code=error_codes.REPORT_ID_AMBIGUOUS,
                    details={"report_id": handle, "candidates": sorted(candidates)},
                )
            raise UserError(
                "Report not found.",
                code=error_codes.REPORT_ID_NOT_FOUND,
                details={"report_id": handle},
            )
        return next(row for row in rows if row["report_id"] == resolution.entity_id)

    def create(
        self,
        *,
        name: str,
        query_sql: str,
        description: str | None = None,
        params: Sequence[ParamSpec] = (),
        actor: str,
    ) -> SaveOutcome:
        """Derive the report's privacy contract and persist it.

        Saving requires a name and a row-returning read-only SELECT over the
        permitted schemas — nothing else. The class map is derived here and
        stored; the user never supplies or sees it unless they ask.
        """
        self._require_free_name(name)
        try:
            derived = derive_classification(
                self._db, query_sql=query_sql, params=params
            )
        except UserError:
            metrics.USER_REPORT_SAVES_TOTAL.labels(outcome="rejected").inc()
            raise

        event = self._repo.create(
            name=name,
            description=description,
            query_sql=query_sql,
            params=stored_params(params, derived.parameter_classes),
            classes={
                column: data_class.value
                for column, data_class in derived.classes.items()
            },
            semantics=_stored_semantics(),
            class_fingerprint=derived.fingerprint,
            actor=actor,
        )
        report_id = _target_of(event)
        _count_save(derived.unresolved_columns)
        return SaveOutcome(
            report_id=report_id,
            name=name,
            unresolved_columns=derived.unresolved_columns,
        )

    def update(
        self,
        handle: str,
        *,
        name: str | Unset = UNSET,
        description: str | None | Unset = UNSET,
        query_sql: str | Unset = UNSET,
        params: Sequence[ParamSpec] | Unset = UNSET,
        is_active: bool | Unset = UNSET,
        actor: str,
    ) -> SaveOutcome:
        """Apply a partial update, re-deriving whenever the query can have moved.

        A request touching ``query_sql`` **or** ``params`` re-runs the whole save
        pipeline and persists the new SQL, class map, parameter classes, and
        fingerprint in a single repo write. Skipping it would re-create the bug
        this spec exists to prevent: re-aliasing an ``AGGREGATE`` projection
        ``x`` to ``routing_number AS x`` would serve a routing number under the
        stale LOW class, because ``run_report`` treats the stored map as
        authoritative. A ``set`` touching neither field skips derivation.
        """
        row = self.resolve(handle)
        report_id = str(row["report_id"])
        if not isinstance(name, Unset):
            self._require_free_name(name, current_report_id=report_id)

        fields: dict[str, Any] = {
            "name": name,
            "description": description,
            "is_active": is_active,
        }
        if isinstance(query_sql, Unset) and isinstance(params, Unset):
            self._repo.set(report_id, actor=actor, **fields)
            return SaveOutcome(
                report_id=report_id,
                name=str(name) if not isinstance(name, Unset) else str(row["name"]),
            )

        effective_sql = (
            str(row["query_sql"]) if isinstance(query_sql, Unset) else query_sql
        )
        effective_params = (
            declared_params(row.get("params") or ())
            if isinstance(params, Unset)
            else tuple(params)
        )
        try:
            derived = derive_classification(
                self._db, query_sql=effective_sql, params=effective_params
            )
        except UserError:
            metrics.USER_REPORT_SAVES_TOTAL.labels(outcome="rejected").inc()
            raise

        # A downgrade is a judgment about one column of one query, so it does not
        # survive a rewrite. Cleared even when the new SQL happens to derive the
        # same class: the approval was granted against text nobody re-read.
        cleared = (
            tuple(sorted(row.get("class_downgrades") or {}))
            if not isinstance(query_sql, Unset)
            else ()
        )
        downgrades: Mapping[str, Mapping[str, str]] = (
            {} if cleared else (row.get("class_downgrades") or {})
        )
        classes = with_downgrades(dict(derived.classes), downgrades)

        self._repo.set(
            report_id,
            query_sql=effective_sql,
            params=stored_params(effective_params, derived.parameter_classes),
            classes={
                column: data_class.value for column, data_class in classes.items()
            },
            class_downgrades=dict(downgrades),
            class_fingerprint=class_fingerprint(
                self._db,
                query_sql=effective_sql,
                classes=classes,
                parameter_classes=derived.parameter_classes,
                class_downgrades=downgrades,
            ),
            actor=actor,
            **fields,
        )
        _count_save(derived.unresolved_columns)
        return SaveOutcome(
            report_id=report_id,
            name=str(name) if not isinstance(name, Unset) else str(row["name"]),
            unresolved_columns=derived.unresolved_columns,
            cleared_downgrades=cleared,
        )

    def delete(self, handle: str, *, actor: str) -> AuditEvent:
        """Remove one saved report permanently.

        There is no soft delete: the repo captures the full prior row in
        ``before_value``, so the generic undo path restores a deleted report
        exactly. ``is_active`` is user intent about visibility, not recovery.
        """
        row = self.resolve(handle)
        return self._repo.delete(str(row["report_id"]), actor=actor)

    def reclassify(
        self,
        handle: str,
        *,
        column: str,
        to_class: DataClass,
        reason: str,
        confirmed: bool | None,
        confirmed_via: ConfirmedVia,
        expected_fingerprint: str,
        actor: str,
    ) -> ReclassifyOutcome:
        """Lower one column's masking floor, permanently, on human approval.

        ``confirmed`` is a required argument rather than a default so no caller
        can reach a durable downgrade by omission. A downgrade lowers the floor
        for that column on every future run and every surface, on the strength
        of a ``reason`` the caller supplies about its own request — which is why
        ``design-principles.md`` puts it outside agent self-accept entirely.

        Three states, not two: ``True`` approves, ``False`` is a human declining,
        and ``None`` is a surface that had no way to ask. All three are honoured
        identically — only ``True`` proceeds — but the last two are counted
        apart, because a surface refusing every downgrade for mechanical reasons
        would otherwise read as users saying no.

        ``confirmed_via`` says *which path* supplied that answer, and is likewise
        required — a default would record an assistant's ``--yes`` as a human at
        a prompt, which is the one thing the audit row exists to distinguish.
        ``actor`` cannot carry it: both paths are the same surface.

        ``expected_fingerprint`` is the ``class_fingerprint`` the caller read
        *before* it asked, and is required for the same reason the other two are:
        an approval is about a specific revision, and a caller cannot opt out of
        saying which. The confirmation is a human decision, so the window between
        reading the row and writing it is seconds to minutes wide — a
        ``reports set --sql`` landing inside it changes what the approved column
        *is*, and the strictly-weaker rule below cannot notice, because it only
        asks that the tier drop. Mismatch refuses; the cost is a re-run against
        the current SQL, versus a permanently lowered floor on SQL nobody read.
        The same guard as ``import_confirm``'s digest re-check, for the same
        reason. ``delete`` needs no equivalent: it is bound to an identity that
        a concurrent edit does not move.

        ``from`` is the class **derivation currently produces**, not the stored
        (possibly already-downgraded) one, so an approval is always recorded
        against the floor it actually waived.
        """
        row = self.resolve(handle)
        report_id = str(row["report_id"])
        if confirmed is not True:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="declined" if confirmed is False else "no_elicitation"
            ).inc()
            raise UserError(
                "A classification downgrade needs explicit confirmation.",
                code=error_codes.REPORT_CLASS_CONFIRM_REQUIRED,
                hint=(
                    "It permanently lowers what is masked for this column on "
                    "every future run of this report."
                )
                if confirmed is False
                else (
                    "This surface had no way to ask. A human must confirm the "
                    "downgrade; an assistant must not supply it on their behalf."
                ),
            )

        if str(row["class_fingerprint"]) != expected_fingerprint:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_revision_moved"
            ).inc()
            raise UserError(
                "This report changed while the confirmation was open, so the "
                "approval no longer applies to it — nothing was reclassified. "
                "Re-run to see the current classification.",
                code=error_codes.REPORT_CHANGED_DURING_CONFIRMATION,
                details={"report_id": report_id, "column": column},
            )

        # Defaults are stripped for derivation, the same way the run path strips
        # them: `_refuse_sensitive_defaults` is a *write* gate on a default being
        # stored, and this request stores no parameters at all. Leaving them on
        # let an upstream reclassification of some unrelated filter's column
        # refuse a downgrade whose caller never mentioned that parameter.
        declared = tuple(
            replace(parameter, default=None, required=True)
            for parameter in declared_params(row.get("params") or ())
        )
        derived = derive_classification(
            self._db, query_sql=str(row["query_sql"]), params=declared
        )
        from_class = derived.classes.get(column)
        if from_class is None:
            # Not `refused_not_weaker`: that label is the abuse signal — someone
            # trying to publish a value everyone agrees is sensitive — and the
            # comparison it names cannot even be evaluated for a column that does
            # not exist. Counting a typo under it inflates the one number here
            # that is supposed to mean something.
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_unknown_column"
            ).inc()
            raise UserError(
                f"This report returns no column named {column!r}.",
                code=error_codes.REPORT_COLUMN_UNKNOWN,
                details={"columns": sorted(derived.classes)},
            )
        if not is_weaker_class(from_class, to_class):
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_not_weaker"
            ).inc()
            raise UserError(
                f"{to_class.value} does not mask less than {from_class.value}: a "
                "downgrade must drop the sensitivity tier and may not mask more "
                "weakly at the same tier.",
                code=error_codes.REPORT_CLASS_NOT_WEAKER,
                details={
                    "column": column,
                    "from": from_class.value,
                    "to": to_class.value,
                },
            )

        downgrades: dict[str, Mapping[str, str]] = {
            **(row.get("class_downgrades") or {}),
            column: {"from": from_class.value, "to": to_class.value, "reason": reason},
        }
        classes = with_downgrades(dict(derived.classes), downgrades)
        self._repo.set(
            report_id,
            classes={
                column_name: data_class.value
                for column_name, data_class in classes.items()
            },
            class_downgrades=downgrades,
            class_fingerprint=class_fingerprint(
                self._db,
                query_sql=str(row["query_sql"]),
                classes=classes,
                parameter_classes=derived.parameter_classes,
                class_downgrades=downgrades,
            ),
            actor=actor,
            context={"confirmed_via": confirmed_via},
        )
        metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
            outcome=f"confirmed_{confirmed_via}"
        ).inc()
        logger.warning(
            f"user_report.reclassify report_id={report_id} column={column} "
            f"from={from_class.value} to={to_class.value}"
        )
        return ReclassifyOutcome(
            report_id=report_id,
            column=column,
            from_class=from_class,
            to_class=to_class,
        )

    def _require_free_name(
        self, name: str, *, current_report_id: str | None = None
    ) -> None:
        """Refuse a name held anywhere in the registry, in both directions.

        The check spans every tier because ``reports`` resolves one name across
        all three: two reports sharing a name make the catalog and its runner
        ambiguous. Defining a precedence order instead would mean a user's saved
        report changes meaning when an unrelated package is installed — a rule
        nobody can see from the catalog.
        """
        if USER_REPORT_NAME.fullmatch(name) is None:
            raise UserError(
                f"{name!r} is not a valid report name.",
                code=error_codes.REPORT_NAME_INVALID,
                hint="Use lowercase letters, digits, hyphens, and underscores.",
            )
        existing = self._repo.find_by_name(name)
        if existing is not None and existing["report_id"] != current_report_id:
            if not existing["is_active"]:
                # An archived name stays taken because `name` is UNIQUE and the
                # row remains. Reporting a bare conflict for a report the default
                # catalog hides is the failure this branch exists to prevent.
                raise UserError(
                    f"An archived report already uses the name {name!r}.",
                    code=error_codes.REPORT_NAME_ARCHIVED,
                    details={"report_id": existing["report_id"]},
                    hint=(
                        "Restore it with `moneybin reports set <id> --restore`, "
                        "or free the name with `moneybin reports delete <id>`."
                    ),
                )
            raise UserError(
                f"A saved report already uses the name {name!r}.",
                code=error_codes.REPORT_NAME_TAKEN,
                details={"report_id": existing["report_id"]},
            )
        # The packaged tiers only: passing `db` here would rebuild every saved
        # report's spec to re-derive names this method already has from the repo.
        for report in get_report_catalog().list():
            if report.name == name:
                raise UserError(
                    f"The name {name!r} is already held by {report.report_id}.",
                    code=error_codes.REPORT_NAME_TAKEN,
                    details={"report_id": report.report_id},
                )


def _stored_semantics() -> dict[str, Any]:
    """The ``semantics`` column for a user query: explicitly unknown.

    ``provenance`` is stored empty on purpose. It is the one derivable field, so
    ``spec_from_row`` reads it off the SQL instead — a stored copy could drift
    from the query it claims to describe.
    """
    semantics = unknown_semantics()
    return {
        "unit": semantics.unit,
        "currency": semantics.currency,
        "sign": semantics.sign,
        "kind": semantics.kind,
        "valuation_basis": semantics.valuation_basis,
        "fx_basis": semantics.fx_basis,
        "time_basis": semantics.time_basis,
        "denominator": semantics.denominator,
        "comparison_window": semantics.comparison_window,
        "exclusions": list(semantics.exclusions),
        "provenance": [],
    }


def _count_save(unresolved_columns: Sequence[str]) -> None:
    """Record one accepted save and any columns it could not resolve."""
    metrics.USER_REPORT_SAVES_TOTAL.labels(outcome="saved").inc()
    if unresolved_columns:
        metrics.USER_REPORT_UNRESOLVED_COLUMNS_TOTAL.inc(len(unresolved_columns))


def _target_of(event: AuditEvent) -> str:
    """The report id a mutation audited, which the repo always sets."""
    if event.target_id is None:  # pragma: no cover — repo always sets the target
        raise ValueError("user report mutation emitted no audit target")
    return event.target_id
