"""The lifecycle capability behind ``moneybin reports create / set / delete``.

R5 of ``docs/specs/reports-dynamic.md``: one service owns save, update, rename,
archive, delete, and classification downgrade, and both surfaces reach the same
outcomes through it. Reading a saved report is *not* here — that is the shipped
``reports`` catalog/runner, which spans all three tiers.

Every mutation goes through :class:`UserReportsRepo` per Invariant 10; this
service composes the repo and never issues DML of its own.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, Literal

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.limits import (
    DESCRIPTION_MAX_LEN,
    IDENTIFIER_MAX_LEN,
    NOTE_MAX_LEN,
    REPORT_DOWNGRADES_MAX_LEN,
    REPORT_PARAMS_MAX_LEN,
    REPORT_QUERY_MAX_LEN,
)
from moneybin.metrics import registry as metrics
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import get_report_catalog
from moneybin.reports._framework.contract import USER_REPORT_NAME, ParamSpec
from moneybin.reports._framework.derive import (
    DerivedClassification,
    class_fingerprint,
    derive_classification,
    drifted_names,
    is_weaker_class,
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
    ``floored_columns`` is its raw/prep counterpart — columns with no declared
    class, protected only by the value-shape scan and its stated gaps.
    ``cleared_downgrades`` names approvals the mutation dropped, because a
    downgrade is a human judgment about one column of one query and carrying it
    onto rewritten SQL would be the same stale-authority failure one level down.
    """

    report_id: str
    name: str
    unresolved_columns: tuple[str, ...] = ()
    floored_columns: tuple[str, ...] = ()
    cleared_downgrades: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ReclassifyOutcome:
    """One approved classification downgrade."""

    report_id: str
    column: str
    from_class: DataClass
    to_class: DataClass


def _require_bounded(value: str | None, *, field: str, limit: int) -> None:
    """Bound one stored text field, because ``VARCHAR`` does not.

    ``security.md`` requires a maximum on user-supplied strings before they reach
    the database. Every field here is reachable from one ``reports create`` —
    ``--sql-file`` reads a file of any size — and an unbounded blob is then
    re-rendered by ``reports list``, ``reports explain``, and every export
    receipt. The length is reported; the value never is.
    """
    if value is not None and len(value) > limit:
        raise UserError(
            f"A report's {field} may not exceed {limit} characters.",
            code=error_codes.REPORT_FIELD_TOO_LONG,
            details={"field": field, "limit": limit, "length": len(value)},
        )


def _require_bounded_params(entries: Sequence[Mapping[str, Any]]) -> None:
    """Bound the stored ``params`` JSON — the one field a declaration can inflate.

    Measured on the serialized block rather than on any single declared string,
    because that JSON is what the row stores, what the catalog republishes on
    every listing, and what each later mutation copies into its before/after audit
    images. One check therefore covers a long default, a long help string, and a
    large number of parameters at once. ``_refuse_sensitive_defaults`` already
    rejects an above-LOW default outright, so what remains here is the legal kind:
    a default compared against a LOW column, unbounded until now.
    """
    _require_bounded(json.dumps(entries), field="params", limit=REPORT_PARAMS_MAX_LEN)


def _require_bounded_downgrades(downgrades: Mapping[str, Mapping[str, str]]) -> None:
    """Bound the stored ``class_downgrades`` JSON, not one entry of it.

    Same measurement as :func:`_require_bounded_params`, and for the same reason:
    the serialized block is what the row stores and what every later mutation
    copies into its audit images, so a per-entry bound leaves the total free to
    grow with the number of downgraded columns.
    """
    _require_bounded(
        json.dumps(downgrades),
        field="class_downgrades",
        limit=REPORT_DOWNGRADES_MAX_LEN,
    )


class UserReportsService:
    """Save, update, archive, delete, and downgrade user-created reports."""

    def __init__(self, db: Database) -> None:
        """Compose the audited repo over one open database connection."""
        self._db = db
        self._repo = UserReportsRepo(db)

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
        with _counting_rejections():
            self._require_free_name(name)
            _require_bounded(
                description, field="description", limit=DESCRIPTION_MAX_LEN
            )
            _require_bounded(query_sql, field="query", limit=REPORT_QUERY_MAX_LEN)
            derived = derive_classification(
                self._db, query_sql=query_sql, params=params
            )
            # Bounded here rather than beside `description` and `query` above: the
            # thing that has to stay bounded is the stored JSON, which does not
            # exist until derivation has supplied the parameter classes it carries.
            entries = stored_params(params, derived.parameter_classes)
            _require_bounded_params(entries)

        event = self._repo.create(
            name=name,
            description=description,
            query_sql=query_sql,
            params=entries,
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
            floored_columns=derived.floored_columns,
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
        # `resolve` stays outside the count: a handle that names no report is a
        # wrong target, not a refused save, and counting it would put a typo in
        # the same bucket as a rejected name.
        row = self.resolve(handle)
        report_id = str(row["report_id"])
        with _counting_rejections():
            if not isinstance(name, Unset):
                self._require_free_name(name, current_report_id=report_id)
            if not isinstance(description, Unset):
                _require_bounded(
                    description, field="description", limit=DESCRIPTION_MAX_LEN
                )
            if not isinstance(query_sql, Unset):
                _require_bounded(query_sql, field="query", limit=REPORT_QUERY_MAX_LEN)

        fields: dict[str, Any] = {
            "name": name,
            "description": description,
            "is_active": is_active,
        }
        if isinstance(query_sql, Unset) and isinstance(params, Unset):
            self._repo.set(report_id, actor=actor, **fields)
            # Counted like every other save, though it derives nothing. Its
            # refusals count `rejected` above, so leaving its successes uncounted
            # would measure the refused share against a population that excludes
            # them.
            _count_save(())
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
        with _counting_rejections():
            derived = derive_classification(
                self._db, query_sql=effective_sql, params=effective_params
            )
            entries = stored_params(effective_params, derived.parameter_classes)
            _require_bounded_params(entries)

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
            params=entries,
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
            floored_columns=derived.floored_columns,
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
        expected_from_class: DataClass,
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

        ``expected_from_class`` closes the half of that window the fingerprint
        cannot see, and is required for the same reason. Only a *write* refreshes
        a stored fingerprint — a read never does — so an upstream reclassification
        that raises the class of a column this report reads leaves the row, and
        therefore the fingerprint, untouched. The caller would then approve a
        downgrade *from whatever derivation now produces*: ``--to aggregate`` on
        ``SUM(amount)`` is ``TXN_AMOUNT → AGGREGATE`` before such a change and
        ``ROUTING_NUMBER → AGGREGATE`` after it, and both drop a tier, so the
        strictly-weaker rule admits the second too. Callers therefore derive the
        class, show it, and pass back what they showed; a mismatch refuses.

        The argument is a **guard, never an input**. Trusting it would make this
        the one path where a class arrives declared rather than derived — the
        widening ``.claude/rules/reports.md`` exists to prevent.

        ``from`` is therefore the class **derivation currently produces**, not the
        argument and not the stored (possibly already-downgraded) class, so an
        approval is always recorded against the floor it actually waived.
        """
        row = self.resolve(handle)
        report_id = str(row["report_id"])
        if not reason.strip():
            # Checked before the confirmation gate: this is a malformed request,
            # not an unauthorized one, and a caller that prompts first would
            # otherwise spend a human decision on a downgrade that cannot be
            # stored. `--reason " "` satisfies a required-option check.
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_blank_reason"
            ).inc()
            raise UserError(
                "A classification downgrade needs a reason.",
                code=error_codes.REPORT_REASON_REQUIRED,
                hint=(
                    "The stored reason is the only record of why this column "
                    "reveals less than its derived class."
                ),
            )
        try:
            # Beside the blank check, and before the gate, for the same reason:
            # a malformed request, not an unauthorized one. Bounded because it is
            # a stored text field like `description` and `query` — and unlike
            # them it is copied into the before/after row images every later
            # mutation audits, so an unbounded blob is duplicated across the
            # audit history rather than stored once. `_require_bounded` owns the
            # condition and the message; only the counter is added here, because
            # this gate labels every arm and an unlabeled one stops its total
            # equalling attempts.
            _require_bounded(reason, field="reason", limit=NOTE_MAX_LEN)
        except UserError:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_reason_too_long"
            ).inc()
            raise
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

        derived = self._derive(row)
        from_class = self._require_derived_class(derived, column)
        if from_class is not expected_from_class:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_derivation_moved"
            ).inc()
            raise UserError(
                "The classification of this column changed since it was shown, so "
                "the approval no longer applies to it — nothing was reclassified. "
                "Re-run to see the current classification.",
                code=error_codes.REPORT_CHANGED_DURING_CONFIRMATION,
                details={
                    "report_id": report_id,
                    "column": column,
                    "shown": expected_from_class.value,
                    "current": from_class.value,
                },
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

        stored_downgrades: Mapping[str, Mapping[str, str]] = (
            row.get("class_downgrades") or {}
        )
        # The approval covers ONE column, but the write below persists the whole
        # freshly derived map. An upstream reclassification that moved a
        # *different* output column therefore rode along on it: no confirmation
        # was shown for that column, no audit row named it, and refreshing the
        # fingerprint told the read path the stale contract was current — so
        # `_reresolved` stopped degrading it. A weakening reached the stored floor
        # without the gate that exists to be the only way there.
        #
        # Neither guard above can see it. The fingerprint compared is the *stored*
        # one, which no read refreshes, so upstream drift leaves it matching; and
        # `expected_from_class` binds the approved column alone.
        #
        # The same comparison the run path makes, against the same two maps.
        unrelated = tuple(
            name
            for name in drifted_names(
                with_downgrades(dict(derived.classes), stored_downgrades),
                {
                    name: DataClass(value)
                    for name, value in (row.get("classes") or {}).items()
                },
            )
            if name != column
        )
        # Parameters need the same guard and cannot borrow the column one: a
        # filter-only parameter is never projected, so it appears in no output map
        # and a rise in its class is invisible above. The write then keys the
        # fingerprint on the *derived* parameter classes while persisting none of
        # them — `set` takes no `params` — and recomputes the read-set term from the
        # live schema, which is what had been carrying the drift signal. So the next
        # read matches, serves the stale weaker class, and republishes the stored
        # default that `_refuse_sensitive_defaults` would now refuse to write, on a
        # row `_reresolved` had been failing closed.
        drifted_parameters = drifted_names(
            derived.parameter_classes,
            {
                parameter.name: parameter.data_class
                for parameter in declared_params(row.get("params") or ())
            },
        )
        if unrelated or drifted_parameters:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_unrelated_drift"
            ).inc()
            raise UserError(
                "Part of this report's classification changed since it was saved, "
                "so approving this column would store that change too — nothing "
                "was reclassified. Run the report to see what moved, then save it "
                "again before downgrading.",
                code=error_codes.REPORT_CLASSIFICATION_STALE,
                details={
                    "report_id": report_id,
                    "columns": list(unrelated),
                    "parameters": list(drifted_parameters),
                },
            )

        downgrades: dict[str, Mapping[str, str]] = {
            **stored_downgrades,
            column: {"from": from_class.value, "to": to_class.value, "reason": reason},
        }
        # `reason` is bounded per entry above; this bounds what they accumulate
        # into. The map gains an entry per downgraded column and every later
        # mutation copies the whole of it into its before/after audit images, so
        # the row, the catalog read, and the audit history all grow together —
        # the same shape that made the `params` block worth bounding.
        #
        # Here rather than beside the reason check, which deliberately precedes
        # the confirmation gate: the composed map does not exist until
        # `from_class` has been derived, and deriving it is what the fingerprint
        # and class guards above are for.
        _require_bounded_downgrades(downgrades)
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
        # The column is withheld for the same reason the catalog's collision
        # warning withholds a report name: a saved report's output alias is
        # user-authored text, and `amazon_spend` is as plausible a merchant name
        # as a column one. `SanitizedLogFormatter` cannot recognize either. The
        # report_id and the two classes identify the event, and `app.audit_log`
        # holds the column inside the encrypted database where it belongs.
        logger.warning(
            f"user_report.reclassify report_id={report_id} "
            f"from={from_class.value} to={to_class.value}"
        )
        return ReclassifyOutcome(
            report_id=report_id,
            column=column,
            from_class=from_class,
            to_class=to_class,
        )

    def derived_class(self, row: Mapping[str, Any], *, column: str) -> DataClass:
        """The class derivation produces for one column of ``row`` right now.

        The read half of :meth:`reclassify`: a caller derives this before it asks,
        shows it, and hands it back as ``expected_from_class``. Reading it here
        rather than letting the prompt name only the *target* class is what makes
        the human's answer about the floor they are actually waiving.

        Refusing an unknown column is shared with the write path rather than
        duplicated, so a typo is refused before anyone is asked about it and the
        two surfaces cannot disagree about which columns a report returns.
        """
        return self._require_derived_class(self._derive(row), column)

    def _derive(self, row: Mapping[str, Any]) -> DerivedClassification:
        """Run derivation over a stored row's SQL exactly as it stands now.

        Defaults are stripped, the same way the run path strips them:
        ``_refuse_sensitive_defaults`` is a *write* gate on a default being
        stored, and a downgrade request stores no parameters at all. Leaving them
        on let an upstream reclassification of some unrelated filter's column
        refuse a downgrade whose caller never mentioned that parameter.
        """
        declared = tuple(
            replace(parameter, default=None, required=True)
            for parameter in declared_params(row.get("params") or ())
        )
        return derive_classification(
            self._db, query_sql=str(row["query_sql"]), params=declared
        )

    def _require_derived_class(
        self, derived: DerivedClassification, column: str
    ) -> DataClass:
        """One column's derived class, refusing a name the report does not return.

        Not `refused_not_weaker`: that label is the abuse signal — someone trying
        to publish a value everyone agrees is sensitive — and the comparison it
        names cannot even be evaluated for a column that does not exist. Counting
        a typo under it inflates the one number here that is supposed to mean
        something.
        """
        from_class = derived.classes.get(column)
        if from_class is None:
            metrics.USER_REPORT_RECLASSIFY_TOTAL.labels(
                outcome="refused_unknown_column"
            ).inc()
            raise UserError(
                "This report returns no column by that name.",
                code=error_codes.REPORT_COLUMN_UNKNOWN,
                details={"column": column, "columns": sorted(derived.classes)},
            )
        return from_class

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
                "That is not a valid report name.",
                code=error_codes.REPORT_NAME_INVALID,
                hint="Use lowercase letters, digits, hyphens, and underscores.",
                details={"name": name},
            )
        # Separate from the pattern, which is unanchored in length: `[a-z][a-z0-9_-]*`
        # matches a megabyte of letters. DuckDB's VARCHAR is unbounded too, so the
        # bound has to live here.
        if len(name) > IDENTIFIER_MAX_LEN:
            raise UserError(
                f"A report name may not exceed {IDENTIFIER_MAX_LEN} characters.",
                code=error_codes.REPORT_NAME_INVALID,
                details={"limit": IDENTIFIER_MAX_LEN, "length": len(name)},
            )
        existing = self._repo.find_by_name(name)
        if existing is not None and existing["report_id"] != current_report_id:
            if not existing["is_active"]:
                # An archived name stays taken because `name` is UNIQUE and the
                # row remains. Reporting a bare conflict for a report the default
                # catalog hides is the failure this branch exists to prevent.
                raise UserError(
                    "An archived report already uses that name.",
                    code=error_codes.REPORT_NAME_ARCHIVED,
                    details={"name": name, "report_id": existing["report_id"]},
                    hint=(
                        "Restore it with `moneybin reports set <id> --restore`, "
                        "or free the name with `moneybin reports delete <id>`."
                    ),
                )
            raise UserError(
                "A saved report already uses that name.",
                code=error_codes.REPORT_NAME_TAKEN,
                details={"name": name, "report_id": existing["report_id"]},
            )
        # The packaged tiers only: passing `db` here would rebuild every saved
        # report's spec to re-derive names this method already has from the repo.
        for report in get_report_catalog().list():
            if report.name == name:
                raise UserError(
                    f"That name is already held by {report.report_id}.",
                    code=error_codes.REPORT_NAME_TAKEN,
                    details={"name": name, "report_id": report.report_id},
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


@contextmanager
def _counting_rejections() -> Generator[None, None, None]:
    """Count every save this validation pipeline refuses, not only a bad query.

    The increment used to sit around ``derive_classification`` alone, so a refused
    name and every length bound raised past it: the counter undercounted exactly
    the boundary validations that ``rejected`` exists to distinguish from
    ``saved``. Wrapping the pipeline rather than incrementing at each ``raise``
    keeps a new check counted by default — the failure mode here was a check
    landing outside a narrow guard, and adding checks is the normal direction of
    travel.

    Wraps validation and derivation, and stops before the repo write: ``rejected``
    means the *input* was refused, and a write that fails is an infrastructure
    outcome the audit path already carries.
    """
    try:
        yield
    except UserError:
        metrics.USER_REPORT_SAVES_TOTAL.labels(outcome="rejected").inc()
        raise


def _target_of(event: AuditEvent) -> str:
    """The report id a mutation audited, which the repo always sets."""
    if event.target_id is None:  # pragma: no cover — repo always sets the target
        raise ValueError("user report mutation emitted no audit target")
    return event.target_id
