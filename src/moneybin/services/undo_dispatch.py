"""Resolve an audit row's ``(schema, table)`` to the repo that owns it (REC-PR3).

The undo consumer reads each audit row's ``target_schema``/``target_table`` and
needs the ``*Repo`` whose generic :meth:`BaseRepo.undo_event` reverses it. Rather
than a hand-maintained map — a second place to update for every new repo, exactly
the parallel pattern coherence forbids — the registry is built by discovery:
import every module under ``moneybin.repositories`` and key each ``BaseRepo``
subclass by its declared ``table_ref``. ``test_undo_dispatch`` guards that the
registry covers every concrete repo, so a new repo is dispatchable the moment it
declares its metadata.
"""

from __future__ import annotations

from moneybin.database import Database
from moneybin.repositories import concrete_repo_classes
from moneybin.repositories.base import BaseRepo
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.tables import (
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    INVESTMENT_EVENT_MEMBERS,
    INVESTMENT_MATCH_DECISIONS,
    INVESTMENT_MATCH_FIELD_RESOLUTIONS,
    LOT_SELECTIONS,
    SECURITY_LINKS,
)


def _build_registry() -> dict[tuple[str, str], type[BaseRepo]]:
    registry: dict[tuple[str, str], type[BaseRepo]] = {}
    for cls in concrete_repo_classes():
        key = (cls.table_ref.schema, cls.table_ref.name)
        existing = registry.get(key)
        if existing is not None and existing is not cls:
            raise RuntimeError(
                f"Two repos own {key[0]}.{key[1]}: "
                f"{existing.__name__} and {cls.__name__}"
            )
        registry[key] = cls
    return registry


#: ``(schema, table) -> RepoClass`` for every audited ``app.*`` table.
_REGISTRY: dict[tuple[str, str], type[BaseRepo]] = _build_registry()


def is_registered(schema: str, table: str) -> bool:
    """Whether a repo owns ``schema.table`` (i.e. it sits on the undoable surface)."""
    return (schema, table) in _REGISTRY


def undo_investment_operation(
    events: list[AuditEvent], db: Database, *, actor: str
) -> set[str]:
    """Dispatch investment decisions before generic per-row reversal."""
    selected = [
        event
        for event in events
        if event.target_schema == INVESTMENT_MATCH_DECISIONS.schema
        and event.target_table == INVESTMENT_MATCH_DECISIONS.name
    ]
    dependencies = [
        event
        for event in events
        if event.target_schema == "app"
        and event.target_table
        in {ACCOUNT_SETTINGS.name, ACCOUNT_LINKS.name, SECURITY_LINKS.name}
    ]
    if dependencies and not any(
        event.action in {"investment_match.accept", "investment_match.reject"}
        for event in selected
    ):
        # Dependency inverses are applied by their repos. Their complete scope
        # derives current successors; captured membership rows are never restored.
        parents = {event.audit_id for event in dependencies}
        return {
            event.audit_id
            for event in events
            if event in selected
            or event.target_schema == "app"
            and event.target_table
            in {INVESTMENT_EVENT_MEMBERS.name, INVESTMENT_MATCH_FIELD_RESOLUTIONS.name}
            or event.target_schema == "app"
            and event.target_table == LOT_SELECTIONS.name
            and event.parent_audit_id in parents
        }
    if not selected:
        return set()
    from moneybin.services.investment_matching_service import InvestmentMatchingService

    acceptances = [
        event for event in selected if event.action == "investment_match.accept"
    ]
    if acceptances:
        from moneybin.services.investment_membership_service import (
            InvestmentMembershipService,
        )

        InvestmentMembershipService(db).undo_acceptances(acceptances, actor=actor)
        rejections = [
            event for event in selected if event.action == "investment_match.reject"
        ]
        if rejections:
            InvestmentMatchingService(db).undo_rejections(rejections, actor=actor)
        parents = {event.audit_id for event in selected}
        return {
            event.audit_id
            for event in events
            if event in selected or event.parent_audit_id in parents
        }
    InvestmentMatchingService(db).undo_rejections(selected, actor=actor)
    return {event.audit_id for event in selected}


def repo_for(
    schema: str, table: str, db: Database, *, audit: AuditService | None = None
) -> BaseRepo:
    """Return a repo instance that owns ``schema.table``, bound to ``db``.

    ``audit`` is forwarded to the repo so a caller (e.g. ``UndoService``) can share
    one ``AuditService`` across every row of an undo rather than minting a fresh
    one per dispatch. Raises ``KeyError`` when no repo owns the table — the audit
    row targets a table outside the undoable ``app.*`` surface (e.g.
    ``raw.manual_transactions`` from manual entry, which is out of undo scope).
    """
    cls = _REGISTRY.get((schema, table))
    if cls is None:
        raise KeyError(f"No repo registered for {schema}.{table}")
    return cls(db, audit=audit)
