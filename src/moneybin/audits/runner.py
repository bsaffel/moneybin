"""Execute SQLMesh standalone audits — the one place an audit's SQL is run.

Every data-quality check on a `core.*` relation is defined once, in
`src/moneybin/sqlmesh/audits/*.sql`. This module renders and executes those
definitions so each consumer reports the same verdict: `DoctorService` folds
the outcomes into its report, and the scenario runner asserts on them from
`assert_transform_audit`.

Standalone audits are non-blocking in SQLMesh (a violation routes to
`console.log_warning`), so nothing fails a transform on their account — the
outcome has to be read deliberately, which is what this module exists for.
"""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass, field

from moneybin.database import Database, sqlmesh_context

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AuditOutcome:
    """One standalone audit's verdict against the live database.

    ``violation_ids`` holds column 0 of the audit's result set — every audit
    projects the offending entity's ID there, by convention.
    """

    name: str
    violation_ids: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def passed(self) -> bool:
        """True when the audit ran and found nothing."""
        return self.error is None and not self.violation_ids


def run_standalone_audits(
    db: Database, *, names: Collection[str] | None = None
) -> list[AuditOutcome]:
    """Run every standalone audit, or just the ones ``names`` lists.

    Raises ``KeyError`` when ``names`` asks for an audit SQLMesh did not
    discover — a typo, or a file that lost its ``standalone TRUE`` header,
    must fail loudly rather than report a clean run.
    """
    outcomes: list[AuditOutcome] = []
    with sqlmesh_context(db) as ctx:
        discovered = dict(ctx.standalone_audits)
        if names is not None:
            missing = sorted(set(names) - discovered.keys())
            if missing:
                raise KeyError(f"unknown standalone audit(s): {missing}")
            selected = {name: discovered[name] for name in sorted(set(names))}
        else:
            selected = discovered
        for name, audit in selected.items():
            try:
                sql = audit.render_audit_query().sql(dialect="duckdb")
                rows = db.execute(sql).fetchall()  # noqa: S608 — rendered from trusted audit files
            except Exception as e:  # noqa: BLE001 — per-audit isolation; one broken audit must not hide the rest
                logger.warning(f"Transform audit {name!r} failed to run: {e}")
                outcomes.append(AuditOutcome(name=name, error=f"audit failed: {e}"))
                continue
            outcomes.append(
                AuditOutcome(name=name, violation_ids=[str(r[0]) for r in rows])
            )
    return outcomes
