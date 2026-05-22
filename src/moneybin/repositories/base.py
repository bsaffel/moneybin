"""``BaseRepo`` — shared contract for audited writes to protected ``app.*`` tables.

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of a
protected ``app.*`` table flows through a ``*Repo`` subclass. ``BaseRepo`` owns the
three pieces of mechanics every repository shares, so the contract is structural
rather than per-method discipline:

- ``_transaction`` — open/commit/rollback a DuckDB transaction, or join a
  caller's transaction (``in_outer_txn=True``). DuckDB has no nested
  transactions, so cascading writes within one user action must share one.
- ``_emit_audit`` — the **single** audit-emission point. It pairs the mutation
  with an ``app.audit_log`` row (via ``AuditService``) and is the only place
  ``app_mutation_audit_emitted_total`` increments, so the metric counts every
  repository mutation exactly once (Req 11).
- ``_serialize_for_audit`` — JSON-friendly view of a row for ``before_value`` /
  ``after_value``. Captures the **full** row (Req 4), not a diff.
"""

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, ClassVar

from moneybin.database import Database
from moneybin.metrics.registry import app_mutation_audit_emitted_total
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.tables import TableRef


class BaseRepo:
    """Base class for repositories that own protected ``app.*`` mutation SQL.

    Subclasses set :attr:`repository` (the metric label) and implement
    table-specific mutation methods that:

    1. open (or join) a transaction via :meth:`_transaction`,
    2. read the full pre-mutation row,
    3. perform the ``INSERT`` / ``UPDATE`` / ``DELETE``,
    4. read the full post-mutation row (for ``INSERT`` / ``UPDATE``),
    5. emit a paired audit row via :meth:`_emit_audit`,

    returning the :class:`AuditEvent` so callers can thread its ``audit_id`` as
    ``parent_audit_id`` on subsequent mutations in the same user action.
    """

    #: Stable repository label for the ``app_mutation_audit_emitted_total`` metric.
    repository: ClassVar[str]

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Bind to an open Database; lazily build an ``AuditService`` if absent."""
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)

    @contextmanager
    def _transaction(self, *, in_outer_txn: bool = False) -> Generator[None]:
        """Run the block in a transaction, or as a no-op inside a caller's txn.

        ``in_outer_txn=True`` skips ``begin``/``commit``/``rollback`` so a service
        (or another repo) that already opened a transaction can compose this
        mutation into it — DuckDB does not support nested transactions.
        """
        if in_outer_txn:
            yield
            return
        self._db.begin()
        try:
            yield
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def _emit_audit(
        self,
        *,
        action: str,
        target: tuple[str | None, str | None, str | None],
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        actor: str,
        parent_audit_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> AuditEvent:
        """Emit one paired ``app.audit_log`` row and bump the repo mutation metric.

        The single audit-emission point for every repository. A repo that mutates
        without routing through here shows up as a gap between
        ``app_mutation_audit_emitted_total`` and ``audit_events_emitted_total`` —
        the contract-violation signal Invariant 10 exists to catch.
        """
        event = self._audit.record_audit_event(
            action=action,
            target=target,
            before=before,
            after=after,
            actor=actor,
            parent_audit_id=parent_audit_id,
            context=context,
        )
        app_mutation_audit_emitted_total.labels(
            repository=self.repository, action=action
        ).inc()
        return event

    def _fetch_one(
        self,
        table_ref: TableRef,
        columns: Sequence[str],
        pk_col: str,
        pk_value: object,
        *,
        decode: Callable[[tuple[Any, ...]], dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        """Read one row by primary key as a ``column → value`` dict, or ``None``.

        The shared full-row read every repo uses to capture ``before``/``after``
        state. ``decode`` lets a repo with encoded columns (e.g. JSON) post-process
        the raw tuple; the default zips ``columns`` to the row. ``pk_col`` is a
        code-supplied column name, never user input.
        """
        cols = ", ".join(columns)
        row = self._db.execute(
            f"SELECT {cols} FROM {table_ref.full_name} WHERE {pk_col} = ?",  # noqa: S608  # TableRef + code-constant columns/pk_col
            [pk_value],
        ).fetchone()
        if row is None:
            return None
        if decode is not None:
            return decode(row)
        return dict(zip(columns, row, strict=True))

    @staticmethod
    def _require(
        row: dict[str, Any] | None, pk_col: str, pk_value: object
    ) -> dict[str, Any]:
        """Return ``row``, or raise ``ValueError`` when the keyed row is absent.

        The shared "read the before-image, fail if it's already gone" guard every
        UPDATE/DELETE uses before mutating — so a mutation on a missing key raises
        instead of emitting a phantom ``before=None``/``after=None`` audit row.
        """
        if row is None:
            raise ValueError(f"{pk_col}={pk_value!r} not found")
        return row

    @staticmethod
    def _serialize_for_audit(
        row: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Return a JSON-serializable copy of a full row for before/after capture.

        ``AuditService`` calls ``json.dumps`` on the payload, which rejects
        ``datetime``/``date`` and ``Decimal``. Convert temporals to ISO-8601 and
        ``Decimal`` to ``str`` (lossless — Phase 2 undo restores exact values);
        pass everything else through unchanged. ``None`` maps to ``None`` so
        ``before_value`` for an INSERT (no prior row) stays null.
        """
        if row is None:
            return None
        out: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, Decimal):
                out[key] = str(value)
            elif hasattr(value, "isoformat"):
                out[key] = value.isoformat()
            else:
                out[key] = value
        return out
