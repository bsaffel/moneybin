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

from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from decimal import Decimal
from typing import Any, ClassVar

from sqlglot import exp

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import app_mutation_audit_emitted_total
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.tables import TableRef


def quote_ident(name: str) -> str:
    """Double-quote a SQL identifier (security.md defense-in-depth for interpolation).

    Repository column/PK names are code-supplied constants, but per
    `.claude/rules/security.md` identifiers interpolated into SQL must be quoted
    regardless — never bare f-string interpolation, even after validation.
    """
    return exp.to_identifier(name, quoted=True).sql("duckdb")


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

    #: The protected ``app.*`` table this repo owns. Drives undo targeting and
    #: the dispatch registry (REC-PR3).
    table_ref: ClassVar[TableRef]

    #: Primary-key column(s) of ``table_ref`` — a tuple so composite keys (e.g.
    #: ``transaction_tags`` = ``(transaction_id, tag)``) work uniformly. The undo
    #: engine builds its WHERE clause from these.
    pk_columns: ClassVar[tuple[str, ...]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Fail at class-definition time if a repo omits required metadata.

        Each repo must declare ``repository`` (metric label), ``table_ref`` (the
        owned table), and ``pk_columns`` (its primary key). Without this, a
        missing attr only surfaces as an ``AttributeError`` deep in a runtime
        path — fail fast at import instead.
        """
        super().__init_subclass__(**kwargs)
        for attr, why in (
            ("repository", "the app_mutation_audit_emitted_total metric label"),
            ("table_ref", "the owned app.* table (undo targeting + dispatch)"),
            ("pk_columns", "the table's primary-key columns (undo WHERE clause)"),
        ):
            if not hasattr(cls, attr):
                raise TypeError(
                    f"{cls.__name__} must set a class-level `{attr}` — {why}."
                )

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Bind to an open Database; lazily build an ``AuditService`` if absent."""
        self._db = db
        self._audit = audit if audit is not None else AuditService(db)

    @property
    def _audit_target(self) -> tuple[str, str]:
        """The (schema, table) audit-target prefix, derived from ``table_ref``.

        Repos pass ``(*self._audit_target, entity_id)`` as the audit target;
        deriving it here keeps the table named in exactly one place per repo.
        """
        return (self.table_ref.schema, self.table_ref.name)

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
        except BaseException:
            # Roll back on BaseException, not just Exception, so a
            # KeyboardInterrupt/SystemExit mid-write doesn't leave the
            # transaction open. Re-raised immediately — never swallowed.
            self._db.rollback()
            raise
        else:
            self._db.commit()

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
        is_undo: bool = False,
        undoes_operation_id: str | None = None,
    ) -> AuditEvent:
        """Emit one paired ``app.audit_log`` row and bump the repo mutation metric.

        The single audit-emission point for every repository. A repo that mutates
        without routing through here shows up as a gap between
        ``app_mutation_audit_emitted_total`` and ``audit_events_emitted_total`` —
        the contract-violation signal Invariant 10 exists to catch.

        Both counters increment here, inside the caller's transaction and before
        its commit (matching the pre-existing ``audit_events_emitted_total``
        behavior). A subsequent commit failure therefore over-counts both by one
        relative to durable rows — an accepted trade-off: the two counters stay in
        lockstep (so the contract-violation gap *between* them is unaffected), and
        a rollback after a successful audit insert is rare. Prometheus counters are
        best-effort telemetry, not a durable ledger.

        ``is_undo`` / ``undoes_operation_id`` mark rows written by :meth:`undo_event`
        so the undo is itself queryable and undoable (REC-PR3).
        """
        event = self._audit.record_audit_event(
            action=action,
            target=target,
            before=before,
            after=after,
            actor=actor,
            parent_audit_id=parent_audit_id,
            context=context,
            is_undo=is_undo,
            undoes_operation_id=undoes_operation_id,
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
        cols = ", ".join(quote_ident(c) for c in columns)
        safe_pk = quote_ident(pk_col)
        row = self._db.execute(
            f"SELECT {cols} FROM {table_ref.full_name} WHERE {safe_pk} = ?",  # noqa: S608  # TableRef + sqlglot-quoted identifiers
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

    def undo_event(
        self,
        event: AuditEvent,
        *,
        actor: str,
        in_outer_txn: bool = False,
    ) -> AuditEvent | None:
        """Synthesize and apply the inverse of one audited mutation.

        The inverse is derived purely from the event's full-row before/after
        capture (Req 4), so one generic implementation reverses every repo:

        - INSERT (``before`` is None) → DELETE the row it created.
        - DELETE (``after`` is None) → re-INSERT the row it removed.
        - UPDATE (both present) → restore every column to its before-image,
          locating the row by the *after* image's primary key so a pk-changing
          update (e.g. a tag rename) is found by its current key.

        A no-op event (``before == after``, covering both-None and a zero-change
        update) writes nothing and returns ``None``. Otherwise the undo is itself
        audited (``is_undo=True``, ``undoes_operation_id=event.operation_id``) and
        the new event is returned — so it is undoable in turn (undo-the-undo).

        Values come straight from the JSON-decoded audit payload: DuckDB
        auto-casts ISO strings to ``DATE``/``TIMESTAMP`` and decimal strings to
        ``DECIMAL``, and binds ``dict``/``list`` directly to ``JSON``/array
        columns — so no per-repo type handling is needed (``match_signals`` JSON
        and ``exemplars`` arrays round-trip natively).
        """
        before = event.before_value
        after = event.after_value
        if before == after:
            return None

        with self._transaction(in_outer_txn=in_outer_txn):
            if before is None and after is not None:
                self._require_capture(after, self.pk_columns, event)
                self._delete_by_pk(after)
            elif after is None and before is not None:
                self._require_capture(before, self._not_null_columns(), event)
                self._insert_row(before)
            elif before is not None and after is not None:
                self._require_capture(after, self.pk_columns, event)
                # before must be as complete as after, or the restore silently
                # writes only the captured columns and leaves the rest at their
                # current (post-mutation) values.
                self._require_capture(before, after.keys(), event)
                self._restore_row(before=before, locate=after)
            return self._emit_audit(
                action=f"{event.action}.undo",
                target=(
                    event.target_schema,
                    event.target_table,
                    event.target_id,
                ),
                before=after,
                after=before,
                actor=actor,
                is_undo=True,
                undoes_operation_id=event.operation_id,
            )

    def _not_null_columns(self) -> set[str]:
        """Column names of ``table_ref`` that reject NULL, from the live catalog.

        Used to reject undo of a legacy partial-capture row *before* it reaches a
        raw DuckDB NOT NULL error — see :meth:`_require_capture`.
        """
        rows = self._db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND is_nullable = 'NO'",
            [self.table_ref.schema, self.table_ref.name],
        ).fetchall()
        return {r[0] for r in rows}

    def _require_capture(
        self, image: dict[str, Any], required: Iterable[str], event: AuditEvent
    ) -> None:
        """Refuse to reverse an audit row whose capture is missing ``required`` keys.

        Pre-PR rows (e.g. a ``note.delete`` that stored only ``note_id``/``text``)
        predate the full-row capture Invariant 10 Req 4 now guarantees. Reversing
        them would re-INSERT a row missing NOT NULL columns or locate by an absent
        primary key — surfacing as a raw DuckDB / ``KeyError`` crash. Fail cleanly
        with a recovery code the surface can render instead.
        """
        missing = set(required) - set(image)
        if missing:
            raise UserError(
                f"Cannot undo {event.action!r}: its audit row predates full-row "
                f"capture and is missing {sorted(missing)} on {event.target_table} "
                "— not reversible.",
                code=error_codes.RECOVERY_NO_PATH,
            )

    def _pk_where(self, row: dict[str, Any]) -> tuple[str, list[Any]]:
        """Build a ``col = ? AND …`` clause + params from ``pk_columns``."""
        clauses = [f"{quote_ident(col)} = ?" for col in self.pk_columns]
        return " AND ".join(clauses), [row[col] for col in self.pk_columns]

    def _delete_by_pk(self, row: dict[str, Any]) -> None:
        """Delete the row whose primary key matches ``row`` (undo of an INSERT)."""
        where, params = self._pk_where(row)
        self._db.execute(
            f"DELETE FROM {self.table_ref.full_name} WHERE {where}",  # noqa: S608  # TableRef + sqlglot-quoted pk; values parameterized
            params,
        )

    def _insert_row(self, row: dict[str, Any]) -> None:
        """Re-insert a full captured row (undo of a DELETE)."""
        cols = list(row.keys())
        col_sql = ", ".join(quote_ident(c) for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        self._db.execute(
            f"INSERT INTO {self.table_ref.full_name} ({col_sql}) "  # noqa: S608  # TableRef + sqlglot-quoted columns; values parameterized
            f"VALUES ({placeholders})",
            [row[c] for c in cols],
        )

    def _restore_row(self, *, before: dict[str, Any], locate: dict[str, Any]) -> None:
        """Restore every column to its before-image (undo of an UPDATE).

        ``locate`` is the after-image, so the WHERE clause keys on the row's
        *current* primary key — correct even when the update changed the key.
        """
        set_cols = list(before.keys())
        set_sql = ", ".join(f"{quote_ident(c)} = ?" for c in set_cols)
        where, where_params = self._pk_where(locate)
        self._db.execute(
            f"UPDATE {self.table_ref.full_name} SET {set_sql} WHERE {where}",  # noqa: S608  # TableRef + sqlglot-quoted columns; values parameterized
            [before[c] for c in set_cols] + where_params,
        )
