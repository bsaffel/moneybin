"""Audited writes to ``app.ai_consent_grants`` (the consent ledger).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every
mutation pairs with an ``app.audit_log`` row in the same DuckDB
transaction. ``ConsentService`` composes this repo; it never issues raw
consent SQL.

At-most-one-active-grant per (feature_category, backend) is enforced
here (check-then-write inside the transaction), not by a DB partial
index — writer serialization makes the check race-free and DuckDB
partial-index support is version-dependent.
"""

from __future__ import annotations

import uuid
from typing import Any

from moneybin.privacy.consent import ConsentMode, GrantInfo
from moneybin.repositories.base import BaseRepo, quote_ident
from moneybin.tables import AI_CONSENT_GRANTS

_COLUMNS = (
    "grant_id",
    "feature_category",
    "backend",
    "consent_mode",
    "granted_at",
    "revoked_at",
    "grant_prompt",
)


def _row_to_grant(row: dict[str, Any]) -> GrantInfo:
    return GrantInfo(
        grant_id=row["grant_id"],
        feature_category=row["feature_category"],
        backend=row["backend"],
        consent_mode=ConsentMode(row["consent_mode"]),
        granted_at=row["granted_at"],
        revoked_at=row["revoked_at"],
    )


class ConsentRepo(BaseRepo):
    """Audited CRUD over ``app.ai_consent_grants``."""

    repository = "ai_consent_grants"

    table_ref = AI_CONSENT_GRANTS
    pk_columns = ("grant_id",)

    def _fetch_row(self, grant_id: str) -> dict[str, Any] | None:
        return self._fetch_one(AI_CONSENT_GRANTS, _COLUMNS, "grant_id", grant_id)

    def _table_exists(self) -> bool:
        """True if app.ai_consent_grants is present in the catalog.

        Read tools open the DB read-only, which skips init_schemas/migrations
        (database.py). On a profile upgraded to this version, a read before any
        write-mode open would otherwise hit a catalog error; the list methods
        short-circuit to empty instead. Mutations always run write-mode (the
        table is created there), so they need no guard.
        """
        row = self._db.execute(
            "SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
            [AI_CONSENT_GRANTS.schema, AI_CONSENT_GRANTS.name],
        ).fetchone()
        return row is not None

    def _active_for(self, feature_category: str, backend: str) -> dict[str, Any] | None:
        cols = ", ".join(quote_ident(c) for c in _COLUMNS)
        row = self._db.execute(
            f"SELECT {cols} FROM {AI_CONSENT_GRANTS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted cols
            "WHERE feature_category = ? AND backend = ? AND revoked_at IS NULL",
            [feature_category, backend],
        ).fetchone()
        return dict(zip(_COLUMNS, row, strict=True)) if row is not None else None

    def grant(
        self,
        *,
        feature_category: str,
        backend: str,
        consent_mode: ConsentMode,
        grant_prompt: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> tuple[GrantInfo, bool]:
        """Grant consent; idempotent per (feature_category, backend).

        Returns ``(grant, created)``. If an active grant already exists for
        the tuple it is returned unchanged with ``created=False`` (no new
        row, no audit) — a different ``consent_mode`` on the re-grant is NOT
        applied; the existing grant and its mode are returned as-is. To change
        a mode, revoke then re-grant. Otherwise a new active grant is inserted
        with a paired audit row and ``created=True``.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            existing = self._active_for(feature_category, backend)
            if existing is not None:
                return _row_to_grant(existing), False
            grant_id = uuid.uuid4().hex[:12]
            self._db.execute(
                f"""
                INSERT INTO {AI_CONSENT_GRANTS.full_name}
                    (grant_id, feature_category, backend, consent_mode,
                     granted_at, revoked_at, grant_prompt)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, NULL, ?)
                """,  # noqa: S608  # TableRef + parameterized values
                [grant_id, feature_category, backend, consent_mode.value, grant_prompt],
            )
            after = self._fetch_row(grant_id)
            if after is None:  # pragma: no cover — just inserted, must exist
                raise RuntimeError(f"grant_id={grant_id!r} not found after insert")
            # Guard precedes _emit_audit so the impossible path never records an
            # after=null audit row or bumps the audit metric before rollback.
            self._emit_audit(
                action="consent.grant",
                target=(*self._audit_target, grant_id),
                before=None,
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
            return _row_to_grant(after), True

    def revoke(
        self,
        *,
        feature_category: str,
        backend: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> int:
        """Revoke the active grant for (feature_category, backend). Returns 0 or 1."""
        with self._transaction(in_outer_txn=in_outer_txn):
            existing = self._active_for(feature_category, backend)
            if existing is None:
                return 0
            grant_id = existing["grant_id"]
            self._db.execute(
                f"UPDATE {AI_CONSENT_GRANTS.full_name} "  # noqa: S608  # TableRef constant
                "SET revoked_at = CURRENT_TIMESTAMP WHERE grant_id = ?",
                [grant_id],
            )
            after = self._fetch_row(grant_id)
            if after is None:  # pragma: no cover — just updated, must exist
                raise RuntimeError(f"grant_id={grant_id!r} not found after revoke")
            self._emit_audit(
                action="consent.revoke",
                target=(*self._audit_target, grant_id),
                before=self._serialize_for_audit(existing),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )
            return 1

    def revoke_all(
        self,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> list[GrantInfo]:
        """Revoke every active grant; one audit row per revoked grant.

        Returns the grants that were revoked (the authoritative set captured
        inside the transaction) so callers emit per-grant side effects from
        exactly what changed — no second, possibly-divergent snapshot.
        """
        with self._transaction(in_outer_txn=in_outer_txn):
            active = self.list_active()
            for grant in active:
                before = self._fetch_row(grant.grant_id)  # before-image, pre-UPDATE
                self._db.execute(
                    f"UPDATE {AI_CONSENT_GRANTS.full_name} "  # noqa: S608  # TableRef constant
                    "SET revoked_at = CURRENT_TIMESTAMP WHERE grant_id = ?",
                    [grant.grant_id],
                )
                after = self._fetch_row(grant.grant_id)
                if after is None:  # pragma: no cover — just updated, must exist
                    raise RuntimeError(
                        f"grant_id={grant.grant_id!r} not found after revoke"
                    )
                self._emit_audit(
                    action="consent.revoke",
                    target=(*self._audit_target, grant.grant_id),
                    before=self._serialize_for_audit(before),
                    after=self._serialize_for_audit(after),
                    actor=actor,
                    parent_audit_id=parent_audit_id,
                )
            return active

    def list_active(self) -> list[GrantInfo]:
        """Return all active (non-revoked) grants, newest first."""
        if not self._table_exists():
            return []
        cols = ", ".join(quote_ident(c) for c in _COLUMNS)
        rows = self._db.execute(
            f"SELECT {cols} FROM {AI_CONSENT_GRANTS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted cols
            "WHERE revoked_at IS NULL ORDER BY granted_at DESC"
        ).fetchall()
        return [_row_to_grant(dict(zip(_COLUMNS, r, strict=True))) for r in rows]

    def list_all(self) -> list[GrantInfo]:
        """Return all grants including revoked, newest first (audit history)."""
        if not self._table_exists():
            return []
        cols = ", ".join(quote_ident(c) for c in _COLUMNS)
        rows = self._db.execute(
            f"SELECT {cols} FROM {AI_CONSENT_GRANTS.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted cols
            "ORDER BY granted_at DESC"
        ).fetchall()
        return [_row_to_grant(dict(zip(_COLUMNS, r, strict=True))) for r in rows]
