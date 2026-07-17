"""V036: rename app.account_settings.iso_currency_code -> currency_code.

Aligns the accounts-side currency column with the name every other core.*
currency column already uses (fct_transactions, fct_investment_transactions,
dim_securities, dim_holdings). Direct rename, no deprecation shim -- confirmed
pre-launch per docs/specs/multi-currency.md Key Decision 5. Idempotent.

Also rewrites historical app.audit_log before_value/after_value JSON payloads
for target_table='account_settings' rows: those columns capture the FULL row
state at mutation time (Invariant 10), so every account_settings.set audit
event written before this migration has iso_currency_code as a captured key --
regardless of which field the user actually changed. Without the rewrite,
BaseRepo._restore_row (undo) would build its UPDATE directly from the stale
key names and fail with a raw DuckDB binder error against a column that no
longer exists. Only payloads still carrying the old key are touched, so
re-running is a safe no-op.
"""

from __future__ import annotations

import json
import logging
from typing import Any, cast

logger = logging.getLogger(__name__)

_OLD_CURRENCY_KEY = "iso_currency_code"
_NEW_CURRENCY_KEY = "currency_code"


def migrate(conn: object) -> None:
    """Rename app.account_settings.iso_currency_code -> currency_code. Idempotent."""
    cols: list[tuple[str]] = conn.execute(  # type: ignore[union-attr]
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'app' AND table_name = 'account_settings'
        """
    ).fetchall()
    existing = {c[0] for c in cols}
    if "currency_code" not in existing:
        logger.debug(
            "V036: renaming app.account_settings.iso_currency_code -> currency_code"
        )
        conn.execute(  # type: ignore[union-attr]
            "ALTER TABLE app.account_settings RENAME COLUMN iso_currency_code TO currency_code"
        )
        conn.execute(  # type: ignore[union-attr]
            "COMMENT ON COLUMN app.account_settings.currency_code IS "
            "'ISO-4217 (USD, EUR, ...); NULL inherits the account''s core.dim_accounts.currency_code fallback'"
        )
    else:
        logger.debug("V036: currency_code already present; skipping column rename")

    _rewrite_audit_log_currency_key(conn)


def _rewrite_audit_log_currency_key(conn: object) -> None:
    """Rename iso_currency_code -> currency_code inside historical audit payloads.

    Scoped to target_table='account_settings' -- the only repo whose captured
    rows ever carried the old key. A Python-side json.loads/rename/json.dumps
    loop (rather than a SQL-only json_merge_patch) keeps the key-existence
    check and the not-null branching explicit and easy to verify against the
    tests below, at the cost of one UPDATE per row needing rewrite -- fine at
    this table's audit-log volume.
    """
    fetched = cast(
        list[tuple[Any, ...]],
        conn.execute(  # type: ignore[union-attr]
            """
            SELECT audit_id, before_value, after_value
            FROM app.audit_log
            WHERE target_table = 'account_settings'
              AND (before_value IS NOT NULL OR after_value IS NOT NULL)
            """
        ).fetchall(),
    )
    rows: list[tuple[str, str | None, str | None]] = [
        (
            str(r[0]),
            None if r[1] is None else str(r[1]),
            None if r[2] is None else str(r[2]),
        )
        for r in fetched
    ]
    rewritten = 0
    for audit_id, before_raw, after_raw in rows:
        new_before, before_changed = _rename_currency_key(before_raw)
        new_after, after_changed = _rename_currency_key(after_raw)
        if not (before_changed or after_changed):
            continue
        conn.execute(  # type: ignore[union-attr]
            "UPDATE app.audit_log SET before_value = ?, after_value = ? "
            "WHERE audit_id = ?",
            [new_before, new_after, audit_id],
        )
        rewritten += 1
    if rewritten:
        logger.debug(
            f"V036: rewrote {rewritten} account_settings audit_log payload(s) "
            "iso_currency_code -> currency_code"
        )


def _rename_currency_key(raw: str | None) -> tuple[str | None, bool]:
    """Rename the old currency key inside one JSON payload string.

    Returns ``(payload, changed)`` -- ``raw`` unchanged (not re-serialized) when
    NULL or already using the new key, so the caller can skip a no-op UPDATE.
    """
    if raw is None:
        return None, False
    payload = json.loads(raw)
    if _OLD_CURRENCY_KEY not in payload:
        return raw, False
    payload[_NEW_CURRENCY_KEY] = payload.pop(_OLD_CURRENCY_KEY)
    return json.dumps(payload), True
