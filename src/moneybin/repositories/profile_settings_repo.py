"""Audited writes to ``app.profile_settings`` (profile-level user settings).

Per ``docs/specs/app-integrity-invariant.md`` (Invariant 10), every mutation of
this table flows through a ``*Repo`` that pairs the write with an
``app.audit_log`` row inside the same DuckDB transaction. It holds the accounting home currency, optional report display
targets, and the persisted reporting calendar.

Both live here rather than in ``config.yaml`` because the no-blend guard and
the report views that read them are SQLMesh models
(``docs/specs/multi-currency.md`` Requirement 4).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import duckdb

from moneybin.limits import DISPLAY_CURRENCY_TARGETS_MAX_COUNT
from moneybin.repositories.base import BaseRepo
from moneybin.services._validators import validate_currency_code
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import PROFILE_SETTINGS

_PROFILE_SETTINGS_COLUMNS = (
    "scope",
    "home_currency",
    "display_currency_targets",
    "reporting_timezone",
    "updated_at",
)
_HOME_CURRENCY_COLUMNS = ("scope", "home_currency", "updated_at")

#: The table holds exactly one row; ``scope`` is its constant primary key.
_SCOPE = "profile"


class ProfileSettingsRepo(BaseRepo):
    """Audited access to the single ``app.profile_settings`` row."""

    repository = "profile_settings"

    table_ref = PROFILE_SETTINGS
    pk_columns = ("scope",)

    def _fetch_row(self) -> dict[str, Any] | None:
        return self._fetch_one(
            PROFILE_SETTINGS, _PROFILE_SETTINGS_COLUMNS, "scope", _SCOPE
        )

    def get_home_currency(self) -> str | None:
        """Return the profile's home currency, or ``None`` if never chosen.

        ``None`` is a real answer, not a missing one: callers must not
        substitute ``'USD'``, which would relabel a foreign-currency profile.

        A database that predates V044 has no table to read (``CatalogException``
        guard, matching the link-decisions repos). Read-only opens skip both
        ``init_schemas`` and the migration runner, so ``moneybin profile show``
        and the ``profile`` MCP tool reach this on an upgrading user's first
        command — before any write-mode open has created the table.
        """
        try:
            row = self._fetch_one(
                PROFILE_SETTINGS, _HOME_CURRENCY_COLUMNS, "scope", _SCOPE
            )
        except duckdb.CatalogException:
            return None
        if row is None:
            return None
        value = row["home_currency"]
        return str(value) if value is not None else None

    def get_display_currency_targets(self) -> tuple[str, ...]:
        """Return the profile's explicit report-currency targets, if any.

        A V062-upgraded database may be read before a write-mode open applies
        the migration, just as V044's table can be absent on an older profile.
        Targets are optional, so that state reads as the empty default.
        """
        try:
            row = self._fetch_row()
        except (duckdb.BinderException, duckdb.CatalogException):
            return ()
        if row is None:
            return ()
        values = row["display_currency_targets"]
        if values is None:
            return ()
        return tuple(str(value) for value in values)

    def undo_event(
        self, event: AuditEvent, *, actor: str, in_outer_txn: bool = False
    ) -> AuditEvent | None:
        """Refuse replay lacking calendar capture rather than erase saved policy."""
        from moneybin import error_codes
        from moneybin.errors import UserError

        if event.before_value != event.after_value:
            if event.before_value is None or any(
                image is not None
                and not isinstance(image.get("reporting_timezone"), str)
                for image in (event.before_value, event.after_value)
            ):
                raise UserError(
                    "Cannot undo this profile setting: its historical capture cannot "
                    "restore the reporting timezone safely. Set the intended setting explicitly.",
                    code=error_codes.RECOVERY_NO_PATH,
                )
        return super().undo_event(event, actor=actor, in_outer_txn=in_outer_txn)

    def get_reporting_timezone(self) -> str:
        """Read the saved calendar; missing policy requires an explicit upgrade."""
        from moneybin.temporal_contract import UTCUpgradeRequiredError

        try:
            row = self._fetch_row()
            value = row["reporting_timezone"] if row else None
            if not isinstance(value, str):
                raise ValueError("Missing reporting timezone")
            validate_reporting_timezone(value)
        except (duckdb.BinderException, duckdb.CatalogException, ValueError) as exc:
            raise UTCUpgradeRequiredError(
                "Profile reporting timezone is missing or invalid. Create a fresh "
                "profile or arrange separate offline maintenance with an explicit "
                "reporting timezone before opening it."
            ) from exc
        return value

    def set_reporting_timezone(self, value: str, *, actor: str) -> None:
        """Persist an IANA reporting calendar through the audited singleton."""
        validate_reporting_timezone(value)
        with self._transaction():
            before = self._fetch_row()
            self._db.execute(
                f"""
                INSERT INTO {PROFILE_SETTINGS.full_name} (scope, reporting_timezone)
                VALUES (?, ?)
                ON CONFLICT (scope) DO UPDATE SET
                    reporting_timezone = excluded.reporting_timezone,
                    updated_at = NOW()
                """,  # noqa: S608  # TableRef and parameterized values
                [_SCOPE, value],
            )
            after = self._fetch_row()
            self._emit_audit(
                action="profile_settings.set",
                target=(*self._audit_target, _SCOPE),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
            )

    def set_home_currency(
        self,
        currency_code: str,
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Set the home currency + audit (``profile_settings.set``).

        Validation runs before the transaction opens so a rejected code cannot
        clobber a previously stored one. ``NOW()`` (not ``CURRENT_TIMESTAMP``)
        refreshes ``updated_at`` in the ``DO UPDATE`` clause: DuckDB parses
        ``CURRENT_TIMESTAMP`` as an identifier in that position, not a call.
        """
        currency_code = _normalize_currency(currency_code)

        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row()
            self._db.execute(
                f"""
                INSERT INTO {PROFILE_SETTINGS.full_name} (scope, home_currency)
                VALUES (?, ?)
                ON CONFLICT (scope) DO UPDATE SET
                    home_currency = excluded.home_currency,
                    updated_at    = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [_SCOPE, currency_code],
            )
            after = self._fetch_row()
            return self._emit_audit(
                action="profile_settings.set",
                target=(*self._audit_target, _SCOPE),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )

    def set_display_currency_targets(
        self,
        currency_codes: Sequence[str],
        *,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> AuditEvent:
        """Set normalized explicit report-currency targets and audit the write."""
        targets = _normalize_currency_targets(currency_codes)

        with self._transaction(in_outer_txn=in_outer_txn):
            before = self._fetch_row()
            self._db.execute(
                f"""
                INSERT INTO {PROFILE_SETTINGS.full_name}
                    (scope, display_currency_targets)
                VALUES (?, ?)
                ON CONFLICT (scope) DO UPDATE SET
                    display_currency_targets = excluded.display_currency_targets,
                    updated_at               = NOW()
                """,  # noqa: S608  # TableRef + parameterized values
                [_SCOPE, list(targets)],
            )
            after = self._fetch_row()
            return self._emit_audit(
                action="profile_settings.set",
                target=(*self._audit_target, _SCOPE),
                before=self._serialize_for_audit(before),
                after=self._serialize_for_audit(after),
                actor=actor,
                parent_audit_id=parent_audit_id,
            )


def _normalize_currency_targets(currency_codes: Sequence[str]) -> tuple[str, ...]:
    """Canonicalize each ISO code while retaining its first declared order.

    Bounded at ``DISPLAY_CURRENCY_TARGETS_MAX_COUNT`` here, the one choke point
    both the CLI's comma-parsed string and MCP's already-typed ``list[str]``
    reach: an unbounded set would make ``plan_rate_backfill`` build a Cartesian
    product against every held currency and issue that many provider calls
    each refresh.
    """
    targets: list[str] = []
    for value in currency_codes:
        currency = _normalize_currency(value)
        if currency not in targets:
            targets.append(currency)
    if len(targets) > DISPLAY_CURRENCY_TARGETS_MAX_COUNT:
        raise ValueError(
            "display_currency_targets exceeds "
            f"{DISPLAY_CURRENCY_TARGETS_MAX_COUNT} distinct codes"
        )
    return tuple(targets)


def _normalize_currency(value: str) -> str:
    """Canonicalize and validate a profile currency without a cold-start import."""
    from moneybin.services.currency_service import canonical_currency

    currency = canonical_currency(value)
    validate_currency_code(currency)
    return currency


def validate_reporting_timezone(value: str) -> None:
    """Reject host-specific, unavailable or malformed IANA keys before mutation."""
    try:
        # Match zoneinfo.available_timezones' special-key exclusions without
        # scanning every timezone file on each profile open.
        if value in {"localtime", "posixrules"} or value.startswith((
            "posix/",
            "right/",
        )):
            raise ValueError("Host-specific timezone keys are not portable")
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            "Expected an available IANA timezone such as America/New_York"
        ) from exc
