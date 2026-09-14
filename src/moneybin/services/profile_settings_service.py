"""Profile-level managed settings, backed by ``app.profile_settings``.

These settings live in the database rather than ``config.yaml`` because the
no-blend guard and the report views that read them are SQLMesh models
(``docs/specs/multi-currency.md`` Requirement 4). The CLI's ``profile set``
dispatches undotted keys here; ``config.yaml`` keeps the dotted
``section.field`` namespace.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.audit_service import AuditService

#: Keys ``profile set`` routes to the database instead of ``config.yaml``.
#: Undotted by construction, so they can never collide with a
#: ``section.field`` config key.
MANAGED_SETTING_KEYS: tuple[str, ...] = ("home_currency", "display_currency_targets")


@dataclass(frozen=True)
class ProfileSettings:
    """Resolved managed settings for one profile."""

    home_currency: str | None
    """ISO 4217 home currency, or ``None`` when the user has not chosen one.

    ``None`` is a real answer. Callers must not substitute ``'USD'`` — that
    would relabel a foreign-currency profile's money.
    """
    display_currency_targets: tuple[str, ...]
    """Explicit report-currency targets; empty until the profile chooses some."""


class ProfileSettingsService:
    """Read and write the profile's managed settings."""

    def __init__(self, db: Database, *, audit: AuditService | None = None) -> None:
        """Initialize with an open Database; composes the audited settings repo."""
        from moneybin.repositories.profile_settings_repo import (
            ProfileSettingsRepo,
        )

        self._db = db
        self._repo = ProfileSettingsRepo(db, audit=audit)

    def get_settings(self) -> ProfileSettings:
        """Return the profile's managed settings."""
        return ProfileSettings(
            home_currency=self._repo.get_home_currency(),
            display_currency_targets=self._repo.get_display_currency_targets(),
        )

    def set_setting(self, key: str, value: str | Sequence[str], *, actor: str) -> None:
        """Set one managed setting, validating both the key and the value."""
        if key not in MANAGED_SETTING_KEYS:
            known = ", ".join(MANAGED_SETTING_KEYS)
            raise UserError(
                f"Unknown profile setting {key!r}. Managed settings: {known}.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if key == "home_currency":
            if not isinstance(value, str):
                raise UserError(
                    "home_currency must be one ISO 4217 code.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            try:
                self._repo.set_home_currency(value, actor=actor)
            except ValueError as exc:
                raise UserError(
                    f"Invalid home currency {value!r}: expected an ISO 4217 code "
                    "such as USD, EUR, or GBP.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                ) from exc
            from moneybin.services.fx_accounting_refresh import (
                restate_fx_accounting,
            )

            restate_fx_accounting(self._db)
            return

        try:
            targets = _parse_display_currency_targets(value)
            self._repo.set_display_currency_targets(targets, actor=actor)
        except ValueError as exc:
            # The repo also enforces DISPLAY_CURRENCY_TARGETS_MAX_COUNT, whose
            # message names the actual bound violated — surface it verbatim
            # instead of the generic malformed-code text below.
            raise UserError(
                f"Invalid display currency target: {exc}. Expected "
                "comma-separated ISO 4217 codes such as USD, EUR, or GBP.",
                code=error_codes.MUTATION_INVALID_INPUT,
            ) from exc


def _parse_display_currency_targets(value: str | Sequence[str]) -> tuple[str, ...]:
    """Accept CLI comma-separated input and MCP's native string list."""
    if isinstance(value, str):
        if not value.strip():
            return ()
        targets = tuple(part.strip() for part in value.split(","))
    else:
        targets = tuple(value)
    if any(not target for target in targets):
        raise ValueError("empty target")
    return targets
