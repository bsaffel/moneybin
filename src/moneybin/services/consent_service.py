"""Consent ledger business logic.

Thin wrapper over ``ConsentRepo`` that resolves the backend, validates
the feature category, and emits a ``privacy.log`` event after each
mutation (fail-soft, outside the DB transaction). The data-withholding
enforcement gate is deferred — this service only records and reports
consent.
"""

from __future__ import annotations

from dataclasses import dataclass

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.consent import FEATURE_CATEGORIES, ConsentMode, GrantInfo
from moneybin.privacy.log import build_consent_event, write_privacy_event
from moneybin.repositories.consent_repo import ConsentRepo


@dataclass(frozen=True, slots=True)
class ConsentStatus:
    """Snapshot of the consent ledger for ``privacy status``."""

    default_backend: str | None
    consent_policy: str
    active_grants: list[GrantInfo]


@dataclass(frozen=True, slots=True)
class GrantResult:
    """Outcome of a grant.

    ``created`` is False when an active grant already existed (idempotent
    no-op) so callers can report ``noop`` and skip a duplicate log event.
    """

    grant: GrantInfo
    created: bool


@dataclass(frozen=True, slots=True)
class RevokeResult:
    """Outcome of a single-grant revoke.

    Carries the *resolved* backend (after default-backend resolution) so the
    caller's confirmation reflects what was actually revoked, not the raw
    (possibly None) argument.
    """

    backend: str
    count: int


class ConsentService:
    """Grant, revoke, and report AI consent."""

    def __init__(self, db: Database) -> None:
        """Bind to an open Database connection."""
        self._db = db
        self._repo = ConsentRepo(db)

    @staticmethod
    def resolve_backend(backend: str | None) -> str:
        """Resolve to a concrete backend, falling back to the configured default.

        Public + static so a CLI confirmation prompt can resolve before
        asking — the user confirms consent for the *actual* backend, not a
        ``(no default configured)`` placeholder that would later error.

        An empty-string ``backend`` is treated as unspecified (falls through
        to the default): an empty string is not a valid backend identifier,
        so accepting it would create a grant with a blank recipient.
        """
        if backend:
            return backend
        default = get_settings().ai.default_backend
        if default:
            return default
        raise UserError(
            "No AI backend specified and no default configured.",
            code=error_codes.MUTATION_INVALID_INPUT,
            hint="Pass --backend, or set MONEYBIN_AI__DEFAULT_BACKEND.",
        )

    @staticmethod
    def validate_category(feature_category: str) -> None:
        """Raise UserError if ``feature_category`` is not a known category.

        Public so a CLI command can validate before prompting — same reason
        as ``resolve_backend``.
        """
        if feature_category not in FEATURE_CATEGORIES:
            raise UserError(
                f"Unknown feature category: {feature_category!r}.",
                code=error_codes.MUTATION_INVALID_INPUT,
                hint=f"Valid categories: {', '.join(sorted(FEATURE_CATEGORIES))}.",
            )

    @staticmethod
    def _build_prompt(feature_category: str, backend: str) -> str:
        """The consent text recorded as grant_prompt (source of truth)."""
        return (
            f"Allow MoneyBin to share {feature_category} data with backend "
            f"'{backend}'. Account numbers and other CRITICAL fields remain "
            f"masked. Revoke anytime with `moneybin privacy revoke "
            f"{feature_category} --backend {backend}`."
        )

    def grant_consent(
        self,
        *,
        feature_category: str,
        backend: str | None,
        consent_mode: ConsentMode,
        actor: str,
    ) -> GrantResult:
        """Grant consent for (feature_category, backend); idempotent.

        Emits a ``privacy.log`` event only when a new grant is actually
        created — a no-op re-grant of an existing active grant adds no event.
        """
        self.validate_category(feature_category)
        resolved_backend = self.resolve_backend(backend)
        prompt = self._build_prompt(feature_category, resolved_backend)
        grant, created = self._repo.grant(
            feature_category=feature_category,
            backend=resolved_backend,
            consent_mode=consent_mode,
            grant_prompt=prompt,
            actor=actor,
        )
        if created:
            write_privacy_event(
                build_consent_event(
                    actor=actor,
                    action="consent.grant",
                    feature_category=feature_category,
                    backend=resolved_backend,
                    consent_mode=consent_mode.value,
                )
            )
        return GrantResult(grant=grant, created=created)

    def revoke_consent(
        self, *, feature_category: str, backend: str | None, actor: str
    ) -> RevokeResult:
        """Revoke the active grant for (feature_category, backend).

        Returns the resolved backend and the number of grants revoked (0 or 1)
        so the caller can confirm exactly which backend was affected.
        """
        self.validate_category(feature_category)
        resolved_backend = self.resolve_backend(backend)
        count = self._repo.revoke(
            feature_category=feature_category, backend=resolved_backend, actor=actor
        )
        if count:
            write_privacy_event(
                build_consent_event(
                    actor=actor,
                    action="consent.revoke",
                    feature_category=feature_category,
                    backend=resolved_backend,
                    consent_mode=None,
                )
            )
        return RevokeResult(backend=resolved_backend, count=count)

    def revoke_all(self, *, actor: str) -> int:
        """Revoke every active grant. Returns count revoked.

        Emits one ``privacy.log`` event per revoked grant (mirroring single
        revoke) so ``privacy log`` can reconstruct exactly which
        (category, backend) pairs were bulk-revoked — the same per-grant
        detail the audit log already records. A single wildcard event would
        lose that granularity.
        """
        active = self._repo.list_active()  # capture before the bulk UPDATE
        count = self._repo.revoke_all(actor=actor)
        for grant in active:
            write_privacy_event(
                build_consent_event(
                    actor=actor,
                    action="consent.revoke",
                    feature_category=grant.feature_category,
                    backend=grant.backend,
                    consent_mode=None,
                )
            )
        return count

    def status(self) -> ConsentStatus:
        """Return the current ledger snapshot."""
        ai = get_settings().ai
        return ConsentStatus(
            default_backend=ai.default_backend,
            consent_policy=ai.consent_policy,
            active_grants=self._repo.list_active(),
        )

    def list_grants(self, *, include_revoked: bool = False) -> list[GrantInfo]:
        """Return active grants, or all grants (incl. revoked) for audit history."""
        return self._repo.list_all() if include_revoked else self._repo.list_active()
