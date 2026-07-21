"""Consent ledger business logic.

Thin wrapper over ``ConsentRepo`` that resolves the backend, validates
the feature category, and emits a ``privacy.log`` event after each
mutation (fail-soft, outside the DB transaction). The data-withholding
enforcement gate is deferred — this service only records and reports
consent.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from moneybin import error_codes
from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.consent import FEATURE_CATEGORIES, ConsentMode, GrantInfo
from moneybin.privacy.log import build_consent_event, write_privacy_event
from moneybin.repositories.consent_repo import ConsentRepo
from moneybin.services.mutation_context import operation


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


@dataclass(frozen=True, slots=True)
class ConsentTargetPlan:
    """Complete no-write preflight for one declarative consent batch."""

    categories: tuple[str, ...]
    state: Literal["granted", "revoked"]
    backend: str
    mode: ConsentMode | None
    before: tuple[GrantInfo, ...]
    changed_categories: tuple[str, ...]

    @property
    def changed(self) -> bool:
        """Return whether at least one grant must change."""
        return bool(self.changed_categories)


@dataclass(frozen=True, slots=True)
class ConsentTargetResult:
    """Committed consent target state and the effective backend set."""

    plan: ConsentTargetPlan
    effective_categories: tuple[str, ...]


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

        ``None`` means "unspecified" → use the default. A provided backend is
        stripped before use, and an empty / whitespace-only string is rejected
        outright: ``--backend ''`` is invalid input, not a request for the
        default. Stripping matters because the stored value is matched
        exactly on revoke — a padded ``" anthropic"`` grant would otherwise be
        unrevocable from the normal surface.
        """
        if backend is not None:
            stripped = backend.strip()
            if not stripped:
                raise UserError(
                    "Backend cannot be empty.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                    hint="Pass a backend name (e.g. anthropic), or omit it to "
                    "use the default.",
                )
            return stripped
        default = get_settings().ai.default_backend
        if default and default.strip():
            return default.strip()
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
        revoked = self._repo.revoke_all(actor=actor)
        for grant in revoked:
            write_privacy_event(
                build_consent_event(
                    actor=actor,
                    action="consent.revoke",
                    feature_category=grant.feature_category,
                    backend=grant.backend,
                    consent_mode=None,
                )
            )
        return len(revoked)

    def plan_targets(
        self,
        categories: Sequence[str],
        *,
        state: Literal["granted", "revoked"],
        backend: str | None,
        mode: ConsentMode = ConsentMode.PERSISTENT,
    ) -> ConsentTargetPlan:
        """Normalize, resolve, and inspect a complete consent target set."""
        normalized = tuple(sorted(category.strip() for category in categories))
        if not normalized:
            raise UserError(
                "categories must contain at least one feature category.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if len(set(normalized)) != len(normalized):
            raise UserError(
                "Each consent category may appear only once.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        for category in normalized:
            self.validate_category(category)
        resolved_backend = self.resolve_backend(backend)
        active = {
            grant.feature_category: grant
            for grant in self._repo.list_active()
            if grant.backend == resolved_backend
            and grant.feature_category in normalized
        }
        before = tuple(
            active[category] for category in normalized if category in active
        )
        changed_categories = tuple(
            category
            for category in normalized
            if (category not in active) == (state == "granted")
        )
        return ConsentTargetPlan(
            categories=normalized,
            state=state,
            backend=resolved_backend,
            mode=mode if state == "granted" else None,
            before=before,
            changed_categories=changed_categories,
        )

    def apply_targets(
        self,
        plan: ConsentTargetPlan,
        *,
        actor: str,
        operation_id: str,
        verify: Callable[[ConsentTargetPlan], None] | None = None,
    ) -> ConsentTargetResult:
        """Revalidate and commit a consent set in one operation and transaction."""
        changed: list[str] = []
        with operation(operation_id):
            self._db.begin()
            try:
                live = self.plan_targets(
                    plan.categories,
                    state=plan.state,
                    backend=plan.backend,
                    mode=plan.mode or ConsentMode.PERSISTENT,
                )
                if verify is not None:
                    verify(live)
                if not live.changed:
                    raise UserError(
                        "Every consent category already has its requested state.",
                        code=error_codes.MUTATION_NOTHING_TO_DO,
                    )
                for category in live.changed_categories:
                    if live.state == "granted":
                        self._repo.grant(
                            feature_category=category,
                            backend=live.backend,
                            consent_mode=live.mode or ConsentMode.PERSISTENT,
                            grant_prompt=self._build_prompt(category, live.backend),
                            actor=actor,
                            in_outer_txn=True,
                        )
                    else:
                        self._repo.revoke(
                            feature_category=category,
                            backend=live.backend,
                            actor=actor,
                            in_outer_txn=True,
                        )
                    changed.append(category)
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise

        for category in changed:
            write_privacy_event(
                build_consent_event(
                    actor=actor,
                    action=(
                        "consent.grant" if plan.state == "granted" else "consent.revoke"
                    ),
                    feature_category=category,
                    backend=plan.backend,
                    consent_mode=(
                        (plan.mode or ConsentMode.PERSISTENT).value
                        if plan.state == "granted"
                        else None
                    ),
                )
            )
        effective = tuple(
            sorted(
                grant.feature_category
                for grant in self._repo.list_active()
                if grant.backend == plan.backend
            )
        )
        return ConsentTargetResult(plan=live, effective_categories=effective)

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
