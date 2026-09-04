"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from sqlglot import exp

from moneybin.audits import recipes as recipe_registry
from moneybin.audits.runner import run_standalone_audits
from moneybin.config import get_settings
from moneybin.database import Database
from moneybin.errors import RecoveryAction, exception_origin
from moneybin.extractors.pdf.fingerprint import PAGE_BUCKETS, serialize_fingerprint
from moneybin.metrics.registry import (
    DUPLICATE_ACCOUNT_PAIRS,
    PROFILE_CURRENCIES,
    UNKNOWN_CURRENCY_ROWS,
)
from moneybin.services.account_resolution_types import (
    UNNAMED_ACCOUNT_LABEL,
    is_reserved_account_name,
)
from moneybin.sqlmesh_registry import model_presence
from moneybin.staleness import (
    SECURITY_TYPE_STALENESS_DAYS,
    is_stale,
    resolve_threshold_days,
)
from moneybin.tables import (
    ACCOUNT_LINK_DECISIONS,
    ACCOUNT_LINKS,
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    BALANCE_ASSERTIONS,
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    CATEGORY_OVERRIDES,
    DIM_ACCOUNTS,
    DIM_HOLDINGS,
    DIM_SECURITIES,
    EXCHANGE_RATE_OVERRIDES,
    FCT_BALANCES,
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
    GSHEET_CONNECTIONS,
    IMPORT_PREVIEWS,
    IMPORTS,
    INT_TRANSACTIONS_MATCHED,
    INT_TRANSACTIONS_UNIONED,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
    MATCH_DECISIONS,
    PDF_FORMATS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_SECURITIES,
    PROFILE_SETTINGS,
    PROPOSED_RULES,
    SECURITIES,
    SECURITY_LINKS,
    SECURITY_PRICE_OVERRIDES,
    SECURITY_PRICES,
    STG_SECURITY_PRICES,
    TABULAR_FORMATS,
    TRANSACTION_CATEGORIES,
    TRANSACTION_ID_ALIASES,
    TRANSACTION_NOTES,
    TRANSACTION_TAGS,
    USER_CATEGORIES,
    USER_MERCHANTS,
    USER_REPORTS,
    TableRef,
)

logger = logging.getLogger(__name__)

# Composite-PK projection for app.balance_assertions audit coverage. Must match
# BalanceAssertionsRepo's `target_id` ("{account_id}|{assertion_date ISO}"):
# DuckDB casts a DATE to the same YYYY-MM-DD string date.isoformat() produces.
_BALANCE_ASSERTIONS_PK_EXPR = (
    f"{exp.to_identifier('account_id', quoted=True).sql('duckdb')} || '|' || "
    f"CAST({exp.to_identifier('assertion_date', quoted=True).sql('duckdb')} AS VARCHAR)"
)

# app.security_price_overrides keys on (security_id, price_date, quote_currency);
# mirrors SecurityPriceRepo._target_id.
_SECURITY_PRICE_OVERRIDES_PK_EXPR = (
    f"{exp.to_identifier('security_id', quoted=True).sql('duckdb')} || '|' || "
    f"CAST({exp.to_identifier('price_date', quoted=True).sql('duckdb')} AS VARCHAR)"
    f" || '|' || {exp.to_identifier('quote_currency', quoted=True).sql('duckdb')}"
)

# app.exchange_rate_overrides keys on (from_currency, to_currency, rate_date);
# mirrors ExchangeRateOverridesRepo._target_id.
_EXCHANGE_RATE_OVERRIDES_PK_EXPR = (
    f"{exp.to_identifier('from_currency', quoted=True).sql('duckdb')} || '|' || "
    f"{exp.to_identifier('to_currency', quoted=True).sql('duckdb')} || '|' || "
    f"CAST({exp.to_identifier('rate_date', quoted=True).sql('duckdb')} AS VARCHAR)"
)

# Raw-interpolated SQL expressions allowed in `_run_app_audit_coverage`. Both
# `updated_expr` and `pk_expr` are spliced into SQL unsanitized (a multi-column
# expression can't be sqlglot-quoted as a single identifier), so each must be a
# hard-coded constant from these sets — a runtime guard that enforces the
# code-supplied-literal contract per `.claude/rules/security.md` (allowlist
# dynamic SQL), closing the door before a future caller passes a tainted value.
_ALLOWED_UPDATED_EXPRS = frozenset({"GREATEST(decided_at, reversed_at)"})
_ALLOWED_PK_EXPRS = frozenset({
    _BALANCE_ASSERTIONS_PK_EXPR,
    _SECURITY_PRICE_OVERRIDES_PK_EXPR,
    _EXCHANGE_RATE_OVERRIDES_PK_EXPR,
})

_FINGERPRINT_KEYS = frozenset({"issuer", "headers", "page_bucket"})

# Used by `_run_investment_unreported_holdings`, which needs "the one whole
# snapshot per item with the latest extracted_at" (never "latest row per
# position": that would let a stale survivor from an earlier pull mask an
# omission in the newest one). Mirrors the identical CTE in `dim_holdings.sql`.
# No leading `WITH` — callers splice this into their own `WITH <this>, ...`
# clause. (`_run_investment_phantom_holdings` no longer needs it: it scopes on
# the position-level `ever_reported_positions` gate instead.)
#
# Derived from the snapshot RECEIPTS (one row per item per pull, written even
# when the item reports zero positions), NOT from the presence of holdings rows.
# Plaid returns no holding entries for an item holding nothing, so the pull
# where a broker's every account is liquidated writes no holdings rows at all —
# a row-derived newest snapshot cannot see it, and silently keeps the last
# NON-EMPTY snapshot from an earlier pull. Keyed on the receipt, an item that
# reported nothing has an EMPTY newest snapshot (its positions are phantoms) and
# an item that never reported has NO newest snapshot (its positions are out of
# scope) — the distinction the raw holdings rows cannot express.
_NEWEST_HOLDINGS_SNAPSHOT_CTE = """
newest_snapshot AS (
    SELECT source_origin, source_file
    FROM (
        SELECT
            source_origin,
            source_file,
            ROW_NUMBER() OVER (
                PARTITION BY source_origin
                ORDER BY extracted_at DESC, source_file DESC
            ) AS snapshot_rank
        FROM prep.stg_plaid__investment_holdings_snapshots
    )
    WHERE snapshot_rank = 1
)
"""


def _is_live_fingerprint(raw: str | None) -> bool:
    """Return whether ``raw`` is a fingerprint the replay path could match.

    A live fingerprint is both structurally what ``compute_fingerprint``
    produces (exactly ``issuer``/``headers``/``page_bucket`` with a string
    ``issuer``, a string-only ``headers`` list, and a ``page_bucket`` drawn
    from ``PAGE_BUCKETS`` — the only values the producer emits) and
    byte-for-byte the canonical encoding ``serialize_fingerprint`` emits — the
    lookup compares it textually. See ``_run_pdf_formats_fingerprint_shape``
    for why both halves are required.
    """
    if raw is None:
        return False
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return False
    if not isinstance(loaded, dict):
        return False
    fp = cast("dict[str, Any]", loaded)
    if frozenset(fp) != _FINGERPRINT_KEYS:
        return False
    if not isinstance(fp["issuer"], str) or fp["page_bucket"] not in PAGE_BUCKETS:
        return False
    headers = fp["headers"]
    if not isinstance(headers, list) or not all(isinstance(h, str) for h in headers):
        return False
    return serialize_fingerprint(fp) == raw


@dataclass(frozen=True)
class InvariantResult:
    """Result of one pipeline invariant check.

    The optional ``recovery_actions`` field carries structured recovery
    hints when an audit fails or warns. ``DoctorService.run_all`` populates
    this from per-audit recipes registered in ``moneybin.audits.recipes``.
    ``None`` for pass/skipped results and for audits without a registered
    recipe.
    """

    name: str
    status: Literal["pass", "fail", "warn", "skipped"]
    detail: str | None
    affected_ids: list[str]
    recovery_actions: list[RecoveryAction] | None = None


@dataclass(frozen=True)
class DoctorReport:
    """Aggregated result of all pipeline invariant checks."""

    invariants: list[InvariantResult]
    transaction_count: int

    @property
    def failing(self) -> int:
        """Count of invariants with status 'fail'."""
        return sum(1 for r in self.invariants if r.status == "fail")

    @property
    def warning(self) -> int:
        """Count of invariants with status 'warn'."""
        return sum(1 for r in self.invariants if r.status == "warn")

    @property
    def passing(self) -> int:
        """Count of invariants with status 'pass'."""
        return sum(1 for r in self.invariants if r.status == "pass")

    @property
    def skipped(self) -> int:
        """Count of invariants with status 'skipped'."""
        return sum(1 for r in self.invariants if r.status == "skipped")


class DoctorService:
    """Run pipeline integrity invariants and aggregate results.

    Reads ``prep.*`` directly, by license. Layer Rule 2 in
    ``docs/specs/architecture-shared-primitives.md`` names doctor's diagnostic
    staging reads as an exception: inspecting pipeline internals is the check
    itself, not a leak of the analysis contract. The license is diagnostic
    only — a check reports what a staging model contains and never feeds a
    value back into ``core``, ``app``, or a report. A ``prep`` shape change may
    therefore break a check here, and repairing it belongs to the refactor that
    changed the shape.
    """

    def __init__(self, db: Database) -> None:
        """Store the open database connection for invariant queries."""
        self._db = db

    def run_all(self, verbose: bool = False, full: bool = False) -> DoctorReport:
        """Run all invariants and return a DoctorReport.

        ``full`` opts the protected-``app.*`` audit-coverage checks out of their
        sampled, lookback-windowed mode and into a full-table scan.
        """
        transaction_count = self._get_transaction_count()
        sqlmesh_results = self._run_sqlmesh_audits(verbose)
        dedup_reconciliation = self._run_dedup_reconciliation()
        duplicate_accounts = self._run_duplicate_account_overlap()
        unproposed_duplicates = self._run_unproposed_cross_source_duplicates()
        categorization = self._run_categorization_coverage()
        currency_integrity = self._run_currency_integrity()
        app_integrity = self._run_app_integrity(full=full)
        orphan_app_state = self._run_orphan_app_state()
        investment_checks = [
            self._run_investment_staging_rejects(),
            self._run_opening_lot_review(),
            self._run_investment_unmodeled_legs(),
            self._run_holdings_snapshot_divergence(),
            self._run_investment_source_overlap(),
            self._run_investment_unresolved_securities(),
            self._run_investment_conflicting_security_refs(),
            self._run_investment_unreported_holdings(),
            self._run_investment_phantom_holdings(),
            self._run_investment_price_disagreement(),
            self._run_investment_unpriced_holdings(),
            self._run_investment_stale_prices(),
            self._run_investment_unmapped_price_source(),
        ]
        raw_invariants = [
            *sqlmesh_results,
            self._run_sqlmesh_model_presence(),
            dedup_reconciliation,
            duplicate_accounts,
            unproposed_duplicates,
            categorization,
            currency_integrity,
            self._run_dim_accounts_reserved_display_name(),
            *app_integrity,
            orphan_app_state,
            *investment_checks,
        ]
        invariants = [self._apply_recipe(r) for r in raw_invariants]
        return DoctorReport(invariants=invariants, transaction_count=transaction_count)

    def _apply_recipe(self, result: InvariantResult) -> InvariantResult:
        """Populate ``recovery_actions`` from the recipe registry, if a recipe exists.

        Single seam: every audit's result passes through here on its way into
        the report, so per-audit ``_run_*`` methods stay unaware of the registry.
        Pass/skipped results don't get recovery actions — there's nothing to fix.

        Per-invariant exception isolation: a recipe that raises (e.g. a future
        recipe that queries the DB via ``RecipeContext.db`` and hits a lock or
        column drift) MUST NOT abort ``run_all``'s aggregation — the invariant
        keeps its computed status and just loses its ``recovery_actions``.
        Matches the broad-except pattern every ``_run_*`` method uses.
        """
        if result.status in ("pass", "skipped"):
            return result
        recipe_fn = recipe_registry.get(result.name)
        if recipe_fn is None:
            return result
        try:
            actions = recipe_fn(
                result.affected_ids, recipe_registry.RecipeContext(db=self._db)
            )
        except Exception:  # noqa: BLE001 — per-invariant isolation; one recipe must not abort the report
            logger.warning(
                f"Recipe for {result.name!r} raised; leaving recovery_actions=None",
                exc_info=True,
            )
            return result
        # `replace()` (not constructor) so a future field on `InvariantResult`
        # carries through automatically — explicit by-field copy would drop it.
        return replace(result, recovery_actions=actions)

    def _run_sqlmesh_model_presence(self) -> InvariantResult:
        """Fail when a model the project registers has no relation in the catalog.

        Every other health signal reads the *built* set — SQLMesh audits run
        against materialised models, freshness compares timestamps that only
        exist once a model is built, and a query against a missing view raises
        rather than reporting. A model that never materialised is therefore
        invisible to all of them: the pipeline reports healthy while a table
        consumers depend on simply does not exist. This is the one check that
        starts from what *should* be there.

        A warehouse with *nothing* built is a different state — it is what a
        profile looks like between ``db init`` and its first ``refresh_run``,
        and calling that a failure would exit non-zero on a healthy first run.
        That reports ``skipped`` with the remedy instead.
        """
        try:
            presence = model_presence(self._db)
        except Exception as e:  # noqa: BLE001 — per-invariant isolation; an unreadable catalog is not a fresh profile
            return InvariantResult(
                name="transform_model_presence",
                status="skipped",
                detail=f"model catalog unavailable: {e}",
                affected_ids=[],
            )
        if presence.never_built:
            return InvariantResult(
                name="transform_model_presence",
                status="skipped",
                detail="no transform models built yet; run refresh_run",
                affected_ids=[],
            )
        if not presence.missing:
            return InvariantResult(
                name="transform_model_presence",
                status="pass",
                detail=None,
                affected_ids=[],
            )
        missing = list(presence.missing)
        return InvariantResult(
            name="transform_model_presence",
            status="fail",
            detail=(
                f"{len(missing)} registered transform model(s) have no table or view: "
                f"{', '.join(missing[:5])}"
                f"{' …' if len(missing) > 5 else ''}"
            ),
            affected_ids=missing,
        )

    def _run_app_integrity(self, *, full: bool) -> list[InvariantResult]:
        """Per-table integrity checks for protected ``app.*`` tables (Invariant 10).

        Covers ``user_categories``, ``category_overrides``, ``gsheet_connections``,
        ``user_merchants``, ``categorization_rules``, ``proposed_rules``,
        ``transaction_categories``, ``account_settings``, ``balance_assertions``,
        ``budgets``, ``profile_settings``, plus the edge writers
        ``tabular_formats``, ``match_decisions``, ``imports``, the
        account-identity tables ``account_links``, ``account_link_decisions``,
        ``transaction_id_aliases``, and the investments tables ``securities``,
        ``lot_selections``, ``security_price_overrides``, and the multi-currency
        table ``exchange_rate_overrides``); later repository PRs
        append one coverage call per
        newly-wrapped table plus that table's FK/orphan specifics.

        This is **not** yet every repo-wrapped table: twelve tables have a repo
        but no coverage call here. That gap is pinned by equality in
        ``test_every_repo_table_has_audit_coverage_or_a_named_exemption``, so it
        can shrink but never grow silently — a new repo without a coverage call
        fails that test. Closing the twelve is planned, not scheduled here.

        Tables without an ``updated_at`` column pass their natural watermark:
        ``proposed_rules`` → ``proposed_at``, ``transaction_categories`` →
        ``categorized_at``, ``match_decisions`` →
        ``GREATEST(decided_at, reversed_at)`` (the latest of any mutation, so a
        bypass insert, status update, *or* reversal is caught) — the
        ``account_links`` / ``account_link_decisions`` link tables use the same
        expression, while the append-only ``transaction_id_aliases`` uses
        ``created_at``. ``balance_assertions``,
        ``security_price_overrides``, and ``exchange_rate_overrides`` have
        composite PKs, so each passes ``pk_expr`` to project the same composite
        ``target_id`` its repo emits. Those watermarks catch bypass mutations that
        advance the watermark; other bypass UPDATEs that don't are the lint rule's
        job — the heuristic limitation the helper documents.
        """
        return [
            self._run_app_audit_coverage(USER_CATEGORIES, "category_id", full=full),
            self._run_app_audit_coverage(CATEGORY_OVERRIDES, "category_id", full=full),
            self._run_app_audit_coverage(
                GSHEET_CONNECTIONS, "connection_id", full=full
            ),
            self._run_app_audit_coverage(USER_MERCHANTS, "merchant_id", full=full),
            self._run_app_audit_coverage(CATEGORIZATION_RULES, "rule_id", full=full),
            self._run_app_audit_coverage(
                PROPOSED_RULES,
                "proposed_rule_id",
                updated_col="proposed_at",
                full=full,
            ),
            self._run_app_audit_coverage(
                TRANSACTION_CATEGORIES,
                "transaction_id",
                updated_col="categorized_at",
                full=full,
            ),
            self._run_app_audit_coverage(ACCOUNT_SETTINGS, "account_id", full=full),
            self._run_app_audit_coverage(
                BALANCE_ASSERTIONS,
                "account_id",
                pk_expr=_BALANCE_ASSERTIONS_PK_EXPR,
                full=full,
            ),
            self._run_app_audit_coverage(
                SECURITY_PRICE_OVERRIDES,
                "security_id",
                pk_expr=_SECURITY_PRICE_OVERRIDES_PK_EXPR,
                full=full,
            ),
            self._run_app_audit_coverage(
                EXCHANGE_RATE_OVERRIDES,
                "from_currency",
                pk_expr=_EXCHANGE_RATE_OVERRIDES_PK_EXPR,
                full=full,
            ),
            self._run_app_audit_coverage(BUDGETS, "budget_id", full=full),
            self._run_app_audit_coverage(TABULAR_FORMATS, "name", full=full),
            self._run_app_audit_coverage(
                MATCH_DECISIONS,
                "match_id",
                updated_expr="GREATEST(decided_at, reversed_at)",
                full=full,
            ),
            self._run_app_audit_coverage(IMPORTS, "import_id", full=full),
            self._run_app_audit_coverage(
                IMPORT_PREVIEWS,
                "preview_id",
                full=full,
            ),
            self._run_app_audit_coverage(PDF_FORMATS, "name", full=full),
            self._run_app_audit_coverage(
                ACCOUNT_LINKS,
                "link_id",
                updated_expr="GREATEST(decided_at, reversed_at)",
                full=full,
            ),
            self._run_app_audit_coverage(
                ACCOUNT_LINK_DECISIONS,
                "decision_id",
                updated_expr="GREATEST(decided_at, reversed_at)",
                full=full,
            ),
            self._run_app_audit_coverage(
                TRANSACTION_ID_ALIASES,
                "old_transaction_id",
                updated_col="created_at",
                full=full,
            ),
            self._run_app_audit_coverage(SECURITIES, "security_id", full=full),
            # lot_selections.set audits the whole disposal as ONE collection-shaped
            # row (LotSelectionsRepo), not one row per (investment_transaction_id,
            # lot_id) — key on investment_transaction_id alone to match, and use
            # created_at (rows are DELETE+INSERT replaced, never updated in place).
            self._run_app_audit_coverage(
                LOT_SELECTIONS,
                "investment_transaction_id",
                updated_col="created_at",
                full=full,
            ),
            self._run_app_audit_coverage(USER_REPORTS, "report_id", full=full),
            # Singleton table: `scope` is a constant PK, so coverage checks the
            # one row's watermark against its audit trail.
            self._run_app_audit_coverage(PROFILE_SETTINGS, "scope", full=full),
            self._run_pdf_formats_recipe_validity(),
            self._run_pdf_formats_bounds(),
            self._run_pdf_formats_fingerprint_shape(),
            self._run_user_categories_uniqueness(),
            self._run_user_merchants_orphans(),
            self._run_proposed_rules_rule_fk(),
            self._run_transaction_categories_fk(),
            self._run_account_settings_account_fk(),
            self._run_account_settings_reserved_display_name(),
            self._run_balance_assertions_account_fk(),
            self._run_budgets_category_fk(),
            self._run_match_decisions_account_fk(),
        ]

    def _run_app_audit_coverage(
        self,
        table_ref: TableRef,
        pk_col: str,
        *,
        updated_col: str = "updated_at",
        updated_expr: str | None = None,
        pk_expr: str | None = None,
        full: bool = False,
    ) -> InvariantResult:
        """Flag rows mutated without a paired ``app.audit_log`` row (Req 9).

        Sampled by default: only rows whose mutation watermark falls within
        ``doctor.audit_coverage_lookback_days`` are checked, capped at
        ``doctor.audit_coverage_sample_cap``. ``full=True`` scans every row.
        ``updated_col`` names the watermark column — ``updated_at`` for most
        protected tables, but tables without one pass their natural mutation
        timestamp (e.g. ``proposed_rules`` → ``proposed_at``,
        ``transaction_categories`` → ``categorized_at``). ``updated_expr`` (a
        code-supplied SQL expression) overrides ``updated_col`` when a single
        column can't express the latest-mutation watermark — e.g.
        ``match_decisions`` uses ``GREATEST(decided_at, reversed_at)`` (DuckDB's
        ``GREATEST`` ignores NULL, so this is the later of the two timestamps, or
        ``decided_at`` when never reversed) so a raw bypass that bumps *either*
        column is flagged. ``updated_expr`` and ``pk_expr`` are interpolated raw
        (not sanitized), so each must be a hard-coded constant — enforced at
        runtime against ``_ALLOWED_UPDATED_EXPRS`` / ``_ALLOWED_PK_EXPRS`` so a
        future caller can't splice in a tainted value (``.claude/rules/security.md``).

        ``pk_expr`` overrides how the row's ``target_id`` is projected: most
        tables key on a single ``pk_col``, but a composite-PK table (e.g.
        ``balance_assertions``) passes a code-supplied SQL expression that
        reconstructs the same composite ``target_id`` its repo emits. The check
        name always derives from ``table_ref.name``; ``pk_col`` is only the
        single-column projection fallback, used when ``pk_expr`` is ``None`` (and
        otherwise ignored).

        Limitations (by design — this is a sampled runtime *heuristic*, not a
        proof): it scans rows that currently exist and keys on the watermark, so
        a raw bypass that (a) deletes a row, or (b) mutates without bumping the
        watermark, leaves nothing for the scan to flag. The *structural* guard
        against raw bypass writes is the lint rule (rejects raw
        ``INSERT``/``UPDATE``/``DELETE`` against protected tables outside
        ``*_repo.py``); content-based coverage is a hosted-tier follow-up.
        ``pk_col`` and ``updated_col`` are code-supplied constants, quoted
        defensively per ``.claude/rules/security.md``.
        """
        # Defense-in-depth: both raw-interpolated expressions must be code-supplied
        # constants from their allowlists (the SQL below splices them unsanitized).
        if updated_expr is not None and updated_expr not in _ALLOWED_UPDATED_EXPRS:
            raise ValueError(f"updated_expr not in allowlist: {updated_expr!r}")
        if pk_expr is not None and pk_expr not in _ALLOWED_PK_EXPRS:
            raise ValueError(f"pk_expr not in allowlist: {pk_expr!r}")
        name = f"app_audit_coverage_{table_ref.name}"
        settings = get_settings().doctor
        # `pk_expr` (a code-supplied composite-key expression) wins over the
        # single-column projection when present; both are code constants.
        safe_pk = (
            pk_expr
            if pk_expr is not None
            else exp.to_identifier(pk_col, quoted=True).sql("duckdb")
        )
        # The watermark SQL is either a code-supplied expression (`updated_expr`,
        # used raw — a trusted constant, NOT a sanitized value) or a single
        # sqlglot-quoted column (`updated_col`). Named `watermark_sql`, not
        # `safe_*`, precisely because the expr branch is unsanitized by design.
        watermark_sql = (
            updated_expr
            if updated_expr is not None
            else exp.to_identifier(updated_col, quoted=True).sql("duckdb")
        )
        if full:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, {watermark_sql} AS updated_at "  # noqa: S608  # TableRef + sqlglot-quoted pk + trusted watermark_sql
                f"FROM {table_ref.full_name}"
            )
            sample_params: list[object] = []
        else:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, {watermark_sql} AS updated_at "  # noqa: S608  # TableRef + sqlglot-quoted pk + trusted watermark_sql
                f"FROM {table_ref.full_name} "
                f"WHERE {watermark_sql} >= (now()::TIMESTAMP - (? * INTERVAL 1 DAY)) "
                f"ORDER BY {watermark_sql} DESC LIMIT ?"
            )
            sample_params = [
                settings.audit_coverage_lookback_days,
                settings.audit_coverage_sample_cap,
            ]
        # Match (schema, table, id) — target_table alone collides across schemas —
        # AND require an audit at-or-after the row's latest mutation. A row audited
        # once then mutated again by a raw bypass advances updated_at past every
        # existing audit row, so "any audit exists" would false-pass; comparing
        # against updated_at catches it. Repo writes set the row's updated_at and
        # the audit's occurred_at from CURRENT_TIMESTAMP in one transaction (equal
        # in DuckDB), so legitimate mutations satisfy occurred_at >= updated_at.
        try:
            rows = self._db.execute(
                f"""
                WITH sampled AS ({sampled_sql})
                SELECT s.pk
                FROM sampled s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {AUDIT_LOG.full_name} a
                    WHERE a.target_schema = ?
                      AND a.target_table = ?
                      AND a.target_id = s.pk
                      AND a.occurred_at >= s.updated_at
                )
                ORDER BY s.pk
                """,  # noqa: S608  # TableRef constants, parameterized values
                [*sample_params, table_ref.schema, table_ref.name],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"coverage check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} {table_ref.name} row(s) mutated without a "
                    "paired app.audit_log row"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_user_categories_uniqueness(self) -> InvariantResult:
        """Flag duplicate ``(category, subcategory)`` rows in ``app.user_categories``.

        V015 relaxed the DB-level ``UNIQUE (category, subcategory)`` constraint
        (``category_id`` is the sole PK now); this is the soft enforcement that
        keeps ``resolve_category_id`` from arbitrating between colliding rows.
        """
        name = "app_user_categories_uniqueness"
        try:
            rows = self._db.execute(
                f"""
                SELECT category, subcategory,
                       string_agg(category_id, ',' ORDER BY category_id) AS ids
                FROM {USER_CATEGORIES.full_name}
                GROUP BY category, subcategory
                HAVING COUNT(*) > 1
                ORDER BY category, subcategory
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"uniqueness check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            # `is not None` (not truthiness): DuckDB groups NULL and "" as
            # distinct collision sets, so render them distinctly in the detail.
            labels = [
                f"{cat}/{sub}" if sub is not None else str(cat) for cat, sub, _ in rows
            ]
            affected: list[str] = []
            for *_unused, ids in rows:
                affected.extend(ids.split(","))
            return InvariantResult(
                name=name,
                status="fail",
                detail=f"duplicate (category, subcategory): {', '.join(labels)}",
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_user_merchants_orphans(self) -> InvariantResult:
        """Warn on merchants with no categorization references (warn-only by design).

        Deleting a categorization does not delete the merchant it referenced
        (categorization-matching-mechanics.md), so orphaned merchants accumulate
        as normal wear, not corruption — hence ``warn``, never ``fail``. Gated on
        the audit lookback window so freshly-created merchants not yet fanned out
        to transactions are not flagged.
        """
        name = "app_user_merchants_orphans"
        settings = get_settings().doctor
        try:
            rows = self._db.execute(
                f"""
                SELECT m.merchant_id
                FROM {USER_MERCHANTS.full_name} m
                WHERE m.updated_at < (now()::TIMESTAMP - (? * INTERVAL 1 DAY))
                  AND NOT EXISTS (
                    SELECT 1 FROM {TRANSACTION_CATEGORIES.full_name} c
                    WHERE c.merchant_id = m.merchant_id
                  )
                ORDER BY m.merchant_id
                """,  # noqa: S608  # TableRef constants, parameterized value
                [settings.audit_coverage_lookback_days],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"orphan check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="warn",
                detail=f"{len(affected)} merchant(s) referenced by no categorization",
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_orphan_app_state(self) -> InvariantResult:
        """Flag orphan ``app.transaction_notes`` / ``app.transaction_tags`` rows.

        A note or tag whose ``transaction_id`` no longer resolves in
        ``core.fct_transactions`` is an orphan — UNLESS it points at a manual
        transaction whose row is still in ``raw.manual_transactions``.
        ``transactions_create`` returns the predicted ``transaction_id``
        immediately, so notes/tags written against it before ``refresh_run``
        materializes the row are legitimate state, not orphans (V026 column
        + suppression).

        **Known limitation — deduped-away manuals.** When a manual joins a
        dedup group during refresh, its predicted id is REPLACED in core by
        the group's canonical id, but the raw row keeps the predicted id
        forever. Notes/tags written against the original predicted id are
        then genuinely orphaned, but this audit still suppresses them
        because the raw row exists. The narrower production-correct
        discriminator would be "manual not yet processed by the matcher,"
        but the obvious signal (``prep.int_transactions__matched``) is a
        live VIEW that reflects raw rows immediately — it cannot distinguish
        pending from processed. Closing this gap requires a real
        materialization signal (e.g., a ``processed_at`` column on
        ``raw.manual_transactions`` set by transform) and is tracked as a
        PR9 follow-up. The trade-off is accepted: the primary protection
        (against destroying notes on freshly-created manuals) is correct,
        and the deduped-away case is rare in practice.

        Skipped before the first transform builds ``core.fct_transactions``.

        ``affected_ids`` are emitted with prefixes so the doctor recipe can
        dispatch to the right MCP tool without re-querying:

        - ``note:<note_id>`` → one row per orphan note, deleted by stable id
        - ``tag:<transaction_id>`` → one row per orphan transaction (tags are
          cleared wholesale per transaction; multiple tag rows on the same
          orphan transaction collapse to one affected_id)
        """
        name = "orphan_app_state"
        try:
            rows = self._db.execute(
                f"""
                -- Materialize valid transaction_ids ONCE (core.fct_transactions is
                -- an expensive view); the prior correlated NOT EXISTS re-evaluated
                -- it per note/tag row — O(N × view). A row is an orphan when its
                -- transaction_id is in neither the fact view nor raw manuals, so
                -- valid_txn = fct ∪ manual and orphan = anti-join miss.
                WITH valid_txn AS MATERIALIZED (
                    SELECT transaction_id FROM {FCT_TRANSACTIONS.full_name}
                    UNION
                    SELECT transaction_id FROM {MANUAL_TRANSACTIONS.full_name}
                )
                SELECT 'note:' || n.note_id AS aid
                FROM {TRANSACTION_NOTES.full_name} n
                LEFT JOIN valid_txn v ON v.transaction_id = n.transaction_id
                WHERE v.transaction_id IS NULL
                UNION
                SELECT 'tag:' || g.transaction_id
                FROM {TRANSACTION_TAGS.full_name} g
                LEFT JOIN valid_txn v ON v.transaction_id = g.transaction_id
                WHERE v.transaction_id IS NULL
                ORDER BY aid
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — per-invariant isolation; one audit's failure must not abort the whole DoctorReport
            # Matches every other ``_run_*`` method in this file: any failure
            # (missing core.fct_transactions on first run, app-table column
            # drift, lock contention) returns ``skipped`` so the rest of the
            # report survives. exc_info on the debug log keeps real faults
            # diagnosable for operators tailing logs.
            logger.debug(f"orphan_app_state skipped: {e}", exc_info=True)
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"orphan check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            # "Entries" not "rows" — each tag entry collapses 1..N raw
            # transaction_tags rows for the same orphan transaction_id, so a
            # raw row count would overstate the orphan count. Notes are 1:1.
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} orphan note/tag entry(s) reference a "
                    "transaction_id absent from core.fct_transactions"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_staging_rejects(self) -> InvariantResult:
        """Plaid investment rows staging routed to review instead of the ledger.

        Staging refuses to guess, and files the row here instead. Three reasons
        today: ``split_underivable`` (a ``split`` whose multiplier MoneyBin
        will not derive), ``transfer_direction_underivable`` (a security-bearing
        ``transfer`` leg whose in/out direction cannot be inferred from
        quantity), and ``unmapped_subtype`` (an unmapped security-bearing
        subtype). All are deliberate refusals, not silent drops — this is where
        they surface. The query is deliberately open (``review_reason IS NOT
        NULL``), so a NEW reason added upstream surfaces here without a code
        change; only this list needs the follow-up.
        """
        name = "investment_staging_rejects"
        try:
            rows = self._db.execute(
                """
                SELECT investment_transaction_id, review_reason
                FROM prep.stg_plaid__investment_transactions
                WHERE review_reason IS NOT NULL
                ORDER BY investment_transaction_id
                """  # noqa: S608  # fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"staging view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            reasons = sorted({str(r[1]) for r in rows})
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} Plaid investment row(s) held out of the ledger "
                    f"pending review ({', '.join(reasons)}) — MoneyBin will not "
                    "guess a derivation for these; cross-check the affected trade "
                    "dates against your broker statement until automatic "
                    "resolution ships"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_opening_lot_review(self) -> InvariantResult:
        """Positions the opening-lot bootstrap refused to synthesize.

        Covers short/non-positive quantity, NULL basis, and
        ``sold_out_prewindow`` gaps the bootstrap declined to reconstruct
        rather than guess (``prep.stg_plaid__opening_lot_review``).
        ``security_id`` falls back to ``source_security_key`` (the same
        fallback ``_run_investment_unreported_holdings`` uses) — an unbound
        security is exactly the kind of gap this check exists to surface, and
        a bare ``security_id`` would render it as an unactionable ``None``.
        """
        name = "investment_opening_lot_review"
        try:
            rows = self._db.execute(
                """
                SELECT account_id,
                       COALESCE(security_id, source_security_key) AS security_key,
                       reason
                FROM prep.stg_plaid__opening_lot_review
                ORDER BY account_id, security_key
                """  # noqa: S608  # fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"bootstrap review view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} position(s) not bootstrapped "
                    "(short/split/negative-gap) — held basis may be missing; "
                    "treat cost basis for these (account, security) pairs as "
                    "incomplete until you can supply the opening lot manually"
                ),
                affected_ids=[f"{r[0]}:{r[1]} ({r[2]})" for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_unmodeled_legs(self) -> InvariantResult:
        """Legs recorded in the ledger but stripped of lot-affecting quantity.

        Staging maps three families of ``provider_subtype`` to
        ``type = 'other'`` with ``ledger_quantity`` forced to NULL: short
        legs (``buy to cover``/``sell short``), option legs
        (``assignment``/``exercise``/``expire``), and catch-all events
        (``adjustment``/``loan payment``/``rebalance``) —
        ``stg_plaid__investment_transactions.sql``'s own comment groups all
        three under "never carry a ledger quantity." All of them carry
        ``ledger_include = TRUE`` and ``review_reason = NULL`` (not staging
        rejects), so this check is the only place they surface; it keys on
        ``provider_subtype`` for exactly that reason. The risk is not
        theoretical for the option-leg family: an ``assignment`` that
        exercises away a covered-call position disposes of real shares, but
        with no ledger quantity to consume, the held lot never closes — a
        phantom position that overstates net worth until reconciled.

        Matched on ``LOWER(provider_subtype)`` because staging classifies on
        ``LOWER(COALESCE(subtype, ''))`` while preserving the provider's raw
        casing in ``provider_subtype``: a case-sensitive list here would miss
        an ``'Assignment'`` that staging still routed to NULL-quantity
        ``other``, and the check would report ``pass`` on exactly the row it
        exists to surface. Normalize identically on both sides.
        """
        name = "investment_unmodeled_legs"
        try:
            rows = self._db.execute(
                f"""
                SELECT investment_transaction_id
                FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
                WHERE LOWER(provider_subtype) IN (
                    'buy to cover', 'sell short',
                    'assignment', 'exercise', 'expire',
                    'adjustment', 'loan payment', 'rebalance'
                )
                ORDER BY investment_transaction_id
                """  # noqa: S608  # TableRef constant
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"ledger unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} leg(s) recorded but kept out of the "
                    "long-only lot engine — short positions, option "
                    "assignment/exercise/expiration, and other catch-all "
                    "events (adjustment/loan payment/rebalance) carry no "
                    "lot-affecting quantity; if one of these disposed of "
                    "shares (e.g. a covered call exercised away), the "
                    "position may never close in MoneyBin — cross-check "
                    "against your broker statement and track it outside "
                    "MoneyBin until short/option accounting ships"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_holdings_snapshot_divergence(self) -> InvariantResult:
        """Engine-derived held lots diverging from the broker's newest snapshot.

        ``core.dim_holdings`` is ``positions LEFT JOIN provider_reported`` — a
        broker-reported position MoneyBin has no lot for produces no row at
        all here (the more dangerous direction; not this check's job — see
        ``prep.stg_plaid__investment_holdings`` for that). This check only
        catches divergence on positions MoneyBin DOES hold a lot for.

        The cost-basis leg is gated on ``provider_reported_cost_basis IS NOT
        NULL``, mirroring the quantity leg's own ``provider_reported_quantity
        IS NOT NULL`` gate above: the raw DDL declares ``cost_basis``
        nullable and brokers routinely omit it, so ``COALESCE(..., 0)`` on
        that column alone would read "the broker didn't say" as "the broker
        says $0" and fire on every quantity-matched position whose
        connection doesn't report basis. The tolerance itself is relative,
        not a flat cent: ``GREATEST(0.01, 1bp of the reported basis)`` so
        many small DRIP/reinvest lots accumulating sub-cent rounding on a
        large position don't cross a fixed absolute floor.
        """
        name = "investment_holdings_divergence"
        try:
            rows = self._db.execute(
                f"""
                SELECT account_id, security_id
                FROM {DIM_HOLDINGS.full_name}
                WHERE provider_reported_quantity IS NOT NULL
                  AND (
                    quantity <> provider_reported_quantity
                    OR (
                      provider_reported_cost_basis IS NOT NULL
                      AND ABS(
                        COALESCE(cost_basis, 0) - provider_reported_cost_basis
                      ) > GREATEST(0.01, ABS(provider_reported_cost_basis) * 0.0001)
                    )
                  )
                ORDER BY account_id, security_id
                """  # noqa: S608  # TableRef constant
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"dim_holdings unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} position(s) where engine-derived held lots "
                    "diverge from the broker snapshot (method mismatch or "
                    "pre-window sell) — the broker snapshot is authoritative "
                    "for the held position; MoneyBin's quantity/cost_basis for "
                    "these should not be trusted until the divergence clears"
                ),
                affected_ids=[f"{r[0]}:{r[1]}" for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_source_overlap(self) -> InvariantResult:
        """Accounts carrying BOTH manual and Plaid investment history.

        Lots and gains double-count until one source is chosen per account
        (investment dedup across sources is a future matching child, unlike
        transactions which already have ``prep.int_transactions__matched``).
        """
        name = "investment_source_overlap"
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT COALESCE(al.account_id, p.account_id) AS account_id
                FROM {PLAID_INVESTMENT_TRANSACTIONS.full_name} AS p
                LEFT JOIN {ACCOUNT_LINKS.full_name} AS al
                  ON al.status = 'accepted' AND al.ref_kind = 'source_native'
                  AND al.source_type = 'plaid' AND al.source_origin = p.source_origin
                  AND al.ref_value = p.account_id
                JOIN {MANUAL_INVESTMENT_TRANSACTIONS.full_name} AS m
                  ON m.account_id = COALESCE(al.account_id, p.account_id)
                ORDER BY account_id
                """  # noqa: S608  # TableRef constants
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — raw tables absent on fresh DBs
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"raw tables unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} account(s) have both manual and Plaid "
                    "investment rows — lots and gains double-count until one "
                    "source is chosen per account; delete or stop importing the "
                    "redundant manual entries (investment dedup is a future "
                    "matching child)"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_unresolved_securities(self) -> InvariantResult:
        """Ledger rows a provider security key never resolved to a canonical security.

        ``cost_basis.py`` silently skips every ``LedgerEvent`` with
        ``security_id is None`` when grouping for lot computation — so an
        unresolved security is not just a missing label, it quietly
        understates lots and gains for that event. Scoped to rows whose
        staging row carried a non-NULL provider security key (a genuine
        security-bearing event); a NULL key is a legitimate cash-only row
        (deposit, withdrawal, ...) and not a gap.
        """
        name = "investment_unresolved_securities"
        try:
            rows = self._db.execute(
                f"""
                SELECT c.investment_transaction_id
                FROM {FCT_INVESTMENT_TRANSACTIONS.full_name} AS c
                JOIN prep.stg_plaid__investment_transactions AS p
                  ON p.investment_transaction_id = c.investment_transaction_id
                WHERE c.security_id IS NULL
                  AND p.source_security_key IS NOT NULL
                ORDER BY c.investment_transaction_id
                """  # noqa: S608  # TableRef constant + fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core/staging view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"ledger/staging unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} investment transaction(s) carry a provider "
                    "security key the resolver never bound to a canonical "
                    "security — cost basis silently drops these events, "
                    "understating lots and gains; run "
                    "`moneybin investments securities links pending` to review "
                    "and resolve them"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_conflicting_security_refs(self) -> InvariantResult:
        """One provider security, bound to two different canonical securities.

        A Plaid security row carries several refs (its plaid_security_id plus an
        institution ref per institution holding it). They describe ONE instrument,
        so they must resolve to one security. When they don't, MoneyBin is holding
        the same instrument as two — its lots and gains are split across both, and
        every report understates each.

        The resolver refuses to repoint either binding on its own: a repoint is a
        reviewed merge, never a sync-time side effect, and rewriting the wrong one
        fuses two real instruments' tax lots irreversibly. So it logs and moves on
        — which made the conflict invisible to everyone not reading server logs.
        This is that missing surface.

        Deliberately NOT filed into the security-link review queue: accepting such
        a decision merges the bound security away, and that path refuses a
        user-authored row outright — it would enqueue a decision the user cannot
        action. The conflict needs a human to decide which security is real, so
        report it and name both.
        """
        name = "investment_conflicting_security_refs"
        try:
            # Reconstruct each provider row's refs exactly as SecurityResolver
            # binds them (plaid_security_id, plus one namespaced institution ref
            # per institution reporting it), then ask how many securities they
            # landed on. A single (ref_kind, ref_value) can only be bound once —
            # the conflict is BETWEEN a row's own refs, so it is invisible to any
            # query that doesn't group them back together.
            rows = self._db.execute(
                f"""
                WITH provider_refs AS (
                    SELECT security_id AS provider_key,
                           'plaid_security_id' AS ref_kind,
                           security_id AS ref_value
                    FROM {PLAID_SECURITIES.full_name}
                    UNION
                    SELECT security_id,
                           'institution_security_id',
                           institution_id || ':' || institution_security_id
                    FROM {PLAID_SECURITIES.full_name}
                    WHERE institution_id IS NOT NULL
                      AND institution_security_id IS NOT NULL
                )
                SELECT r.provider_key
                FROM provider_refs AS r
                JOIN {SECURITY_LINKS.full_name} AS l
                  ON l.ref_kind = r.ref_kind
                 AND l.ref_value = r.ref_value
                 AND l.source_type = 'plaid'
                 AND l.status = 'accepted'
                GROUP BY r.provider_key
                HAVING COUNT(DISTINCT l.security_id) > 1
                ORDER BY r.provider_key
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — raw/app tables absent before first sync
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"security links unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} provider security row(s) have refs bound to more "
                    "than one canonical security — the same instrument is held as "
                    "two, so its lots and gains are split across both and each is "
                    "understated; decide which security is real and merge the "
                    "other away"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_unreported_holdings(self) -> InvariantResult:
        """Broker-reported positions with no matching ``core.dim_holdings`` row.

        ``core.dim_holdings`` is ``positions LEFT JOIN provider_reported`` — a
        position the broker's newest snapshot reports that MoneyBin has no
        open lot for produces NO row there at all (unlike a divergence, which
        at least produces a row to compare). That makes this the more
        dangerous direction: a position the user may hold without MoneyBin
        knowing it, caused by an unbound security, a declined opening-lot
        bootstrap, or a holdings snapshot that landed before its
        transactions. Checked directly against the staging view — the only
        place this direction is visible — scoped to each item's newest
        snapshot (mirrors ``dim_holdings.sql``'s own scoping) so a position
        the broker has since stopped reporting (sold, disconnected) is not
        flagged as a live gap. ``h.quantity > 0`` excludes a broker row
        reporting a CLOSED position (quantity 0 or NULL, both allowed by the
        raw DDL) — the same "holds nothing" case
        ``is_short_or_nonpositive`` already treats as expected data upstream
        (the opening-lot bootstrap), not a secretly-held position.
        """
        name = "investment_unreported_holdings"
        try:
            rows = self._db.execute(
                f"""
                WITH {_NEWEST_HOLDINGS_SNAPSHOT_CTE}
                SELECT DISTINCT h.account_id,
                       COALESCE(h.security_id, h.source_security_key) AS security_key
                FROM prep.stg_plaid__investment_holdings AS h
                JOIN newest_snapshot AS ns
                  ON ns.source_file = h.source_file
                  AND ns.source_origin = h.source_origin
                LEFT JOIN {DIM_HOLDINGS.full_name} AS d
                  ON d.account_id = h.account_id AND d.security_id = h.security_id
                WHERE d.account_id IS NULL
                  AND COALESCE(h.quantity, 0) > 0
                ORDER BY h.account_id, security_key
                """  # noqa: S608  # TableRef constant + fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — staging/core view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"holdings staging/core view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} position(s) the broker's newest snapshot "
                    "reports holding that MoneyBin has no open lot for at all "
                    "— you may hold this position without MoneyBin knowing it "
                    "(an unresolved security, a declined opening-lot "
                    "bootstrap, or a snapshot that arrived before its "
                    "transactions); cross-check against your broker statement "
                    "and resolve any pending securities via "
                    "`moneybin investments securities links pending`"
                ),
                affected_ids=[f"{r[0]}:{r[1]}" for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_phantom_holdings(self) -> InvariantResult:
        """Open lots the broker once reported but its newest snapshot no longer does.

        The mirror image of ``_run_investment_unreported_holdings``, and the more
        dangerous direction: MoneyBin claiming a position the broker's current
        data disagrees with OVERSTATES net worth, rather than understating it.
        ``core.dim_holdings.provider_reported_quantity`` is already NULL exactly
        when the broker's newest snapshot omits a position MoneyBin holds a lot for
        (``dim_holdings.sql`` — "that NULL is itself the signal"); this check reads
        it, scoped to phantoms.

        Scoped to positions the broker ONCE reported — ``ever_reported_positions``,
        the same position-level gate ``dim_holdings.sql`` withholds on. A position
        that never appears in any holdings snapshot is a manual holding, not a
        phantom, and flagging it would tell the user their share count is wrong when
        it is not — the false alarm this scope exists to avoid. "The broker stopped
        reporting this" is only meaningful for a position it once reported, and a
        holdings row exists only for a broker-covered account, so this one gate
        subsumes the older account-level coverage scope.

        A row surfaced here is a lot the ledger never closed against a once-reported
        position: an option assignment/exercise mapped to ``other`` with no ledger
        quantity (see ``_run_investment_unmodeled_legs``), an early sale the ledger
        never recorded, or a lot the engine otherwise failed to close.

        Known limitation: a Plaid security id that churned across a corporate action
        — old id bound and reported, new id unbound — still resolves to the same
        canonical id and surfaces here; that case wants the new id bound, not a
        reconciliation. Matches ``dim_holdings.sql``'s ``ever_reported_positions``.
        """
        name = "investment_phantom_holdings"
        try:
            rows = self._db.execute(
                f"""
                WITH ever_reported_positions AS (
                    SELECT DISTINCT account_id, security_id
                    FROM prep.stg_plaid__investment_holdings
                    WHERE NOT security_id IS NULL
                )
                SELECT d.account_id, d.security_id
                FROM {DIM_HOLDINGS.full_name} AS d
                JOIN ever_reported_positions AS erp
                  ON erp.account_id = d.account_id
                  AND erp.security_id = d.security_id
                WHERE d.provider_reported_quantity IS NULL
                ORDER BY d.account_id, d.security_id
                """  # noqa: S608  # TableRef constant + fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — staging/core view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"holdings staging/core view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} position(s) MoneyBin holds an open lot for "
                    "that the broker's newest (live) snapshot no longer "
                    "reports for the account — you likely do NOT hold this "
                    "position (an option assignment/exercise, an early sale "
                    "the ledger never recorded, or a lot the engine failed "
                    "to close); this overstates net worth until reconciled "
                    "— cross-check against your broker statement and record "
                    "the missing disposal"
                ),
                affected_ids=[f"{r[0]}:{r[1]}" for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_price_disagreement(self) -> InvariantResult:
        """Two provider feeds holding materially different closes for one security-date.

        Two sources agreeing is uninteresting; two disagreeing means one is
        wrong, and resolution picks a winner by rank without saying so. This is
        where that inference becomes visible.

        Reads ``prep.stg_security_prices`` rather than the resolved fact table
        for two reasons: the fact table has already collapsed the pair to one
        winner, and staging carries provider observations *only*. Overrides and
        trade-implied prices are derived at model build and never land in
        ``raw.security_prices``, so they cannot reach this comparison — which is
        the point. Both are *expected* to differ from a provider close (an
        override exists to correct one; a trade-implied price reflects one
        execution's size and spread), so including them would raise a standing
        warning on every ordinary correction and every intraday fill.

        Deliberately not scoped to an enumerated provider list. The set of
        providers grows and an enumeration can only go stale in the direction
        that hides disagreements; the derived sources are excluded structurally
        by reading staging instead.

        A grain the user has already marked is excluded, because reading staging
        means the competing rows survive the correction that settled them. The
        mark fixes the winner in ``core``, `prep` keeps both provider closes, and
        without this the check would report the same disagreement on every run
        forever — after following its own remediation, with nothing left to do.
        The exclusion matches the mark's full key (security, date, currency), not
        the security: a mark settles the date it names, and silencing a security
        outright would hide the wrong-key binding this check exists to catch.
        """
        name = "investment_price_disagreement"
        tolerance = get_settings().investments.price_disagreement_tolerance_pct / 100
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT a.security_id
                FROM {STG_SECURITY_PRICES.full_name} AS a
                JOIN {STG_SECURITY_PRICES.full_name} AS b
                  ON b.security_id = a.security_id
                  AND b.price_date = a.price_date
                  AND b.quote_currency = a.quote_currency
                  AND b.source_type > a.source_type
                WHERE ABS(a.close - b.close) / ((a.close + b.close) / 2) > ?
                  AND NOT EXISTS (
                    SELECT 1
                    FROM {SECURITY_PRICE_OVERRIDES.full_name} AS o
                    WHERE o.security_id = a.security_id
                      AND o.price_date = a.price_date
                      AND o.quote_currency = a.quote_currency
                  )
                ORDER BY a.security_id
                """,  # noqa: S608  # TableRef constants + bound parameter, no user input
                [tolerance],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — staging view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"price staging view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            pct = get_settings().investments.price_disagreement_tolerance_pct
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} security(s) have two price feeds disagreeing "
                    f"by more than {pct}% on the same date and currency — one "
                    "of them is wrong, and valuation picks a winner by source "
                    "rank without flagging the conflict; compare the competing "
                    'quotes with `moneybin db query "SELECT price_date, '
                    "source_type, close FROM prep.stg_security_prices WHERE "
                    "security_id = '<id>' ORDER BY price_date DESC\"` — the "
                    "losing quote never reaches `investments prices list`, "
                    "which reads the already-resolved series, and staging is "
                    "outside the schemas `sql query` can read — then record a "
                    "mark with `moneybin investments prices set` if the winning "
                    "feed is the wrong one, which settles that date and drops it "
                    "from this check"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_unpriced_holdings(self) -> InvariantResult:
        """Open positions carrying no usable price on the current valuation date.

        ``valuation_status`` already records this per position, but only where
        someone is already looking at that position — nothing surfaces it in the
        place users go to ask "is anything wrong with my data?". A position whose
        feed key never bound simply reads blank forever.

        Scoped to ``unpriced`` alone. ``withheld`` also publishes no value, but
        its remedy is reconciling a share count, not adding a price source, and
        routing it here would send the user to fix something that was never
        broken. ``carried_forward`` has a usable price whose age the staleness
        surface carries.

        Reported per security rather than per position: the remedy — bind a feed
        key or record a mark — applies to the security, so one holding in three
        accounts is one piece of work, not three.
        """
        name = "investment_unpriced_holdings"
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT security_id
                FROM {DIM_HOLDINGS.full_name}
                WHERE valuation_status = 'unpriced'
                ORDER BY security_id
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"holdings view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} held security(s) have no usable price, so "
                    "their positions report no market value at all and are "
                    "absent from every total that sums one — no provider "
                    "covers them, or their feed key never bound; run "
                    "`moneybin investments prices pull` and review anything it "
                    "queues, or record a valuation with "
                    "`moneybin investments prices set`"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_stale_prices(self) -> InvariantResult:
        """Positions valued from a close older than its security type allows.

        The surface ``_run_investment_unpriced_holdings`` defers to when it skips
        ``carried_forward``. Without it a feedless position keeps its last
        observed close indefinitely — a years-old ``trade_implied`` purchase
        price — and is summed into portfolio totals as though it were current.
        Nothing else says otherwise: ``holdings`` publishes
        ``max_days_since_observed`` as a number rather than a warning, precisely
        so that reporting an age is not mistaken for judging one.

        Scoped to ``carried_forward``. ``valued`` means the close is dated today
        and can never be stale; ``unpriced`` and ``withheld`` publish no value,
        and their remedies — a price source, a reconciled share count — are owned
        by their own checks.

        The threshold resolves per security type rather than globally: markets
        close ~114 days a year, so an equity threshold tight enough to catch a
        neglected crypto position would fire on most days for most users and
        train the reader to ignore every staleness warning. Comparison lives in
        Python rather than SQL because ``moneybin.staleness`` is the shared
        vocabulary `asset-tracking.md` values appraisals against too, and one
        rule with two implementations is how the two drift.
        """
        name = "investment_stale_prices"
        default_days = get_settings().investments.price_staleness_default_days
        try:
            rows = self._db.execute(
                f"""
                SELECT h.security_id,
                       COALESCE(s.security_type, 'other') AS security_type,
                       MAX(h.days_since_observed) AS days_since_observed
                FROM {DIM_HOLDINGS.full_name} AS h
                LEFT JOIN {DIM_SECURITIES.full_name} AS s
                  ON s.security_id = h.security_id
                WHERE h.valuation_status = 'carried_forward'
                  AND h.days_since_observed IS NOT NULL
                GROUP BY h.security_id, s.security_type
                ORDER BY h.security_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core views absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"holdings view unavailable: {e}",
                affected_ids=[],
            )
        stale = [
            str(security_id)
            for security_id, security_type, days in rows
            if is_stale(
                int(days),
                resolve_threshold_days(
                    str(security_type),
                    type_defaults=SECURITY_TYPE_STALENESS_DAYS,
                    global_default=default_days,
                ),
            )
        ]
        if stale:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(stale)} held security(s) are valued from a close older "
                    "than their type allows, so their market values and every "
                    "total that sums them are carried forward from a price that "
                    "may no longer hold — run `moneybin investments prices pull` "
                    "to refresh them, or record a current valuation with "
                    "`moneybin investments prices set` for a security no "
                    "provider covers"
                ),
                affected_ids=stale,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_investment_unmapped_price_source(self) -> InvariantResult:
        """Price rows whose ``source_type`` the staging view cannot resolve.

        ``prep.stg_security_prices`` takes each ``source_type``'s ``ref_kind``
        from ``seeds.price_source_map``, then INNER JOINs on the result. A source
        absent from that registry — or carrying no ``ref_kind`` — matches
        nothing, and the join drops the row silently and permanently: unlike an
        unresolved binding, no number of later accepted bindings will ever
        surface it, because the failure is in the registry rather than the
        binding. C.2 shipped a writer one commit ahead of its mapping and every
        row it wrote in between was discarded this way, which is why this check
        exists. Declaring a source now declares both halves at once, so this
        covers the rows already written and a registry row someone deletes.

        The accepted-binding join is what separates the two conditions without
        parsing the model's SQL. A row reaching this query already has an
        accepted binding whose ``source_type`` and ``ref_value`` match the
        staging join exactly, so ``ref_kind`` is the only remaining condition
        that can be failing. Without that clause the check would fire on every
        ordinary first pull, since the Plaid extractor writes prices during
        ingestion, before the resolver has minted a canonical security.

        The staging test asks whether the SOURCE stages at all, not whether this
        row did. An unmapped ``source_type`` is a property of the registry, so it
        costs the source every row it will ever write. Correlating row-by-row
        instead read a second, unrelated exclusion as the same failure:
        ``prep.stg_security_prices`` also bounds each key to the interval its
        current link owns, so a close predating a key's handover to a new owner
        — or postdating an automatic retirement — is absent from staging by
        design, and the row-wise form reported its source as unmapped when the
        registry row exists and is correct. What that costs: a mapped source whose
        entire staging output is empty for some other reason still reports here.
        """
        name = "investment_unmapped_price_source"
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT p.source_type
                FROM {SECURITY_PRICES.full_name} AS p
                JOIN {SECURITY_LINKS.full_name} AS l
                  ON l.status = 'accepted'
                  AND l.source_type = p.source_type
                  AND l.ref_value = p.provider_security_key
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {STG_SECURITY_PRICES.full_name} AS s
                    WHERE s.source_type = p.source_type
                )
                ORDER BY p.source_type
                """  # noqa: S608  # TableRef constants + fixed view name, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — staging view absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"price staging view unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} price source(s) write rows that resolve to "
                    "nothing: their securities are bound, but "
                    "prep.stg_security_prices has no ref_kind mapping for the "
                    "source, so every observation is discarded and no holding "
                    "is valued from it — this cannot be fixed by accepting "
                    "bindings and needs the source given a ref_kind in the "
                    "seeds.price_source_map registry"
                ),
                affected_ids=[str(r[0]) for r in rows],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_proposed_rules_rule_fk(self) -> InvariantResult:
        """Flag ``proposed_rules.rule_id`` that doesn't resolve in ``categorization_rules``.

        ``rule_id`` is NULL until a proposal is approved and links to its
        promoted rule; a non-NULL value that names no existing rule is a
        dangling FK (V016 replaced the text-keyed linkage with this id). NULL is
        not a violation — only set-but-unresolved ids are flagged.
        """
        name = "app_proposed_rules_rule_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT p.proposed_rule_id
                FROM {PROPOSED_RULES.full_name} p
                WHERE p.rule_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {CATEGORIZATION_RULES.full_name} r
                    WHERE r.rule_id = p.rule_id
                  )
                ORDER BY p.proposed_rule_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} proposed_rules row(s) reference a "
                    "categorization_rules.rule_id that does not exist"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_transaction_categories_fk(self) -> InvariantResult:
        """Flag ``transaction_categories`` rows with no ``core.fct_transactions`` row.

        ``transaction_id`` is a 1:1 FK to the canonical transaction; a
        categorization referencing a transaction that no longer exists (e.g. a
        re-import dropped it) is an orphan. Skipped before the first transform
        builds ``core.fct_transactions``.
        """
        name = "app_transaction_categories_fk"
        try:
            rows = self._db.execute(
                f"""
                -- Anti-join against a once-materialized id set, NOT a correlated
                -- NOT EXISTS. core.fct_transactions is an expensive view (the full
                -- merge/dedup/categorization pipeline); a correlated subquery
                -- re-evaluates it per app.transaction_categories row — O(N × view) —
                -- which wedges the doctor once this table is populated and the
                -- optimizer can't decorrelate the view.
                SELECT c.transaction_id
                FROM {TRANSACTION_CATEGORIES.full_name} c
                LEFT JOIN (
                    SELECT DISTINCT transaction_id FROM {FCT_TRANSACTIONS.full_name}
                ) t ON t.transaction_id = c.transaction_id
                WHERE t.transaction_id IS NULL
                ORDER BY c.transaction_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.fct_transactions may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} transaction_categories row(s) reference a "
                    "transaction_id absent from core.fct_transactions"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_match_decisions_account_fk(self) -> InvariantResult:
        """Flag ``match_decisions`` whose account references are absent from ``dim_accounts``.

        ``match_decisions`` stores *source-native* transaction ids
        (``source_transaction_id_a``/``_b`` + type + origin), which resolve in the
        staging layer, not in ``core.fct_transactions`` — so there is no clean
        transaction FK here (see the spec's Batch D reconciliation note). But
        ``account_id`` IS the gold account key, and ``account_id_b`` is the
        transfer counterparty (NULL for dedup); an account referenced by a
        decision but missing from ``core.dim_accounts`` means a re-import dropped
        the account while leaving its decisions (orphan). Skipped before the first
        transform builds ``core.dim_accounts``.
        """
        name = "app_match_decisions_account_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT m.match_id
                FROM {MATCH_DECISIONS.full_name} m
                WHERE NOT EXISTS (
                        SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                        WHERE a.account_id = m.account_id
                      )
                   OR (m.account_id_b IS NOT NULL AND NOT EXISTS (
                        SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                        WHERE a.account_id = m.account_id_b
                      ))
                ORDER BY m.match_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_accounts may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} match_decisions row(s) reference an "
                    "account_id absent from core.dim_accounts"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_account_settings_account_fk(self) -> InvariantResult:
        """Flag ``account_settings.account_id`` with no ``core.dim_accounts`` row.

        ``account_id`` is the NOT-NULL PK and a 1:1 FK to the canonical account;
        a settings row for an account that no longer exists (e.g. a re-import
        dropped it) is an orphan. Skipped before the first transform builds
        ``core.dim_accounts``.
        """
        name = "app_account_settings_account_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT s.account_id
                FROM {ACCOUNT_SETTINGS.full_name} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                    WHERE a.account_id = s.account_id
                )
                ORDER BY s.account_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_accounts may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account_settings row(s) reference an "
                    "account_id absent from core.dim_accounts"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_account_settings_reserved_display_name(self) -> InvariantResult:
        """Flag ``account_settings.display_name`` rows folding onto the reserved label.

        Every write path refuses a ``display_name`` that
        ``is_reserved_account_name`` folds onto ``UNNAMED_ACCOUNT_LABEL`` (a
        case variant, padding, a doubled space, an NFKC-equivalent), because
        ``core.dim_accounts`` shows that exact label for an account it could
        not name and every resolver reads it that way. Those guards only bind
        writes made through them: a row stored before either shipped still
        holds whatever it holds, and ``is_a_name`` stays byte-exact by design
        (normalizing it would silently drop a real user-set name that happens
        to equal the label) — so such a row keeps reading as a name and can
        resolve a request for the reserved label to it. Not auto-fixable:
        MoneyBin cannot guess the account's real name, only tell the user to
        pick one.
        """
        name = "app_account_settings_reserved_display_name"
        try:
            rows = self._db.execute(
                f"""
                SELECT account_id, display_name
                FROM {ACCOUNT_SETTINGS.full_name}
                WHERE display_name IS NOT NULL
                ORDER BY account_id
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"reserved-label check unavailable: {e}",
                affected_ids=[],
            )
        affected = [
            str(account_id)
            for account_id, display_name in rows
            if is_reserved_account_name(display_name)
        ]
        if affected:
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account(s) have a stored display_name that "
                    f"folds onto the reserved {UNNAMED_ACCOUNT_LABEL!r} label. "
                    "MoneyBin shows that label for an account it could not name, "
                    "so a name-based lookup can resolve to this row instead of "
                    "the account you mean; rename it with `moneybin accounts set "
                    "<account> --display-name <new name>`."
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_dim_accounts_reserved_display_name(self) -> InvariantResult:
        """Flag source-authored ``dim_accounts`` labels folding onto the reserved label.

        The sibling check above covers the ``app.account_settings`` override.
        The other human-authored rung is the source's own ``account_label`` — a
        sheet's Account column, ``--account-name``, Plaid's per-account name —
        and ``dim_accounts.sql`` filters that rung with a byte-exact
        ``<> 'Unnamed account'``. SQL cannot NFKC-casefold, so a label reaching
        it as ``unnamed account`` or ``Unnamed  account`` is promoted, arrives
        with ``display_name_is_user_set = TRUE``, and collides with the label
        MoneyBin displays for accounts it could not name — the same defect the
        write-path guards reject, entered through a channel that never touches
        ``app.*``. An export publishes that label as ``account_name`` and can be
        re-imported, so this is an ordinary route, not a contrived one.

        Scoped to rows with no settings override, so an account already
        reported by the sibling check is not reported twice. Remediation is the
        same rename: the override outranks the source label.

        Detection only. Teaching the model to reject the fold would change
        ``core.dim_accounts.display_name`` for rows that already have it, which
        is a core-schema change, not a doctor check.
        """
        name = "dim_accounts_reserved_display_name"
        try:
            rows = self._db.execute(
                f"""
                SELECT d.account_id, d.display_name
                FROM {DIM_ACCOUNTS.full_name} AS d
                LEFT JOIN {ACCOUNT_SETTINGS.full_name} AS s
                  ON s.account_id = d.account_id
                WHERE d.display_name_is_user_set
                  AND s.display_name IS NULL
                ORDER BY d.account_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core may not be built yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"reserved-label check unavailable: {e}",
                affected_ids=[],
            )
        affected = [
            str(account_id)
            for account_id, display_name in rows
            if is_reserved_account_name(display_name)
        ]
        if affected:
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account(s) took their name from a source "
                    f"label that folds onto the reserved {UNNAMED_ACCOUNT_LABEL!r} "
                    "label. MoneyBin shows that label for an account it could not "
                    "name, so a name-based lookup can resolve to one of these "
                    "instead of the account you mean; override it with `moneybin "
                    "accounts set <account> --display-name <new name>`."
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_balance_assertions_account_fk(self) -> InvariantResult:
        """Flag ``balance_assertions.account_id`` with no ``core.dim_accounts`` row.

        Reports distinct violating ``account_id`` values (one account may have
        assertions across many dates). Skipped before ``core.dim_accounts`` exists.
        """
        name = "app_balance_assertions_account_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT b.account_id
                FROM {BALANCE_ASSERTIONS.full_name} b
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                    WHERE a.account_id = b.account_id
                )
                ORDER BY b.account_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_accounts may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account(s) with balance_assertions reference an "
                    "account_id absent from core.dim_accounts"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_budgets_category_fk(self) -> InvariantResult:
        """Flag ``budgets.category_id`` that doesn't resolve in ``core.dim_categories``.

        ``category_id`` is nullable (NULL for orphaned legacy rows from V014's
        dual-write backfill); NULL is not a violation, so only set-but-unresolved
        ids are flagged. Skipped before ``core.dim_categories`` exists.
        """
        name = "app_budgets_category_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT b.budget_id
                FROM {BUDGETS.full_name} b
                WHERE b.category_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {CATEGORIES.full_name} c
                    WHERE c.category_id = b.category_id
                  )
                ORDER BY b.budget_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_categories may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} budgets row(s) reference a "
                    "category_id absent from core.dim_categories"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_pdf_formats_recipe_validity(self) -> InvariantResult:
        """Flag ``app.pdf_formats.extraction_recipe`` rows that fail Recipe schema.

        Every stored recipe is replayed by the deterministic executor on later
        imports, so a row whose JSON no longer round-trips through
        ``Recipe.model_validate`` is a silent landmine — it survives until the
        next matching fingerprint arrives, then the replay raises and the file
        falls back to the seed path. Surfacing it at doctor time lets an operator
        either re-derive (delete + replay first contact) or restore from
        ``app.audit_log`` undo before the next import.
        """
        name = "app_pdf_formats_recipe_validity"
        try:
            rows = self._db.execute(
                f"""
                SELECT name, CAST(extraction_recipe AS VARCHAR)
                FROM {PDF_FORMATS.full_name}
                ORDER BY name
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"recipe validity check unavailable: {e}",
                affected_ids=[],
            )
        if not rows:
            return InvariantResult(
                name=name, status="pass", detail=None, affected_ids=[]
            )
        # Imported lazily — extractors.pdf.recipe pulls in pydantic + the `regex`
        # package, which is unnecessary cost for doctor runs against installs
        # whose pdf_formats table is empty (the no-rows branch above returns
        # early).
        from moneybin.extractors.pdf.recipe import Recipe  # noqa: PLC0415

        bad: list[str] = []
        for name_, recipe_json in rows:
            try:
                Recipe.model_validate_json(recipe_json)
            except Exception:  # noqa: BLE001 — pydantic ValidationError + JSONDecodeError + bound-validator ValueErrors
                bad.append(str(name_))
        if bad:
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(bad)} pdf_formats row(s) carry an extraction_recipe "
                    "that no longer validates against Recipe"
                ),
                affected_ids=bad,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_pdf_formats_bounds(self) -> InvariantResult:
        """Flag ``app.pdf_formats`` rows that violate numeric/temporal bounds.

        Three invariants the schema cannot express as cheap CHECK constraints
        (``last_used_at >= created_at`` cuts across two columns, and a CHECK
        on a defaulted INTEGER doesn't catch raw bypass writes that supply an
        explicit out-of-range value):

        - ``version >= 1`` — ``save_new`` inserts at 1; ``bump_version`` only
          increments. A row at 0 or negative is corruption.
        - ``times_used >= 0`` — monotonically incremented by ``record_use``.
          Negative counts are corruption.
        - ``last_used_at >= created_at`` when ``last_used_at`` is set —
          last-use can never precede creation by the table's own clock.
        """
        name = "app_pdf_formats_bounds"
        try:
            rows = self._db.execute(
                f"""
                SELECT name
                FROM {PDF_FORMATS.full_name}
                WHERE version < 1
                   OR times_used < 0
                   OR (last_used_at IS NOT NULL AND last_used_at < created_at)
                ORDER BY name
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"bounds check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} pdf_formats row(s) violate version/"
                    "times_used/timestamp ordering bounds"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_pdf_formats_fingerprint_shape(self) -> InvariantResult:
        """Flag ``app.pdf_formats.layout_fingerprint`` rows that can never match.

        The replay path looks a format up with ``layout_fingerprint = ?::JSON``,
        which DuckDB evaluates as a *textual* match against the canonical string
        ``serialize_fingerprint`` emits. A stored fingerprint is therefore live
        only if it is both (1) structurally what ``compute_fingerprint``
        produces — exactly ``issuer``/``headers``/``page_bucket``, with a string
        ``issuer``, a string-only ``headers`` list, and a ``page_bucket`` from
        the producer's closed ``PAGE_BUCKETS`` set — and (2) byte-for-byte that
        canonical serialization. Any other shape (missing or extra key, wrong
        value type, out-of-domain ``page_bucket``) or any non-canonical encoding
        (different key order, extra whitespace) is dead in the table and can
        never match a future import. Validating against ``serialize_fingerprint`` itself ties
        this invariant to the one encoding the replay path actually searches
        for, instead of re-enumerating corruption modes one SQL predicate at a
        time. Surfacing a dead row at doctor time lets an operator delete or
        re-derive before the format silently rots.
        """
        name = "app_pdf_formats_fingerprint_shape"
        try:
            rows = self._db.execute(
                f"""
                SELECT name, CAST(layout_fingerprint AS VARCHAR)
                FROM {PDF_FORMATS.full_name}
                ORDER BY name
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"fingerprint shape check unavailable: {e}",
                affected_ids=[],
            )
        bad = [str(name_) for name_, raw in rows if not _is_live_fingerprint(raw)]
        if bad:
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(bad)} pdf_formats row(s) carry a layout_fingerprint "
                    "that is malformed or is not the canonical serialization "
                    "the replay path matches on"
                ),
                affected_ids=bad,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _get_transaction_count(self) -> int:
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            logger.debug(
                "core.fct_transactions not available; transaction count unavailable"
            )
            return 0

    def _run_sqlmesh_audits(self, verbose: bool) -> list[InvariantResult]:
        """Report every SQLMesh standalone audit, via the shared audit runner.

        The audit SQL under ``src/moneybin/sqlmesh/audits/`` is the canonical
        definition of each check; doctor renders none of it itself, so a
        scenario run and a ``moneybin system doctor`` cannot disagree about
        what an audit means.
        """
        try:
            outcomes = run_standalone_audits(self._db)
        except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
            logger.warning(f"Transform audit discovery failed: {e}")
            return [
                InvariantResult(
                    name="transform_audits_unavailable",
                    status="skipped",
                    detail=f"Transform audit discovery failed: {e}",
                    affected_ids=[],
                )
            ]
        results: list[InvariantResult] = []
        for outcome in outcomes:
            if outcome.error is not None:
                results.append(
                    InvariantResult(
                        name=outcome.name,
                        status="skipped",
                        detail=outcome.error,
                        affected_ids=[],
                    )
                )
            elif outcome.violation_ids:
                results.append(
                    InvariantResult(
                        name=outcome.name,
                        status="fail",
                        detail=f"{len(outcome.violation_ids)} violation(s)",
                        affected_ids=outcome.violation_ids if verbose else [],
                    )
                )
            else:
                results.append(
                    InvariantResult(
                        name=outcome.name,
                        status="pass",
                        detail=None,
                        affected_ids=[],
                    )
                )
        return results

    def _run_dedup_reconciliation(self) -> InvariantResult:
        """Reconcile raw→core row counts against recorded dedup decisions.

        Every imported row that disappears between the unioned staging layer
        and the core fact table must be explained by an accepted dedup match
        decision. The invariant:

            raw_total - core_count == dedup_absorbed

        where ``dedup_absorbed`` is ``Σ(group_size - 1)`` over every connected
        component in ``prep.int_transactions__matched`` — equivalently,
        ``COUNT(*) - COUNT(DISTINCT match_group_id)`` over rows where
        ``match_group_id IS NOT NULL``.

        This formula is exact for any group topology including N-way merges and
        cyclic accepted-edge sets (e.g. three accepted edges over a 3-node
        group still absorbs only 2 rows). A mismatch means rows collapsed
        without a recorded match (a leak) or a match failed to collapse its
        rows (an un-applied decision).
        """
        try:
            raw_total = self._scalar_int(
                f"SELECT COUNT(*) FROM {INT_TRANSACTIONS_UNIONED.full_name}"  # noqa: S608 — TableRef constant
            )
            core_count = self._scalar_int(
                f"SELECT COUNT(DISTINCT transaction_id) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
            )
            dedup_absorbed = self._scalar_int(
                f"""
                SELECT COUNT(*) - COUNT(DISTINCT match_group_id)
                FROM {INT_TRANSACTIONS_MATCHED.full_name}
                WHERE match_group_id IS NOT NULL
                """  # noqa: S608 — TableRef constant; = SUM(group_size - 1) over components
            )
        except Exception as e:  # noqa: BLE001 — degrade gracefully; surface cause at DEBUG
            # Expected case: prep/core views absent before the first transform.
            # Bind + exc_info so a real fault (renamed column, permissions) is
            # diagnosable rather than masked by the static skip detail.
            logger.debug(f"dedup_reconciliation skipped: {e}", exc_info=True)
            return InvariantResult(
                name="dedup_reconciliation",
                status="skipped",
                detail="prep/core layer not available; run transform first",
                affected_ids=[],
            )
        observed_absorbed = raw_total - core_count
        if observed_absorbed < 0:
            # core can't legitimately hold more rows than staging — report the
            # impossible direction plainly instead of a nonsensical negative
            # "absorbed" count an agent can't act on.
            return InvariantResult(
                name="dedup_reconciliation",
                status="fail",
                detail=(
                    f"core has more rows than staging "
                    f"(core_count={core_count} > raw_total={raw_total}); "
                    "a transaction reached core without passing through the "
                    "staging layer"
                ),
                affected_ids=[],
            )
        if observed_absorbed == dedup_absorbed:
            return InvariantResult(
                name="dedup_reconciliation",
                status="pass",
                detail=None,
                affected_ids=[],
            )
        return InvariantResult(
            name="dedup_reconciliation",
            status="fail",
            detail=(
                f"expected {dedup_absorbed} absorbed row(s) from accepted dedup "
                f"decisions but observed {observed_absorbed} "
                f"(raw_total={raw_total}, core_count={core_count}). "
                "If you imported data since the last transform, this is expected "
                "until you re-run `moneybin transform`: staging counts new rows "
                "(and pending, not-yet-accepted matches) immediately, but core only "
                "reflects them after a transform. A mismatch that persists after a "
                "fresh transform indicates a dedup leak or an un-applied decision."
            ),
            affected_ids=[],
        )

    def _scalar_int(self, sql: str) -> int:
        """Execute a single-row query and return column 0 as an int (0 if no row)."""
        row = self._db.execute(sql).fetchone()
        return int(row[0]) if row else 0

    def _run_duplicate_account_overlap(self) -> InvariantResult:
        """One real account imported under two canonical identities.

        The transaction matcher blocks candidate pairs on ``account_id``, so it
        is structurally blind to this: when the same account arrives from two
        sources and account identity fails to bind them, every transaction is
        held twice under two ids and no dedup pair is ever even considered.
        Nothing else in the pipeline reports it — the ledger simply reads
        double. This check is the only surface that can see across the split,
        which is why it queries the ledger directly rather than reusing
        ``dedup_reconciliation``'s staging counts (those reconcile perfectly
        while the split persists: nothing was lost, the same money was counted
        twice).

        The evidence is an overlapping date+amount set between two accounts at
        the same institution. Three conditions narrow it to the real failure:

        - **Same institution.** Two accounts at one bank are the only pair a
          split identity can produce.
        - **Exact amount, matcher date window.** Reuses
          ``matching.date_window_days`` deliberately: an exact-date formulation
          would have missed the case that motivated this check, where only 23%
          of the mirrored pairs landed on the same day and the rest were spread
          across posting lag. Amount equality carries the SIGN, which is what
          keeps same-institution transfers (equal magnitude, opposite sign) out.
        - **Enough of the account, over enough distinct amounts.** The ratio
          separates a duplicated account from two real ones that share a few
          amounts by coincidence; the distinct-amount floor rejects the
          degenerate mirror — twin savings accounts posting identical interest
          every month cover each other 100% on a single amount.

        ``warn``, not ``fail``: only the user knows whether two accounts at one
        bank are really one, and a merge is not reversible by re-running
        anything. Reported once per unordered pair, at the higher of the two
        directional coverage ratios (a small duplicate fully contained in a
        large sibling covers little of the sibling but all of itself).
        """
        name = "duplicate_account_overlap"
        settings = get_settings()
        try:
            rows = self._db.execute(
                f"""
                -- Prune to institutions holding more than one account before
                -- touching the (expensive) fact view: a profile's transactions
                -- are overwhelmingly at institutions with a single account,
                -- and those can never form a pair.
                WITH contested_accounts AS (
                    SELECT account_id, institution_slug
                    FROM {DIM_ACCOUNTS.full_name}
                    WHERE NOT institution_slug IS NULL
                      AND institution_slug IN (
                        SELECT institution_slug
                        FROM {DIM_ACCOUNTS.full_name}
                        WHERE NOT institution_slug IS NULL
                        GROUP BY institution_slug
                        HAVING COUNT(*) > 1
                      )
                ),
                scoped AS MATERIALIZED (
                    SELECT t.transaction_id, t.account_id, t.transaction_date,
                           t.amount, t.currency_code, c.institution_slug
                    FROM {FCT_TRANSACTIONS.full_name} AS t
                    JOIN contested_accounts AS c ON c.account_id = t.account_id
                ),
                totals AS (
                    SELECT account_id, COUNT(*) AS row_count
                    FROM scoped GROUP BY account_id
                ),
                -- DISTINCT on the LEFT row: one transaction with three
                -- counterparts on the sibling still covers exactly one row, so
                -- a repeating amount cannot inflate the ratio past 100%.
                mirrored AS (
                    SELECT DISTINCT x.account_id AS lhs, y.account_id AS rhs,
                           x.transaction_id, x.amount
                    FROM scoped AS x
                    JOIN scoped AS y
                      ON y.institution_slug = x.institution_slug
                     AND y.account_id <> x.account_id
                     AND y.amount = x.amount
                     -- Equal numerals across currencies are not mirroring: one
                     -- bank's USD checking and EUR travel accounts can align on
                     -- nominal amounts by coincidence, which is exactly what the
                     -- distinct-amount floor and coverage ratio below exist to
                     -- rule out. NULL-tolerant for the same reason as the
                     -- matcher's blocking join — an unrecorded currency is not a
                     -- known mismatch, and treating it as one would hide a real
                     -- split account behind a quiet source.
                     AND (
                         y.currency_code IS NULL
                         OR x.currency_code IS NULL
                         OR y.currency_code = x.currency_code
                     )
                     AND ABS(
                         DATEDIFF('day', x.transaction_date, y.transaction_date)
                     ) <= ?
                ),
                directional AS (
                    SELECT lhs, rhs, COUNT(*) AS mirrored_rows,
                           COUNT(DISTINCT amount) AS distinct_amounts
                    FROM mirrored GROUP BY lhs, rhs
                ),
                qualifying AS (
                    SELECT d.lhs, d.rhs,
                           d.mirrored_rows * 1.0 / t.row_count AS ratio
                    FROM directional AS d
                    JOIN totals AS t ON t.account_id = d.lhs
                    WHERE d.distinct_amounts >= ?
                      AND d.mirrored_rows >= t.row_count * ?
                )
                SELECT LEAST(lhs, rhs) AS account_a,
                       GREATEST(lhs, rhs) AS account_b,
                       MAX(ratio) AS ratio
                FROM qualifying
                GROUP BY account_a, account_b
                ORDER BY account_a, account_b
                """,  # noqa: S608 — TableRef constants, parameterized values
                [
                    settings.matching.date_window_days,
                    settings.doctor.duplicate_account_min_distinct_amounts,
                    settings.doctor.duplicate_account_overlap_ratio,
                ],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core views absent before first transform
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"accounts/ledger unavailable: {e}",
                affected_ids=[],
            )
        # Only after the query succeeded: a skipped check leaves the gauge alone
        # rather than publishing a zero that reads as "no duplicate accounts".
        DUPLICATE_ACCOUNT_PAIRS.set(len(rows))
        if rows:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"{len(rows)} account pair(s) at the same institution mirror "
                    "each other's transactions on date and amount — most likely "
                    "one real account imported under two identities, which "
                    "double-counts its balance and its spending; the transaction "
                    "matcher blocks on account_id and cannot see across the "
                    "split. Try `moneybin accounts links run`, then "
                    "`accounts links pending`; decide a proposal with "
                    "`accounts links set <decision_id> --into <account_id>`, or "
                    "`--standalone` if the accounts are genuinely distinct and "
                    "this should stay a warning. Note that identity resolution "
                    "matches on institution+last-four and name similarity, not "
                    "on the transaction overlap this check measures, so a pair "
                    "flagged here may raise no proposal at all — that is the "
                    "same binding failure that produced the split. For that "
                    "residue, name the pair yourself with `moneybin accounts "
                    "links run <account_id> <candidate_account_id>`, which "
                    "queues the same reviewable proposal from the ids above"
                ),
                affected_ids=[
                    f"{a}:{b} ({round(float(ratio) * 100)}% overlap)"
                    for a, b, ratio in rows
                ],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_unproposed_cross_source_duplicates(self) -> InvariantResult:
        """Two sources co-resident in one account that the matcher never considered.

        The blind spot ``dedup_reconciliation`` cannot see. That check asserts
        ``raw_total - core_count == dedup_absorbed``, which balances whether or
        not a duplicate was ever *proposed* — a pair nobody looked at moves both
        sides of the equation together, so it stayed green through all 377 rows
        of the 2026-08-08 incident. ``duplicate_account_overlap`` saw that split
        while it was two accounts and stopped applying the instant the link was
        accepted, which is precisely when this one starts.

        The evidence is a pair the matcher's own blocking join would have
        produced — same account, equal amount (sign included), inside
        ``matching.date_window_days``, neither side manual — where **neither
        side carries any ``app.match_decisions`` row, in any status, in either
        direction**.

        That last clause is what makes silence mean something, because the
        matcher drops candidate pairs for two legitimate reasons and both leave
        the *other* side holding a decision:

        - ``assign_components`` skips a redundant edge when the two rows are
          already connected, and skips an edge that would co-locate two rows
          from one physical source. In both cases the row is spoken for by the
          pairing that won.
        - Tier 2b declines a within-source pair by writing nothing at all
          (``engine._classify_pair`` returns ``None``), which is why this check
          requires the matcher's own Tier 3 test — differing ``source_type``
          **or** differing ``source_origin``. For a pair matching on both, no
          row is the normal resting state and flagging it would nag on every
          ordinary near-duplicate. Testing ``source_type`` alone would be the
          opposite error: two CSV bank integrations, or two Plaid connections,
          are cross-source candidates the matcher does consider.

        Cross-source is the case where silence is diagnostic: every Tier 3 pair
        that survives assignment is persisted, ``pending`` if nothing else. So a
        cross-source pair with no decision on either side was never a candidate
        — the two rows were not in the same account when matching last ran.

        ``warn``, not ``fail``: equal amounts days apart can be two real
        charges, and only a match pass can tell. A user-rejected pair keeps its
        row, so dismissing a proposal silences this permanently rather than
        nagging.
        """
        name = "unproposed_cross_source_duplicates"
        settings = get_settings()
        try:
            rows = self._db.execute(
                f"""
                WITH RECURSIVE candidates AS (
                    SELECT a.account_id,
                           a.source_type AS type_a,
                           a.source_origin AS origin_a,
                           a.source_transaction_id AS id_a,
                           a.source_file AS file_a,
                           b.source_type AS type_b,
                           b.source_origin AS origin_b,
                           b.source_transaction_id AS id_b,
                           b.source_file AS file_b
                    FROM {INT_TRANSACTIONS_UNIONED.full_name} AS a
                    JOIN {INT_TRANSACTIONS_UNIONED.full_name} AS b
                      ON a.account_id = b.account_id
                     AND a.amount = b.amount
                     -- Mirrors the matcher's blocking join exactly, including
                     -- its NULL tolerance: currency_code is nullable at this
                     -- layer, and reading NULL as "differs" would hide the
                     -- duplicate wherever one source is quiet.
                     AND (
                         a.currency_code IS NULL
                         OR b.currency_code IS NULL
                         OR a.currency_code = b.currency_code
                     )
                     AND ABS(
                         DATEDIFF('day', a.transaction_date, b.transaction_date)
                     ) <= ?
                     AND a.source_type <> 'manual'
                     AND b.source_type <> 'manual'
                     -- The matcher's Tier 3 test verbatim (scoring.py
                     -- _get_candidates): differing type OR differing origin.
                     -- Type alone would miss two CSV bank integrations and two
                     -- Plaid connections, which are cross-source to the matcher
                     -- and so are exactly as capable of holding a duplicate.
                     AND (
                         a.source_type <> b.source_type
                         OR a.source_origin <> b.source_origin
                     )
                     AND (
                         a.source_type, a.source_origin, a.source_transaction_id
                     ) < (
                         b.source_type, b.source_origin, b.source_transaction_id
                     )
                ),
                -- Both directions flattened: the matcher writes a pair once, in
                -- whichever order the blocking join produced, so a row can be
                -- spoken for from either column. Node identity is the matcher's
                -- NodeKey exactly -- (source_type, source_transaction_id) keyed
                -- per account, per assignment.py's _node_a/_node_b. The account
                -- scoping is load-bearing: a source-native id is unique only
                -- within its account, so an un-namespaced FITID reused by
                -- another account would otherwise mark this row "already
                -- decided" and suppress the warning, most likely on two
                -- accounts at one institution -- precisely the pair an
                -- account-link merge just joined.
                --
                -- source_origin is deliberately absent, because NodeKey omits
                -- it: two rows alike on (source_type, source_transaction_id)
                -- under one account ARE one node to the matcher, so
                -- find(a) == find(b) and assign_components drops the candidate
                -- without writing anything. Adding origin here would split that
                -- single node in two and warn about a pair no refresh can
                -- clear. Rejected pairs key the same way below, and for the
                -- same reason: get_rejected_pairs selects origin, but scoring.py
                -- drops it when building rejected_set, so the matcher skips a
                -- rejected pair whatever origin the rows now carry. Demanding
                -- origin there would warn about a pair no refresh can clear --
                -- the identical failure, one CTE down.
                -- Two kinds of decision suppress, at two different grains,
                -- because the matcher treats them differently.
                --
                -- accepted/pending: a live union-find edge. These are read at
                -- *component* grain, not node grain -- see the two suppression
                -- clauses on the final SELECT, which together mirror the two
                -- reasons assign_components drops an edge.
                --
                -- rejected: NOT a union-find seed. The matcher excludes only the
                -- exact rejected pair, so a row rejected against one partner is
                -- still a live candidate against every other. Suppressing at
                -- node grain would hide a genuinely unproposed pair behind an
                -- unrelated rejection -- the blind spot this check exists to
                -- close. Pair grain, both orders.
                --
                -- reversed rows suppress nothing: the decision was undone, so
                -- the pair is undecided again.
                --
                -- match_type = 'dedup' throughout: Tier 4 transfers are
                -- cross-account and never run through the Tier 3 blocking join
                -- this mirrors, and on a transfer row account_id names only
                -- side A anyway.
                -- Live dedup edges, both directions, as opaque node keys.
                -- chr(31) is the ASCII unit separator, which no source_type or
                -- source-native id contains -- so the concatenation cannot
                -- alias two different pairs onto one key.
                --
                -- Pending counts here, unlike in the transfer retirement
                -- (AccountLinksService), which asks accepted-only. This
                -- invariant asks whether a pair has been *proposed*, and a
                -- pending row is a proposal sitting in the review queue -- the
                -- user has been told. The retirement asks what actually
                -- collapsed in core, which the prep fold answers with accepted
                -- alone. Both readings are correct for their question; do not
                -- align them.
                dedup_edges AS (
                    SELECT account_id AS acct,
                           source_type_a || chr(31)
                               || source_transaction_id_a AS n1,
                           source_type_b || chr(31)
                               || source_transaction_id_b AS n2
                    FROM {MATCH_DECISIONS.full_name}
                    WHERE match_type = 'dedup'
                      AND match_status IN ('accepted', 'pending')
                    UNION
                    SELECT account_id,
                           source_type_b || chr(31)
                               || source_transaction_id_b,
                           source_type_a || chr(31)
                               || source_transaction_id_a
                    FROM {MATCH_DECISIONS.full_name}
                    WHERE match_type = 'dedup'
                      AND match_status IN ('accepted', 'pending')
                ),
                -- Transitive closure, then each node labelled by the smallest
                -- node it can reach. Two nodes share a label exactly when
                -- union-find would put them in one component.
                reach AS (
                    SELECT acct, n1 AS src, n1 AS dst FROM dedup_edges
                    UNION
                    SELECT e.acct, r.src, e.n2
                    FROM reach AS r
                    JOIN dedup_edges AS e
                      ON e.acct = r.acct AND e.n1 = r.dst
                ),
                component AS (
                    SELECT acct, src AS node, MIN(dst) AS comp
                    FROM reach
                    GROUP BY acct, src
                ),
                rejected_pairs AS (
                    SELECT account_id AS acct,
                           source_transaction_id_a AS stid_a,
                           source_type_a AS stype_a,
                           source_transaction_id_b AS stid_b,
                           source_type_b AS stype_b
                    FROM {MATCH_DECISIONS.full_name}
                    WHERE match_type = 'dedup' AND match_status = 'rejected'
                    UNION
                    SELECT account_id,
                           source_transaction_id_b,
                           source_type_b,
                           source_transaction_id_a,
                           source_type_a
                    FROM {MATCH_DECISIONS.full_name}
                    WHERE match_type = 'dedup' AND match_status = 'rejected'
                ),
                -- Each endpoint's component, or the endpoint itself when it is
                -- in none -- a lone row is a component of one.
                resolved AS (
                    SELECT c.*,
                           COALESCE(
                               ca.comp, c.type_a || chr(31) || c.id_a
                           ) AS comp_a,
                           COALESCE(
                               cb.comp, c.type_b || chr(31) || c.id_b
                           ) AS comp_b
                    FROM candidates AS c
                    LEFT JOIN component AS ca
                      ON ca.acct = c.account_id
                     AND ca.node = c.type_a || chr(31) || c.id_a
                    LEFT JOIN component AS cb
                      ON cb.acct = c.account_id
                     AND cb.node = c.type_b || chr(31) || c.id_b
                ),
                -- The matcher's candidate list, which a rejected pair never
                -- reaches: scoring.py skips it before appending to `results`,
                -- and everything downstream reads that list. So the filter
                -- belongs here, ahead of both consumers, not only in the final
                -- SELECT -- a rejected row left in would lend its file to
                -- component_sources below and let the cardinality guard drop a
                -- live pair the matcher would propose.
                live_candidates AS (
                    SELECT r.*
                    FROM resolved AS r
                    WHERE NOT EXISTS (
                        SELECT 1 FROM rejected_pairs AS rp
                        WHERE rp.acct = r.account_id
                          AND rp.stid_a = r.id_a
                          AND rp.stype_a = r.type_a
                          AND rp.stid_b = r.id_b
                          AND rp.stype_b = r.type_b
                    )
                ),
                -- Physical sources per component, for the cardinality guard
                -- below -- registered from candidate *endpoints* only, which is
                -- what assign_components does: it walks this run's candidate
                -- list, so a node no candidate names contributes nothing and
                -- never blocks (assignment.py, "seed-only nodes"). Reading a
                -- component's other members instead would let a row the matcher
                -- never looks at supply the shared file that suppresses a live
                -- pair -- silence in the check whose whole job is to break it.
                -- A component of one carries its own file in via resolved's
                -- COALESCE. NULL source_file imposes no constraint, matching
                -- the Python. The registration set now matches the matcher's:
                -- both dedup tiers pass excluded_ids=None (engine.py) and
                -- neither the blocking SQL nor the scoring loop applies a
                -- score cutoff, so every surviving blocking pair registers.
                component_sources AS (
                    SELECT account_id AS acct,
                           comp_a AS comp,
                           type_a AS s_type,
                           origin_a AS s_origin,
                           file_a AS s_file
                    FROM live_candidates
                    WHERE file_a IS NOT NULL
                    UNION
                    SELECT account_id, comp_b, type_b, origin_b, file_b
                    FROM live_candidates
                    WHERE file_b IS NOT NULL
                )
                -- The two clauses below are assign_components' two skips, in
                -- its own order (assignment.py) -- but read from *persisted*
                -- decisions only. assign_components additionally unions
                -- candidates as it walks them inside one run, which nothing here
                -- can see: a cluster of N mutually-duplicate rows with no prior
                -- decision has no edges yet, so all N*(N-1)/2 pairs survive
                -- while the matcher will persist N-1. The count is therefore an
                -- upper bound on the proposals a rematch produces, and the
                -- detail below says so rather than promising one each. It is
                -- not a false positive -- those pairs genuinely have no decision
                -- -- and the next doctor run, reading the now-persisted edges,
                -- reports the settled number.
                --
                -- Both clauses are component-grain, not
                -- node-grain: two endpoints carrying *unrelated* decisions in
                -- disjoint components are still a live candidate the matcher
                -- would evaluate, and suppressing those hides exactly the
                -- unproposed pair this check exists to find.
                --
                -- 1. find(a) == find(b) -- the redundant edge inside one
                --    component, already spoken for by the pairing that won.
                -- 2. sources_a & sources_b -- the cardinality guard. Two rows
                --    of one import file are distinct transactions, so an edge
                --    joining their components is refused. Omitting this warns
                --    about a pair no refresh can ever clear, since the remedy
                --    this check recommends is the very pass that re-drops it.
                --    Origin compares NULL-equal because the Python intersects
                --    SourceKey *tuples*, and (type, None, file) equals itself
                --    where SQL '=' reads unknown. V003 leaves legacy OFX rows'
                --    source_origin NULL, so plain '=' would diverge -- today
                --    unobservably, because app.match_decisions demands a
                --    non-NULL origin and so keeps every such row a component
                --    of one. That is another table's constraint, not this
                --    query's, which is why the mirror is written out here.
                SELECT r.account_id, COUNT(*) AS pairs
                FROM live_candidates AS r
                WHERE r.comp_a <> r.comp_b
                  AND NOT EXISTS (
                    SELECT 1
                    FROM component_sources AS sa
                    JOIN component_sources AS sb
                      ON sb.acct = sa.acct
                     AND sb.s_type = sa.s_type
                     AND sb.s_origin IS NOT DISTINCT FROM sa.s_origin
                     AND sb.s_file = sa.s_file
                    WHERE sa.acct = r.account_id
                      AND sa.comp = r.comp_a
                      AND sb.comp = r.comp_b
                )
                GROUP BY r.account_id
                ORDER BY r.account_id
                """,  # noqa: S608 — TableRef constants, parameterized values
                [settings.matching.date_window_days],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — prep views absent before first transform
            # `detail` is returned verbatim by doctor and system_status over
            # both surfaces, and this query joins on amounts, dates, and
            # descriptions — so a conversion failure can carry a user's row in
            # its message. Same split as `refresh._step_error`: the frame chain
            # to the local log, a fixed string on the wire. The frame chain
            # rather than the message for the reason `exception_origin`
            # documents — a traceback's last line *is* the message, and
            # AGENTS.md's no-financial-data rule has no local-log carve-out.
            logger.error(
                f"{name} skipped — matcher input unavailable at "
                f"{exception_origin(e.__cause__ or e)}"
            )
            return InvariantResult(
                name=name,
                status="skipped",
                detail="matcher input unavailable — the cause is in the local log",
                affected_ids=[],
            )
        if rows:
            total = sum(int(pairs) for _, pairs in rows)
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"up to {total} cross-source transaction pair(s) across "
                    f"{len(rows)} account(s) match on amount and date but have "
                    "never been considered by the matcher — neither side "
                    "carries a match decision. This is what an accepted "
                    "account link leaves behind when nothing re-runs matching "
                    "afterwards: the rows became comparable, but no pass ever "
                    "looked. The figure is an upper bound — three mutually "
                    "duplicate rows count as three pairs here and become two "
                    "proposals, because the matcher links them into one "
                    "component as it goes. Run `moneybin refresh --step match "
                    "--step transform` to propose them, then "
                    "`moneybin review --type matches` to decide. Note that "
                    "`doctor`'s dedup reconciliation cannot see this — its "
                    "staging counts balance whether or not a duplicate was "
                    "ever proposed"
                ),
                affected_ids=[
                    f"{account_id} (up to {pairs} unreviewed "
                    f"pair{'' if pairs == 1 else 's'})"
                    for account_id, pairs in rows
                ],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_currency_integrity(self) -> InvariantResult:
        """Report unknown-currency rows, then merely-mixed currency.

        multi-currency.md Requirement 6. Reports segment per ``currency_code``
        rather than blending (Requirement 5), so neither condition corrupts a
        figure — but both change what the user can be shown, and only one of
        them is fixable:

        - **fail** — a row or account whose currency is ``NULL``. Its amount has
          no unit, so it can never join a total. The user assigns one with
          ``accounts set --currency``.
        - **warn** — two or more known currencies and nothing unknown. Legal,
          but every cross-currency total is withheld until conversion ships
          (M1K.2), which is worth saying out loud rather than leaving the user
          to wonder why net worth reads as segments.
        """
        name = "currency_integrity"
        # (table, id column) — the grains that carry a monetary amount. Balances
        # is a view over the others and has no stable row id, so it contributes
        # to the counts without naming ids.
        try:
            unknown_transactions = [
                str(row[0])
                for row in self._db.execute(
                    f"""
                    SELECT transaction_id
                    FROM {FCT_TRANSACTIONS.full_name}
                    WHERE currency_code IS NULL
                    ORDER BY transaction_id
                    LIMIT 100
                    """  # noqa: S608 — TableRef constant, not user input
                ).fetchall()
            ]
            unknown_accounts = [
                str(row[0])
                for row in self._db.execute(
                    f"""
                    SELECT account_id
                    FROM {DIM_ACCOUNTS.full_name}
                    WHERE currency_code IS NULL
                    ORDER BY account_id
                    LIMIT 100
                    """  # noqa: S608 — TableRef constant, not user input
                ).fetchall()
            ]
            currencies = [
                str(row[0])
                for row in self._db.execute(
                    f"""
                    SELECT DISTINCT currency_code FROM (
                        SELECT currency_code FROM {FCT_TRANSACTIONS.full_name}
                        UNION ALL
                        SELECT currency_code FROM {DIM_ACCOUNTS.full_name}
                        UNION ALL
                        SELECT currency_code FROM {FCT_BALANCES.full_name}
                    )
                    WHERE currency_code IS NOT NULL
                    ORDER BY currency_code
                    """  # noqa: S608 — TableRef constants, not user input
                ).fetchall()
            ]
            unknown_balances = self._scalar_int(
                f"""
                SELECT COUNT(*) FROM {FCT_BALANCES.full_name}
                WHERE currency_code IS NULL
                """  # noqa: S608 — TableRef constant, not user input
            )
            # Counted separately from the id lists above, which are capped at
            # 100 so the envelope stays bounded. Deriving the count from
            # len(ids) would saturate at 100 and hide the real backlog — the
            # user would fix rows and see the same number every run.
            unknown_transaction_count = self._scalar_int(
                f"""
                SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}
                WHERE currency_code IS NULL
                """  # noqa: S608 — TableRef constant, not user input
            )
            unknown_account_count = self._scalar_int(
                f"""
                SELECT COUNT(*) FROM {DIM_ACCOUNTS.full_name}
                WHERE currency_code IS NULL
                """  # noqa: S608 — TableRef constant, not user input
            )
        except Exception as e:  # noqa: BLE001 — degrade gracefully; surface cause at DEBUG
            logger.debug(f"currency_integrity skipped: {e}", exc_info=True)
            return InvariantResult(
                name=name,
                status="skipped",
                detail="core layer not available; run transform first",
                affected_ids=[],
            )

        PROFILE_CURRENCIES.set(len(currencies))
        UNKNOWN_CURRENCY_ROWS.labels(grain="accounts").set(unknown_account_count)
        UNKNOWN_CURRENCY_ROWS.labels(grain="transactions").set(
            unknown_transaction_count
        )
        UNKNOWN_CURRENCY_ROWS.labels(grain="balances").set(unknown_balances)

        unknown_total = (
            unknown_transaction_count + unknown_account_count + unknown_balances
        )
        if unknown_total:
            parts: list[str] = []
            if unknown_account_count:
                parts.append(f"{unknown_account_count} account(s)")
            if unknown_transaction_count:
                parts.append(f"{unknown_transaction_count} transaction(s)")
            if unknown_balances:
                parts.append(f"{unknown_balances} balance observation(s)")
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{', '.join(parts)} have an unknown currency. Their amounts "
                    "are segmented out of every total until you assign one — "
                    "run `moneybin accounts set <account> --currency <ISO 4217>`, "
                    "then `moneybin transform`: the setting is app state, and "
                    "core.* only picks it up on the next transform, so this check "
                    "keeps failing until you re-run one. "
                    "MoneyBin never guesses a currency, because a wrong guess "
                    "would silently blend into a figure nothing could flag."
                ),
                # Prefixed by grain, matching orphan_app_state's note:/tag:
                # convention: the two id kinds need different fixes, and a
                # recipe reading a bare mixed list cannot tell which is which
                # without re-querying.
                affected_ids=[
                    *(f"account:{account_id}" for account_id in unknown_accounts),
                    *(
                        f"transaction:{transaction_id}"
                        for transaction_id in unknown_transactions
                    ),
                ],
            )
        if len(currencies) > 1:
            return InvariantResult(
                name=name,
                status="warn",
                detail=(
                    f"This profile holds {len(currencies)} currencies "
                    f"({', '.join(currencies)}). Reports sub-total each currency "
                    "separately and withhold any combined figure. A transaction "
                    "denominated differently from its account is also left out of "
                    "that account's carried daily balance — it cannot be added "
                    "without a rate — and shows up as the account's "
                    "reconciliation drift in `moneybin reports balance_drift`. "
                    "Conversion to a single display currency is not built yet."
                ),
                affected_ids=[],
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_categorization_coverage(self) -> InvariantResult:
        """Warn (not fail) when <50% of non-transfer transactions are categorized."""
        try:
            row = self._db.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE category IS NULL) AS uncategorized,
                    COUNT(*) AS total
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE NOT COALESCE(is_transfer, FALSE)
                """  # noqa: S608 — TableRef constant, not user input
            ).fetchone()
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            return InvariantResult(
                name="categorization_coverage",
                status="skipped",
                detail="fct_transactions not available",
                affected_ids=[],
            )
        if not row or row[1] == 0:
            return InvariantResult(
                name="categorization_coverage",
                status="pass",
                detail=None,
                affected_ids=[],
            )
        uncategorized, total = int(row[0]), int(row[1])
        # Use unrounded ratio for the threshold so values like 49.6% categorized
        # correctly trigger the warning instead of rounding up to 50 and passing.
        pct_categorized = (total - uncategorized) / total * 100
        if pct_categorized < 50:
            pct_uncategorized = round(uncategorized / total * 100)
            return InvariantResult(
                name="categorization_coverage",
                status="warn",
                detail=f"{pct_uncategorized}% of non-transfer transactions are uncategorized",
                affected_ids=[],
            )
        return InvariantResult(
            name="categorization_coverage",
            status="pass",
            detail=None,
            affected_ids=[],
        )
