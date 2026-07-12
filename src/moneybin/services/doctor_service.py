"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from sqlglot import exp

from moneybin.audits import recipes as recipe_registry
from moneybin.config import get_settings
from moneybin.database import Database, sqlmesh_context
from moneybin.errors import RecoveryAction
from moneybin.extractors.pdf.fingerprint import PAGE_BUCKETS, serialize_fingerprint
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
    FCT_INVESTMENT_TRANSACTIONS,
    FCT_TRANSACTIONS,
    GSHEET_CONNECTIONS,
    IMPORTS,
    INT_TRANSACTIONS_MATCHED,
    INT_TRANSACTIONS_UNIONED,
    LOT_SELECTIONS,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
    MATCH_DECISIONS,
    PDF_FORMATS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PROPOSED_RULES,
    SECURITIES,
    TABULAR_FORMATS,
    TRANSACTION_CATEGORIES,
    TRANSACTION_ID_ALIASES,
    TRANSACTION_NOTES,
    TRANSACTION_TAGS,
    USER_CATEGORIES,
    USER_MERCHANTS,
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

# Raw-interpolated SQL expressions allowed in `_run_app_audit_coverage`. Both
# `updated_expr` and `pk_expr` are spliced into SQL unsanitized (a multi-column
# expression can't be sqlglot-quoted as a single identifier), so each must be a
# hard-coded constant from these sets — a runtime guard that enforces the
# code-supplied-literal contract per `.claude/rules/security.md` (allowlist
# dynamic SQL), closing the door before a future caller passes a tainted value.
_ALLOWED_UPDATED_EXPRS = frozenset({"GREATEST(decided_at, reversed_at)"})
_ALLOWED_PK_EXPRS = frozenset({_BALANCE_ASSERTIONS_PK_EXPR})

_FINGERPRINT_KEYS = frozenset({"issuer", "headers", "page_bucket"})

# Shared by `_run_investment_unreported_holdings` and
# `_run_investment_phantom_holdings` — both need "the one whole snapshot per
# item with the latest extracted_at" (never "latest row per position": that
# would let a stale survivor from an earlier pull mask an omission in the
# newest one). Mirrors the identical CTE in `dim_holdings.sql`. No leading
# `WITH` — callers splice this into their own `WITH <this>, ...` clause.
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
        FROM (
            SELECT DISTINCT source_origin, source_file, extracted_at
            FROM prep.stg_plaid__investment_holdings
        )
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
    """Run pipeline integrity invariants and aggregate results."""

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
        categorization = self._run_categorization_coverage()
        app_integrity = self._run_app_integrity(full=full)
        orphan_app_state = self._run_orphan_app_state()
        investment_checks = [
            self._run_investment_staging_rejects(),
            self._run_opening_lot_review(),
            self._run_investment_unmodeled_legs(),
            self._run_holdings_snapshot_divergence(),
            self._run_investment_source_overlap(),
            self._run_investment_unresolved_securities(),
            self._run_investment_unreported_holdings(),
            self._run_investment_phantom_holdings(),
        ]
        raw_invariants = [
            *sqlmesh_results,
            dedup_reconciliation,
            categorization,
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

    def _run_app_integrity(self, *, full: bool) -> list[InvariantResult]:
        """Per-table integrity checks for protected ``app.*`` tables (Invariant 10).

        Covers every table wrapped in a repo so far (``user_categories``,
        ``category_overrides``, ``gsheet_connections``, ``user_merchants``,
        ``categorization_rules``, ``proposed_rules``, ``transaction_categories``,
        ``account_settings``, ``balance_assertions``, ``budgets``, plus the edge
        writers ``tabular_formats``, ``match_decisions``, ``imports``, the
        account-identity tables ``account_links``, ``account_link_decisions``,
        ``transaction_id_aliases``, and the investments tables ``securities``,
        ``lot_selections``); later repository PRs append one coverage call per
        newly-wrapped table plus that table's FK/orphan specifics.

        Tables without an ``updated_at`` column pass their natural watermark:
        ``proposed_rules`` → ``proposed_at``, ``transaction_categories`` →
        ``categorized_at``, ``match_decisions`` →
        ``GREATEST(decided_at, reversed_at)`` (the latest of any mutation, so a
        bypass insert, status update, *or* reversal is caught) — the
        ``account_links`` / ``account_link_decisions`` link tables use the same
        expression, while the append-only ``transaction_id_aliases`` uses
        ``created_at``. ``balance_assertions``
        has a composite PK, so it passes ``pk_expr`` to project the same composite
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
            self._run_app_audit_coverage(BUDGETS, "budget_id", full=full),
            self._run_app_audit_coverage(TABULAR_FORMATS, "name", full=full),
            self._run_app_audit_coverage(
                MATCH_DECISIONS,
                "match_id",
                updated_expr="GREATEST(decided_at, reversed_at)",
                full=full,
            ),
            self._run_app_audit_coverage(IMPORTS, "import_id", full=full),
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
            self._run_pdf_formats_recipe_validity(),
            self._run_pdf_formats_bounds(),
            self._run_pdf_formats_fingerprint_shape(),
            self._run_user_categories_uniqueness(),
            self._run_user_merchants_orphans(),
            self._run_proposed_rules_rule_fk(),
            self._run_transaction_categories_fk(),
            self._run_account_settings_account_fk(),
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
        ``*_repo.py``); content-based coverage is a hosted-tier follow-up (see
        ``private/followups.md``). ``pk_col`` and ``updated_col`` are
        code-supplied constants, quoted defensively per
        ``.claude/rules/security.md``.
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

        - ``note:<note_id>`` → one note per row (each note has its own PK)
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

        Every Plaid ``transfer``/``split`` row is excluded from the ledger with
        ``review_reason = 'split_underivable'`` (MoneyBin refuses to derive a
        possibly-wrong split multiplier) and every unmapped security-bearing
        subtype carries ``review_reason = 'unmapped_subtype'``. Both are
        deliberate refusals, not silent drops — this is where they surface.
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
        """
        name = "investment_unmodeled_legs"
        try:
            rows = self._db.execute(
                f"""
                SELECT investment_transaction_id
                FROM {FCT_INVESTMENT_TRANSACTIONS.full_name}
                WHERE provider_subtype IN (
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
        """Open lots MoneyBin holds that the broker's newest snapshot no longer reports.

        The mirror image of ``_run_investment_unreported_holdings``, and the
        more dangerous direction the OTHER way: MoneyBin claiming a position
        the broker's current data disagrees with OVERSTATES net worth,
        rather than understating it. ``core.dim_holdings.provider_reported_quantity``
        is already NULL exactly when the broker's newest snapshot omits a
        position MoneyBin holds a lot for (``dim_holdings.sql`` — "that NULL
        is itself the signal") — this check is what actually reads it.

        Scoped to accounts whose item's newest snapshot reports SOMETHING
        (any position, for that account) — the guard that keeps a stale or
        disconnected item, whose snapshot never arrived at all, from flooding
        this check with every position on the account as a false "phantom."
        Only a LIVE snapshot that positively omits the security counts.

        A row surfaced here is a lot the ledger never closed: an option
        assignment/exercise mapped to ``other`` with no ledger quantity (see
        ``_run_investment_unmodeled_legs``), an early sale the ledger never
        recorded, or a lot the engine otherwise failed to close.
        """
        name = "investment_phantom_holdings"
        try:
            rows = self._db.execute(
                f"""
                WITH {_NEWEST_HOLDINGS_SNAPSHOT_CTE},
                reported_accounts AS (
                    SELECT DISTINCT h.account_id
                    FROM prep.stg_plaid__investment_holdings AS h
                    JOIN newest_snapshot AS ns
                      ON ns.source_file = h.source_file
                      AND ns.source_origin = h.source_origin
                )
                SELECT d.account_id, d.security_id
                FROM {DIM_HOLDINGS.full_name} AS d
                JOIN reported_accounts AS ra ON ra.account_id = d.account_id
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
        """Discover and run all named SQLMesh standalone audits."""
        results: list[InvariantResult] = []
        try:
            with sqlmesh_context(self._db) as ctx:
                for name, audit in ctx.standalone_audits.items():
                    try:
                        sql = audit.render_audit_query().sql(dialect="duckdb")
                        # Audit SQL must return the violation entity ID in column 0.
                        rows = self._db.execute(sql).fetchall()  # noqa: S608 — rendered from trusted audit files
                    except Exception as e:  # noqa: BLE001 — per-audit isolation
                        results.append(
                            InvariantResult(
                                name=name,
                                status="skipped",
                                detail=f"audit failed: {e}",
                                affected_ids=[],
                            )
                        )
                        continue
                    if rows:
                        affected = [str(r[0]) for r in rows] if verbose else []
                        results.append(
                            InvariantResult(
                                name=name,
                                status="fail",
                                detail=f"{len(rows)} violation(s)",
                                affected_ids=affected,
                            )
                        )
                    else:
                        results.append(
                            InvariantResult(
                                name=name,
                                status="pass",
                                detail=None,
                                affected_ids=[],
                            )
                        )
        except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
            logger.warning(f"SQLMesh audit discovery failed: {e}")
            results.append(
                InvariantResult(
                    name="sqlmesh_audits_unavailable",
                    status="skipped",
                    detail=f"SQLMesh audit discovery failed: {e}",
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
