"""GSheetConnectionService — connect, list, get, disconnect, reconnect.

Orchestrates the connect flow: parse URL, fetch workbook metadata, run the
chosen adapter's detection, persist via ``GSheetConnectionsRepo``, then
optionally fire the initial pull (delegated to ``GSheetPullService`` via
late import to avoid the circular dependency).

Disconnect has two modes: soft (status='disconnected', raw rows retained
for analytics) and purge (drop seed view, wipe raw rows, hard-delete the
connection row).
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl
from sqlglot import exp

from moneybin.config import get_settings
from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.errors import (
    GSheetError,
    GSheetSignConfirmationRequiredError,
    GSheetUnreachableError,
)
from moneybin.connectors.gsheet.sheets_api import SheetsAPI
from moneybin.connectors.gsheet.url_parser import parse_sheet_url
from moneybin.database import Database
from moneybin.extractors.confidence import tier_for
from moneybin.extractors.tabular.formats import SignConventionType
from moneybin.metrics.registry import (
    IMPORT_CONFIRMATIONS_TOTAL,
    IMPORT_DETECTION_SCORE,
    IMPORT_OVERRIDE_TOTAL,
)
from moneybin.repositories.gsheet_connections_repo import GSheetConnectionsRepo
from moneybin.services.import_confirmation import (
    MappingValidationError,
    validate_partial_mapping,
)
from moneybin.tables import GSHEET_SEEDS, TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)

# Defense-in-depth: disconnect(purge=True) re-validates the alias before
# string-interpolating it into DROP VIEW. This pattern is intentionally
# LOOSER than ``view_generator._SAFE_ALIAS_RE`` (≤56 chars): it enforces
# SQL-safety (chars + DuckDB's 63-char identifier limit) so legacy long
# aliases — created before view_generator tightened to 56 chars — can
# still be purged cleanly. Create-time validation lives in
# view_generator; this re-check is purely about safe interpolation.
_SAFE_ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class LowConfidenceError(GSheetError):
    """Transactions adapter returned low confidence and no override was given."""


class AmbiguousDetectionError(GSheetError):
    """Detection returned medium confidence and user has not accepted with --yes."""


@dataclass
class ConnectionRequest:
    """Inputs for ``GSheetConnectionService.connect``."""

    url: str
    adapter: str | None = None
    alias: str | None = None
    account_name: str | None = None
    account_id: str | None = None
    column_mapping: dict[str, str] | None = None
    sign: SignConventionType | None = None
    # Internal MCP retry signal after human elicitation; never persisted.
    human_sign_confirmation: bool = False
    yes: bool = False
    accept_seed_fallback: bool = False
    no_initial_pull: bool = False


@dataclass
class ConnectResult:
    """Outputs of ``GSheetConnectionService.connect``.

    ``initial_pull_status`` and ``initial_pull_error`` surface auto-pull
    failures that previously got swallowed when callers only retained
    ``.load_result``. The connection is persisted regardless — callers
    can inspect the pull state and decide whether to retry.
    """

    connection: GSheetConnection
    detection: DetectionResult
    initial_pull: LoadResult | None
    initial_pull_status: str | None = None
    initial_pull_error: str | None = None


@dataclass(frozen=True)
class GSheetPurgePlan:
    """Exact live state whose deletion requires confirmation."""

    connection_id: str
    connection_before_state: dict[str, Any]
    raw_before_state: tuple[dict[str, Any], ...]
    blast_radius: dict[str, int]


def _inferred_sign_evidence_header(detection: DetectionResult) -> str | None:
    """Return the exact mapped amount header behind an inferred inversion."""
    if detection.sign_convention != "negative_is_income":
        return None
    return next(
        (
            source_header
            for source_header, destination in detection.column_mapping.items()
            if destination == "amount"
        ),
        None,
    )


def _resolve_transactions_sign_convention(
    *,
    detection: DetectionResult,
    column_mapping: dict[str, str],
    explicit_sign: SignConventionType | None,
) -> tuple[str | None, str | None]:
    """Resolve polarity from the final mapping shape and detected source role."""
    dest_to_src = {dest: src for src, dest in column_mapping.items()}
    has_split = (
        "debit_amount" in dest_to_src
        and "credit_amount" in dest_to_src
        and "amount" not in dest_to_src
    )
    if has_split:
        if explicit_sign is not None and explicit_sign != "split_debit_credit":
            raise GSheetError(
                f"--sign={explicit_sign!r} contradicts the resolved split "
                "debit/credit mapping. Drop --sign (the shape implies "
                "split_debit_credit) or change the column mapping to a "
                "single amount column."
            )
        return "split_debit_credit", None

    if explicit_sign == "split_debit_credit":
        raise GSheetError(
            "--sign='split_debit_credit' contradicts the resolved single "
            "amount mapping. Drop --sign (the shape requires a single-column "
            "convention) or map both debit_amount and credit_amount."
        )

    if explicit_sign is not None:
        return explicit_sign, None

    if detection.sign_convention != "split_debit_credit":
        selected_source = dest_to_src.get("amount")
        detected_source = next(
            (
                source_header
                for source_header, destination in detection.column_mapping.items()
                if destination == "amount"
            ),
            None,
        )
        if selected_source != detected_source:
            source_label = (
                repr(selected_source) if selected_source is not None else "None"
            )
            raise GSheetError(
                "The mapping replaces the detected amount source with "
                f"{source_label}, so MoneyBin cannot reuse the discarded "
                "column's inferred polarity. Re-run with --sign "
                "negative_is_expense or --sign negative_is_income; nothing "
                "was saved or pulled."
            )
        return detection.sign_convention, _inferred_sign_evidence_header(detection)

    selected_source = dest_to_src.get("amount")
    selected_role = (
        detection.column_mapping.get(selected_source) if selected_source else None
    )
    if selected_role == "debit_amount":
        # The split transform records positive Debit cells as negative expenses.
        # A single-column transform needs inversion to preserve that polarity.
        return "negative_is_income", selected_source
    if selected_role == "credit_amount":
        # The split transform records positive Credit cells as positive income,
        # which is already the native single-column convention.
        return "negative_is_expense", None

    source_label = repr(selected_source) if selected_source is not None else "None"
    raise GSheetError(
        "The mapping selects single amount source "
        f"{source_label} from a detected split debit/credit layout, but MoneyBin "
        "cannot derive that source's polarity. Re-run with --sign "
        "negative_is_expense or --sign negative_is_income; nothing was saved "
        "or pulled."
    )


def _require_inferred_sign_confirmation(
    *,
    resolved_convention: str | None,
    sign_was_explicit: bool,
    evidence_header: str | None,
    human_sign_confirmation: bool,
) -> None:
    """Gate an inferred whole-ledger inversion on a human-owned signal."""
    if (
        resolved_convention != "negative_is_income"
        or sign_was_explicit
        or human_sign_confirmation
    ):
        return
    if evidence_header is None:
        raise RuntimeError("negative_is_income inference is missing header evidence")
    raise GSheetSignConfirmationRequiredError(
        proposed_convention="negative_is_income",
        evidence_header=evidence_header,
    )


class GSheetConnectionService:
    """Lifecycle owner for ``app.gsheet_connections`` rows."""

    def __init__(
        self,
        *,
        db: Database,
        sheets_client: SheetsAPI,
        oauth_client: Any,
    ) -> None:
        """Bind to a database, a SheetsAPI implementation, and an OAuth client."""
        self._db = db
        self._sheets = sheets_client
        self._oauth = oauth_client
        self._repo = GSheetConnectionsRepo(db)

    def connect(self, req: ConnectionRequest, *, actor: str = "cli") -> ConnectResult:
        """Detect, persist, and optionally pull the initial snapshot."""
        try:
            spreadsheet_id, gid = parse_sheet_url(req.url)
        except ValueError as exc:
            raise GSheetError(f"Invalid Google Sheets URL: {exc}") from exc
        self._repo.assert_not_export_destination(spreadsheet_id)
        if not self._oauth.is_authorized(require_write=False):
            self._oauth.authorize(require_write=False)
        meta = self._sheets.get_workbook_metadata(spreadsheet_id, require_write=False)
        sheet = next((s for s in meta.sheets if s.gid == gid), None)
        if sheet is None:
            # Use the workbook title, not spreadsheet_id: the raw id uniquely
            # identifies a private document and aids phishing if it leaks to
            # the user-facing error / MCP envelope.
            raise GSheetUnreachableError(
                f"gid={gid} not found in workbook {meta.title!r}"
            )

        rows = self._sheets.read_sheet_values(
            spreadsheet_id, sheet.name, require_write=False
        )
        if not rows:
            raise GSheetError("Sheet has no data")

        df = rows_to_df(rows)

        # Adapter selection: explicit override wins; None tries transactions first.
        target_adapter = req.adapter or "transactions"
        if target_adapter not in ADAPTERS:
            raise GSheetError(
                f"Unknown adapter: {target_adapter!r}. "
                f"Valid options: {sorted(ADAPTERS)}"
            )
        adapter = ADAPTERS[target_adapter]
        detection = adapter.detect(df, account_name=req.account_name)

        # Derive tier from normalized score + shared confidence bands.
        if target_adapter == "transactions":
            bands = get_settings().import_.confidence
            tier = tier_for(detection.score, t_high=bands.t_high, t_med=bands.t_med)
        else:
            tier = detection.confidence  # seed adapter uses the categorical value

        # Fall-through: auto-detect → low-confidence transactions → maybe seed.
        if (
            target_adapter == "transactions"
            and tier == "low"
            and req.column_mapping is None
        ):
            if req.adapter is None and req.accept_seed_fallback:
                target_adapter = "seed"
                adapter = ADAPTERS["seed"]
                detection = adapter.detect(df, account_name=None)
                tier = detection.confidence
            else:
                IMPORT_DETECTION_SCORE.observe(detection.score)
                IMPORT_CONFIRMATIONS_TOTAL.labels(
                    channel="gsheet", tier=tier, outcome="declined"
                ).inc()
                raise LowConfidenceError(
                    "Low-confidence transactions detection. "
                    "Provide --column-mapping or retry with "
                    "--adapter=seed --alias=<name>."
                )

        # Medium confidence: ambiguous column matches. Require explicit
        # acceptance (--yes) or an override (--column-mapping) before
        # persisting — otherwise wrong mappings can land silently and
        # corrupt the initial pull.
        if (
            target_adapter == "transactions"
            and tier == "medium"
            and req.column_mapping is None
            and not req.yes
        ):
            IMPORT_DETECTION_SCORE.observe(detection.score)
            IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="gsheet", tier=tier, outcome="declined"
            ).inc()
            raise AmbiguousDetectionError(
                "Medium-confidence transactions detection. "
                "Re-run with --yes to accept the inferred mapping, "
                "or pass --column-mapping to override."
            )

        if target_adapter == "seed" and not req.alias:
            raise GSheetError(
                "--alias=<slug> is required when --adapter=seed. "
                "Pick a short identifier; it becomes the view name "
                "raw.gsheet_<alias>."
            )

        # TransactionsAdapter.transform requires account_id (see transactions.py).
        # Persisting without one creates a row that fails every pull. Accept
        # account_name as a free-text alias and resolve to the canonical id
        # at the service boundary (identifiers.md Guard 2 — bind filters to
        # the id; resolve free-text at the boundary).
        resolved_account_id: str | None = req.account_id
        if target_adapter == "transactions" and not resolved_account_id:
            if req.account_name:
                from moneybin.services.account_service import (  # noqa: PLC0415
                    AccountService,
                )

                # resolve_strict accepts an account_id or a display_name and
                # raises AccountNotFoundError / AmbiguousAccountError (both
                # UserError subclasses, surface cleanly via the MCP/CLI
                # boundary handlers).
                resolved_account_id = AccountService(self._db).resolve_strict(
                    req.account_name
                )
            else:
                raise GSheetError(
                    "--account-id or --account-name is required for the "
                    "transactions adapter. Pass --account-name=<display> "
                    "(resolved via dim_accounts) or "
                    "--account-id=<dim_accounts.account_id>."
                )

        # For seed adapter the column_mapping field holds inferred typed_columns
        # (the raw_seed adapter reuses the field for its typed view).
        # For transactions, merge the user-supplied override onto detection and
        # validate the merged result. The override is partial: only the
        # destination fields the user names are replaced; others fall back to
        # the detector's proposal (partial-merge per spec Req 6).
        # sign_convention_for_save is overwritten by the override path when the
        # merged mapping is split debit/credit; defaults to detection otherwise.
        sign_convention_for_save = detection.sign_convention
        sign_evidence_header: str | None = None
        if target_adapter == "seed":
            column_mapping = detection.typed_columns
        elif target_adapter == "transactions":
            # detection.column_mapping is source→dest; validate_partial_mapping
            # expects dest→source for both proposed and override.
            proposed_dest_to_src = {
                dest: src for src, dest in detection.column_mapping.items()
            }
            override_dest_to_src: dict[str, str] = {}
            if req.column_mapping:
                override_dest_to_src = {
                    dest: src for src, dest in req.column_mapping.items()
                }
            # Required-amount shape derives from the MERGED dest set so a
            # user override can swap a split debit/credit detection to a
            # single ``amount`` column (or vice versa). The transactions
            # adapter shares ``map_columns`` with tabular and can therefore
            # produce a ``debit_amount``+``credit_amount`` proposal that
            # satisfies the score-1.0 path without a literal ``amount``.
            from moneybin.extractors.tabular.field_aliases import FIELD_ALIASES

            # Pre-compute the effective amount-shape the same way
            # validate_partial_mapping will resolve it, so the required-
            # fields check agrees with the override-driven shape change.
            # See _import_tabular for the equivalent tabular logic.
            override_has_amount_only = (
                "amount" in override_dest_to_src
                and "debit_amount" not in override_dest_to_src
                and "credit_amount" not in override_dest_to_src
            )
            override_has_split_only = (
                "amount" not in override_dest_to_src
                and "debit_amount" in override_dest_to_src
                and "credit_amount" in override_dest_to_src
            )
            proposed_is_split = (
                "debit_amount" in proposed_dest_to_src
                and "credit_amount" in proposed_dest_to_src
                and "amount" not in proposed_dest_to_src
            )
            if override_has_amount_only:
                required_for_amount: tuple[str, ...] = ("amount",)
            elif override_has_split_only:
                required_for_amount = ("debit_amount", "credit_amount")
            elif proposed_is_split:
                required_for_amount = ("debit_amount", "credit_amount")
            else:
                required_for_amount = ("amount",)
            required_fields_dynamic = ("transaction_date", *required_for_amount)
            try:
                merged_dest_to_src = validate_partial_mapping(
                    proposed=proposed_dest_to_src,
                    override=override_dest_to_src,
                    available_columns=tuple(df.columns),
                    required_fields=required_fields_dynamic,
                    valid_destinations=tuple(FIELD_ALIASES.keys()),
                )
            except MappingValidationError as e:
                raise GSheetError(str(e)) from e
            # Invert back to source→dest for storage.
            column_mapping = {src: dest for dest, src in merged_dest_to_src.items()}
            (
                sign_convention_for_save,
                sign_evidence_header,
            ) = _resolve_transactions_sign_convention(
                detection=detection,
                column_mapping=column_mapping,
                explicit_sign=req.sign,
            )
            IMPORT_DETECTION_SCORE.observe(detection.score)
            if req.column_mapping:
                IMPORT_OVERRIDE_TOTAL.labels(channel="gsheet").inc()
        else:
            column_mapping = detection.column_mapping

        _require_inferred_sign_confirmation(
            resolved_convention=sign_convention_for_save,
            sign_was_explicit=req.sign is not None,
            evidence_header=sign_evidence_header,
            human_sign_confirmation=req.human_sign_confirmation,
        )

        if target_adapter == "transactions":
            outcome = "overridden" if req.column_mapping else "accepted"
            IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="gsheet",
                tier=tier,
                outcome=outcome,
            ).inc()

        connection_id = self._repo.insert(
            spreadsheet_id=spreadsheet_id,
            sheet_gid=gid,
            sheet_name=sheet.name,
            workbook_name=meta.title,
            adapter=target_adapter,
            alias=req.alias,
            account_id=resolved_account_id,
            account_name=req.account_name,
            column_mapping=column_mapping,
            header_signature=detection.header_signature,
            date_format=detection.date_format,
            sign_convention=sign_convention_for_save,
            number_format=detection.number_format,
            skip_rows=detection.skip_rows,
            skip_trailing_patterns=detection.skip_trailing_patterns or None,
            actor=actor,
        )
        stored = self._repo.get(connection_id)
        if stored is None:
            raise RuntimeError(
                f"insert succeeded but get returned None: {connection_id}"
            )
        connection = row_to_connection(stored)

        initial_pull: LoadResult | None = None
        initial_pull_status: str | None = None
        initial_pull_error: str | None = None
        if not req.no_initial_pull:
            # Late import — Task 21 (pull_service) imports helpers from here.
            from moneybin.connectors.gsheet.pull_service import GSheetPullService

            pull_svc = GSheetPullService(
                db=self._db,
                sheets_client=self._sheets,
                oauth_client=self._oauth,
            )
            pull = pull_svc.pull_connection(connection_id)
            initial_pull = pull.load_result
            initial_pull_status = pull.status
            initial_pull_error = pull.error_message
            # Refresh the connection state after the pull updated counters.
            stored = self._repo.get(connection_id)
            if stored is None:
                raise RuntimeError(f"connection vanished mid-pull: {connection_id}")
            connection = row_to_connection(stored)

        logger.info(
            f"gsheet connect: connection_id={connection_id} "
            f"adapter={target_adapter} initial_pull_status={initial_pull_status}"
        )
        return ConnectResult(
            connection=connection,
            detection=detection,
            initial_pull=initial_pull,
            initial_pull_status=initial_pull_status,
            initial_pull_error=initial_pull_error,
        )

    def list_connections(self) -> list[GSheetConnection]:
        """Return every connection, audited reads."""
        return [row_to_connection(r) for r in self._repo.list_all()]

    def get(self, connection_id: str) -> GSheetConnection | None:
        """Return one connection by id, or None."""
        row = self._repo.get(connection_id)
        return row_to_connection(row) if row else None

    def disconnect(
        self, connection_id: str, *, purge: bool = False, actor: str = "cli"
    ) -> None:
        """Soft-disconnect (default) or purge raw rows + delete row (purge=True)."""
        if not purge:
            self._repo.soft_disconnect(connection_id, actor=actor)
            return

        self.purge_confirmed(connection_id, verify=None, actor=actor)

    def _raw_before_state(self, conn: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        """Return every raw row owned by one connection in canonical order."""
        if conn["adapter"] == "seed":
            cursor = self._db.execute(
                f"""
                SELECT *
                FROM {GSHEET_SEEDS.full_name}
                WHERE connection_id = ?
                ORDER BY row_hash
                """,  # noqa: S608  # TableRef + parameterized value
                [conn["connection_id"]],
            )
        else:
            cursor = self._db.execute(
                f"""
                SELECT *
                FROM {TABULAR_TRANSACTIONS.full_name}
                WHERE source_origin = ?
                ORDER BY transaction_id, account_id, source_file
                """,  # noqa: S608  # TableRef + parameterized value
                [conn["connection_id"]],
            )
        columns = [str(column[0]) for column in cursor.description]
        return tuple(dict(zip(columns, row, strict=True)) for row in cursor.fetchall())

    def plan_purge(self, connection_id: str) -> GSheetPurgePlan:
        """Snapshot the complete connection/raw before-state for confirmation."""
        conn = self._repo.get(connection_id)
        if conn is None:
            raise GSheetError(f"Unknown connection: {connection_id}")
        raw_rows = self._raw_before_state(conn)
        return GSheetPurgePlan(
            connection_id=connection_id,
            connection_before_state=conn,
            raw_before_state=raw_rows,
            blast_radius={
                "connections": 1,
                "raw_rows": len(raw_rows),
                "views": int(conn["adapter"] == "seed" and bool(conn.get("alias"))),
            },
        )

    def purge_confirmed(
        self,
        connection_id: str,
        *,
        verify: Callable[[GSheetPurgePlan], None] | None,
        actor: str = "cli",
    ) -> None:
        """Revalidate a purge plan and apply it inside one transaction."""
        self._db.begin()
        try:
            live_plan = self.plan_purge(connection_id)
            if verify is not None:
                verify(live_plan)
            conn = live_plan.connection_before_state
            if conn["adapter"] == "seed":
                alias = conn.get("alias")
                if alias:
                    # Defense-in-depth: re-validate the alias before
                    # interpolating into DROP VIEW. The insert path
                    # already validated via view_generator, but a
                    # malformed alias on disk would otherwise land here
                    # unchecked.
                    if not _SAFE_ALIAS_RE.fullmatch(alias):
                        raise GSheetError(
                            f"Refusing to DROP VIEW for unsafe alias: {alias!r}"
                        )
                    # security.md: quote dynamic identifiers via sqlglot
                    # even after regex validation — defense in depth, the
                    # rule is explicit.
                    safe_view = exp.to_identifier(f"gsheet_{alias}", quoted=True).sql(
                        "duckdb"
                    )
                    self._db.execute(f"DROP VIEW IF EXISTS raw.{safe_view};")  # noqa: S608  # alias regex-validated + sqlglot-quoted
                self._db.execute(
                    f"DELETE FROM {GSHEET_SEEDS.full_name} WHERE connection_id = ?",  # noqa: S608  # TableRef + parameterized value
                    [connection_id],
                )
            else:
                self._db.execute(
                    f"DELETE FROM {TABULAR_TRANSACTIONS.full_name} WHERE source_origin = ?",  # noqa: S608  # TableRef + parameterized value
                    [connection_id],
                )

            self._repo.delete(connection_id, actor=actor, in_outer_txn=True)
            self._db.commit()
        except BaseException:
            # BaseException, not Exception: a KeyboardInterrupt/SystemExit between
            # the raw DELETEs and the repo delete must still roll back the open
            # transaction. Matches BaseRepo._transaction / MatchApplier._transaction.
            self._db.rollback()
            raise

    def reconnect(
        self,
        connection_id: str,
        *,
        yes: bool = False,
        sign: SignConventionType | None = None,
        human_sign_confirmation: bool = False,
        actor: str = "cli",
    ) -> ConnectResult:
        """Re-detect, re-pin, and pull.

        ``human_sign_confirmation`` is an internal MCP retry signal and is
        never persisted.
        """
        existing = self._repo.get(connection_id)
        if existing is None:
            raise GSheetError(f"Unknown connection: {connection_id}")

        # Resolve the current tab title by gid — sheet_name on the stored row
        # may be stale if the user renamed the tab between connect and reconnect.
        spreadsheet_id = existing["spreadsheet_id"]
        meta = self._sheets.get_workbook_metadata(spreadsheet_id, require_write=False)
        sheet = next((s for s in meta.sheets if s.gid == existing["sheet_gid"]), None)
        if sheet is None:
            # Workbook title, not spreadsheet_id — see connect() for why the
            # raw id must not surface in user-facing errors.
            raise GSheetUnreachableError(
                f"gid={existing['sheet_gid']} no longer present in workbook "
                f"{meta.title!r}; the tab was deleted"
            )
        rows = self._sheets.read_sheet_values(
            spreadsheet_id, sheet.name, require_write=False
        )
        if not rows:
            raise GSheetError("Sheet has no data")
        df = rows_to_df(rows)

        adapter = ADAPTERS[existing["adapter"]]
        detection = adapter.detect(df, account_name=existing.get("account_name"))
        if existing["adapter"] == "transactions":
            bands = get_settings().import_.confidence
            tier = tier_for(detection.score, t_high=bands.t_high, t_med=bands.t_med)
        else:
            tier = detection.confidence

        if existing["adapter"] == "transactions" and tier == "low":
            IMPORT_DETECTION_SCORE.observe(detection.score)
            IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="gsheet", tier=tier, outcome="declined"
            ).inc()
            raise LowConfidenceError(
                "Reconnect detection returned low confidence; "
                "the sheet structure may have changed substantially."
            )

        # Symmetric to connect(): a medium-confidence remap can silently
        # re-pin the wrong mapping, so require explicit acceptance via --yes.
        if existing["adapter"] == "transactions" and tier == "medium" and not yes:
            IMPORT_DETECTION_SCORE.observe(detection.score)
            IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="gsheet", tier=tier, outcome="declined"
            ).inc()
            raise AmbiguousDetectionError(
                "Reconnect detection returned medium confidence. "
                "Re-run with --yes to accept the inferred mapping."
            )

        column_mapping = (
            detection.typed_columns
            if existing["adapter"] == "seed"
            else detection.column_mapping
        )
        if existing["adapter"] == "transactions":
            IMPORT_DETECTION_SCORE.observe(detection.score)

        if existing["adapter"] == "transactions":
            (
                sign_convention_to_save,
                sign_evidence_header,
            ) = _resolve_transactions_sign_convention(
                detection=detection,
                column_mapping=column_mapping,
                explicit_sign=sign,
            )
        else:
            sign_convention_to_save = sign or detection.sign_convention
            sign_evidence_header = None

        _require_inferred_sign_confirmation(
            resolved_convention=sign_convention_to_save,
            sign_was_explicit=sign is not None,
            evidence_header=sign_evidence_header,
            human_sign_confirmation=human_sign_confirmation,
        )

        if existing["adapter"] == "transactions":
            IMPORT_CONFIRMATIONS_TOTAL.labels(
                channel="gsheet",
                tier=tier,
                outcome="accepted",
            ).inc()

        self._repo.update_mapping(
            connection_id,
            column_mapping=column_mapping,
            header_signature=detection.header_signature,
            date_format=detection.date_format,
            sign_convention=sign_convention_to_save,
            number_format=detection.number_format,
            skip_rows=detection.skip_rows,
            skip_trailing_patterns=detection.skip_trailing_patterns or None,
            actor=actor,
        )

        from moneybin.connectors.gsheet.pull_service import GSheetPullService

        pull_svc = GSheetPullService(
            db=self._db,
            sheets_client=self._sheets,
            oauth_client=self._oauth,
        )
        pull = pull_svc.pull_connection(connection_id)
        refreshed = self._repo.get(connection_id)
        if refreshed is None:
            raise RuntimeError(f"connection vanished mid-reconnect: {connection_id}")
        return ConnectResult(
            connection=row_to_connection(refreshed),
            detection=detection,
            initial_pull=pull.load_result,
            initial_pull_status=pull.status,
            initial_pull_error=pull.error_message,
        )


def rows_to_df(rows: list[list[str]]) -> pl.DataFrame:
    """Convert raw cell values (first row headers) into a Polars DataFrame.

    Ragged rows (Google Sheets trims trailing empty cells) are padded to the
    header width with ``None`` so polars receives uniform-length columns.

    Rejects duplicate header text — keying by header collapses duplicates
    into one dict entry and silently corrupts row cardinality.
    """
    if not rows:
        return pl.DataFrame()
    headers, *data = rows
    seen: set[str] = set()
    duplicates: list[str] = []
    for h in headers:
        if h in seen and h not in duplicates:
            duplicates.append(h)
        seen.add(h)
    if duplicates:
        raise GSheetError(
            f"Duplicate header(s) in sheet: {duplicates}. "
            "Rename to make headers unique before connecting."
        )
    columns: dict[str, list[str | None]] = {h: [] for h in headers}
    for row in data:
        for i, header in enumerate(headers):
            columns[header].append(row[i] if i < len(row) else None)
        # Extra columns past the header width have no header to bind to and
        # are dropped implicitly by the header-keyed loop above.
    return pl.DataFrame(columns)


def row_to_connection(row: dict[str, Any]) -> GSheetConnection:
    """Convert a ``GSheetConnectionsRepo.get`` row dict to a GSheetConnection.

    The repo decodes JSON columns and returns timestamps as ``datetime``
    objects; this helper stringifies the timestamps to match the
    ``GSheetConnection`` dataclass contract (``str | None``).
    """
    return GSheetConnection(
        connection_id=row["connection_id"],
        spreadsheet_id=row["spreadsheet_id"],
        sheet_gid=row["sheet_gid"],
        sheet_name=row["sheet_name"],
        workbook_name=row["workbook_name"],
        adapter=row["adapter"],
        alias=row.get("alias"),
        account_id=row.get("account_id"),
        account_name=row.get("account_name"),
        column_mapping=row.get("column_mapping") or {},
        header_signature=row.get("header_signature") or [],
        date_format=row.get("date_format"),
        sign_convention=row.get("sign_convention"),
        number_format=row.get("number_format"),
        skip_rows=row.get("skip_rows") or 0,
        skip_trailing_patterns=row.get("skip_trailing_patterns") or [],
        status=row["status"],
        last_pull_at=_to_iso(row.get("last_pull_at")),
        last_pull_import_id=row.get("last_pull_import_id"),
        last_success_at=_to_iso(row.get("last_success_at")),
        last_status_reason=row.get("last_status_reason"),
        consecutive_failure_count=row.get("consecutive_failure_count") or 0,
    )


def _to_iso(value: Any) -> str | None:
    """Stringify a datetime to ISO format; pass through None and strings."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
