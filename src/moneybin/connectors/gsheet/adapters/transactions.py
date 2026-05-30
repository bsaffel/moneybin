"""TransactionsAdapter — strict Tiller-style adapter for `raw.tabular_transactions`.

Delegates detection + transformation to the existing tabular pipeline
(`moneybin.extractors.tabular`) and adds the gsheet-specific live-mirror
load contract: diff against currently-active rows, soft-delete missing,
upsert present, undelete returning rows.

`source_type='gsheet'` and `source_origin=<connection_id>` are stamped on
every row so cross-source dedup and audit downstream can scope to a single
connection.
"""

from __future__ import annotations

import logging
from typing import cast

import polars as pl

from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.diff import compute_diff
from moneybin.connectors.gsheet.drift import DriftReport, detect_drift
from moneybin.database import Database
from moneybin.extractors.tabular.column_mapper import map_columns
from moneybin.extractors.tabular.formats import (
    NumberFormatType,
    SignConventionType,
)
from moneybin.extractors.tabular.transforms import transform_dataframe
from moneybin.tables import TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)


_SOURCE_TYPE = "gsheet"

# transform_dataframe requires an import_id, but the real one is only known
# at load() time. transform() stamps this placeholder; load() overwrites it
# per-call. Named so a standalone transform() caller (e.g. a test inspecting
# transformed output) sees an obvious sentinel, not a magic string.
_IMPORT_ID_PLACEHOLDER = "__pending__"

# Dest fields the transform requires to produce a non-empty row. Two uses:
# (1) connect-time mapping validation (a mapping omitting these makes every
# pull load zero rows); (2) drift detection — these are the ONLY columns whose
# emptiness counts as drift. Optional columns (description, notes) are routinely
# blank in real exports and must not pin a connection in drift_detected forever.
# Defined here (the adapter owns the requirement); imported by connection_service.
#
# Amount has two shapes: a single ``amount`` column OR split
# ``debit_amount`` + ``credit_amount`` columns. ``REQUIRED_DEST_FIELDS`` lists
# the canonical single-column form (used at connect-time when no mapping
# exists yet); ``required_sources_for_mapping`` resolves the actual required
# source columns for a concrete mapping so a split-column connection isn't
# silently approved as "all required fields present" when only one of the
# split pair is empty.
REQUIRED_DEST_FIELDS = ("transaction_date", "amount")


def _is_null_or_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _split_pair_both_null_ratio(
    df: pl.DataFrame, debit_col: str, credit_col: str
) -> float:
    """Fraction of rows where BOTH split-amount sources are null/blank.

    A normal credit-card statement (every row in Debit, every row blank in
    Credit) returns 0.0 — Debit fills both halves of the pair. The pair
    drifts only when every row leaves *both* columns empty, which is the
    only state that actually breaks transform_dataframe.
    """
    if df.height == 0:
        return 0.0
    debit_vals = df[debit_col].cast(pl.String, strict=False).to_list()
    credit_vals = df[credit_col].cast(pl.String, strict=False).to_list()
    both_empty = sum(
        1
        for d, c in zip(debit_vals, credit_vals, strict=False)
        if _is_null_or_blank(d) and _is_null_or_blank(c)
    )
    return both_empty / df.height


def required_sources_for_mapping(column_mapping: dict[str, str]) -> set[str]:
    """Source columns whose emptiness counts as drift for this mapping.

    Always includes the source mapped to ``transaction_date``. For amount,
    accepts either a single ``amount`` mapping OR the ``debit_amount`` +
    ``credit_amount`` pair — whichever the connection was actually saved
    with. Mapping that satisfies neither returns an empty amount set
    (drift check still gates on transaction_date).
    """
    by_dest = {dest: src for src, dest in column_mapping.items()}
    required: set[str] = set()
    if "transaction_date" in by_dest:
        required.add(by_dest["transaction_date"])
    if "amount" in by_dest:
        required.add(by_dest["amount"])
    elif "debit_amount" in by_dest and "credit_amount" in by_dest:
        required.add(by_dest["debit_amount"])
        required.add(by_dest["credit_amount"])
    return required


class TransactionsAdapter:
    """Strict Tiller-style adapter targeting `raw.tabular_transactions`."""

    name: str = "transactions"

    def detect(
        self,
        df: pl.DataFrame,
        *,
        account_name: str | None,
    ) -> DetectionResult:
        """Detect the column mapping for a transactions-shaped sheet."""
        _ = account_name  # accepted for Protocol parity; unused by map_columns
        # MappingResult.confidence here is informational (forwarded onto
        # DetectionResult.confidence for display). The gsheet control-flow
        # path computes its own Confidence via to_confidence(bands) in
        # connection_service, so we don't import settings here — that would
        # trip the first-run wizard for CliRunner-driven tests that haven't
        # initialized a profile.
        mapping_result = map_columns(df)
        # MappingResult.field_mapping is dest_field → source_column; invert
        # to source_header → dest_field for the DetectionResult contract.
        column_mapping = {
            src: dest for dest, src in mapping_result.field_mapping.items()
        }
        return DetectionResult(
            confidence=mapping_result.confidence,
            column_mapping=column_mapping,
            header_signature=list(df.columns),
            date_format=mapping_result.date_format,
            sign_convention=mapping_result.sign_convention,
            number_format=mapping_result.number_format,
            skip_rows=0,
            skip_trailing_patterns=[],
            notes=[],
            score=mapping_result.score,
        )

    def check_drift(
        self,
        connection: GSheetConnection,
        current_df: pl.DataFrame,
    ) -> DriftReport:
        """Compare the current pull against the pinned header signature.

        Only the source columns required by this mapping's amount-shape gate
        drift on emptiness — a mostly-blank optional column (Description,
        Notes) is normal and must not trigger drift_detected.

        Split-amount handling: for a debit/credit split connection,
        transform_dataframe accepts rows where only ONE of debit_amount /
        credit_amount has a value (a normal credit-card statement has every
        row in Debit and zero rows in Credit). A naive per-column null
        ratio check would mark Credit as drifted in that shape. We instead
        pass the non-split required sources to ``detect_drift`` and run a
        row-level ``both null`` check for the split pair; the pair drifts
        only when every sampled row has neither debit nor credit populated.
        """
        by_dest = {dest: src for src, dest in connection.column_mapping.items()}
        non_split_required = required_sources_for_mapping(connection.column_mapping)
        split_pair: tuple[str, str] | None = None
        if (
            "debit_amount" in by_dest
            and "credit_amount" in by_dest
            and "amount" not in by_dest
        ):
            split_pair = (by_dest["debit_amount"], by_dest["credit_amount"])
            non_split_required = non_split_required - set(split_pair)
        report = detect_drift(
            pinned_signature=connection.header_signature,
            current_headers=list(current_df.columns),
            sample_df=current_df,
            mapped_columns=non_split_required,
        )
        if split_pair is None:
            return report
        debit_col, credit_col = split_pair
        if debit_col not in current_df.columns or credit_col not in current_df.columns:
            # detect_drift already flags missing headers from the pinned
            # signature, so neither extending nor short-circuiting is needed.
            return report
        if _split_pair_both_null_ratio(current_df, debit_col, credit_col) > 0.5:
            empties = sorted([*report.empty_mapped_columns, debit_col, credit_col])
            reason_parts = [report.reason] if report.reason != "no drift" else []
            reason_parts.append(
                f"split debit/credit pair {debit_col}+{credit_col} both empty"
            )
            from dataclasses import replace as _replace

            return _replace(
                report,
                is_drift=True,
                reason="; ".join(reason_parts),
                empty_mapped_columns=empties,
            )
        return report

    def transform(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
    ) -> pl.DataFrame:
        """Apply the pinned mapping + typed transforms; produce a load-ready frame.

        Returns the transformed DataFrame with `source_type='gsheet'` and
        `source_origin=connection.connection_id` stamped. The caller passes
        the resulting frame to `load()` along with the `import_id`.
        """
        if connection.account_id is None:
            raise ValueError(
                "TransactionsAdapter.transform requires connection.account_id; "
                "transactions adapter is single-account by design"
            )

        # Connection column_mapping is source_header → dest_field; invert to
        # dest_field → source_column for transform_dataframe.
        field_mapping = {dest: src for src, dest in connection.column_mapping.items()}

        # date_format / sign_convention / number_format are pinned at connect
        # time; transform_dataframe requires concrete values, so fall back to
        # safe defaults if the connection didn't pin them.
        date_format = connection.date_format or "%Y-%m-%d"
        sign_convention = cast(
            SignConventionType,
            connection.sign_convention or "negative_is_expense",
        )
        number_format = cast(
            NumberFormatType,
            connection.number_format or "us",
        )

        result = transform_dataframe(
            df=df,
            field_mapping=field_mapping,
            date_format=date_format,
            sign_convention=sign_convention,
            number_format=number_format,
            account_id=connection.account_id,
            source_file=f"gsheet://{connection.spreadsheet_id}/{connection.sheet_gid}",
            source_type=_SOURCE_TYPE,
            source_origin=connection.connection_id,
            import_id=_IMPORT_ID_PLACEHOLDER,  # overwritten in load() per-call
        )
        return result.transactions

    def load(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
        db: Database,
        import_id: str,
    ) -> LoadResult:
        """Diff vs. existing rows, soft-delete missing, upsert present, undelete returning.

        Soft-delete state machine per `transaction_id` within this connection:
          - Row in current pull, not previously stored → INSERT (deleted_from_source_at NULL).
          - Row in current pull, was previously soft-deleted → UPSERT resets
            deleted_from_source_at to NULL.
          - Row not in current pull, was active → UPDATE deleted_from_source_at = NOW.
          - Empty current pull is a no-op for upsert; previously-active rows are
            still eligible for soft-delete.
        """
        # Stamp the import_id on every row (transform left a placeholder).
        # Also explicitly NULL deleted_from_source_at — DuckDB's INSERT OR
        # REPLACE BY NAME carries over unnamed columns from the prior row,
        # which would leave a returning row stuck in the soft-deleted state.
        df = df.with_columns(
            pl.lit(import_id).alias("import_id"),
            pl.lit(None, dtype=pl.Datetime("us")).alias("deleted_from_source_at"),
        )

        current_ids: set[str] = set(df["transaction_id"].to_list())

        # Fetch all currently-active (not soft-deleted) ids for this connection.
        active_rows = db.execute(
            f"SELECT transaction_id FROM {TABULAR_TRANSACTIONS.full_name} "  # noqa: S608  # TableRef constant, no user input
            "WHERE source_origin = ? AND deleted_from_source_at IS NULL",
            [connection.connection_id],
        ).fetchall()
        active_ids: set[str] = {r[0] for r in active_rows}

        diff = compute_diff(current_ids=current_ids, active_ids=active_ids)

        rows_inserted = 0
        rows_upserted = 0
        if len(df) > 0:
            # Upsert every current row. INSERT OR REPLACE clears any prior
            # soft-delete state because the new row has
            # deleted_from_source_at = NULL (Polars frame has no such column,
            # so DuckDB applies the table default of NULL).
            db.ingest_dataframe(
                TABULAR_TRANSACTIONS.full_name, df, on_conflict="upsert"
            )
            rows_inserted = len(diff.to_insert)
            rows_upserted = len(df) - rows_inserted

        # Soft-delete rows that were active but are no longer present.
        rows_soft_deleted = 0
        if diff.to_soft_delete:
            ids = sorted(diff.to_soft_delete)
            placeholders = ",".join(["?"] * len(ids))
            sql = f"UPDATE {TABULAR_TRANSACTIONS.full_name} SET deleted_from_source_at = CURRENT_TIMESTAMP WHERE source_origin = ? AND transaction_id IN ({placeholders})"  # noqa: S608  # placeholders are "?"-only, ids parameterized
            db.execute(sql, [connection.connection_id, *ids])
            rows_soft_deleted = len(ids)

        logger.info(
            f"gsheet transactions load: connection={connection.connection_id} "
            f"import_id={import_id} inserted={rows_inserted} "
            f"upserted={rows_upserted} soft_deleted={rows_soft_deleted}"
        )

        return LoadResult(
            rows_inserted=rows_inserted,
            rows_soft_deleted=rows_soft_deleted,
            rows_upserted=rows_upserted,
        )


# Register the adapter exactly once at import time.
ADAPTERS.setdefault("transactions", TransactionsAdapter())
