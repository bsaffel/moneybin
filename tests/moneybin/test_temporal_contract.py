"""Consumer contracts for maintained warehouse instants and source dates."""

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import sqlglot
from sqlglot import expressions as exp

from moneybin.database import Database
from moneybin.privacy.payloads.accounts import LinkHistoryRow
from moneybin.privacy.payloads.investments import SecurityLinkHistoryRow
from moneybin.privacy.payloads.merchants import MerchantLinkHistoryRow
from moneybin.protocol.envelope import serialize_payload
from moneybin.services.transform_service import TransformService

_PERSISTED_INSTANTS = {
    "app.account_link_decisions": "decided_at reversed_at",
    "app.account_links": "decided_at reversed_at",
    "app.account_settings": "updated_at investment_source_type_changed_at archived_changed_at currency_code_changed_at default_cost_basis_method_changed_at account_type_changed_at account_subtype_changed_at",
    "app.ai_consent_grants": "granted_at revoked_at",
    "app.audit_log": "occurred_at",
    "app.balance_assertions": "created_at updated_at",
    "app.budgets": "created_at updated_at",
    "app.categorization_decisions": "proposed_at decided_at reversed_at",
    "app.categorization_rules": "created_at updated_at",
    "app.category_overrides": "updated_at",
    "app.category_source_map": "created_at updated_at",
    "app.exchange_rate_overrides": "created_at updated_at",
    "app.export_destinations": "created_at updated_at",
    "app.gsheet_connections": "last_pull_at last_success_at created_at updated_at",
    "app.import_log": "started_at completed_at reverted_at",
    "app.import_previews": "issued_at expires_at consumed_at updated_at",
    "app.imports": "updated_at",
    "app.investment_match_decisions": "created_at updated_at decided_at accepted_at",
    "app.lot_selections": "created_at",
    "app.match_decisions": "decided_at reversed_at",
    "app.merchant_link_decisions": "decided_at reversed_at",
    "app.merchant_links": "decided_at reversed_at",
    "app.metrics": "recorded_at",
    "app.pdf_formats": "last_used_at created_at updated_at",
    "app.profile_settings": "updated_at",
    "app.proposed_rules": "proposed_at decided_at",
    "app.rule_conflicts": "existing_rule_updated_at detected_at resolved_at",
    "app.schema_migrations": "applied_at",
    "app.securities": "created_at updated_at",
    "app.security_link_decisions": "decided_at reversed_at",
    "app.security_links": "decided_at reversed_at",
    "app.security_price_overrides": "created_at updated_at",
    "app.sync_job_state": "ingested_at acked_at updated_at",
    "app.tabular_formats": "last_used_at created_at updated_at",
    "app.transaction_categories": "categorized_at",
    "app.transaction_id_aliases": "created_at",
    "app.transaction_notes": "created_at",
    "app.transaction_splits": "created_at",
    "app.transaction_tags": "applied_at",
    "app.user_categories": "created_at updated_at",
    "app.user_merchants": "created_at updated_at",
    "app.user_reports": "created_at updated_at",
    "app.versions": "updated_at installed_at",
    "raw.exchange_rate_coverage": "loaded_at",
    "raw.exchange_rate_currencies": "loaded_at",
    "raw.exchange_rates": "loaded_at",
    "raw.gsheet_seeds": "deleted_from_source_at loaded_at",
    "raw.import_preview_snapshots": "created_at",
    "raw.manual_investment_transactions": "created_at",
    "raw.manual_transactions": "created_at",
    "raw.ofx_accounts": "extracted_at loaded_at",
    "raw.ofx_balances": "statement_start_date statement_end_date ledger_balance_date extracted_at loaded_at",
    "raw.ofx_transactions": "date_posted extracted_at loaded_at",
    "raw.pdf_seeds": "loaded_at",
    "raw.plaid_accounts": "extracted_at loaded_at",
    "raw.plaid_balances": "last_updated_datetime extracted_at loaded_at",
    "raw.plaid_investment_holding_lots": "original_purchase_datetime extracted_at loaded_at",
    "raw.plaid_investment_holdings": "extracted_at loaded_at",
    "raw.plaid_investment_holdings_snapshots": "extracted_at loaded_at",
    "raw.plaid_investment_transaction_receipts": "extracted_at loaded_at",
    "raw.plaid_investment_transactions": "transaction_datetime",
    "raw.plaid_securities": "extracted_at loaded_at",
    "raw.plaid_transactions": "extracted_at loaded_at",
    "raw.security_prices": "extracted_at loaded_at",
    "raw.simplefin_accounts": "fetched_at loaded_at",
    "raw.simplefin_balances": "fetched_at loaded_at balance_at",
    "raw.simplefin_responses": "fetched_at window_start window_end loaded_at",
    "raw.simplefin_sync_receipts": "started_at completed_at loaded_at",
    "raw.simplefin_transactions": "fetched_at loaded_at posted_at transacted_at",
    "raw.tabular_accounts": "extracted_at loaded_at",
    "raw.tabular_transactions": "extracted_at loaded_at deleted_from_source_at",
    "synthetic.ground_truth": "generated_at",
}

_DERIVED_INSTANTS = {
    "prep.int_plaid__opening_positions": "snapshot_extracted_at",
    "prep.int_transactions__matched": "source_extracted_at loaded_at",
    "prep.int_transactions__merged": "source_extracted_at loaded_at",
    "prep.int_transactions__unioned": "source_extracted_at loaded_at",
    "prep.stg_exchange_rates": "loaded_at",
    "prep.stg_manual__investment_transactions": "created_at",
    "prep.stg_manual__transactions": "created_at",
    "prep.stg_ofx__accounts": "extracted_at loaded_at",
    "prep.stg_ofx__balances": "extracted_at loaded_at",
    "prep.stg_ofx__transactions": "extracted_at loaded_at",
    "prep.stg_plaid__accounts": "extracted_at loaded_at",
    "prep.stg_plaid__balances": "last_updated_datetime extracted_at loaded_at",
    "prep.stg_plaid__investment_holdings": "extracted_at loaded_at",
    "prep.stg_plaid__investment_holdings_snapshots": "extracted_at loaded_at",
    "prep.stg_plaid__investment_transactions": "extracted_at created_at",
    "prep.stg_plaid__opening_lots": "created_at",
    "prep.stg_plaid__securities": "extracted_at loaded_at",
    "prep.stg_plaid__transactions": "extracted_at loaded_at",
    "prep.stg_security_prices": "extracted_at loaded_at",
    "prep.int_accounts__merged": "extracted_at loaded_at updated_at investment_source_type_changed_at archived_changed_at currency_code_changed_at default_cost_basis_method_changed_at account_type_changed_at account_subtype_changed_at",
    "prep.stg_simplefin__accounts": "extracted_at loaded_at",
    "prep.stg_simplefin__balances": "balance_at extracted_at loaded_at",
    "prep.stg_simplefin__transactions": "posted_at transacted_at extracted_at loaded_at",
    "prep.stg_tabular__accounts": "extracted_at loaded_at",
    "prep.stg_tabular__transactions": "extracted_at loaded_at",
    "core.bridge_currency_conversions": "updated_at",
    "core.dim_accounts": "extracted_at loaded_at updated_at investment_source_type_changed_at archived_changed_at",
    "core.dim_categories": "created_at updated_at",
    "core.dim_holdings": "provider_reported_as_of updated_at",
    "core.dim_merchants": "created_at updated_at",
    "core.fct_balances": "updated_at",
    "core.fct_currency_lots": "updated_at",
    "core.fct_exchange_rates": "updated_at",
    "core.fct_investment_lots": "updated_at",
    "core.fct_investment_transactions": "updated_at",
    "core.fct_realized_fx_gains": "updated_at",
    "core.fct_realized_gains": "updated_at",
    "core.fct_security_prices": "updated_at",
    "core.fct_transactions": "source_extracted_at loaded_at updated_at notes[].created_at",
    "meta.fct_transaction_provenance": "source_extracted_at",
    "meta.model_freshness": "last_changed_at last_applied_at last_executed_at",
    "reports.realized_fx": "updated_at",
}


def _catalog_type_paths(db: Database) -> dict[str, exp.DataType]:
    paths: dict[str, exp.DataType] = {}

    def visit(path: str, kind: exp.DataType) -> None:
        paths[path] = kind
        if kind.is_type(exp.DataType.Type.ARRAY):
            element = kind.expressions[0]
            assert isinstance(element, exp.DataType), path
            visit(f"{path}[]", element)
        elif kind.is_type(exp.DataType.Type.STRUCT):
            for field in kind.expressions:
                assert isinstance(field, exp.ColumnDef), path
                field_type = field.args["kind"]
                assert isinstance(field_type, exp.DataType), path
                visit(f"{path}.{field.name}", field_type)

    for schema, table, column, type_name in db.execute(
        "SELECT schema_name, table_name, column_name, data_type FROM duckdb_columns() "
        "WHERE database_name = current_database() AND NOT internal "
        "AND schema_name IN ('raw', 'app', 'synthetic', 'prep', 'core', 'reports', 'meta')"
    ).fetchall():
        visit(
            f"{schema}.{table}.{column}",
            exp.DataType.build(type_name, dialect="duckdb"),
        )
    return paths


def _assert_instant_inventory(
    paths: dict[str, exp.DataType], expected: set[str]
) -> None:
    missing = sorted(expected - paths.keys())
    wrong_types = {
        path: paths[path].sql(dialect="duckdb")
        for path in sorted(expected & paths.keys())
        if not paths[path].is_type(exp.DataType.Type.TIMESTAMPTZ)
    }
    unclassified = sorted(
        path
        for path, kind in paths.items()
        if kind.is_type(exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ)
        and path not in expected
    )
    assert not (missing or wrong_types or unclassified), {
        "missing": missing,
        "wrong_types": wrong_types,
        "unclassified": unclassified,
    }


@pytest.mark.parametrize(
    "row_type", [LinkHistoryRow, MerchantLinkHistoryRow, SecurityLinkHistoryRow]
)
def test_link_history_payload_keeps_canonical_instant_and_null(row_type: Any) -> None:
    row = {
        "decision_id": "synthetic-decision",
        "provisional_account_id": "provisional",
        "candidate_account_id": "account",
        "ref_value": "reference",
        "ref_kind": "ticker",
        "source_type": "synthetic",
        "candidate_merchant_id": "merchant",
        "candidate_security_id": "security",
        "status": "accepted",
        "decided_by": "user",
        "decided_at": datetime(
            2025, 11, 2, 1, 30, 0, 123456, tzinfo=timezone(timedelta(hours=-4))
        ),
    }
    assert serialize_payload(row_type.from_decision_row(row))["decided_at"] == (
        "2025-11-02T05:30:00.123456Z"
    )
    assert (
        serialize_payload(row_type.from_decision_row(row | {"decided_at": None}))[
            "decided_at"
        ]
        is None
    )


def test_owned_schema_and_models_preserve_semantic_temporal_inventory(
    db: Database,
) -> None:
    from moneybin.schema import (
        _all_schema_files,  # pyright: ignore[reportPrivateUsage]  # independently enumerate bootstrap declarations
    )

    optional_synthetic = (
        Path(__file__).parents[2] / "src/moneybin/sql/schema/synthetic_ground_truth.sql"
    )
    db.execute(optional_synthetic.read_text())
    expected: set[tuple[str, str, str]] = set()
    for path in [*_all_schema_files(), optional_synthetic]:
        for statement in sqlglot.parse(path.read_text(), dialect="duckdb"):
            if not isinstance(statement, exp.Create) or statement.kind != "TABLE":
                continue
            table = statement.find(exp.Table)
            assert table is not None
            for column in statement.find_all(exp.ColumnDef):
                kind = column.args.get("kind")
                if isinstance(kind, exp.DataType) and kind.is_type(
                    exp.DataType.Type.TIMESTAMPTZ, exp.DataType.Type.TIMESTAMP
                ):
                    expected.add((table.db, table.name, column.name))

    assert expected == {
        (*table.split("."), column)
        for table, columns in _PERSISTED_INSTANTS.items()
        for column in columns.split()
    }
    result = TransformService(db).apply()
    assert result.applied, result.error
    _assert_instant_inventory(
        _catalog_type_paths(db),
        {
            f"{table}.{path}"
            for inventory in (_PERSISTED_INSTANTS, _DERIVED_INSTANTS)
            for table, paths in inventory.items()
            for path in paths.split()
        },
    )
    assert db.execute(
        "SELECT data_type FROM duckdb_columns() WHERE schema_name = 'raw' "
        "AND table_name = 'manual_transactions' AND column_name = 'transaction_date'"
    ).fetchone() == ("DATE",)
    clock_row = db.execute("SELECT CURRENT_TIMESTAMP").fetchone()
    assert clock_row is not None
    instant = clock_row[0]
    assert isinstance(instant, datetime)
    assert instant.utcoffset() == timedelta(0)


@pytest.mark.parametrize(
    ("projection", "expected_path", "sibling_path"),
    [
        (
            "CAST(NULL AS STRUCT(created_at VARCHAR)[]) AS notes, "
            "CURRENT_TIMESTAMP AS updated_at",
            "notes[].created_at",
            "updated_at",
        ),
        ("CURRENT_TIMESTAMP AS last_changed_at", "last_executed_at", "last_changed_at"),
    ],
)
def test_instant_inventory_rejects_wrong_nested_type_and_missing_clock(
    db: Database, projection: str, expected_path: str, sibling_path: str
) -> None:
    db.execute(
        f"CREATE VIEW core.utc_inventory_probe AS SELECT {projection}"  # fixed test projections, not user input
    )
    probe = {
        path: kind
        for path, kind in _catalog_type_paths(db).items()
        if path.startswith("core.utc_inventory_probe.")
    }
    with pytest.raises(AssertionError, match=re.escape(expected_path)):
        _assert_instant_inventory(
            probe,
            {
                f"core.utc_inventory_probe.{expected_path}",
                f"core.utc_inventory_probe.{sibling_path}",
            },
        )
