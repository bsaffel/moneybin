"""Light tests for GSheetAdapter Protocol and shared dataclasses."""

from __future__ import annotations

import dataclasses

from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetAdapter,
    GSheetConnection,
    LoadResult,
)


def test_dataclasses_importable():
    """All exports are importable."""
    assert callable(DetectionResult)
    assert callable(LoadResult)
    assert callable(GSheetConnection)
    assert callable(GSheetAdapter)


def test_detection_result_defaults():
    """DetectionResult has correct defaults."""
    result = DetectionResult(
        confidence="high",
        column_mapping={"Date": "date"},
        header_signature=["Date"],
    )
    assert result.confidence == "high"
    assert result.column_mapping == {"Date": "date"}
    assert result.header_signature == ["Date"]
    assert result.date_format is None
    assert result.sign_convention is None
    assert result.number_format is None
    assert result.skip_rows == 0
    assert result.skip_trailing_patterns == []
    assert result.typed_columns == {}
    assert result.notes == []


def test_load_result_defaults():
    """LoadResult has correct defaults."""
    result = LoadResult(
        rows_inserted=0,
        rows_soft_deleted=0,
        rows_upserted=0,
    )
    assert result.rows_inserted == 0
    assert result.rows_soft_deleted == 0
    assert result.rows_upserted == 0
    assert result.rows_rejected == 0
    assert result.notes == []


def test_detection_result_frozen():
    """DetectionResult is frozen."""
    result = DetectionResult(
        confidence="high",
        column_mapping={"Date": "date"},
        header_signature=["Date"],
    )
    try:
        result.confidence = "low"  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except dataclasses.FrozenInstanceError:
        pass


def test_load_result_frozen():
    """LoadResult is frozen."""
    result = LoadResult(
        rows_inserted=0,
        rows_soft_deleted=0,
        rows_upserted=0,
    )
    try:
        result.rows_inserted = 1  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except dataclasses.FrozenInstanceError:
        pass


def test_gsheet_connection_frozen():
    """GSheetConnection is frozen."""
    conn = GSheetConnection(
        connection_id="c1",
        spreadsheet_id="s1",
        sheet_gid=1,
        sheet_name="Sheet1",
        workbook_name="Book1",
        adapter="transactions",
        alias=None,
        account_id=None,
        account_name=None,
        column_mapping={},
        header_signature=[],
        date_format=None,
        sign_convention=None,
        number_format=None,
        skip_rows=0,
        skip_trailing_patterns=[],
        status="active",
        last_pull_at=None,
        last_pull_import_id=None,
        last_success_at=None,
        last_drift_reason=None,
        consecutive_failure_count=0,
    )
    try:
        conn.connection_id = "c2"  # type: ignore
        raise AssertionError("Should have raised FrozenInstanceError")
    except dataclasses.FrozenInstanceError:
        pass


def test_adapters_registry_is_a_dict():
    """ADAPTERS registry is a dict; populated by adapter modules at import time."""
    assert isinstance(ADAPTERS, dict)
