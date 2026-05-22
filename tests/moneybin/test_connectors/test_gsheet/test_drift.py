"""Tests for drift detection in gsheet connector."""

from __future__ import annotations

import dataclasses

import polars as pl
import pytest

from moneybin.connectors.gsheet.drift import DriftReport, detect_drift


class TestDetectDrift:
    """Test detect_drift() behavior."""

    def test_no_drift_when_headers_match_signature(self):
        """Exact header match and all mapped columns present + non-null."""
        pinned_sig = ["id", "name", "amount"]
        current = ["id", "name", "amount"]
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "amount": [100.0, 200.0, 300.0],
        })
        mapped = {"id", "name", "amount"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is False
        assert report.reason == "no drift"
        assert report.missing_headers == []
        assert report.empty_mapped_columns == []
        assert report.new_columns == []

    def test_no_drift_when_headers_reordered(self):
        """Headers are the same, just reordered."""
        pinned_sig = ["id", "name", "amount"]
        current = ["amount", "id", "name"]  # reordered
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "amount": [100.0, 200.0, 300.0],
        })
        mapped = {"id", "name", "amount"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is False
        assert report.reason == "no drift"
        assert report.missing_headers == []
        assert report.empty_mapped_columns == []

    def test_drift_when_pinned_header_missing(self):
        """A header in pinned signature is missing from current."""
        pinned_sig = ["id", "name", "amount"]
        current = ["id", "name"]  # missing "amount"
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        })
        mapped = {"id", "name", "amount"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is True
        assert "missing headers: ['amount']" in report.reason
        assert report.missing_headers == ["amount"]

    def test_no_drift_when_new_columns_added(self):
        """New columns in current sheet (not in signature) are not drift."""
        pinned_sig = ["id", "name"]
        current = ["id", "name", "extra", "metadata"]
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "extra": [None, None, None],
            "metadata": ["x", "y", "z"],
        })
        mapped = {"id", "name"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is False
        assert report.reason == "no drift"
        assert report.new_columns == ["extra", "metadata"]

    def test_drift_when_mapped_column_mostly_null(self):
        """A mapped column is >50% null in sample."""
        pinned_sig = ["id", "name", "amount"]
        current = ["id", "name", "amount"]
        df = pl.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "b", "c", "d", "e"],
            "amount": [100.0, None, None, None, None],  # 4/5 null = 80%
        })
        mapped = {"id", "name", "amount"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is True
        assert "empty mapped columns: ['amount']" in report.reason
        assert report.empty_mapped_columns == ["amount"]

    def test_drift_when_multiple_failures(self):
        """Both missing headers AND empty mapped columns."""
        pinned_sig = ["id", "name", "amount", "date"]
        current = ["id", "name", "amount"]  # missing "date"
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "amount": [None, None, None],  # 100% null
        })
        mapped = {"id", "name", "amount", "date"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is True
        assert "missing headers: ['date']" in report.reason
        assert "empty mapped columns: ['amount']" in report.reason
        assert report.missing_headers == ["date"]
        assert report.empty_mapped_columns == ["amount"]

    def test_no_drift_when_all_inputs_empty(self):
        """Empty signature, empty headers, empty df, empty mapped_columns."""
        report = detect_drift(
            pinned_signature=[],
            current_headers=[],
            sample_df=pl.DataFrame(),
            mapped_columns=set(),
        )

        assert report.is_drift is False
        assert report.reason == "no drift"
        assert report.missing_headers == []
        assert report.empty_mapped_columns == []
        assert report.new_columns == []

    def test_drift_when_mapped_column_is_all_blank_strings(self):
        """Mapped column with all empty strings is flagged as empty."""
        pinned_sig = ["id", "description"]
        current = ["id", "description"]
        df = pl.DataFrame({
            "id": ["1", "2", "3", "4", "5"],
            "description": ["", "", "", "", ""],  # All blank strings
        })
        mapped = {"id", "description"}

        report = detect_drift(
            pinned_signature=pinned_sig,
            current_headers=current,
            sample_df=df,
            mapped_columns=mapped,
        )

        assert report.is_drift is True
        assert "empty mapped columns: ['description']" in report.reason
        assert report.empty_mapped_columns == ["description"]

    def test_drift_report_is_frozen(self):
        """DriftReport instances are frozen and cannot be modified."""
        report = DriftReport(is_drift=False, reason="test")

        with pytest.raises(dataclasses.FrozenInstanceError):
            report.is_drift = True  # type: ignore[misc]
