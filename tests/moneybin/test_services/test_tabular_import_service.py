"""Tests for the tabular import service layer."""

from pathlib import Path

import pytest

from moneybin.services.import_service import (
    _detect_file_type,  # type: ignore[reportPrivateUsage]  # testing private function
)


class TestDetectFileType:
    """Test that file extensions are detected correctly."""

    def test_csv_detected(self) -> None:
        assert _detect_file_type(Path("test.csv")) == "tabular"

    def test_tsv_detected(self) -> None:
        assert _detect_file_type(Path("test.tsv")) == "tabular"

    def test_xlsx_detected(self) -> None:
        assert _detect_file_type(Path("test.xlsx")) == "tabular"

    def test_parquet_detected(self) -> None:
        assert _detect_file_type(Path("test.parquet")) == "tabular"

    def test_feather_detected(self) -> None:
        assert _detect_file_type(Path("test.feather")) == "tabular"

    def test_txt_detected(self) -> None:
        assert _detect_file_type(Path("test.txt")) == "tabular"

    def test_ofx_still_works(self) -> None:
        assert _detect_file_type(Path("test.ofx")) == "ofx"

    def test_pdf_still_works(self) -> None:
        assert _detect_file_type(Path("test.pdf")) == "w2"

    def test_unsupported_extension_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported file type"):
            _detect_file_type(Path("test.jpg"))
