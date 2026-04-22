"""Tests for file format detection (Stage 1)."""

from pathlib import Path

import pytest

from moneybin.extractors.tabular.format_detector import (
    detect_delimiter,
    detect_encoding,
    detect_format,
)


class TestDetectFormat:
    """Tests for detect_format function."""

    def test_csv_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.csv"
        f.write_text("a,b,c\n1,2,3\n")
        info = detect_format(f)
        assert info.file_type == "csv"
        assert info.delimiter == ","

    def test_tsv_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.tsv"
        f.write_text("a\tb\tc\n1\t2\t3\n")
        info = detect_format(f)
        assert info.file_type == "tsv"
        assert info.delimiter == "\t"

    def test_tab_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "data.tab"
        f.write_text("a\tb\tc\n1\t2\t3\n")
        info = detect_format(f)
        assert info.file_type == "tsv"

    def test_txt_sniffs_delimiter(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("a|b|c\n1|2|3\n4|5|6\n")
        info = detect_format(f)
        assert info.file_type == "pipe"
        assert info.delimiter == "|"

    def test_xlsx_extension(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Desc"])
        ws.append(["2026-01-01", 42.50, "Test"])
        wb.save(tmp_path / "data.xlsx")

        info = detect_format(tmp_path / "data.xlsx")
        assert info.file_type == "excel"

    def test_parquet_extension(self, tmp_path: Path) -> None:
        import polars as pl

        df = pl.DataFrame({"a": [1], "b": [2]})
        path = tmp_path / "data.parquet"
        df.write_parquet(path)
        info = detect_format(path)
        assert info.file_type == "parquet"

    def test_feather_extension(self, tmp_path: Path) -> None:
        import polars as pl

        df = pl.DataFrame({"a": [1], "b": [2]})
        path = tmp_path / "data.feather"
        df.write_ipc(path)
        info = detect_format(path)
        assert info.file_type == "feather"

    def test_unsupported_extension_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "data.json"
        f.write_text("{}")
        with pytest.raises(ValueError, match="Unsupported"):
            detect_format(f)


class TestDetectDelimiter:
    """Tests for delimiter detection."""

    def test_comma(self) -> None:
        lines = ["a,b,c", "1,2,3", "4,5,6"]
        assert detect_delimiter(lines) == ","

    def test_tab(self) -> None:
        lines = ["a\tb\tc", "1\t2\t3", "4\t5\t6"]
        assert detect_delimiter(lines) == "\t"

    def test_pipe(self) -> None:
        lines = ["a|b|c", "1|2|3", "4|5|6"]
        assert detect_delimiter(lines) == "|"

    def test_semicolon(self) -> None:
        lines = ["a;b;c", "1;2;3", "4;5;6"]
        assert detect_delimiter(lines) == ";"

    def test_fallback_to_comma(self) -> None:
        """If no clear winner, default to comma."""
        lines = ["hello world"]
        assert detect_delimiter(lines) == ","


class TestDetectEncoding:
    """Tests for encoding detection."""

    def test_utf8(self, tmp_path: Path) -> None:
        f = tmp_path / "utf8.csv"
        f.write_text("café,naïve\n", encoding="utf-8")
        assert detect_encoding(f) == "utf-8"

    def test_latin1(self, tmp_path: Path) -> None:
        f = tmp_path / "latin1.csv"
        # Use a realistic CSV-shaped file with diverse accented chars so
        # charset-normalizer has enough signal to converge on a Latin-family
        # encoding (short files are too ambiguous).
        header = b"date,amount,description,merchant,category\n"
        row = b"2026-01-01,42.50,Caf\xe9 du March\xe9,Caf\xe9 Napol\xe9on,Food\n"
        f.write_bytes(header + row * 50)
        enc = detect_encoding(f)
        # charset-normalizer may return cp1250 or cp1252 for Western European
        # accented content — both are Latin-family and will decode the file.
        assert enc in (
            "iso-8859-1",
            "latin-1",
            "cp1252",
            "windows-1252",
            "cp1250",
            "windows-1250",
        )


class TestSizeGuardrails:
    """Tests for file size limit enforcement."""

    def test_text_file_over_25mb_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "big.csv"
        f.write_bytes(b"a,b,c\n" + b"1,2,3\n" * (25 * 1024 * 1024 // 6 + 1))
        with pytest.raises(ValueError, match="25 MB"):
            detect_format(f)

    def test_text_file_over_25mb_with_override(self, tmp_path: Path) -> None:
        f = tmp_path / "big.csv"
        f.write_bytes(b"a,b,c\n" + b"1,2,3\n" * (25 * 1024 * 1024 // 6 + 1))
        info = detect_format(f, no_size_limit=True)
        assert info.file_type == "csv"
