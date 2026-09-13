"""Tests for Stage 2 file readers."""

import datetime
from pathlib import Path

import polars as pl
import pytest
from pytest_mock import MockerFixture

from moneybin.extractors.tabular.format_detector import FormatInfo
from moneybin.extractors.tabular.readers import (
    _classify_excel_headerless_via_fastexcel,  # pyright: ignore[reportPrivateUsage]
    _detect_header,  # pyright: ignore[reportPrivateUsage]
    _excel_native_date_columns,  # pyright: ignore[reportPrivateUsage]
    _row_looks_like_data_at,  # pyright: ignore[reportPrivateUsage]
    date_format_has_time_component,
    normalize_excel_date_columns,
    normalize_excel_date_columns_after_mapping,
    normalize_excel_date_columns_before_mapping,
    read_file,
)


def _write_csv(path: Path, content: str) -> Path:
    path.write_text(content)
    return path


class TestCSVReader:
    """Tests for CSV/text file reading."""

    def test_basic_csv(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "basic.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1
        assert list(result.df.columns) == ["Date", "Amount", "Description"]

    def test_unicode_separator_does_not_change_physical_header_index(
        self,
        tmp_path: Path,
    ) -> None:
        """Polars skip_rows and header detection count the same physical rows."""
        path = tmp_path / "unicode-separator.csv"
        source_bytes = (
            "Statement\u2028continued\n"
            "Date,Description,Amount\n"
            "2026-07-01,Coffee,-4.50\n"
        ).encode()
        path.write_bytes(source_bytes)
        info = FormatInfo(
            file_type="csv",
            delimiter=",",
            encoding="utf-8",
            file_size=len(source_bytes),
        )

        result = read_file(path, info, source_bytes=source_bytes)

        assert result.skip_rows == 1
        assert result.df["Description"].to_list() == ["Coffee"]

    @pytest.mark.parametrize("use_source_bytes", [False, True])
    def test_header_helpers_split_bare_cr_but_not_unicode_separators(
        self,
        tmp_path: Path,
        use_source_bytes: bool,
    ) -> None:
        source_bytes = (
            "Statement\u0085and\u2028continued\r"
            "Date,Description,Amount\r"
            "2026-07-01,Coffee,-4.50\r"
        ).encode()
        path = tmp_path / "bare-cr.csv"
        path.write_bytes(source_bytes)
        materialized = source_bytes if use_source_bytes else None

        assert _detect_header(
            path,
            "utf-8",
            ",",
            source_bytes=materialized,
        ) == (1, True, False, ())
        assert _row_looks_like_data_at(
            path,
            "utf-8",
            ",",
            2,
            source_bytes=materialized,
        )

    def test_skip_preamble_rows(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "preamble.csv",
            "Bank Summary Report\nGenerated: 2026-01-15\n\nDate,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1
        assert "Date" in result.df.columns
        assert result.skip_rows > 0

    def test_trailing_total_row_removed(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "trailing.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n2026-01-02,10.00,Tea\nTotal,,52.50\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 2
        assert result.rows_skipped_trailing >= 1

    def test_headerless_csv_keeps_first_data_row(self, tmp_path: Path) -> None:
        """Headerless bank CSV (e.g. Wells Fargo) must not lose row 0.

        WF exports are headerless: ``Date,Amount,*,,Description`` with no
        header line. A real transaction row leads with a date and carries a
        description, so it has a low numeric ratio and the old header
        heuristic mistook row 0 for a header — silently dropping the most
        recent transaction. Regression: all three rows must survive.
        """
        f = _write_csv(
            tmp_path / "wf.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n'
            '"04/14/2026","-50.00","*","","BILL PAY ACME 8230"\n',
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 3

    def test_summary_row_above_header_not_headerless(self, tmp_path: Path) -> None:
        """A summary/opening-balance line above the real header is preamble.

        A single date+amount row (e.g. an opening-balance line) before the real
        header used to trip headerless detection: the scanner saw row 0 parse
        as a data record and returned ``(0, headerless)`` before reaching the
        ``Date,Amount,Description`` header. Polars then ingested the header as a
        data row under generated column names, breaking downstream mapping.
        The real header must win, with the summary line skipped as preamble.
        """
        f = _write_csv(
            tmp_path / "summary.csv",
            "2026-01-01,100.00\n"
            "Date,Amount,Description\n"
            "2026-01-02,42.50,Coffee\n"
            "2026-01-03,10.00,Tea\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert len(result.df) == 2
        assert result.skip_rows == 1

    def test_multiple_summary_rows_above_header_not_headerless(
        self, tmp_path: Path
    ) -> None:
        """Several data-like preamble rows above the header are all skipped.

        A statement can carry both an opening-balance and a closing-balance
        line (each a date + amount, so each looks like data) before the real
        header. A single-row peek isn't enough — the scan must look past every
        data-like preamble row and still find the ``Date,Amount,Description``
        header, skipping both summary lines.
        """
        f = _write_csv(
            tmp_path / "two_summary.csv",
            "2026-01-01,100.00\n"
            "2026-01-31,150.00\n"
            "Date,Amount,Description\n"
            "2026-01-02,42.50,Coffee\n"
            "2026-01-03,10.00,Tea\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert len(result.df) == 2
        assert result.skip_rows == 2

    def test_parenthesized_amount_rows_recognized_in_header_scan(
        self, tmp_path: Path
    ) -> None:
        """Header detection must recognize the amount formats the importer does.

        The loader's ``parse_amount_str`` accepts ``(42.50)`` as a negative, but
        a narrower float check does not. If header detection can't see those as
        amounts, a summary preamble carrying a parenthesized amount above the
        real header is not classified as data, the follow-by-data gate refuses
        the real header, and the file falls back to treating the preamble as the
        header. Recognizing the amount keeps the real header winning.
        """
        f = _write_csv(
            tmp_path / "paren.csv",
            "2026-01-01,(100.00)\n"
            "Date,Amount,Description\n"
            "2026-01-02,(42.50),Refund\n"
            "2026-01-03,(10.00),Fee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert len(result.df) == 2
        assert result.skip_rows == 1

    def test_headerless_with_footer_not_mistaken_for_header(
        self, tmp_path: Path
    ) -> None:
        """A date-bearing footer in a headerless file must not become the header.

        A genuinely headerless export (Wells-Fargo-style, every row a
        date+amount) can carry a trailer like ``Downloaded On,2026-04-17``
        within the first 30 lines. That row reads as labels (low numeric ratio)
        and is not a data row (a date but no amount), so a header scan that
        ignores position would pick the footer as the header and skip every
        real data row above it. The file is headerless: the data rows survive
        and the footer is stripped as a trailing row.
        """
        f = _write_csv(
            tmp_path / "wf_footer.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n'
            "Downloaded On,2026-04-17\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.skip_rows == 0
        assert len(result.df) == 2
        # The most recent transaction (row 0) survived rather than being
        # consumed as a skipped pre-header row.
        assert "04/16/2026" in str(result.df.row(0))

    def test_bom_handled(self, tmp_path: Path) -> None:
        f = tmp_path / "bom.csv"
        f.write_bytes(b"\xef\xbb\xbfDate,Amount\n2026-01-01,42.50\n")
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8-sig")
        result = read_file(f, info)
        assert "Date" in result.df.columns

    def test_pipe_delimiter(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "pipe.txt",
            "Date|Amount|Description\n2026-01-01|42.50|Coffee\n",
        )
        info = FormatInfo(file_type="pipe", delimiter="|", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 1

    def test_row_limit_warning(self, tmp_path: Path) -> None:
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(10_001))
        f = _write_csv(
            tmp_path / "big.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert len(result.df) == 10_001
        assert result.row_count_warning is True

    def test_row_limit_refuse(self, tmp_path: Path) -> None:
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(50_001))
        f = _write_csv(
            tmp_path / "huge.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        with pytest.raises(ValueError, match="50,000"):
            read_file(f, info)

    def test_row_limit_refuse_with_override(self, tmp_path: Path) -> None:
        rows = "\n".join(f"2026-01-01,{i},Item{i}" for i in range(50_001))
        f = _write_csv(
            tmp_path / "huge.csv",
            f"Date,Amount,Description\n{rows}\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info, no_row_limit=True)
        assert len(result.df) == 50_001


class TestReadResultTransparency:
    """has_header / rows_in_file / header_row_looks_like_data on ReadResult.

    Regression coverage for the import_preview transparency gap: silent
    header-eating was invisible because ReadResult carried skip_rows and
    rows_skipped_trailing but never surfaced whether a header row was
    consumed, nor a reconcilable total row count.
    """

    def test_has_header_true_for_normal_csv(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "basic.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.has_header is True

    def test_has_header_false_for_headerless_csv(self, tmp_path: Path) -> None:
        f = _write_csv(
            tmp_path / "wf.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n'
            '"04/14/2026","-50.00","*","","BILL PAY ACME 8230"\n',
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.has_header is False

    def test_rows_in_file_counts_header_plus_data(self, tmp_path: Path) -> None:
        """1-header + 1-data file totals 2 physical rows (property, not stored)."""
        f = _write_csv(
            tmp_path / "basic.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.rows_in_file == 2  # 1 header + 1 data row
        assert result.has_header is True

    def test_rows_in_file_counts_headerless_data(self, tmp_path: Path) -> None:
        """A 3-row headerless file totals 3 — no phantom header inflates it."""
        f = _write_csv(
            tmp_path / "wf.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n'
            '"04/14/2026","-50.00","*","","BILL PAY ACME 8230"\n',
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.rows_in_file == 3
        assert result.has_header is False

    def test_rows_in_file_counts_preamble_header_data_trailing(
        self, tmp_path: Path
    ) -> None:
        """3 preamble + 1 header + 2 data + 1 trailing = 7 physical rows."""
        f = _write_csv(
            tmp_path / "combo.csv",
            "Bank Summary Report\nGenerated: 2026-01-15\n\n"
            "Date,Amount,Description\n"
            "2026-01-01,42.50,Coffee\n2026-01-02,10.00,Tea\n"
            "Total,,52.50\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.skip_rows == 3
        assert result.has_header is True
        assert len(result.df) == 2
        assert result.rows_skipped_trailing == 1
        assert result.rows_in_file == 7

    def test_parquet_has_no_header_and_reconciles(self, tmp_path: Path) -> None:
        """Columnar formats consume no header row: has_header=False, total=len."""
        f = tmp_path / "data.parquet"
        pl.DataFrame({
            "date": ["2026-01-01", "2026-01-02"],
            "amount": ["1.00", "2.00"],
        }).write_parquet(f)
        info = FormatInfo(file_type="parquet", delimiter=None, encoding="utf-8")
        result = read_file(f, info)
        assert result.has_header is False
        assert result.rows_in_file == len(result.df) == 2

    def test_feather_has_no_header_and_reconciles(self, tmp_path: Path) -> None:
        f = tmp_path / "data.feather"
        pl.DataFrame({
            "date": ["2026-01-01", "2026-01-02"],
            "amount": ["1.00", "2.00"],
        }).write_ipc(f)
        info = FormatInfo(file_type="feather", delimiter=None, encoding="utf-8")
        result = read_file(f, info)
        assert result.has_header is False
        assert result.rows_in_file == len(result.df) == 2

    def test_bom_headerless_autodetect_keeps_row0(self, tmp_path: Path) -> None:
        """A BOM'd headerless CSV must auto-detect as headerless on the default path.

        Regression for the review finding: Excel's "CSV UTF-8" export prepends a
        BOM. Opening as utf-8 leaves U+FEFF on physical line 0's first cell,
        which defeated ``detect_date_format`` in ``_detect_header`` — so a
        headerless BOM'd file (row 0 a real transaction) was misread as having a
        header, silently eating the first transaction with NO red flag (the flag
        isn't computed on the auto-detect path). ``_detect_header`` must strip
        the BOM so it correctly returns headerless and keeps every row.
        """
        f = tmp_path / "bom_headerless.csv"
        f.write_bytes(b"\xef\xbb\xbf2026-01-01,42.50,Coffee\n2026-01-02,10.00,Tea\n")
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8-sig")
        result = read_file(f, info)  # no explicit skip_rows — the default path
        assert result.has_header is False
        assert len(result.df) == 2  # neither transaction eaten as a header
        assert result.rows_in_file == 2

    def test_bom_headerless_explicit_skip_still_flags_row0(
        self, tmp_path: Path
    ) -> None:
        """A BOM must not suppress the row-0-is-data red flag on explicit skip.

        A utf-8-sig (BOM) headerless CSV imported with an explicit skip_rows=0
        (wrongly declaring a header) must still flag header_row_looks_like_data:
        the BOM on physical line 0 cannot be allowed to defeat date detection.
        """
        f = tmp_path / "bom.csv"
        f.write_bytes(b"\xef\xbb\xbf2026-01-01,42.50,Coffee\n2026-01-02,10.00,Tea\n")
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8-sig")
        result = read_file(f, info, skip_rows=0)
        assert result.has_header is True
        assert result.header_row_looks_like_data is True

    def test_header_row_looks_like_data_false_for_normal_header(
        self, tmp_path: Path
    ) -> None:
        f = _write_csv(
            tmp_path / "basic.csv",
            "Date,Amount,Description\n2026-01-01,42.50,Coffee\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.header_row_looks_like_data is False

    def test_header_row_looks_like_data_false_when_headerless(
        self, tmp_path: Path
    ) -> None:
        f = _write_csv(
            tmp_path / "wf.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n',
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.header_row_looks_like_data is False

    def test_header_row_looks_like_data_true_for_wrong_explicit_skip_rows(
        self, tmp_path: Path
    ) -> None:
        """An explicit skip_rows pointed at a real data row must be flagged.

        Explicit skip_rows always implies has_header=True with no safety
        check today — a caller (human or agent) that mis-specifies skip_rows
        against a genuinely headerless file silently consumes a real
        transaction as a "header". header_row_looks_like_data is the
        defense-in-depth signal for exactly this case.
        """
        f = _write_csv(
            tmp_path / "wf.csv",
            '"04/16/2026","150.00","*","","RECURRING TRANSFER FROM ACME"\n'
            '"04/15/2026","-150.00","*","","RECURRING TRANSFER TO ACME"\n',
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info, skip_rows=0)
        assert result.has_header is True
        assert result.header_row_looks_like_data is True


class TestExcelReader:
    """Tests for Excel file reading."""

    def test_basic_excel(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "test.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert len(result.df) == 1
        assert "Date" in result.df.columns

    def test_multi_sheet_picks_largest(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        ws1 = wb.active
        assert ws1 is not None
        ws1.title = "Summary"
        ws1.append(["Total", 100])
        ws2 = wb.create_sheet("Transactions")
        ws2.append(["Date", "Amount", "Desc"])
        ws2.append(["2026-01-01", 42.50, "Coffee"])
        ws2.append(["2026-01-02", 10.00, "Tea"])
        ws2.append(["2026-01-03", 5.00, "Water"])
        path = tmp_path / "multi.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert len(result.df) == 3

    def test_normal_header_not_flagged(self, tmp_path: Path) -> None:
        """A real Excel header row (labels) must not raise the red flag."""
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "normal.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))
        assert result.has_header is True
        assert result.header_row_looks_like_data is False

    def test_summary_row_above_header_not_headerless(self, tmp_path: Path) -> None:
        """A summary/opening-balance row above the real header is preamble.

        Mirrors the CSV test of the same name. Proves fastexcel's
        ``header_row`` read option is an ABSOLUTE physical row index in the
        sheet, not relative to some other offset: ``_classify_header_rows``
        returns ``skip_rows=1`` (the physical row holding "Date,Amount,
        Description"), and passing that straight through as ``header_row``
        must make ``pl.read_excel`` skip the summary row above it and land on
        the real header — asserted via both the resulting column names and
        the two data rows' values, not just ``has_header``.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-01-01", 100.00])
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-02", 42.50, "Coffee"])
        ws.append(["2026-01-03", 10.00, "Tea"])
        path = tmp_path / "summary.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))
        assert result.skip_rows == 1
        assert result.has_header is True
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert result.df["Date"].to_list() == ["2026-01-02", "2026-01-03"]
        assert result.df["Amount"].to_list() == ["42.5", "10"]
        assert result.df["Description"].to_list() == ["Coffee", "Tea"]

    def test_multiple_summary_rows_above_header_not_headerless(
        self, tmp_path: Path
    ) -> None:
        """Several data-like preamble rows above the header are all skipped.

        Mirrors the CSV test of the same name, with two summary rows instead
        of one. Confirms ``header_row``'s absolute-index semantics hold past
        a single-row preamble too: ``skip_rows=2`` must make
        ``pl.read_excel`` skip both summary rows and land on the physical
        row actually holding the header, not one relative to them.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-01-01", 100.00])
        ws.append(["2026-01-31", 150.00])
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-02", 42.50, "Coffee"])
        ws.append(["2026-01-03", 10.00, "Tea"])
        path = tmp_path / "two_summary.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))
        assert result.skip_rows == 2
        assert result.has_header is True
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert result.df["Date"].to_list() == ["2026-01-02", "2026-01-03"]
        assert result.df["Amount"].to_list() == ["42.5", "10"]
        assert result.df["Description"].to_list() == ["Coffee", "Tea"]

    def test_headerless_excel_keeps_row0(self, tmp_path: Path) -> None:
        """A headerless Excel sheet must not lose its first transaction.

        Regression for MB-449: pl.read_excel used to always consume row 0 as
        the header with no headerless detection, silently eating the first
        transaction. Excel now shares _classify_header_rows with CSV/Parquet,
        so a genuinely headerless sheet is detected as such on first contact.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        # No header row — every row is a real transaction (date + amount).
        ws.append(["2026-01-01", 42.50, "Coffee"])
        ws.append(["2026-01-02", 10.00, "Tea"])
        path = tmp_path / "headerless.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))
        assert result.has_header is False
        assert result.header_row_looks_like_data is False
        assert len(result.df) == 2
        assert result.rows_in_file == 2

    def test_headerless_native_date_single_row_is_typed_correctly(
        self, tmp_path: Path
    ) -> None:
        """The native-date type probe must not skip a headerless sheet's row 0.

        Bug found while verifying this PR: ``_excel_native_date_columns``
        assumed its row argument always named a HEADER row (data starts the
        row after it) and the caller passed ``skip_rows`` unconditionally —
        correct when the sheet has a header, but ``skip_rows`` names the
        first DATA row itself when headerless (see ``_read_excel``'s own
        ``skip_rows`` docstring). Scanning from one row too late silently
        emptied the candidate set on a sheet with exactly one data row (no
        row after it to scan), so a genuinely native-date column reported
        no native date columns at all.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        # Headerless, single row — the empty-scan trap: with the old
        # off-by-one there is no row left to inspect at all.
        ws.append([datetime.date(2026, 7, 1), -4.50, "Coffee"])
        path = tmp_path / "headerless_native_single_row.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))

        assert result.has_header is False
        assert len(result.df) == 1
        assert result.excel_native_date_columns == frozenset({"column_1"})

    def test_native_date_typed_probe_tolerates_one_dirty_value(
        self, tmp_path: Path
    ) -> None:
        """A dirty placeholder must not void the typed native-date probe.

        ``excel_native_date_columns`` must not disqualify a column on its
        FIRST non-``datetime.date`` value with no majority tolerance — a
        real Date column with 3 genuine dates and one "pending" placeholder
        must still qualify.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        ws.append([datetime.date(2026, 1, 1), -4.50, "Coffee"])
        ws.append([datetime.date(2026, 1, 2), 100.00, "Salary"])
        ws.append(["pending", 5.00, "Something"])
        ws.append([datetime.date(2026, 1, 4), 1.00, "Tea"])
        path = tmp_path / "dirty_native_dates.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))

        assert result.excel_native_date_columns == frozenset({"Date"})

    def test_native_date_typed_probe_excludes_a_majority_non_date_column(
        self, tmp_path: Path
    ) -> None:
        """A column that is mostly NOT dates must never qualify.

        The majority-tolerance fix must not become "any date counts" — a
        column holding one incidental ``datetime.date``-typed cell among
        mostly non-date values stays excluded.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Notes", "Description"])
        ws.append([datetime.date(2026, 1, 1), "not a date", "Coffee"])
        ws.append([datetime.date(2026, 1, 2), "also not", "Salary"])
        ws.append([datetime.date(2026, 1, 3), datetime.date(2026, 1, 3), "Tea"])
        path = tmp_path / "mostly_text_notes_column.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))

        assert result.excel_native_date_columns == frozenset({"Date"})

    def test_native_date_probe_survives_a_blank_spacer_column(
        self, tmp_path: Path
    ) -> None:
        """A blank spacer column before Date must not misattribute dates.

        Codex finding, confirmed empirically: ``pl.read_excel``'s
        ``drop_empty_cols=True`` default elides a column whose header AND
        every data cell are blank, so ``df.columns`` (3 entries: Date,
        Amount, Description) drifts out of alignment with openpyxl's raw
        physical row (4 entries: blank spacer, Date, Amount, Description).
        Indexing ``column_names[i]`` against physical position ``i``
        directly used to attribute the REAL Date column's typed values to
        ``column_names[1]`` ("Amount"), while ``column_names[0]`` ("Date")
        never saw any of its own values (it read the blank spacer's Nones
        instead) — so a valid file was refused with "no recognized date
        format" while "Amount" was wrongly flagged as a native date column.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        # Column 0 is an entirely blank spacer — header AND every data
        # cell blank — which is exactly what pl.read_excel's
        # drop_empty_cols default elides from df.columns.
        ws.append([None, "Date", "Amount", "Description"])
        ws.append([None, datetime.date(2026, 1, 1), -4.50, "Coffee"])
        ws.append([None, datetime.date(2026, 1, 2), 100.00, "Salary"])
        path = tmp_path / "blank_spacer_before_date.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))

        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert result.excel_native_date_columns == frozenset({"Date"})

    def test_native_date_typed_scan_is_bounded_to_sample_rows(
        self, tmp_path: Path
    ) -> None:
        """The typed scan stops at ``sample_rows``, not the whole column.

        Performance finding: an unconditional full-sheet ``openpyxl`` pass
        cost ~8.0s of a ~14.0s ``read_file()`` call on a real 49,999-row x
        15-column file (measured; see ``_EXCEL_NATIVE_DATE_SAMPLE_ROWS``).
        Fixed by capping the scan at ``sample_rows`` data rows (production
        default 2,000). Proves the bound is actually applied — not just
        documented — by adding data ONLY beyond row 3 and showing a
        ``sample_rows=3`` probe never sees it: 7 total rows, 4 of them
        genuine native dates (a clear whole-column majority), but the
        first 3 rows are all non-date placeholders, so a probe bounded to
        those first 3 rows correctly reports no majority within the
        sample. This is the accepted cost named in the module docstring —
        a dirty run at the very START of a column, longer than
        ``sample_rows``, now reads as non-date — verified here directly
        rather than only asserted.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        for i in range(3):
            ws.append(["pending", i, "x"])
        for i in range(4):
            ws.append([datetime.date(2026, 1, i + 1), i, "x"])
        path = tmp_path / "dirty_prefix_exceeds_sample_bound.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"))
        assert result.sheet_used is not None
        column_names = list(result.df.columns)

        # Unbounded (sample_rows covers every data row): whole-column
        # majority is 4-of-7 dates -> qualifies. Positive control proving
        # the fixture itself has a real whole-column majority.
        unbounded = _excel_native_date_columns(
            path,
            result.sheet_used,
            data_start_row=1,
            has_header=True,
            column_names=column_names,
            sample_rows=7,
        )
        assert unbounded == frozenset({"Date"})

        # Bounded to the first 3 (all-dirty) rows: 0-of-3 -> no majority
        # within the sample, even though the whole column would qualify.
        bounded = _excel_native_date_columns(
            path,
            result.sheet_used,
            data_start_row=1,
            has_header=True,
            column_names=column_names,
            sample_rows=3,
        )
        assert bounded == frozenset()

    def test_explicit_skip_rows_pointed_at_data_row_is_flagged(
        self, tmp_path: Path
    ) -> None:
        """An explicit skip_rows pointed at a real data row must be flagged.

        Mirrors the CSV defense-in-depth check
        (test_header_row_looks_like_data_true_for_wrong_explicit_skip_rows):
        an explicit skip_rows always implies has_header=True with no safety
        check of its own, so a caller that mis-specifies skip_rows against a
        genuinely headerless sheet must still surface the red flag.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-01-01", 42.50, "Coffee"])
        ws.append(["2026-01-02", 10.00, "Tea"])
        path = tmp_path / "headerless.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"), skip_rows=0)
        assert result.has_header is True
        assert result.header_row_looks_like_data is True

    def test_explicit_skip_rows_native_date_pointed_at_data_row_is_flagged(
        self, tmp_path: Path
    ) -> None:
        """Same red flag, but the skipped row's date is a native Excel date.

        Regression for the reopened MB-449 gap: the explicit-skip_rows check
        used to classify ``df.columns`` (fastexcel's post-hoc stringification
        of the consumed header row) instead of the raw sampled cell. A
        ``datetime.date`` value (not a string) makes openpyxl store the date
        column as a native date cell — the shape
        test_explicit_skip_rows_pointed_at_data_row_is_flagged's
        string-valued date does not cover, and the one fastexcel's column
        naming doesn't reliably render back into a recognized date string.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append([datetime.date(2026, 1, 1), 42.50, "Coffee"])
        ws.append([datetime.date(2026, 1, 2), 10.00, "Tea"])
        path = tmp_path / "headerless_native_date.xlsx"
        wb.save(path)

        result = read_file(path, FormatInfo(file_type="excel"), skip_rows=0)
        assert result.has_header is True
        assert result.header_row_looks_like_data is True

    def test_persisted_headerless_decision_survives_explicit_skip_rows(
        self, tmp_path: Path
    ) -> None:
        """A confirm/replay read must honor a persisted has_header=False.

        The reviewed-plan replay path (import_service._import_tabular) always
        passes the previewed skip_rows explicitly, so has_header must be
        threaded through separately or a previewed headerless decision would
        flip back to has_header=True on the confirming read.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["2026-01-01", 42.50, "Coffee"])
        ws.append(["2026-01-02", 10.00, "Tea"])
        path = tmp_path / "headerless.xlsx"
        wb.save(path)

        result = read_file(
            path,
            FormatInfo(file_type="excel"),
            skip_rows=0,
            has_header=False,
        )
        assert result.has_header is False
        assert len(result.df) == 2

    def test_openpyxl_failure_falls_back_instead_of_raising(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """A workbook openpyxl can't open must not crash the read.

        The auto-detection sample (_excel_sample_rows) is new in this PR and
        runs whenever skip_rows is None, even when the caller supplied
        --sheet — a combination that used to bypass openpyxl entirely and
        let calamine/fastexcel (which reads legacy .xls, unlike openpyxl)
        handle the file alone. Simulates openpyxl's real failure mode
        (InvalidFileException — exactly what it raises for a file format it
        was never built to open, such as legacy binary .xls) and confirms
        the reader falls back to the pre-detection default (row 0 is the
        header) instead of propagating the exception. Mocks only
        openpyxl.load_workbook; pl.read_excel underneath is real, so this
        also proves the fallback's read actually succeeds.
        """
        import openpyxl
        from openpyxl.utils.exceptions import InvalidFileException

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "unreadable_by_openpyxl.xlsx"
        wb.save(path)

        mocker.patch(
            "openpyxl.load_workbook",
            side_effect=InvalidFileException("unsupported format"),
        )

        result = read_file(path, FormatInfo(file_type="excel"), sheet="Sheet")
        assert result.skip_rows == 0
        assert result.has_header is True
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert result.df["Date"].to_list() == ["2026-01-01"]

    def test_bytes_path_openpyxl_bad_zip_falls_back_instead_of_raising(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """The MCP replay path (source_bytes, no filesystem path) fails differently.

        Given a path, openpyxl runs its own extension check first and raises
        InvalidFileException for a legacy .xls (the case the test above
        covers). Given bytes (BytesIO — the shape import_service.py's
        reviewed_plan replay passes: ``read_file(..., source_bytes=...)``
        with no path openpyxl ever inspects), openpyxl never sees a
        filename, skips straight to ``ZipFile(...)``, and a legacy .xls
        (an OLE2 compound file, not a zip) fails there instead with
        zipfile.BadZipFile — a second, distinct exception type the sampler's
        except clause must also catch, or an agent replaying a legacy .xls
        through MCP still hits an uncaught exception even though the CLI
        path-based case is fixed.

        Drives the sampler with the real leading bytes of an OLE2 compound
        file (legacy .xls's actual container format) so openpyxl raises
        BadZipFile on its own, not because a mock says so. Only the
        downstream pl.read_excel is mocked -- standing in for calamine
        actually parsing a well-formed legacy .xls stream, which this
        fabricated 8-byte header alone is not enough to be -- so the
        assertions stay clean while the failure mode under test is real.

        The stub carries a genuine header row (column labels) followed by a
        data row: the fallback now actually classifies header/headerless via
        a raw fastexcel probe (see _classify_excel_headerless_via_fastexcel)
        rather than hardcoding has_header=True, so a stub shaped like real
        headered data is what makes has_header=True a classification result
        rather than an unverified assumption.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "column_1": ["Date", "2026-01-01"],
            "column_2": ["Amount", "42.5"],
            "column_3": ["Description", "Coffee"],
        })
        mocker.patch("polars.read_excel", return_value=stub_df)

        result = read_file(
            tmp_path / "legacy_replay.xls",
            FormatInfo(file_type="excel"),
            sheet="Sheet1",
            source_bytes=ole2_magic_bytes,
        )
        assert result.skip_rows == 0
        assert result.has_header is True

    def test_explicit_skip_rows_openpyxl_failure_falls_back_instead_of_raising(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """A saved/matched format's explicit skip_rows must not crash either.

        Before this fix, the EXPLICIT skip_rows branch's defense-in-depth
        check (_excel_row_looks_like_data_at, called only when
        explicit_skip and resolved_has_header) had no guard around openpyxl
        at all — unlike the auto-detect branch above, which already falls
        back on InvalidFileException/BadZipFile. A .xls-sourced saved/matched
        TabularFormat (file_type is always "excel" — see
        format_detector.py's _EXTENSION_MAP — never the literal "xls") with
        skip_rows > 0 went from working (pre-PR: calamine/fastexcel handled
        the whole read, openpyxl was never touched) to crashing with an
        unhandled InvalidFileException.
        Mirrors test_openpyxl_failure_falls_back_instead_of_raising's mocking
        shape, but with an explicit skip_rows so the OTHER call site is the
        one under test.
        """
        import openpyxl
        from openpyxl.utils.exceptions import InvalidFileException

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "unreadable_by_openpyxl.xlsx"
        wb.save(path)

        mocker.patch(
            "openpyxl.load_workbook",
            side_effect=InvalidFileException("unsupported format"),
        )

        result = read_file(
            path, FormatInfo(file_type="excel"), skip_rows=0, sheet="Sheet"
        )
        assert result.skip_rows == 0
        assert result.has_header is True
        assert result.header_row_looks_like_data is False
        assert list(result.df.columns) == ["Date", "Amount", "Description"]

    def test_explicit_skip_rows_bytes_bad_zip_falls_back_instead_of_raising(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Same defect, MCP confirm-after-preview replay (bytes) shape.

        Mirrors test_bytes_path_openpyxl_bad_zip_falls_back_instead_of_raising,
        but with an explicit skip_rows so the defense-in-depth call
        (_excel_row_looks_like_data_at) is what hits the real OLE2 bytes —
        given bytes, openpyxl skips its filename check and fails inside
        ZipFile(...) with zipfile.BadZipFile rather than InvalidFileException.
        Drives the sampler with the real leading bytes of an OLE2 compound
        file so openpyxl raises BadZipFile on its own, not because a mock
        says so; only the downstream pl.read_excel is mocked, standing in for
        calamine actually parsing a well-formed legacy .xls stream.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "Date": ["2026-01-01"],
            "Amount": ["42.5"],
            "Description": ["Coffee"],
        })
        mocker.patch("polars.read_excel", return_value=stub_df)

        result = read_file(
            tmp_path / "legacy_replay.xls",
            FormatInfo(file_type="excel"),
            skip_rows=0,
            sheet="Sheet1",
            source_bytes=ole2_magic_bytes,
        )
        assert result.skip_rows == 0
        assert result.has_header is True
        assert result.header_row_looks_like_data is False
        assert list(result.df.columns) == ["Date", "Amount", "Description"]

    def test_headerless_legacy_xls_not_eaten_as_header(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """A genuinely headerless legacy .xls must not lose its first row.

        When openpyxl can't open the container at all (a real legacy .xls —
        openpyxl never supported the binary format) AND no explicit
        skip_rows or sheet is supplied, ``sheet_used`` stays ``None``
        through auto-detection. Unconditionally setting ``skip_rows = 0``
        with no classification signal at all would reintroduce MB-449 for
        exactly this one file variant, since a headerless file's real first
        transaction row would be eaten as the header. Classifying via a raw
        unheadered fastexcel/calamine read instead (the engine that CAN open
        a legacy .xls) gives the same ``_classify_header_rows`` signal the
        openpyxl-backed path gets.

        Drives the real (unmocked) openpyxl.load_workbook with the actual
        leading bytes of an OLE2 compound file (legacy .xls's real container
        format) via ``source_bytes`` — no ``sheet=`` is passed, so this
        genuinely reaches the sheet-detection openpyxl call (unlike the
        existing InvalidFileException/BadZipFile fallback tests, which all
        pass an explicit ``sheet=`` and so never leave ``sheet_used`` as
        ``None``) — so openpyxl fails on its own, not because a mock says
        so. Only ``polars.read_excel`` is mocked, standing in for
        calamine actually parsing a well-formed legacy .xls stream.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        # infer_schema_length=0 forces every column to string dtype (see
        # _excel_native_date_columns's docstring) — mirror that shape so the
        # stub behaves like a real unheadered read for _classify_header_rows,
        # which indexes raw string cells.
        stub_df = pl.DataFrame({
            "column_1": ["2026-01-01", "2026-01-02"],
            "column_2": ["42.5", "10"],
            "column_3": ["Coffee", "Tea"],
        })
        mocker.patch("polars.read_excel", return_value=stub_df)

        result = read_file(
            tmp_path / "legacy_headerless.xls",
            FormatInfo(file_type="excel"),
            source_bytes=ole2_magic_bytes,
        )

        assert result.has_header is False
        assert len(result.df) == 2
        assert result.excel_native_date_columns is None

    def test_headerless_legacy_xls_native_date_not_eaten_as_header(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """A headerless legacy .xls with native-date cells must not be eaten.

        pl.read_excel(infer_schema_length=0) renders a native Excel
        date/datetime cell as calamine's own text shape
        ("2026-01-01 00:00:00", NOT the bare "2026-01-01" the sibling test
        above stubs), but the rows are passed to _classify_header_rows
        unnormalized. _DATE_FORMATS is date-only, so that time suffix defeats
        date detection exactly as it would for the openpyxl-backed probe
        (_excel_cell_text exists for precisely this reason) — the classifier
        finds no row that looks like data, falls through to its own
        (0, True, False) default, and a genuinely headerless file has its
        first real transaction consumed as the header.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "column_1": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
            "column_2": ["42.5", "10"],
            "column_3": ["Coffee", "Tea"],
        })
        mocker.patch("polars.read_excel", return_value=stub_df)

        result = read_file(
            tmp_path / "legacy_headerless_nativedate.xls",
            FormatInfo(file_type="excel"),
            source_bytes=ole2_magic_bytes,
        )

        assert result.has_header is False
        assert len(result.df) == 2

    def test_headerless_legacy_xls_with_explicit_sheet_not_eaten_as_header(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """The explicit-``--sheet`` legacy-.xls path needs the same classification.

        The calamine-based headerless classification above only runs when
        ``sheet_used is None`` (no
        ``--sheet``/saved-format sheet given). A caller-supplied sheet name
        skips straight to ``_excel_sample_rows``, which raises
        InvalidFileException/BadZipFile on a genuine legacy .xls same as
        before — but the except clause hardcoded ``skip_rows = 0``
        (headered) instead of running the SAME fastexcel fallback, silently
        reintroducing MB-449 for exactly this one variant. Mirrors the
        no-``--sheet`` test above but passes ``sheet="Sheet1"`` explicitly.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "column_1": ["2026-01-01", "2026-01-02"],
            "column_2": ["42.5", "10"],
            "column_3": ["Coffee", "Tea"],
        })
        mocker.patch("polars.read_excel", return_value=stub_df)

        result = read_file(
            tmp_path / "legacy_headerless.xls",
            FormatInfo(file_type="excel"),
            sheet="Sheet1",
            source_bytes=ole2_magic_bytes,
        )

        assert result.has_header is False
        assert len(result.df) == 2

    def test_invalid_sheet_name_raises_clean_error_not_keyerror(
        self, tmp_path: Path
    ) -> None:
        """An invalid --sheet name must raise a classified error, not KeyError.

        The auto-detect classification branch calls ``_excel_sample_rows``
        whenever ``sheet_used`` is non-None and no explicit ``skip_rows`` is
        given. ``_excel_sample_rows`` does ``wb[sheet_name]`` with no KeyError
        guard, and openpyxl's ``Workbook.__getitem__`` raises a bare
        ``KeyError`` for an unknown sheet name — which
        ``src/moneybin/errors.py`` deliberately excludes from its generic
        ``LookupError`` classification, so it used to reach the caller as an
        unclassified traceback instead of a clean error. The real
        ``pl.read_excel`` read independently raises its own ``ValueError``
        for the same bad sheet name (already classified by
        ``handle_cli_errors``), so the fix routes the KeyError into the same
        fastexcel-fallback helper the legacy-.xls case uses, which lets that
        clean ValueError propagate instead of the raw KeyError.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.title = "Sheet1"
        ws.append(["Date", "Amount", "Description"])
        ws.append(["2026-01-01", 42.50, "Coffee"])
        path = tmp_path / "basic.xlsx"
        wb.save(path)

        with pytest.raises(ValueError, match="no matching sheet"):
            read_file(path, FormatInfo(file_type="excel"), sheet="NoSuchSheet")

    def test_headerless_classification_probe_bounds_the_read_to_30_rows(
        self, mocker: MockerFixture
    ) -> None:
        """The legacy-.xls classification probe must cap the read at 30 rows.

        Performance finding: materializing the WHOLE sheet and slicing
        with ``.head(30)`` afterward parses every row this function never
        uses, and ``_read_excel``'s real read parses the whole sheet again
        right after — a large legacy ``.xls`` paid for two complete
        parses, including files later rejected by the row limit. Pins the
        call shape (Mock Boundaries, testing.md): the fix asks
        ``pl.read_excel`` for ``n_rows=30`` directly rather than relying on
        a post-hoc slice, so this asserts the real argument reaches the
        read instead of trusting the returned value alone (which looks
        identical either way).
        """
        stub_df = pl.DataFrame({"c1": ["Date"], "c2": ["Amount"]})
        mock_read = mocker.patch("polars.read_excel", return_value=stub_df)

        _classify_excel_headerless_via_fastexcel(
            Path("/nonexistent.xls"), sheet_name="Sheet1", source_bytes=b"stub"
        )

        _, kwargs = mock_read.call_args
        assert kwargs["read_options"].get("n_rows") == 30


class TestNormalizeExcelDateColumns:
    """Unit tests for readers.normalize_excel_date_columns.

    This is Excel-only and public (no leading underscore): import_service.py's
    _import_tabular calls it after Stage 3 resolves column mapping and the
    effective date format, NOT this module's own _read_excel (which cannot
    decide correctly — see the function's docstring for why). These tests
    exercise the pure function directly rather than through read_file.
    """

    def test_non_midnight_timestamp_text_passes_through_unmodified(self) -> None:
        """A real (non-midnight) timestamp string must not be truncated.

        normalize_excel_date_columns only collapses the exact
        "<date> 00:00:00" shape fastexcel renders for a native Excel *date*
        cell (always midnight — Excel has no separate date type). A string
        cell that happens to hold a genuine timestamp with a real time of
        day must be left alone: pinning this stops the regex from silently
        re-widening to match any time, which would mutate a raw column value
        AGENTS.md's data-layer contract says loaders must leave untouched.
        """
        df = pl.DataFrame({
            "Posted At": ["2026-01-01 14:30:00"],
            "Amount": ["42.5"],
            "Description": ["Coffee"],
        })

        result, rewritten = normalize_excel_date_columns(df)

        assert result["Posted At"].to_list() == ["2026-01-01 14:30:00"]
        assert rewritten == frozenset()

    def test_minority_midnight_match_column_left_untouched(self) -> None:
        """A column where midnight-shaped values are the MINORITY is untouched.

        No ``native_date_columns`` is supplied here, so this exercises the
        tolerant-MAJORITY text-shape fallback (openpyxl couldn't type the
        file). A 1-of-2 match is not a majority, so the Description column
        must be left alone — raw is untouched data from loaders (AGENTS.md's
        Data Layers table) — while a genuine all-midnight native date column
        beside it is still normalized.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
            "Amount": ["42.5", "10"],
            "Description": ["2026-01-01 00:00:00", "Coffee"],
        })

        result, rewritten = normalize_excel_date_columns(df)

        assert result["Date"].to_list() == ["2026-01-01", "2026-01-02"]
        assert result["Description"].to_list() == ["2026-01-01 00:00:00", "Coffee"]
        assert rewritten == frozenset({"Date"})

    def test_majority_shape_fallback_tolerates_one_dirty_value(self) -> None:
        """A dirty minority must not void normalization for the whole column.

        A single stray non-date value (e.g. "pending") must not disqualify
        an otherwise-native-date column and revert the file to a "no
        recognized date format" refusal. The fallback heuristic (no
        ``native_date_columns`` — openpyxl couldn't type the file) qualifies
        a column when a STRICT MAJORITY of its non-null values
        match, matching the dirty-minority tolerance ``_parse_dates``/
        ``_validate_date_format_override`` already apply downstream.
        """
        df = pl.DataFrame({
            "Date": [
                "2026-01-01 00:00:00",
                "2026-01-02 00:00:00",
                "2026-01-03 00:00:00",
                "pending",
            ],
        })

        result, rewritten = normalize_excel_date_columns(df)

        assert result["Date"].to_list() == [
            "2026-01-01",
            "2026-01-02",
            "2026-01-03",
            "pending",
        ]
        assert rewritten == frozenset({"Date"})

    def test_columns_restriction_protects_a_qualifying_column_outside_it(
        self,
    ) -> None:
        """Scoping to the resolved date column protects an unrelated match.

        import_service.py passes columns=[mapped_date_col] once Stage 3
        knows the mapping, so an all-midnight-shaped column that ISN'T the
        mapped date field (e.g. a coincidentally uniform second export
        column) is never touched, even though it would qualify under a
        broad scan — the auto-detect case (no mapping yet) still passes
        columns=None and scans broadly, matching what map_columns itself
        needs to find an unaliased date column via content.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
            "Memo": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
        })

        result, rewritten = normalize_excel_date_columns(df, columns=["Date"])

        assert result["Date"].to_list() == ["2026-01-01", "2026-01-02"]
        assert result["Memo"].to_list() == [
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
        ]
        assert rewritten == frozenset({"Date"})

    def test_native_date_columns_ignores_shape_and_uses_typed_identity(
        self,
    ) -> None:
        """Typed selection never touches a column that only LOOKS like a date.

        An unscoped whole-column shape scan can truncate a genuine text
        column (e.g. Memo) if every value happens to render in
        the "<date> 00:00:00" shape. When ``native_date_columns`` is
        supplied (openpyxl typed the file), selection is exact-identity —
        driven by which columns openpyxl reports as natively date-typed, not
        by scanning rendered text — so a coincidentally uniform Memo column
        is left alone even though it would qualify under the shape-based
        fallback.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
            "Memo": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
        })

        result, rewritten = normalize_excel_date_columns(
            df, native_date_columns=frozenset({"Date"})
        )

        assert result["Date"].to_list() == ["2026-01-01", "2026-01-02"]
        assert result["Memo"].to_list() == [
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
        ]
        assert rewritten == frozenset({"Date"})

    def test_native_date_columns_empty_set_normalizes_nothing(self) -> None:
        """Openpyxl typing the file but finding no date column normalizes nothing.

        An empty ``frozenset`` (as opposed to ``None``) means openpyxl
        opened the file fine and found no natively-typed date column — even
        a perfectly shaped midnight-text column must not be rewritten from
        shape alone once a typed answer is available.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
        })

        result, rewritten = normalize_excel_date_columns(
            df, native_date_columns=frozenset()
        )

        assert result["Date"].to_list() == [
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
        ]
        assert rewritten == frozenset()


class TestDateFormatHasTimeComponent:
    """Unit tests for readers.date_format_has_time_component."""

    def test_locale_dependent_x_directive_counts_as_time_bearing(self) -> None:
        """``%X`` (locale time representation) must count as a time directive.

        Reviewer NIT: the original check only recognized %H/%M/%S/%I/%p, so
        a declared format using the locale-dependent %X directive would be
        misjudged as date-only and have its native-date column normalized
        (and truncated) out from under it.
        """
        assert date_format_has_time_component("%Y-%m-%d %X") is True

    def test_bare_date_format_has_no_time_component(self) -> None:
        assert date_format_has_time_component("%m/%d/%Y") is False

    def test_none_has_no_time_component(self) -> None:
        assert date_format_has_time_component(None) is False

    def test_literal_midnight_suffix_counts_as_time_bearing(self) -> None:
        """A LITERAL time suffix (no strptime directive) must count too.

        Codex finding: the prior implementation only scanned for known
        directives (%H/%M/%S/%I/%p/%X), so a format like
        ``"%Y-%m-%d 00:00:00"`` — spelling the time as literal characters,
        not a directive, e.g. built by formatting a sample value rather
        than hand-written — was misjudged as date-only. That let
        normalize_excel_date_columns_before_mapping rewrite the column to a
        bare date, after which the declared format could no longer parse
        it (a trailing literal "00:00:00" the rewritten text no longer
        has). The fix tries the actual parse against the representative raw
        shape (``_excel_cell_text``'s "2026-01-01 00:00:00") instead of
        scanning for substrings, so any spelling of a time-bearing raw
        shape is caught, not just known directives.
        """
        assert date_format_has_time_component("%Y-%m-%d 00:00:00") is True

    def test_literal_midnight_suffix_skips_normalization(self) -> None:
        """End-to-end: the literal-suffix format must survive normalization.

        Exercises normalize_excel_date_columns_before_mapping directly (not
        just the boolean helper) to prove the literal-suffix format
        actually prevents the rewrite that would otherwise break it.
        """
        df = pl.DataFrame({"Date": ["2026-01-01 00:00:00"], "Amount": ["1.00"]})
        normalized, effective_format = normalize_excel_date_columns_before_mapping(
            df,
            file_type="excel",
            date_format="%Y-%m-%d 00:00:00",
            date_column="Date",
            native_date_columns=frozenset({"Date"}),
        )
        # Unchanged: the literal-suffix format expects the raw text as-is.
        assert normalized["Date"].to_list() == ["2026-01-01 00:00:00"]
        assert effective_format == "%Y-%m-%d 00:00:00"

    def test_fractional_seconds_format_recognized_via_actual_column(self) -> None:
        """A raw shape neither prior probe carries must still be recognized.

        Codex finding: the representative-shape probe ("2000-01-02
        00:00:00") has no fractional-second suffix, so a declared
        "%Y-%m-%d %H:%M:%S.%f" — a real raw shape for a native Excel
        datetime with sub-second precision — tested False against it. This
        is the fourth iteration on this helper (directive list -> %X ->
        synthetic-probe -> now this), and each fix left the same residual
        weakness: guessing the raw shape instead of reading it. The fix
        reads the actual column instead of guessing: when date_column names
        a real df column, it checks whether date_format parses that
        column's own text (format_parses), so any raw shape is recognized,
        not just ones a probe author anticipated.
        """
        assert date_format_has_time_component("%Y-%m-%d %H:%M:%S.%f") is False, (
            "sanity check: the synthetic-probe fallback (no column given) "
            "still can't recognize this shape -- confirms the old behavior "
            "is unchanged when no column is available, per this function's "
            "own documented fallback."
        )
        df = pl.DataFrame({
            "Date": ["2026-01-02 00:00:00.123000", "2026-01-03 00:00:00.456000"],
            "Amount": ["42.5", "10"],
        })
        assert (
            date_format_has_time_component(
                "%Y-%m-%d %H:%M:%S.%f", df=df, date_column="Date"
            )
            is True
        )

    def test_fractional_seconds_format_skips_normalization(self) -> None:
        """End-to-end: the fractional-seconds format must survive normalization.

        Mirrors test_literal_midnight_suffix_skips_normalization for the
        raw shape the representative-probe approach could never anticipate.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-02 00:00:00.123000", "2026-01-03 00:00:00.456000"],
            "Amount": ["42.5", "10"],
        })
        normalized, effective_format = normalize_excel_date_columns_before_mapping(
            df,
            file_type="excel",
            date_format="%Y-%m-%d %H:%M:%S.%f",
            date_column="Date",
            native_date_columns=frozenset({"Date"}),
        )
        assert normalized["Date"].to_list() == [
            "2026-01-02 00:00:00.123000",
            "2026-01-03 00:00:00.456000",
        ]
        assert effective_format == "%Y-%m-%d %H:%M:%S.%f"


# Case grid for normalize_excel_date_columns_before_mapping's core invariant:
# after normalization, every non-null value in every mapped date column is in
# a representation the returned format parses. transaction_date is always
# mapped; post_date is optional. "native" cells are fastexcel's raw rendering
# of an Excel-native date cell; "text" cells are already in the shape a
# reader would parse (the declared format when known, ISO when not -- a file
# whose text dates are ISO-shaped needs no format declared to be readable);
# "mixed" alternates the two.
_ROLE_DATES = [
    datetime.date(2026, 1, 1),
    datetime.date(2026, 1, 2),
    datetime.date(2026, 1, 3),
    datetime.date(2026, 1, 4),
]
_POST_DATES = [
    datetime.date(2026, 1, 5),
    datetime.date(2026, 1, 6),
    datetime.date(2026, 1, 7),
    datetime.date(2026, 1, 8),
]


def _render_role(dates: list[datetime.date], role: str, text_format: str) -> list[str]:
    def native(d: datetime.date) -> str:
        return f"{d.isoformat()} 00:00:00"

    def text(d: datetime.date) -> str:
        return d.strftime(text_format)

    if role == "native":
        return [native(d) for d in dates]
    if role == "text":
        return [text(d) for d in dates]
    assert role == "mixed"
    return [native(d) if i % 2 == 0 else text(d) for i, d in enumerate(dates)]


_CELL_KINDS = (
    "native_midnight",
    "valid_text_declared_shape",
    "iso_text",
    "iso_text_invalid",
    "midnight_invalid",
    "none",
)


def _cell_kind_case(
    kind: str, declared_format: str | None, *, month: int, day: int
) -> tuple[str | None, str | None]:
    """(input cell text, expected output text) for one cell kind.

    ``month`` has 30 days (April/June) so ``day=31`` is unambiguously
    calendar-invalid for the two "invalid" kinds, without touching February.
    """
    valid_date = datetime.date(2026, month, day)
    if kind == "native_midnight":
        value = f"{valid_date.isoformat()} 00:00:00"
        if declared_format is None:
            return value, valid_date.isoformat()
        return value, valid_date.strftime(declared_format)
    if kind == "valid_text_declared_shape":
        rendered = (
            valid_date.strftime(declared_format)
            if declared_format is not None
            else valid_date.isoformat()
        )
        return rendered, rendered
    if kind == "iso_text":
        return valid_date.isoformat(), valid_date.isoformat()
    if kind == "iso_text_invalid":
        invalid = f"2026-{month:02d}-31"
        return invalid, invalid
    if kind == "midnight_invalid":
        invalid_iso = f"2026-{month:02d}-31"
        value = f"{invalid_iso} 00:00:00"
        if declared_format is None:
            return value, invalid_iso
        return value, value
    assert kind == "none"
    return None, None


class TestNormalizeExcelDateCellInvariant:
    """Cell-level grid: cell kind x declared format x column role.

    The invariant: in every mapped date column of the FINAL mapping,
    rewrite exactly the cells whose text matches the native-midnight
    rendering. Render them into the effective date_format, or bare ISO
    when no format is known yet. Every other cell -- already-correct text,
    ISO-shaped text (valid or calendar-invalid), an invalid midnight cell --
    passes through byte-identical, and nothing ever raises.
    """

    @pytest.mark.parametrize(
        "declared_format",
        [None, "%Y-%m-%d", "%m/%d/%Y", "%Y-%d-%m", "%m/%d/%Y %H:%M:%S"],
    )
    @pytest.mark.parametrize(
        "column_role",
        [
            "transaction_date_only",
            "post_date_mapped_upfront",
            "post_date_mapped_later",
        ],
    )
    def test_every_cell_kind_survives_or_rewrites_correctly(
        self, column_role: str, declared_format: str | None
    ) -> None:
        cases = {
            kind: _cell_kind_case(kind, declared_format, month=4, day=5)
            for kind in _CELL_KINDS
        }
        date_col = [cases[k][0] for k in _CELL_KINDS]
        expected_date = [cases[k][1] for k in _CELL_KINDS]

        if column_role == "transaction_date_only":
            df = pl.DataFrame({"Date": date_col})
            normalized, effective_format = normalize_excel_date_columns_before_mapping(
                df, file_type="excel", date_format=declared_format, date_column="Date"
            )
            assert normalized["Date"].to_list() == expected_date
            assert effective_format == declared_format
            return

        # post_date uses a different month/day so a transposition bug
        # between the two columns is visible in the assertion.
        posted_cases = {
            kind: _cell_kind_case(kind, declared_format, month=6, day=6)
            for kind in _CELL_KINDS
        }
        posted_col = [posted_cases[k][0] for k in _CELL_KINDS]
        posted_expected = [posted_cases[k][1] for k in _CELL_KINDS]

        if column_role == "post_date_mapped_upfront":
            df = pl.DataFrame({"Date": date_col, "Posted": posted_col})
            normalized, _ = normalize_excel_date_columns_before_mapping(
                df,
                file_type="excel",
                date_format=declared_format,
                date_column="Date",
                additional_date_columns=["Posted"],
            )
            assert normalized["Date"].to_list() == expected_date
            assert normalized["Posted"].to_list() == posted_expected
            return

        assert column_role == "post_date_mapped_later"
        df = pl.DataFrame({"Date": date_col, "Posted": posted_col})
        # Only transaction_date is known before mapping resolves (a partial
        # first-contact --mapping override names just the one column).
        stage1, _ = normalize_excel_date_columns_before_mapping(
            df, file_type="excel", date_format=declared_format, date_column="Date"
        )
        assert stage1["Date"].to_list() == expected_date
        # Posted is untouched -- it wasn't a known mapped column yet.
        assert stage1["Posted"].to_list() == posted_col
        # map_columns later aliases "Posted" to post_date; the after-mapping
        # pass must finish normalizing it against the final mapping.
        stage2 = normalize_excel_date_columns_after_mapping(
            stage1,
            file_type="excel",
            field_mapping={"transaction_date": "Date", "post_date": "Posted"},
            date_format=declared_format,
        )
        assert stage2["Date"].to_list() == expected_date
        assert stage2["Posted"].to_list() == posted_expected


class TestNormalizeExcelDateColumnsBeforeMappingGrid:
    """Parametrized grid: transaction_date shape x post_date shape x format.

    Exercises every combination of column-level native/text/mixed shape
    against format known/unknown, so no mapped column (or cell within one)
    is left in a shape the eventual format cannot parse.
    """

    @pytest.mark.parametrize(
        ("transaction_date_role", "post_date_role", "format_known"),
        [
            (txn_role, post_role, format_known)
            for txn_role in ("native", "text", "mixed")
            for post_role in ("absent", "native", "text", "mixed")
            for format_known in (True, False)
        ],
    )
    def test_every_non_null_date_survives_normalization(
        self,
        transaction_date_role: str,
        post_date_role: str,
        format_known: bool,  # parametrize id, not a call-site bool trap
    ) -> None:
        declared_format = "%m/%d/%Y" if format_known else None
        # The shape a genuinely non-native text cell already carries when no
        # format is declared -- ISO, since that's the one shape this
        # function's own auto-detect-free path can still leave consistent.
        text_shape = declared_format or "%Y-%m-%d"

        columns: dict[str, list[str]] = {
            "Date": _render_role(_ROLE_DATES, transaction_date_role, text_shape),
            "Amount": ["-4.50", "10.00", "42.50", "5.00"],
        }
        additional_date_columns: list[str] | None = None
        if post_date_role != "absent":
            columns["Posted"] = _render_role(_POST_DATES, post_date_role, text_shape)
            additional_date_columns = ["Posted"]

        df = pl.DataFrame(columns)
        rows_before = len(df)

        normalized, effective_format = normalize_excel_date_columns_before_mapping(
            df,
            file_type="excel",
            date_format=declared_format,
            date_column="Date",
            additional_date_columns=additional_date_columns,
        )

        assert len(normalized) == rows_before

        parse_format = effective_format if effective_format is not None else "%Y-%m-%d"
        expectations = [("Date", _ROLE_DATES)]
        if post_date_role != "absent":
            expectations.append(("Posted", _POST_DATES))
        for column, expected_dates in expectations:
            values = normalized[column].to_list()
            assert len(values) == len(expected_dates)
            for value, expected in zip(values, expected_dates, strict=True):
                assert value is not None, f"{column} lost a non-null value"
                parsed = datetime.datetime.strptime(value, parse_format).date()
                assert parsed == expected, f"{column}: {value!r} != {expected}"

    def test_mapped_column_normalizes_even_when_native_typing_excluded_it(
        self,
    ) -> None:
        """A mapped column normalizes regardless of ``native_date_columns`` membership.

        ``_excel_native_date_columns`` excludes a column from its frozenset
        when native cells are a MINORITY of the sample, so a
        mapped date column with SOME (but not most) native cells used to
        fail the same membership check here — those native cells stayed
        ``"YYYY-MM-DD 00:00:00"`` and failed the saved ``"%m/%d/%Y"``,
        silently dropping them. A mapped column is a date column regardless
        of its own native/text mix; ``native_date_columns`` no longer
        matters once the caller already knows the mapping.
        """
        df = pl.DataFrame({
            "Date": [
                "01/01/2026",
                "01/02/2026",
                "01/03/2026",
                "2026-01-04 00:00:00",  # the native minority
            ],
        })

        normalized, effective_format = normalize_excel_date_columns_before_mapping(
            df,
            file_type="excel",
            date_format="%m/%d/%Y",
            date_column="Date",
            # Deliberately excludes "Date" -- simulates
            # _excel_native_date_columns voting it out because native cells
            # (1 of 4) are a minority of the sample.
            native_date_columns=frozenset(),
        )

        assert normalized["Date"].to_list() == [
            "01/01/2026",
            "01/02/2026",
            "01/03/2026",
            "01/04/2026",
        ]
        assert effective_format == "%m/%d/%Y"


class TestParquetReader:
    """Tests for Parquet file reading."""

    def test_basic_parquet(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "date": ["2026-01-01"],
            "amount": [42.50],
            "description": ["Coffee"],
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        info = FormatInfo(file_type="parquet")
        result = read_file(path, info)
        assert len(result.df) == 1
        assert list(result.df.columns) == ["date", "amount", "description"]


class TestFeatherReader:
    """Tests for Feather/Arrow IPC file reading."""

    def test_basic_feather(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "date": ["2026-01-01"],
            "amount": [42.50],
            "description": ["Coffee"],
        })
        path = tmp_path / "test.feather"
        df.write_ipc(path)

        info = FormatInfo(file_type="feather")
        result = read_file(path, info)
        assert len(result.df) == 1


class TestHeaderlessAcrossFormats:
    """MB-449: a headerless file must read identically across all formats.

    The same 6-row headerless bank export, expressed as CSV, Parquet, and
    Excel, must each report has_header=False and rows_read == rows_in_file.
    Regression coverage for the Excel reader silently eating row 0 as a
    header while CSV and Parquet already detected the condition correctly.
    """

    _ROWS: tuple[tuple[str, str, str], ...] = (
        ("2026-01-01", "-42.50", "COFFEE SHOP 1111"),
        ("2026-01-02", "-10.00", "TEA HOUSE 1111"),
        ("2026-01-03", "-5.00", "BAKERY 1111"),
        ("2026-01-04", "1200.00", "PAYROLL DEPOSIT"),
        ("2026-01-05", "-25.00", "GROCERY STORE 1111"),
        ("2026-01-06", "-8.75", "PARKING GARAGE 1111"),
    )

    def test_csv(self, tmp_path: Path) -> None:
        content = "\n".join(",".join(row) for row in self._ROWS) + "\n"
        f = _write_csv(tmp_path / "headerless.csv", content)
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info)
        assert result.has_header is False
        assert len(result.df) == len(self._ROWS)
        assert result.rows_in_file == len(self._ROWS)

    def test_parquet(self, tmp_path: Path) -> None:
        df = pl.DataFrame({
            "column_1": [row[0] for row in self._ROWS],
            "column_2": [row[1] for row in self._ROWS],
            "column_3": [row[2] for row in self._ROWS],
        })
        path = tmp_path / "headerless.parquet"
        df.write_parquet(path)

        info = FormatInfo(file_type="parquet")
        result = read_file(path, info)
        assert result.has_header is False
        assert len(result.df) == len(self._ROWS)
        assert result.rows_in_file == len(self._ROWS)

    def test_xlsx(self, tmp_path: Path) -> None:
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        for row in self._ROWS:
            ws.append(list(row))
        path = tmp_path / "headerless.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert result.has_header is False
        assert len(result.df) == len(self._ROWS)
        assert result.rows_in_file == len(self._ROWS)

    def test_xlsx_with_native_date_cells(self, tmp_path: Path) -> None:
        """A spreadsheet-native export stores dates as date cells, not text.

        openpyxl hands those back as ``datetime``, whose ``str()`` carries a
        time suffix that the date-only ``_DATE_FORMATS`` cannot match — so
        without normalization the classifier sees no date anywhere, falls back
        to "row 0 is a header", and eats the first transaction exactly as
        before the fix. Every other fixture here writes the date as a string,
        which is a text cell, so this path is the one they leave uncovered.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        for row in self._ROWS:
            ws.append([
                datetime.date.fromisoformat(row[0]),
                float(row[1]),
                row[2],
            ])
        path = tmp_path / "headerless_native_dates.xlsx"
        wb.save(path)

        info = FormatInfo(file_type="excel")
        result = read_file(path, info)
        assert result.has_header is False
        assert len(result.df) == len(self._ROWS)
        assert result.rows_in_file == len(self._ROWS)
