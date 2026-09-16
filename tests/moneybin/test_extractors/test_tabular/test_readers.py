"""Tests for Stage 2 file readers."""

import datetime
from pathlib import Path

import polars as pl
import pytest
from pytest_mock import MockerFixture

from moneybin.extractors.tabular.column_mapper import map_columns
from moneybin.extractors.tabular.format_detector import FormatInfo
from moneybin.extractors.tabular.readers import (
    _classify_excel_headerless_via_fastexcel,  # pyright: ignore[reportPrivateUsage]
    _detect_header,  # pyright: ignore[reportPrivateUsage]
    _row_looks_like_data_at,  # pyright: ignore[reportPrivateUsage]
    normalize_excel_date_columns_after_mapping,
    normalize_excel_date_columns_for_detection,
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
        ) == (1, True, False, (), ())
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

    def test_headerless_csv_declared_date_format_keeps_first_row(
        self, tmp_path: Path
    ) -> None:
        """A declared date format outside ``_DATE_FORMATS`` must be recognized.

        Issue #604: ``_looks_like_data_row`` only recognized a date via the
        built-in ``_DATE_FORMATS`` list, so a genuinely headerless file whose
        dates use a format outside it (``%Y%m%d``) had no row that read as
        data — ``_classify_header_rows`` fell back to ``(0, True)`` and ate
        row 0 as a header with no way to recover it. Without the declared
        format this same file loses row 0; passing it through recognizes the
        row as data instead.
        """
        f = _write_csv(
            tmp_path / "headerless_yyyymmdd.csv",
            "20260105,42.50,Coffee\n20260106,10.00,Tea\n20260107,-20.00,Groceries\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")

        # Baseline: with no declared format, the pre-existing gap this issue
        # tracks still applies — row 0 is eaten as a header.
        baseline = read_file(f, info)
        assert baseline.has_header is True
        assert len(baseline.df) == 2

        result = read_file(f, info, declared_date_format="%Y%m%d")
        assert result.has_header is False
        assert len(result.df) == 3

    def test_declared_date_format_does_not_break_header_detection(
        self, tmp_path: Path
    ) -> None:
        """A declared format must not make a real header row look like data.

        Header labels (``Date``, ``Amount``, ``Description``) never parse as
        a date under any format, so passing a caller's ``--date-format``
        alongside a normally-headered file must leave detection unchanged.
        """
        f = _write_csv(
            tmp_path / "headered_yyyymmdd.csv",
            "Date,Amount,Description\n20260105,42.50,Coffee\n20260106,10.00,Tea\n",
        )
        info = FormatInfo(file_type="csv", delimiter=",", encoding="utf-8")
        result = read_file(f, info, declared_date_format="%Y%m%d")
        assert result.has_header is True
        assert list(result.df.columns) == ["Date", "Amount", "Description"]
        assert len(result.df) == 2

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

    def test_headerless_excel_declared_date_format_keeps_row0(
        self, tmp_path: Path
    ) -> None:
        """Excel mirrors the CSV fix for a declared date format (#604).

        A compact numeric date like ``20260105`` written as a plain number
        (not a native Excel date cell) reaches the classifier as literal
        text via ``_excel_cell_text`` (``str(value)``, since it is neither
        ``datetime.datetime`` nor ``datetime.date``) — ``%Y%m%d`` is outside
        ``_DATE_FORMATS``, so without the declared format this file loses
        row 0 the same way the CSV case does.
        """
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append([20260105, 42.50, "Coffee"])
        ws.append([20260106, 10.00, "Tea"])
        path = tmp_path / "headerless_yyyymmdd.xlsx"
        wb.save(path)

        baseline = read_file(path, FormatInfo(file_type="excel"))
        assert baseline.has_header is True
        assert len(baseline.df) == 1

        result = read_file(
            path, FormatInfo(file_type="excel"), declared_date_format="%Y%m%d"
        )
        assert result.has_header is False
        assert len(result.df) == 2

    def test_blank_spacer_column_before_a_native_date_column_imports_correctly(
        self, tmp_path: Path
    ) -> None:
        """A blank interior spacer column must not misalign the date column.

        A column-selection step that indexes a physical row position
        against ``df.columns`` is fragile: ``pl.read_excel``'s
        ``drop_empty_cols=True`` default elides a header-and-data-blank
        spacer, shifting ``df.columns`` out of alignment with the
        worksheet's physical layout. Rendering every string column by its
        OWN cell shape needs no such indexing step, so a blank spacer
        carries no column-identity risk at all.
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

        read_result = read_file(path, FormatInfo(file_type="excel"))
        assert list(read_result.df.columns) == ["Date", "Amount", "Description"]

        detection_df = normalize_excel_date_columns_for_detection(
            read_result.df, file_type="excel", date_format=None
        )
        mapping_result = map_columns(detection_df)
        assert mapping_result.field_mapping.get("transaction_date") == "Date"
        assert mapping_result.field_mapping.get("amount") == "Amount"
        assert mapping_result.date_format is not None

        rendered = normalize_excel_date_columns_after_mapping(
            read_result.df,
            file_type="excel",
            field_mapping=mapping_result.field_mapping,
            date_format=mapping_result.date_format,
        )
        dates = rendered["Date"].to_list()
        assert None not in dates
        assert [
            datetime.datetime.strptime(d, mapping_result.date_format).date()
            for d in dates
        ] == [datetime.date(2026, 1, 1), datetime.date(2026, 1, 2)]

    def test_date_column_with_8_iso_text_and_2_native_cells_imports_all_10(
        self, tmp_path: Path
    ) -> None:
        """A genuine native/text mix must not fail a strict-majority gate.

        A native-typed selection probe requiring a STRICT MAJORITY of
        native cells before a column qualifies for rendering would leave a
        Date column with only 2 native cells among 8 ISO-text ones (20%)
        unrendered — its 2 midnight-shaped cells would fail to parse under
        the detected format, dropping the parse rate to 80%, below the 90%
        auto-detect confidence bar. Rendering by per-cell shape, not by a
        whole-column vote, means the 2 native cells always render
        regardless of how the other 8 read.
        """
        import datetime

        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        assert ws is not None
        ws.append(["Date", "Amount", "Description"])
        for i in range(8):
            ws.append([f"2026-01-{i + 1:02d}", i, "text"])
        for i in range(2):
            ws.append([datetime.date(2026, 2, i + 1), i, "native"])
        path = tmp_path / "mixed_majority_text.xlsx"
        wb.save(path)

        read_result = read_file(path, FormatInfo(file_type="excel"))

        detection_df = normalize_excel_date_columns_for_detection(
            read_result.df, file_type="excel", date_format=None
        )
        mapping_result = map_columns(detection_df)
        assert mapping_result.field_mapping.get("transaction_date") == "Date"
        assert mapping_result.date_format is not None

        rendered = normalize_excel_date_columns_after_mapping(
            read_result.df,
            file_type="excel",
            field_mapping=mapping_result.field_mapping,
            date_format=mapping_result.date_format,
        )
        dates = rendered["Date"].to_list()
        assert len(dates) == 10
        assert None not in dates
        for d in dates:
            # Must not raise -- every one of the 10 cells parses cleanly.
            datetime.datetime.strptime(d, mapping_result.date_format)

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
        (``_excel_row_looks_like_data_at_bounded``) is what hits the real
        OLE2 bytes — given bytes, openpyxl skips its filename check and
        fails inside ZipFile(...) with zipfile.BadZipFile rather than
        InvalidFileException. Drives the sampler with the real leading
        bytes of an OLE2 compound file so openpyxl raises BadZipFile on its
        own, not because a mock says so.

        Round 19: this safety check now genuinely falls back to a bounded
        fastexcel/calamine probe rather than hardcoding False (the fixed
        regression), so ``polars.read_excel`` is mocked with a
        ``side_effect`` distinguishing the two DIFFERENT calls this read
        now makes — the bounded probe (``has_header=False``, standing in
        for calamine sampling one raw row) and the real full read
        (``has_header=True``, standing in for calamine actually parsing a
        well-formed legacy .xls stream) — rather than one mock serving
        both indiscriminately. The probed row here is a genuine header
        (labels, not a transaction), so the safety check still reads
        False; the positive case (the probed row IS a transaction) is
        covered separately.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "Date": ["2026-01-01"],
            "Amount": ["42.5"],
            "Description": ["Coffee"],
        })
        # The bounded probe reads row 0 unheadered — a genuine label row,
        # not a transaction, so header_row_looks_like_data stays False.
        probe_df = pl.DataFrame({
            "column_1": ["Date"],
            "column_2": ["Amount"],
            "column_3": ["Description"],
        })

        def _fake_read_excel(*args: object, **kwargs: object) -> pl.DataFrame:
            return probe_df if kwargs.get("has_header") is False else stub_df

        mocker.patch("polars.read_excel", side_effect=_fake_read_excel)

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

    def test_explicit_skip_rows_on_legacy_xls_catches_a_real_transaction(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Round 19 regression: a stale skip_rows must not silently drop a row.

        Before this fix, ``header_row_looks_like_data`` was hardcoded False
        for any Excel container openpyxl can't open (legacy .xls) — the
        safety check never even attempted a fastexcel/calamine fallback, so
        a stale saved skip_rows offset landing on a genuine transaction row
        kept high confidence and the row was silently dropped as "the
        header" instead of raising ``header_row_consumed``. Same OLE2/bytes
        shape as the sibling "falls back instead of raising" test, but the
        bounded probe here reads a row that DOES look like a transaction.
        """
        ole2_magic_bytes = bytes.fromhex("d0cf11e0a1b11ae1") + b"\x00" * 512

        stub_df = pl.DataFrame({
            "Date": ["2026-01-01"],
            "Amount": ["42.5"],
            "Description": ["Coffee"],
        })
        # The bounded probe reads row 0 unheadered as a genuine transaction
        # (date + amount) -- the row skip_rows=0 is about to consume.
        probe_df = pl.DataFrame({
            "column_1": ["2026-01-01"],
            "column_2": ["42.50"],
            "column_3": ["Coffee"],
        })

        def _fake_read_excel(*args: object, **kwargs: object) -> pl.DataFrame:
            return probe_df if kwargs.get("has_header") is False else stub_df

        mocker.patch("polars.read_excel", side_effect=_fake_read_excel)

        result = read_file(
            tmp_path / "legacy_replay.xls",
            FormatInfo(file_type="excel"),
            skip_rows=0,
            sheet="Sheet1",
            source_bytes=ole2_magic_bytes,
        )
        assert result.skip_rows == 0
        assert result.has_header is True
        assert result.header_row_looks_like_data is True

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

        # infer_schema_length=0 forces every column to string dtype — mirror
        # that shape so the stub behaves like a real unheadered read for
        # _classify_header_rows, which indexes raw string cells.
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


class TestTimeBearingFormatRenderIsIdempotent:
    """A declared time-bearing format needs no detection-time special-casing.

    Rendering a native midnight cell through ``strftime`` into a
    time-bearing format reproduces the exact raw text (parse the captured
    ISO date, then ``strftime`` back through the same directives/literals
    that produced the "00:00:00" suffix in the first place), so no format-
    inspection guard is needed before including such a column in a scan.
    These two tests prove that directly against
    ``normalize_excel_date_columns_after_mapping``.
    """

    def test_literal_midnight_suffix_round_trips(self) -> None:
        """A literal (non-directive) time suffix reproduces the raw text."""
        df = pl.DataFrame({"Date": ["2026-01-01 00:00:00"], "Amount": ["1.00"]})
        normalized = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping={"transaction_date": "Date"},
            date_format="%Y-%m-%d 00:00:00",
        )
        # Unchanged: the literal-suffix format expects the raw text as-is.
        assert normalized["Date"].to_list() == ["2026-01-01 00:00:00"]

    def test_fractional_seconds_format_leaves_non_matching_cells_untouched(
        self,
    ) -> None:
        """A raw shape with fractional seconds never matches the midnight regex.

        ``_EXCEL_MIDNIGHT_DATETIME_RE`` anchors on an exact "00:00:00" with
        nothing after it, so a cell carrying fractional seconds was never a
        rewrite candidate in the first place -- passes through untouched
        regardless of the declared format.
        """
        df = pl.DataFrame({
            "Date": ["2026-01-02 00:00:00.123000", "2026-01-03 00:00:00.456000"],
            "Amount": ["42.5", "10"],
        })
        normalized = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping={"transaction_date": "Date"},
            date_format="%Y-%m-%d %H:%M:%S.%f",
        )
        assert normalized["Date"].to_list() == [
            "2026-01-02 00:00:00.123000",
            "2026-01-03 00:00:00.456000",
        ]


class TestNormalizeExcelDateColumnsForDetectionRendersEveryColumn:
    """Grid: native/text/mixed per date column x mapped-upfront or not x format.

    ``normalize_excel_date_columns_for_detection`` takes no mapping input
    at all — it renders every string column's midnight-shaped cells
    unconditionally, so there is no column-selection decision left for a
    probe (native-type scan, majority-shape gate) to get wrong. This grid
    proves the FULL pipeline (detection copy -> ``map_columns`` ->
    after-mapping render) stays correct across column shape, whether the
    caller pre-declares the mapping via an override, and the declared
    format.
    """

    @pytest.mark.parametrize(
        "declared_format", [None, "%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"]
    )
    @pytest.mark.parametrize("post_date_role", ["native", "text", "mixed"])
    @pytest.mark.parametrize("transaction_date_role", ["native", "text", "mixed"])
    @pytest.mark.parametrize("mapped_upfront", ["none", "both"])
    def test_transaction_date_and_post_date_always_import_correctly(
        self,
        mapped_upfront: str,
        transaction_date_role: str,
        post_date_role: str,
        declared_format: str | None,
    ) -> None:
        text_shape = declared_format or "%Y-%m-%d"
        df = pl.DataFrame({
            "Date": _render_role(_ROLE_DATES, transaction_date_role, text_shape),
            "Post Date": _render_role(_POST_DATES, post_date_role, text_shape),
            "Amount": ["-4.50", "10.00", "42.50", "5.00"],
        })
        overrides = (
            {"transaction_date": "Date", "post_date": "Post Date"}
            if mapped_upfront == "both"
            else None
        )

        detection_df = normalize_excel_date_columns_for_detection(
            df, file_type="excel", date_format=declared_format
        )
        mapping_result = map_columns(detection_df, overrides=overrides)

        # (a) map_columns finds both fields, and -- when NOTHING is already
        # declared -- a non-None auto-detected format. _DATE_FORMATS
        # (date_detection.py) is date-only, so this only succeeds if the
        # detection copy handed it recognizable (rendered) content. A
        # time-bearing declared_format is never in _DATE_FORMATS's fixed
        # candidate list regardless of rendering -- a disclosed,
        # pre-existing limit of auto-detection -- so once a caller already
        # declares a format, detect_date_format's own guess no longer
        # matters: the declared format wins outright downstream.
        assert mapping_result.field_mapping.get("transaction_date") == "Date"
        assert mapping_result.field_mapping.get("post_date") == "Post Date"
        if declared_format is None:
            assert mapping_result.date_format is not None, (
                f"map_columns could not auto-detect a format for "
                f"{transaction_date_role} transaction_date "
                f"(mapped_upfront={mapped_upfront!r})"
            )

        # (b) the imported frame parses with no NULLs in either date column.
        final_format = declared_format or mapping_result.date_format
        assert final_format is not None, (
            "declared_format is None only when the (a) assertion above "
            "already proved mapping_result.date_format is not None"
        )
        rendered = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping=mapping_result.field_mapping,
            date_format=final_format,
        )
        for column, expected in (("Date", _ROLE_DATES), ("Post Date", _POST_DATES)):
            values = rendered[column].to_list()
            assert None not in values, f"{column} lost a value to NULL"
            for value, expected_date in zip(values, expected, strict=True):
                assert (
                    datetime.datetime.strptime(value, final_format).date()
                    == expected_date
                )


# Case grid for normalize_excel_date_columns_after_mapping's core invariant:
# after rendering, every non-null value in every mapped date column is in
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

    The invariant: the frame that gets imported is rewritten EXACTLY ONCE,
    by ``normalize_excel_date_columns_after_mapping``, after the final
    field mapping and effective date format are known. Anything before
    mapping (``normalize_excel_date_columns_for_detection``) only ever
    produces a throwaway COPY for ``map_columns``/format detection -- the
    real frame stays untouched (still native/mixed) until that one render,
    regardless of whether a column was named up front or only aliased in
    by the final mapping. Within that render: rewrite exactly the cells
    whose text matches the native-midnight rendering, into the effective
    date_format (or bare ISO when still unknown); every other cell --
    already-correct text, ISO-shaped text (valid or calendar-invalid), an
    invalid midnight cell -- passes through byte-identical, and nothing
    ever raises.
    """

    @pytest.mark.parametrize(
        "declared_format",
        [None, "%Y-%m-%d", "%m/%d/%Y", "%Y-%d-%m", "%m/%d/%Y %H:%M:%S"],
    )
    @pytest.mark.parametrize(
        "column_role",
        [
            "transaction_date_mapped_upfront",
            "post_date_mapped_upfront",
            "detection_copy_then_final_mapping",
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

        if column_role == "transaction_date_mapped_upfront":
            # Mirrors the matched_format/reviewed_plan branches: the whole
            # mapping is known before any normalization runs, so the real
            # frame renders directly -- no detection copy needed at all.
            df = pl.DataFrame({"Date": date_col})
            normalized = normalize_excel_date_columns_after_mapping(
                df,
                file_type="excel",
                field_mapping={"transaction_date": "Date"},
                date_format=declared_format,
            )
            assert normalized["Date"].to_list() == expected_date
            return

        # post_date uses a different month/day so a transposition bug
        # between the two columns is visible in the assertion.
        posted_cases = {
            kind: _cell_kind_case(kind, declared_format, month=6, day=6)
            for kind in _CELL_KINDS
        }
        posted_col = [posted_cases[k][0] for k in _CELL_KINDS]
        posted_expected = [posted_cases[k][1] for k in _CELL_KINDS]
        full_mapping = {"transaction_date": "Date", "post_date": "Posted"}

        if column_role == "post_date_mapped_upfront":
            # Both fields known before any normalization runs -- same
            # single-render shape as the case above, with two columns.
            df = pl.DataFrame({"Date": date_col, "Posted": posted_col})
            normalized = normalize_excel_date_columns_after_mapping(
                df,
                file_type="excel",
                field_mapping=full_mapping,
                date_format=declared_format,
            )
            assert normalized["Date"].to_list() == expected_date
            assert normalized["Posted"].to_list() == posted_expected
            return

        assert column_role == "detection_copy_then_final_mapping"
        # normalize_excel_date_columns_for_detection takes no mapping input
        # at all -- it renders its OWN copy's midnight-shaped cells
        # unconditionally, regardless of what map_columns will later
        # resolve. The real frame stays untouched until the one final
        # render.
        df = pl.DataFrame({"Date": date_col, "Posted": posted_col})
        detection_df = normalize_excel_date_columns_for_detection(
            df, file_type="excel", date_format=None
        )
        del detection_df  # only used to prove it's a separate object below
        assert df["Date"].to_list() == date_col, "real frame must stay untouched"
        assert df["Posted"].to_list() == posted_col, "real frame must stay untouched"
        normalized = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping=full_mapping,
            date_format=declared_format,
        )
        assert normalized["Date"].to_list() == expected_date
        assert normalized["Posted"].to_list() == posted_expected


class TestNormalizeExcelDateColumnsAfterMappingGrid:
    """Parametrized grid: transaction_date shape x post_date shape x format.

    Exercises every combination of column-level native/text/mixed shape
    against format known/unknown, so no mapped column (or cell within one)
    is left in a shape the eventual format cannot parse. field_mapping is
    already fully known here (mirrors the matched_format/reviewed_plan
    branches, and the auto-detect branch once map_columns has resolved it)
    — normalize_excel_date_columns_after_mapping is the one function that
    ever renders the real frame, regardless of how the mapping got built.
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
        field_mapping = {"transaction_date": "Date"}
        if post_date_role != "absent":
            columns["Posted"] = _render_role(_POST_DATES, post_date_role, text_shape)
            field_mapping["post_date"] = "Posted"

        df = pl.DataFrame(columns)
        rows_before = len(df)

        normalized = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping=field_mapping,
            date_format=declared_format,
        )

        assert len(normalized) == rows_before

        parse_format = declared_format if declared_format is not None else "%Y-%m-%d"
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

    def test_mapped_column_normalizes_regardless_of_native_typing(
        self,
    ) -> None:
        """A mapped column normalizes regardless of its own native/text mix.

        ``normalize_excel_date_columns_after_mapping`` renders per-cell, not
        by a whole-column vote: a mapped column is a date column regardless
        of its own native/text mix, once the caller already knows the
        mapping, so there is no column-level membership check to fail.
        """
        df = pl.DataFrame({
            "Date": [
                "01/01/2026",
                "01/02/2026",
                "01/03/2026",
                "2026-01-04 00:00:00",  # the native minority
            ],
        })

        normalized = normalize_excel_date_columns_after_mapping(
            df,
            file_type="excel",
            field_mapping={"transaction_date": "Date"},
            date_format="%m/%d/%Y",
        )

        assert normalized["Date"].to_list() == [
            "01/01/2026",
            "01/02/2026",
            "01/03/2026",
            "01/04/2026",
        ]


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
