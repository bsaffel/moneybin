"""Tests for Google Sheets URL parser."""

import pytest

from moneybin.connectors.gsheet.url_parser import parse_sheet_url


class TestParseSheetURLValid:
    """Valid Google Sheets URLs."""

    def test_edit_with_gid_fragment(self):
        """Standard /edit#gid=N form."""
        url = "https://docs.google.com/spreadsheets/d/1abc_DEF-ghi/edit#gid=0"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "1abc_DEF-ghi"
        assert gid == 0

    def test_edit_with_gid_query_string(self):
        """Share-URL /edit?gid=N form."""
        url = "https://docs.google.com/spreadsheets/d/2xyz_ABC-def/edit?gid=123"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "2xyz_ABC-def"
        assert gid == 123

    def test_share_url_with_usp_and_gid(self):
        """Share-URL with both ?usp=sharing&gid=0."""
        url = (
            "https://docs.google.com/spreadsheets/d/3pqr_STU-vwx/edit?usp=sharing&gid=0"
        )
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "3pqr_STU-vwx"
        assert gid == 0

    def test_gid_zero(self):
        """gid=0 (the default sheet)."""
        url = "https://docs.google.com/spreadsheets/d/4jkl_MNO-pqr/edit#gid=0"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "4jkl_MNO-pqr"
        assert gid == 0

    def test_gid_large_number(self):
        """Large gid value."""
        url = "https://docs.google.com/spreadsheets/d/5stu_VWX-yz/edit#gid=9999999"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "5stu_VWX-yz"
        assert gid == 9999999

    def test_spreadsheet_id_with_underscore(self):
        """Spreadsheet ID with underscore."""
        url = "https://docs.google.com/spreadsheets/d/abc_123_def/edit#gid=0"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "abc_123_def"
        assert gid == 0

    def test_spreadsheet_id_with_dash(self):
        """Spreadsheet ID with dash."""
        url = "https://docs.google.com/spreadsheets/d/abc-123-def/edit#gid=0"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "abc-123-def"
        assert gid == 0

    def test_spreadsheet_id_mixed_underscore_dash(self):
        """Spreadsheet ID with mixed underscore and dash."""
        url = "https://docs.google.com/spreadsheets/d/abc_123-def_456/edit#gid=42"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "abc_123-def_456"
        assert gid == 42

    def test_url_with_trailing_slash(self):
        """URL with trailing slash after spreadsheet ID."""
        url = "https://docs.google.com/spreadsheets/d/trailing_slash/edit#gid=1"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "trailing_slash"
        assert gid == 1

    def test_gid_in_fragment_takes_precedence(self):
        """Fragment gid takes precedence over query string gid."""
        url = (
            "https://docs.google.com/spreadsheets/d/precedence_test/edit?gid=99#gid=42"
        )
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "precedence_test"
        assert gid == 42

    def test_multi_account_url_with_user_path(self):
        """Google adds /u/<n>/ to URLs copied from a multi-account session."""
        url = "https://docs.google.com/u/1/spreadsheets/d/multi_account/edit#gid=7"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "multi_account"
        assert gid == 7

    def test_multi_account_url_with_user_path_and_query_gid(self):
        """/u/<n>/ form with gid in query string (no fragment)."""
        url = "https://docs.google.com/u/0/spreadsheets/d/multi_q/edit?gid=42"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "multi_q"
        assert gid == 42

    def test_user_path_segment_inside_spreadsheets(self):
        """The /spreadsheets/u/<n>/d/<id> variant (u/ between spreadsheets and d)."""
        url = "https://docs.google.com/spreadsheets/u/2/d/inner_user/edit#gid=3"
        spreadsheet_id, gid = parse_sheet_url(url)
        assert spreadsheet_id == "inner_user"
        assert gid == 3


class TestParseSheetURLInvalid:
    """Invalid or malformed Google Sheets URLs."""

    def test_missing_gid(self):
        """Missing gid= entirely."""
        url = "https://docs.google.com/spreadsheets/d/no_gid/edit"
        with pytest.raises(ValueError, match="missing gid"):
            parse_sheet_url(url)

    def test_empty_spreadsheet_id(self):
        """Empty spreadsheet ID."""
        url = "https://docs.google.com/spreadsheets/d//edit#gid=0"
        with pytest.raises(ValueError, match="spreadsheet_id"):
            parse_sheet_url(url)

    def test_non_url_string(self):
        """Non-URL input."""
        with pytest.raises(ValueError):
            parse_sheet_url("not-a-url")

    def test_wrong_host(self):
        """Wrong host."""
        url = "https://example.com/spreadsheets/d/1abc/edit#gid=0"
        with pytest.raises(ValueError, match="host"):
            parse_sheet_url(url)

    def test_non_integer_gid(self):
        """Non-integer gid value."""
        url = "https://docs.google.com/spreadsheets/d/abc/edit#gid=abc"
        with pytest.raises(ValueError, match="not an integer"):
            parse_sheet_url(url)

    def test_no_spreadsheets_path(self):
        """No /spreadsheets/ in path."""
        url = "https://docs.google.com/forms/d/abc/edit#gid=0"
        with pytest.raises(ValueError, match="spreadsheet_id"):
            parse_sheet_url(url)

    def test_google_sheets_host_variant(self):
        """sheets.google.com (wrong host, should be docs.google.com)."""
        url = "https://sheets.google.com/spreadsheets/d/abc/edit#gid=0"
        with pytest.raises(ValueError, match="host"):
            parse_sheet_url(url)

    def test_empty_url(self):
        """Empty string as URL."""
        with pytest.raises(ValueError):
            parse_sheet_url("")

    def test_gid_empty_string(self):
        """Gid with empty value."""
        url = "https://docs.google.com/spreadsheets/d/abc/edit#gid="
        with pytest.raises(ValueError, match="not an integer"):
            parse_sheet_url(url)
