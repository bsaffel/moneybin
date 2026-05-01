"""Tests for _detect_file_type, including magic-byte sniffing."""

from pathlib import Path

import pytest

from moneybin.services.import_service import _detect_file_type


class TestDetectFileType:
    """Tests for _detect_file_type extension routing and magic-byte sniffing."""

    def test_routes_ofx_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "x.ofx"
        f.write_text("dummy")
        assert _detect_file_type(f) == "ofx"

    def test_routes_qfx_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "x.qfx"
        f.write_text("dummy")
        assert _detect_file_type(f) == "ofx"

    def test_routes_qbo_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "x.qbo"
        f.write_text("dummy")
        assert _detect_file_type(f) == "ofx"

    def test_sniffs_ofx_content_in_unknown_extension(self, tmp_path: Path) -> None:
        f = tmp_path / "renamed.txt"
        f.write_text("OFXHEADER:100\nDATA:OFXSGML\n<OFX></OFX>")
        assert _detect_file_type(f) == "ofx"

    def test_sniffs_xml_ofx_content(self, tmp_path: Path) -> None:
        f = tmp_path / "renamed.txt"
        f.write_text('<?xml version="1.0"?>\n<OFX><BANKMSGSRSV1/></OFX>')
        assert _detect_file_type(f) == "ofx"

    def test_extension_takes_precedence_over_sniffing(self, tmp_path: Path) -> None:
        # CSV that incidentally contains <OFX> in a description should still route as tabular
        f = tmp_path / "x.csv"
        f.write_text("date,amount,description\n2026-01-01,10.00,About <OFX> tag\n")
        assert _detect_file_type(f) == "tabular"

    def test_unknown_extension_with_no_magic_bytes_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "x.bin"
        f.write_text("not a recognized format")
        with pytest.raises(ValueError, match="Unsupported file type"):
            _detect_file_type(f)
