"""Tests for _detect_file_type, including magic-byte sniffing."""

from pathlib import Path
from typing import Any

import pytest

from moneybin.services.import_service import (
    _detect_file_type,  # pyright: ignore[reportPrivateUsage]
)


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

    def test_sniffs_ofx_inside_misnamed_pdf(self, tmp_path: Path) -> None:
        """A .pdf-named file with OFX magic bytes routes to OFX, not PDF.

        The sniffer runs before the .pdf extension check so misnamed files
        get a clear OFX-import error rather than an opaque pdfplumber
        failure on what is not actually a PDF.
        """
        f = tmp_path / "misnamed.pdf"
        f.write_text("OFXHEADER:100\nDATA:OFXSGML\n<OFX></OFX>")
        assert _detect_file_type(f) == "ofx"

    def test_genuine_pdf_extension_routes_pdf(self, tmp_path: Path) -> None:
        f = tmp_path / "statement.pdf"
        f.write_bytes(b"%PDF-1.4 fake header for routing test")
        assert _detect_file_type(f) == "pdf"


class TestDetectFileTypePermission:
    """An unreadable file must not be reported as an unsupported one."""

    def test_unreadable_extensionless_file_raises_permission_not_unsupported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Being unable to read a file is not evidence that it is not OFX.

        The sniff swallowed every OSError and returned False, so an unreadable
        file fell through to the extension checks. With no recognized suffix
        that produces `ValueError: Unsupported file type` — blaming the file for
        a permission problem the user can fix. Same shape as the OFX read
        boundary, one function earlier.

        The denial is injected rather than done with `chmod(0o000)`: a runner
        executing as root reads a mode-000 file happily, which would silently
        turn this into a no-op test on some CI images.
        """
        import builtins

        target = tmp_path / "statement"
        target.write_bytes(b"OFXHEADER:100")
        real_open = builtins.open

        def _deny(file: object, *args: object, **kwargs: object) -> Any:
            if str(file) == str(target):
                raise PermissionError(1, "Operation not permitted", str(target))
            return real_open(file, *args, **kwargs)  # pyright: ignore[reportCallIssue,reportArgumentType]

        monkeypatch.setattr(builtins, "open", _deny)

        with pytest.raises(PermissionError):
            _detect_file_type(target)
