"""Tests for file utilities module."""

import hashlib
from pathlib import Path

import pytest

from moneybin.utils.file import (
    _files_are_identical,  # type: ignore[reportPrivateUsage] - testing private function
    copy_to_raw,
)


@pytest.fixture
def source_file(tmp_path: Path) -> Path:
    """Create a temporary source file for testing.

    Args:
        tmp_path: Pytest temporary directory fixture

    Returns:
        Path to the created source file
    """
    file_path = tmp_path / "source" / "test_file.qfx"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("Test OFX content\nLine 2\nLine 3")
    return file_path


@pytest.fixture
def base_data_path(tmp_path: Path) -> Path:
    """Create a temporary base data path for testing.

    Args:
        tmp_path: Pytest temporary directory fixture

    Returns:
        Path to the created base data directory
    """
    data_path = tmp_path / "data" / "raw"
    data_path.mkdir(parents=True, exist_ok=True)
    return data_path


class TestCopyToRaw:
    """Test cases for copy_to_raw function."""

    def test_copy_ofx_file_to_raw(
        self, source_file: Path, base_data_path: Path
    ) -> None:
        """Test copying an OFX file to the raw data directory.

        Args:
            source_file: Source file fixture
            base_data_path: Base data path fixture
        """
        result_path = copy_to_raw(source_file, "ofx", base_data_path)

        assert result_path.exists()
        assert result_path.parent == base_data_path / "ofx"
        assert result_path.name == source_file.name
        assert result_path.read_text() == source_file.read_text()

    def test_copy_qfx_file_to_ofx_directory(
        self, source_file: Path, base_data_path: Path
    ) -> None:
        """Test that QFX files are copied to the OFX directory.

        Args:
            source_file: Source file fixture
            base_data_path: Base data path fixture
        """
        result_path = copy_to_raw(source_file, "qfx", base_data_path)

        # QFX files should go to the ofx directory
        assert result_path.parent == base_data_path / "ofx"
        assert result_path.exists()

    def test_copy_pdf_file_to_raw(self, tmp_path: Path, base_data_path: Path) -> None:
        """Test copying a PDF file to the raw data directory.

        Args:
            tmp_path: Pytest temporary directory fixture
            base_data_path: Base data path fixture
        """
        pdf_file = tmp_path / "source" / "statement.pdf"
        pdf_file.parent.mkdir(parents=True, exist_ok=True)
        pdf_file.write_bytes(b"PDF content")

        result_path = copy_to_raw(pdf_file, "pdf", base_data_path)

        assert result_path.exists()
        assert result_path.parent == base_data_path / "pdf"
        assert result_path.name == "statement.pdf"

    def test_copy_csv_file_to_raw(self, tmp_path: Path, base_data_path: Path) -> None:
        """Test copying a CSV file to the raw data directory.

        Args:
            tmp_path: Pytest temporary directory fixture
            base_data_path: Base data path fixture
        """
        csv_file = tmp_path / "transactions.csv"
        csv_file.write_text("date,amount\n2025-01-01,100.00")

        result_path = copy_to_raw(csv_file, "csv", base_data_path)

        assert result_path.exists()
        assert result_path.parent == base_data_path / "csv"
        assert result_path.read_text() == csv_file.read_text()

    def test_copy_with_string_paths(self, tmp_path: Path) -> None:
        """Test copy_to_raw accepts string paths.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        source = tmp_path / "source.qfx"
        source.write_text("content")
        base_path = tmp_path / "data" / "raw"

        result_path = copy_to_raw(str(source), "ofx", str(base_path))

        assert result_path.exists()
        assert isinstance(result_path, Path)

    def test_copy_with_home_directory_expansion(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that tilde (~) in paths is properly expanded.

        Args:
            tmp_path: Pytest temporary directory fixture
            monkeypatch: Pytest monkeypatch fixture
        """
        # Create a test file
        test_file = tmp_path / "test.qfx"
        test_file.write_text("content")

        # Mock Path.expanduser to return our test path
        original_expanduser = Path.expanduser

        def mock_expanduser(self: Path) -> Path:
            if str(self).startswith("~"):
                return test_file
            return original_expanduser(self)

        monkeypatch.setattr(Path, "expanduser", mock_expanduser)

        base_path = tmp_path / "data" / "raw"
        result_path = copy_to_raw("~/test.qfx", "ofx", base_path)

        assert result_path.exists()

    def test_copy_creates_target_directory(
        self, source_file: Path, tmp_path: Path
    ) -> None:
        """Test that target directory is created if it doesn't exist.

        Args:
            source_file: Source file fixture
            tmp_path: Pytest temporary directory fixture
        """
        base_path = tmp_path / "new_data" / "raw"
        # Directory doesn't exist yet
        assert not base_path.exists()

        result_path = copy_to_raw(source_file, "ofx", base_path)

        assert result_path.exists()
        assert result_path.parent.exists()

    def test_idempotent_copy_identical_file(
        self, source_file: Path, base_data_path: Path
    ) -> None:
        """Test that copying identical file twice is idempotent.

        Args:
            source_file: Source file fixture
            base_data_path: Base data path fixture
        """
        # First copy
        result_path1 = copy_to_raw(source_file, "ofx", base_data_path)

        # Second copy of same file
        result_path2 = copy_to_raw(source_file, "ofx", base_data_path)

        assert result_path1 == result_path2
        assert result_path1.exists()

    def test_copy_overwrites_different_file(
        self, source_file: Path, base_data_path: Path
    ) -> None:
        """Test that copying a file with different content overwrites existing.

        Args:
            source_file: Source file fixture
            base_data_path: Base data path fixture
        """
        # First copy
        result_path1 = copy_to_raw(source_file, "ofx", base_data_path)
        original_content = result_path1.read_text()

        # Modify source file
        source_file.write_text("New content that is different")

        # Second copy with different content
        result_path2 = copy_to_raw(source_file, "ofx", base_data_path)

        assert result_path1 == result_path2
        assert result_path2.read_text() == "New content that is different"
        assert result_path2.read_text() != original_content

    def test_copy_preserves_filename(
        self, tmp_path: Path, base_data_path: Path
    ) -> None:
        """Test that original filename is preserved.

        Args:
            tmp_path: Pytest temporary directory fixture
            base_data_path: Base data path fixture
        """
        source = tmp_path / "my_special_file_2025.qfx"
        source.write_text("content")

        result_path = copy_to_raw(source, "ofx", base_data_path)

        assert result_path.name == "my_special_file_2025.qfx"

    def test_copy_nonexistent_file_raises_error(self, base_data_path: Path) -> None:
        """Test that copying a non-existent file raises FileNotFoundError.

        Args:
            base_data_path: Base data path fixture
        """
        nonexistent_file = Path("/nonexistent/path/file.qfx")

        with pytest.raises(FileNotFoundError, match="Source file not found"):
            copy_to_raw(nonexistent_file, "ofx", base_data_path)

    def test_copy_case_insensitive_file_type(
        self, source_file: Path, base_data_path: Path
    ) -> None:
        """Test that file type is case-insensitive.

        Args:
            source_file: Source file fixture
            base_data_path: Base data path fixture
        """
        result_path1 = copy_to_raw(source_file, "OFX", base_data_path)
        result_path2 = copy_to_raw(source_file, "OfX", base_data_path)
        result_path3 = copy_to_raw(source_file, "ofx", base_data_path)

        assert result_path1.parent == base_data_path / "ofx"
        assert result_path2.parent == base_data_path / "ofx"
        assert result_path3.parent == base_data_path / "ofx"

    def test_copy_custom_file_type(self, tmp_path: Path, base_data_path: Path) -> None:
        """Test copying a file with custom file type.

        Args:
            tmp_path: Pytest temporary directory fixture
            base_data_path: Base data path fixture
        """
        source = tmp_path / "data.json"
        source.write_text('{"key": "value"}')

        result_path = copy_to_raw(source, "json", base_data_path)

        assert result_path.parent == base_data_path / "json"
        assert result_path.exists()


class TestFilesAreIdentical:
    """Test cases for _files_are_identical helper function."""

    def test_identical_files(self, tmp_path: Path) -> None:
        """Test that identical files are detected.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        content = "Same content in both files"
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text(content)
        file2.write_text(content)

        assert _files_are_identical(file1, file2)

    def test_different_content(self, tmp_path: Path) -> None:
        """Test that files with different content are detected.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("Content A")
        file2.write_text("Content B")

        assert not _files_are_identical(file1, file2)

    def test_different_sizes(self, tmp_path: Path) -> None:
        """Test that files with different sizes are detected quickly.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("Short")
        file2.write_text("Much longer content")

        assert not _files_are_identical(file1, file2)

    def test_identical_binary_files(self, tmp_path: Path) -> None:
        """Test that identical binary files are detected.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        content = b"\x00\x01\x02\x03\xff\xfe\xfd"
        file1 = tmp_path / "file1.bin"
        file2 = tmp_path / "file2.bin"
        file1.write_bytes(content)
        file2.write_bytes(content)

        assert _files_are_identical(file1, file2)

    def test_different_binary_files(self, tmp_path: Path) -> None:
        """Test that different binary files are detected.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        file1 = tmp_path / "file1.bin"
        file2 = tmp_path / "file2.bin"
        file1.write_bytes(b"\x00\x01\x02")
        file2.write_bytes(b"\x00\x01\x03")

        assert not _files_are_identical(file1, file2)

    def test_identical_large_files(self, tmp_path: Path) -> None:
        """Test comparison of identical large files.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        # Create large files (1MB each)
        content = b"X" * (1024 * 1024)
        file1 = tmp_path / "large1.bin"
        file2 = tmp_path / "large2.bin"
        file1.write_bytes(content)
        file2.write_bytes(content)

        assert _files_are_identical(file1, file2)

    def test_hash_comparison(self, tmp_path: Path) -> None:
        """Test that SHA-256 hash is used for comparison.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        content = "Test content for hash comparison"
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text(content)
        file2.write_text(content)

        # Verify files have same hash
        hash1 = hashlib.sha256(file1.read_bytes()).hexdigest()
        hash2 = hashlib.sha256(file2.read_bytes()).hexdigest()
        assert hash1 == hash2

        assert _files_are_identical(file1, file2)

    def test_empty_files_are_identical(self, tmp_path: Path) -> None:
        """Test that two empty files are considered identical.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        file1 = tmp_path / "empty1.txt"
        file2 = tmp_path / "empty2.txt"
        file1.write_text("")
        file2.write_text("")

        assert _files_are_identical(file1, file2)
