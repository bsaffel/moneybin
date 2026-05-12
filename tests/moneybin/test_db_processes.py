"""Tests for db_processes utility — process-name classification."""

import pytest


@pytest.mark.unit
class TestDescribeProcess:
    """Tests for _describe_process process-name classification."""
    def _desc(self, cmdline: str) -> str:
        from moneybin.utils.db_processes import _describe_process

        return _describe_process(cmdline)

    def test_mcp_serve(self) -> None:
        assert self._desc("moneybin mcp serve") == "MCP server"
        assert self._desc("/home/user/.venv/bin/moneybin mcp serve") == "MCP server"

    def test_transform_apply(self) -> None:
        assert self._desc("moneybin transform apply") == "transform pipeline"

    def test_import_inbox_sync(self) -> None:
        assert self._desc("moneybin import inbox sync") == "inbox sync"

    def test_import_generic(self) -> None:
        assert self._desc("moneybin import formats list") == "import command"

    def test_sync(self) -> None:
        assert self._desc("moneybin sync") == "Plaid sync"

    def test_web(self) -> None:
        assert self._desc("moneybin web") == "Web UI server"
        assert self._desc("uvicorn moneybin.web:app") == "Web UI server"

    def test_moneybin_other(self) -> None:
        assert self._desc("moneybin reports spending") == "moneybin reports"

    def test_duckdb_ui(self) -> None:
        assert self._desc("duckdb --ui") == "DuckDB UI"
        assert self._desc("duckdb-ui") == "DuckDB UI"

    def test_duckdb_shell(self) -> None:
        assert self._desc("duckdb /path/to/db.duckdb") == "DuckDB shell"

    def test_fallback(self) -> None:
        result = self._desc("python /some/random/script.py with args here")
        assert len(result) <= 40
        assert result == "python /some/random/script.py with args"
