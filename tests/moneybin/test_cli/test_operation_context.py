"""The CLI command boundary establishes one operation per invocation (REC-PR1).

Both CLI seams bind one operation per command run so every audit row a command
writes shares one operation_id: ``handle_cli_errors`` (most commands) and
``sqlmesh_command`` (transform commands routed through SQLMesh).
"""

from __future__ import annotations

import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.cli.utils import handle_cli_errors, sqlmesh_command
from moneybin.services.mutation_context import current_operation_id

_OP_ID = re.compile(r"^op_[0-9a-f]{32}$")


def test_one_command_groups_reads_under_one_operation_id() -> None:
    with handle_cli_errors():
        first = current_operation_id()
        second = current_operation_id()
    assert first == second
    assert _OP_ID.match(first)


def test_separate_commands_get_distinct_operation_ids() -> None:
    with handle_cli_errors():
        first = current_operation_id()
    with handle_cli_errors():
        second = current_operation_id()
    assert first != second


@pytest.fixture()
def stub_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub get_database so sqlmesh_command's seam can be tested without a DB."""

    @contextmanager
    def _fake_get_database(*_a: Any, **_k: Any) -> Generator[MagicMock, None, None]:
        yield MagicMock()

    monkeypatch.setattr(db_module, "get_database", _fake_get_database)


def test_sqlmesh_command_groups_reads_under_one_operation_id(stub_db: None) -> None:
    with sqlmesh_command("Test op"):
        first = current_operation_id()
        second = current_operation_id()
    assert first == second
    assert _OP_ID.match(first)


def test_separate_sqlmesh_commands_get_distinct_operation_ids(stub_db: None) -> None:
    with sqlmesh_command("Test op"):
        first = current_operation_id()
    with sqlmesh_command("Test op"):
        second = current_operation_id()
    assert first != second
