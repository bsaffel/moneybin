"""The CLI command boundary establishes one operation per invocation (REC-PR1).

``handle_cli_errors`` wraps every adopting command body; binding the operation
context there means one CLI command run = one operation_id shared by all the
audit rows it writes.
"""

from __future__ import annotations

import re

from moneybin.cli.utils import handle_cli_errors
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
