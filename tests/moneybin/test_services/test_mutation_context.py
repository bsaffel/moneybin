"""Tests for MutationContext — per-call operation_id grouping via contextvars."""

from __future__ import annotations

import re

from moneybin.services.mutation_context import (
    current_operation_id,
    new_operation_id,
    operation,
)

# op_<uuid4_hex>: the literal prefix plus 32 lowercase hex chars.
_OP_ID = re.compile(r"^op_[0-9a-f]{32}$")


class TestNewOperationId:
    """new_operation_id mints fresh op_<uuid4_hex> values."""

    def test_matches_format(self) -> None:
        assert _OP_ID.match(new_operation_id())

    def test_each_call_is_distinct(self) -> None:
        assert new_operation_id() != new_operation_id()


class TestCurrentOperationIdNoContext:
    """A bare call outside any operation() is its own one-row operation."""

    def test_mints_valid_id_when_none_active(self) -> None:
        assert _OP_ID.match(current_operation_id())

    def test_two_bare_calls_get_distinct_ids(self) -> None:
        # No active context: the getter must NOT cache — each lone mutation
        # is its own operation.
        assert current_operation_id() != current_operation_id()


class TestOperationContext:
    """operation() binds one id for every read inside the block."""

    def test_all_reads_inside_share_one_id(self) -> None:
        with operation():
            first = current_operation_id()
            second = current_operation_id()
        assert first == second
        assert _OP_ID.match(first)

    def test_yields_the_active_id(self) -> None:
        with operation() as op_id:
            assert current_operation_id() == op_id

    def test_separate_blocks_get_distinct_ids(self) -> None:
        with operation():
            first = current_operation_id()
        with operation():
            second = current_operation_id()
        assert first != second

    def test_resets_to_no_context_on_exit(self) -> None:
        with operation():
            inside = current_operation_id()
        # After exit there is no active context again, so a fresh id is minted
        # that differs from the block's id.
        assert current_operation_id() != inside

    def test_accepts_caller_supplied_id(self) -> None:
        # Self-heal recipes (a later REC-PR) pass their own prefixed id.
        custom = "op_self_heal_drift_0123456789abcdef0123456789abcdef"
        with operation(custom):
            assert current_operation_id() == custom
