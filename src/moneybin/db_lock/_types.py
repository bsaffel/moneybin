"""Closed-vocabulary Literal types for the write-coordination surface.

These mirror the label vocabularies on
``DB_WRITE_LOCK_TIMEOUT_TOTAL`` and ``DB_CHECKPOINT_TOTAL`` (see
``src/moneybin/metrics/registry.py``). Adding a new value here requires
updating the metric registry comment in the same change so dashboards
and alerting stay coherent.
"""

from __future__ import annotations

from typing import Literal

OperationType = Literal["interactive", "migration", "transform_apply", "backup"]
"""Classifies a write-mode get_database() call for telemetry + recovery action."""

CheckpointReason = Literal[
    "post_migration",
    "post_transform",
    "pre_backup",
    "post_compact",
    "post_large_import",
]
"""Names the durable boundary that motivated a CHECKPOINT call."""
