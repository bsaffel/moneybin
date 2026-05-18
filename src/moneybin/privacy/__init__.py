"""Privacy taxonomy, runtime classification, and audit log.

PR 1 established the column-level registry (``CLASSIFICATION``) and the
catalog sigil sync. PR 2 adds the runtime middleware: type-hint
introspection (``derive_tier``), field-level redaction (``redact_typed``),
profile-scoped HMAC key seeding, and the ``privacy.log.jsonl`` writer.
Later PRs add consent gates (PR 3) and SQL lineage (PR 4).
"""

from moneybin.privacy.comment_sync import sync_classification_comments
from moneybin.privacy.introspection import (
    PrivacyContractError,
    derive_tier,
    extract_data_classes,
)
from moneybin.privacy.log import read_privacy_events, write_privacy_event
from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass, Tier

__all__ = [
    "CLASSIFICATION",
    "DataClass",
    "PrivacyContractError",
    "Tier",
    "derive_tier",
    "extract_data_classes",
    "read_privacy_events",
    "sync_classification_comments",
    "write_privacy_event",
]
