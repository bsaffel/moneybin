"""Privacy taxonomy and catalog-sync (PR 1 foundation).

Later PRs add redaction middleware, consent gates, and SQL lineage on
top of this registry. This module exposes only the source-of-truth
classification.
"""

from moneybin.privacy.taxonomy import CLASSIFICATION, DataClass, Tier

__all__ = ["CLASSIFICATION", "DataClass", "Tier"]
