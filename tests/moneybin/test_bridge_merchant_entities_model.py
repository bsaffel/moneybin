"""Structural tests for the core.bridge_merchant_entities model.

MB-53 promotes the source system's merchant-entity reference out of
``prep.int_transactions__merged`` — which the shared-primitives spec declares
internal and free to change shape without notice — into a licensed ``core``
surface that categorization and merchant resolution may bind to.
"""

from __future__ import annotations

import pytest

from moneybin.database import SQLMESH_ROOT

MODEL_PATH = SQLMESH_ROOT / "models" / "core" / "bridge_merchant_entities.sql"


@pytest.mark.unit
class TestBridgeMerchantEntitiesModel:
    """The model exists, is a view over the merged layer, and exposes the key."""

    def test_model_file_exists(self) -> None:
        assert MODEL_PATH.exists(), (
            "core.bridge_merchant_entities must exist so consumers stop binding "
            "to prep.int_transactions__merged"
        )

    def test_is_a_core_view(self) -> None:
        content = MODEL_PATH.read_text()
        assert "name core.bridge_merchant_entities" in content
        assert "kind VIEW" in content

    def test_reads_the_merged_layer(self) -> None:
        content = MODEL_PATH.read_text()
        assert "prep.int_transactions__merged" in content

    def test_exposes_the_entity_key_and_source_name(self) -> None:
        """The entity id is meaningless without the source_type that issued it.

        ``app.merchant_links`` binds on the ``(source_type, ref_value)`` pair,
        so the bridge carries both halves plus the name the source gave the
        entity — the raw value ``core.fct_transactions.merchant_name`` has
        already replaced with the resolved canonical name.
        """
        content = MODEL_PATH.read_text()
        for column in (
            "transaction_id",
            "merchant_entity_source_type",
            "merchant_entity_id",
            "source_merchant_name",
        ):
            assert column in content, f"{column} must be projected"

    def test_keeps_only_entity_bearing_transactions(self) -> None:
        """Non-Plaid rows carry no entity id and must not reach the bridge."""
        content = MODEL_PATH.read_text()
        assert "NOT merchant_entity_id IS NULL" in content
