"""Typed payloads for dynamically classified SQL tools."""

from pydantic import BaseModel, JsonValue, SkipValidation


class SQLQueryPayload(BaseModel):
    """Rows returned by an ad-hoc read-only query."""

    # DuckDB values such as Decimal stay native until PayloadEncoder normalizes
    # them; SkipValidation preserves that path while retaining the JsonValue schema.
    rows: list[dict[str, SkipValidation[JsonValue]]]


class SQLSchemaPayload(BaseModel):
    """Curated schema document for SQL composition."""

    document: dict[str, JsonValue]
