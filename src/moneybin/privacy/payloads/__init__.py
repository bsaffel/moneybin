"""Typed payload dataclasses for the privacy middleware.

Each payload carries ``Annotated[T, DataClass.X]`` metadata on every field so
``derive_tier`` can derive the tool's sensitivity class via introspection.
Import the payload you need from the surface-specific submodule, e.g.::

    from moneybin.privacy.payloads.accounts import AccountListPayload

One module per service surface; add new surfaces as additional modules.
"""
