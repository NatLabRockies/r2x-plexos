"""Utilities for the models."""

from functools import cache

from pydantic import BaseModel


@cache
def _field_names_by_alias(model_class: type[BaseModel]) -> dict[str, str]:
    # Model fields are fixed per class, so the parser can reuse this map for every object.
    field_names: dict[str, str] = {}
    for field_name, field_info in model_class.model_fields.items():
        if field_info.alias is not None:
            field_names.setdefault(field_info.alias, field_name)
    return field_names


def get_field_name_by_alias(model: BaseModel, alias_name: str) -> str | None:
    """Return the Pydantic field name corresponding to a given alias."""
    return _field_names_by_alias(model.__class__).get(alias_name)
