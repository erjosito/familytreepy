"""Schema loader and validator for person attribute configuration."""

import json
from typing import Any

from pydantic import BaseModel


class FieldConfig(BaseModel):
    type: str
    required: bool = False
    label: str
    default: Any = None
    visible_when: dict | None = None


_TYPE_VALIDATORS: dict[str, type] = {
    "string": str,
    "date": str,
    "boolean": bool,
    "image_url": str,
    "image_url_array": list,
}


class PersonSchema:
    def __init__(self, config_path: str) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("fields", {})
        self._fields: dict[str, FieldConfig] = {
            name: FieldConfig(**cfg) for name, cfg in raw.items()
        }

    def get_field(self, name: str) -> FieldConfig | None:
        return self._fields.get(name)

    def get_all_fields(self) -> dict[str, FieldConfig]:
        return dict(self._fields)

    def validate_attributes(self, attrs: dict) -> dict:
        """Return validated/cleaned attributes. Raises ValueError on bad data."""
        cleaned: dict[str, Any] = {}
        for name, cfg in self._fields.items():
            if name in attrs:
                value = attrs[name]
                if value is not None:
                    expected = _TYPE_VALIDATORS.get(cfg.type)
                    if expected and not isinstance(value, expected):
                        raise ValueError(
                            f"Field '{name}' expects {cfg.type}, "
                            f"got {type(value).__name__}"
                        )
                cleaned[name] = value
            elif cfg.required:
                raise ValueError(f"Missing required field: {name}")
            elif cfg.default is not None:
                cleaned[name] = cfg.default
        return cleaned

    def to_dict(self) -> dict[str, Any]:
        return {name: cfg.model_dump() for name, cfg in self._fields.items()}


def load_person_schema(config_path: str) -> PersonSchema:
    return PersonSchema(config_path)
