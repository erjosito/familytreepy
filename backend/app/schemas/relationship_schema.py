"""Schema loader and validator for relationship types configuration."""

import json
from typing import Any

from pydantic import BaseModel


class RelationshipTypeConfig(BaseModel):
    directed: bool
    permanent: bool
    color: str
    label: str
    date_fields: list[str] = []


class RelationshipSchema:
    def __init__(self, config_path: str) -> None:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        raw = data.get("relationship_types", {})
        self._types: dict[str, RelationshipTypeConfig] = {
            name: RelationshipTypeConfig(**cfg) for name, cfg in raw.items()
        }

    def get_type(self, name: str) -> RelationshipTypeConfig | None:
        return self._types.get(name)

    def get_all_types(self) -> dict[str, RelationshipTypeConfig]:
        return dict(self._types)

    def is_valid_type(self, name: str) -> bool:
        return name in self._types

    def is_permanent(self, name: str) -> bool:
        rel = self._types.get(name)
        if rel is None:
            raise ValueError(f"Unknown relationship type: {name}")
        return rel.permanent

    def to_dict(self) -> dict[str, Any]:
        return {name: cfg.model_dump() for name, cfg in self._types.items()}


def load_relationship_schema(config_path: str) -> RelationshipSchema:
    return RelationshipSchema(config_path)
