"""Dependency injection for shared resources (FamilyTree instance, schemas)."""

import os
import sys
from functools import lru_cache

# Add project root to path so familytree.py is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from familytree import FamilyTree
from backend.app.schemas.relationship_schema import RelationshipSchema, load_relationship_schema
from backend.app.schemas.person_schema import PersonSchema, load_person_schema


def _config_path(filename: str) -> str:
    return os.path.join(os.path.dirname(__file__), "..", "..", "config", filename)


@lru_cache
def get_relationship_schema() -> RelationshipSchema:
    return load_relationship_schema(_config_path("relationship_types.json"))


@lru_cache
def get_person_schema() -> PersonSchema:
    return load_person_schema(_config_path("person_schema.json"))


_tree_instance: FamilyTree | None = None


def get_tree() -> FamilyTree:
    """Return the singleton FamilyTree instance, creating it on first call."""
    global _tree_instance
    if _tree_instance is None:
        backend = os.getenv("TREE_BACKEND", "local")
        schema = get_relationship_schema()
        if backend == "azstorage":
            _tree_instance = FamilyTree(
                backend="azstorage",
                azstorage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
                azstorage_key=os.getenv("AZURE_STORAGE_KEY"),
                azstorage_container=os.getenv("AZURE_STORAGE_CONTAINER", "familytreejson"),
                azstorage_blob=os.getenv("AZURE_STORAGE_BLOB", "familytree.gml"),
                relationship_schema=schema,
            )
        else:
            local_path = os.getenv("TREE_LOCAL_FILE", "familytree.gml")
            _tree_instance = FamilyTree(
                backend="local",
                localfile=local_path,
                relationship_schema=schema,
            )
    return _tree_instance
